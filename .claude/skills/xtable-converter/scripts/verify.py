#!/usr/bin/env python3
"""Verify target metadata on storage and merge with log results into the final verdict.

Per (table, target), checks that the expected metadata exists at tableBasePath
AND is newer than the run start timestamp:
  DELTA   -> _delta_log/*.json
  ICEBERG -> metadata/*.metadata.json
  HUDI    -> .hoodie timeline files (.commit/.deltacommit/.replacecommit, incl. .hoodie/timeline/)

Local paths: checked directly. s3:// or s3a://: aws CLI. Other schemes: hadoop fs.
If no tooling/access -> storage_status = "not_possible" -> verdict UNVERIFIED.

Writes final_report.json with verdicts: SUCCESS | FAILED | UNVERIFIED.
Exit code: 0 if all SUCCESS, 3 if any UNVERIFIED (and none failed), 1 if any FAILED.
"""
import argparse
import glob
import json
import os
import re
import shutil
import subprocess
import sys

METADATA_CHECKS = {
    "DELTA":   {"subdir": "_delta_log",  "pattern": r"\.json$"},
    "ICEBERG": {"subdir": "metadata",    "pattern": r"\.metadata\.json$"},
    "HUDI":    {"subdir": ".hoodie",     "pattern": r"(\.commit|\.deltacommit|\.replacecommit|hoodie\.properties)$"},
}

# Patterns used to detect new commits on the SOURCE side (local paths only).
SOURCE_COMMIT_CHECKS = {
    "HUDI":  {"subdir": ".hoodie",    "pattern": r"\.(commit|deltacommit|replacecommit)$"},
    "DELTA": {"subdir": "_delta_log", "pattern": r"\.json$"},
}


def run_cmd(cmd):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        return p.returncode, p.stdout, p.stderr
    except FileNotFoundError:
        return None, "", "tool not found"
    except subprocess.TimeoutExpired:
        return 124, "", "timeout"


def check_local(base, target, start_epoch):
    cfg = METADATA_CHECKS[target]
    base = re.sub(r"^file://", "", base)
    d = os.path.join(base, cfg["subdir"])
    if not os.path.isdir(d):
        return "missing", f"{d} does not exist"
    pat = re.compile(cfg["pattern"])
    files = [f for f in glob.glob(os.path.join(d, "**", "*"), recursive=True)
             if os.path.isfile(f) and pat.search(os.path.basename(f))]
    if not files:
        return "missing", f"no matching metadata files under {d}"
    newest = max(files, key=os.path.getmtime)
    mtime = os.path.getmtime(newest)
    if mtime >= start_epoch:
        return "fresh", f"{os.path.relpath(newest, base)} (mtime {int(mtime)})"
    return "stale", f"newest is {os.path.relpath(newest, base)}, older than run start"


def check_s3(base, target, start_epoch):
    if not shutil.which("aws"):
        return "not_possible", "aws CLI not available"
    cfg = METADATA_CHECKS[target]
    url = re.sub(r"^s3a://", "s3://", base).rstrip("/") + "/" + cfg["subdir"] + "/"
    rc, out, err = run_cmd(["aws", "s3", "ls", url, "--recursive"])
    if rc is None or rc != 0:
        return "not_possible", f"aws s3 ls failed: {err.strip()[:160] or 'rc=' + str(rc)}"
    pat = re.compile(cfg["pattern"])
    newest = None  # (date_str, key)
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 4 and pat.search(parts[-1]):
            stamp = parts[0] + " " + parts[1]
            if newest is None or stamp > newest[0]:
                newest = (stamp, parts[-1])
    if not newest:
        return "missing", f"no matching metadata at {url}"
    import datetime
    try:
        ts = datetime.datetime.strptime(newest[0], "%Y-%m-%d %H:%M:%S").timestamp()
        if ts >= start_epoch - 120:  # tolerance for clock/listing-time skew
            return "fresh", f"{newest[1]} ({newest[0]})"
        return "stale", f"newest {newest[1]} at {newest[0]}, older than run start"
    except ValueError:
        return "fresh", f"{newest[1]} (timestamp unparsed: {newest[0]})"


def check_hadoop(base, target, start_epoch):
    if not shutil.which("hadoop"):
        return "not_possible", "hadoop CLI not available for scheme"
    cfg = METADATA_CHECKS[target]
    url = base.rstrip("/") + "/" + cfg["subdir"]
    rc, out, err = run_cmd(["hadoop", "fs", "-ls", "-R", url])
    if rc is None or rc != 0:
        return "not_possible", f"hadoop fs -ls failed: {err.strip()[:160] or 'rc=' + str(rc)}"
    pat = re.compile(cfg["pattern"])
    matches = [ln for ln in out.splitlines() if pat.search(ln.split()[-1] if ln.split() else "")]
    if not matches:
        return "missing", f"no matching metadata at {url}"
    # hadoop ls shows 'YYYY-MM-dd HH:mm' — best-effort freshness
    import datetime
    newest_ts, newest_path = 0, None
    for ln in matches:
        m = re.search(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2})\s+(\S+)$", ln)
        if m:
            ts = datetime.datetime.strptime(m.group(1), "%Y-%m-%d %H:%M").timestamp()
            if ts > newest_ts:
                newest_ts, newest_path = ts, m.group(2)
    if newest_path is None:
        return "fresh", "metadata present (timestamps unparsed)"
    if newest_ts >= start_epoch - 120:
        return "fresh", f"{newest_path}"
    return "stale", f"newest {newest_path}, older than run start"


def check_storage(base, target, start_epoch):
    if base.startswith(("s3://", "s3a://")):
        return check_s3(base, target, start_epoch)
    if base.startswith(("gs://", "abfs://", "abfss://", "hdfs://")):
        return check_hadoop(base, target, start_epoch)
    return check_local(base, target, start_epoch)


def get_newest_local_mtime(base, target):
    """Return mtime of the newest matching metadata file, or None if none found."""
    cfg = METADATA_CHECKS.get(target)
    if not cfg:
        return None
    base = re.sub(r"^file://", "", base)
    d = os.path.join(base, cfg["subdir"])
    if not os.path.isdir(d):
        return None
    pat = re.compile(cfg["pattern"])
    files = [f for f in glob.glob(os.path.join(d, "**", "*"), recursive=True)
             if os.path.isfile(f) and pat.search(os.path.basename(f))]
    return max((os.path.getmtime(f) for f in files), default=None)


def source_has_new_commits_since(base, source_format, since_mtime):
    """Check if the source table has commits newer than since_mtime (local paths only).
    Returns True (new commits exist), False (source unchanged), or None (can't determine)."""
    cfg = SOURCE_COMMIT_CHECKS.get(source_format)
    if not cfg:
        return None  # ICEBERG source or unknown — can't check
    base = re.sub(r"^file://", "", base)
    d = os.path.join(base, cfg["subdir"])
    if not os.path.isdir(d):
        return None
    pat = re.compile(cfg["pattern"])
    newer = [f for f in glob.glob(os.path.join(d, "**", "*"), recursive=True)
             if os.path.isfile(f) and pat.search(os.path.basename(f))
             and os.path.getmtime(f) > since_mtime]
    return len(newer) > 0


def get_source_format(config_path):
    """Extract sourceFormat from the datasetConfig YAML."""
    with open(config_path) as f:
        for line in f:
            m = re.match(r"^\s*sourceFormat:\s*(\w+)", line)
            if m:
                return m.group(1).strip().upper()
    return None


VERDICT = {
    ("ok", "fresh"): ("SUCCESS", None),
    ("ok", "missing"): ("FAILED", "log/storage mismatch: log reports success but metadata missing"),
    ("ok", "stale"): ("FAILED", "log/storage mismatch: metadata predates run (or no new commits — see troubleshooting)"),
    ("ok", "not_possible"): ("UNVERIFIED", "log reports success; storage could not be checked"),
    ("unknown", "fresh"): ("SUCCESS", "storage is ground truth (log inconclusive)"),
    ("unknown", "missing"): ("FAILED", "no success in log and no metadata on storage"),
    ("unknown", "stale"): ("FAILED", "no success in log; metadata predates run"),
    ("unknown", "not_possible"): ("UNVERIFIED", "log inconclusive and storage could not be checked"),
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_meta")
    ap.add_argument("--log-results", required=True)
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    with open(args.run_meta) as f:
        meta = json.load(f)
    with open(args.log_results) as f:
        log_results = json.load(f)["results"]

    # table -> basePath from config (reuse parse from parse_result)
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from parse_result import parse_config
    _, tables, names = parse_config(args.config)
    base_by_name = {n: t.get("tableBasePath", "") for n, t in zip(names, tables)}

    start_epoch = float(meta.get("start_epoch", 0))
    exit_code = meta.get("exit_code", 1)
    source_format = get_source_format(args.config)
    rows = []
    for r in log_results:
        base = base_by_name.get(r["table"], "")
        if r["log_status"] == "error":
            verdict, note = "FAILED", r.get("message") or "error in log"
            evidence = note
        else:
            storage_status, detail = check_storage(base, r["target"], start_epoch) if base \
                else ("not_possible", "basePath unknown")
            verdict, note = VERDICT[(r["log_status"], storage_status)]
            evidence = detail if verdict == "SUCCESS" else f"{note} — {detail}"

            # No-op sync detection: (unknown, stale) + exit 0 may mean the source
            # hasn't changed since the last sync, so XTable correctly skipped writing.
            if r["log_status"] == "unknown" and storage_status == "stale" and exit_code == 0:
                meta_mtime = get_newest_local_mtime(base, r["target"]) if base else None
                if meta_mtime is not None and not base.startswith(("s3://", "s3a://", "gs://", "abfs://", "hdfs://")):
                    new_commits = source_has_new_commits_since(base, source_format, meta_mtime)
                    if new_commits is False:
                        verdict = "SUCCESS"
                        evidence = f"{detail} (already synced — no new {source_format} commits since last sync)"
                    elif new_commits is None:
                        verdict = "UNVERIFIED"
                        evidence = f"metadata stale and exit 0; could not verify {source_format} source commit state"
                    # new_commits is True → real FAILED, keep existing verdict

            if r.get("message") and verdict != "SUCCESS":
                evidence += f" | log: {r['message'][:140]}"
        rows.append({"table": r["table"], "target": r["target"], "verdict": verdict, "evidence": evidence})

    report = {"run": meta, "rows": rows}
    out = os.path.join(os.path.dirname(os.path.abspath(args.run_meta)), "final_report.json")
    with open(out, "w") as f:
        json.dump(report, f, indent=2)

    icon = {"SUCCESS": "✅", "FAILED": "❌", "UNVERIFIED": "⚠️"}
    print(f"\n== Conversion verdict (log: {meta.get('log')}) ==")
    for r in rows:
        print(f"{icon[r['verdict']]} {r['verdict']:<11} {r['table']:<24} -> {r['target']:<8} | {r['evidence']}")
    print(f"\nWrote {out}")

    if any(r["verdict"] == "FAILED" for r in rows):
        sys.exit(1)
    if any(r["verdict"] == "UNVERIFIED" for r in rows):
        sys.exit(3)
    sys.exit(0)


if __name__ == "__main__":
    main()
