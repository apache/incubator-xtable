#!/usr/bin/env python3
"""Parse the XTable run log into per-(table, target) outcomes.

Reads run_meta.json (from run_xtable.sh) and the datasetConfig YAML, scans the
log, and writes log_results.json next to run_meta.json.

log_status per (table, target): "ok" | "error" | "unknown".
"unknown" is fine — verify.py's storage check is the ground truth.

If the wording of your XTable version's logs differs, pin the patterns below
(see references/troubleshooting.md, "Pinning log markers").
"""
import argparse
import json
import os
import re
import sys

# Generic markers; pin to your XTable version for sharper "ok" detection.
SUCCESS_PATTERNS = [
    r"completed sync",
    r"sync completed",
    r"completed successfully",
    r"running sync .* completed",
]
ERROR_PATTERNS = [
    r"\bERROR\b",
    r"\bException\b",
    r"\bFAILED\b",
    r"Error running sync",
]


def parse_config(path):
    """Minimal YAML reader for the flat datasetConfig structure (no deps)."""
    targets, tables = [], []
    section = None
    current = None
    with open(path) as f:
        for raw in f:
            line = raw.rstrip("\n")
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if s.startswith("targetFormats:"):
                section = "targets"; continue
            if s.startswith("datasets:"):
                section = "datasets"; continue
            if s.startswith("sourceFormat:"):
                section = None; continue
            if section == "targets" and s.startswith("- "):
                targets.append(s[2:].strip()); continue
            if section == "datasets":
                if s.startswith("- "):
                    current = {}; tables.append(current)
                    s = s[2:].strip()
                if ":" in s and current is not None:
                    k, v = s.split(":", 1)
                    current[k.strip()] = v.strip()
    names = [t.get("tableName") or t.get("tableBasePath", "?").rstrip("/").rsplit("/", 1)[-1] for t in tables]
    return targets, tables, names


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("run_meta")
    ap.add_argument("--config", required=True)
    args = ap.parse_args()

    with open(args.run_meta) as f:
        meta = json.load(f)
    targets, tables, names = parse_config(args.config)
    out_path = os.path.join(os.path.dirname(os.path.abspath(args.run_meta)), "log_results.json")

    results = []

    def emit():
        with open(out_path, "w") as f:
            json.dump({"run_meta": meta, "results": results}, f, indent=2)
        print(f"Wrote {out_path}")
        for r in results:
            print(f"  {r['table']:<24} -> {r['target']:<8} log_status={r['log_status']}"
                  + (f"  ({r['message'][:100]})" if r.get("message") else ""))

    log_path = meta.get("log")
    if not log_path or not os.path.exists(log_path):
        msg = meta.get("error") or f"log file missing (exit_code={meta.get('exit_code')})"
        for name in names or ["<all>"]:
            for tgt in targets or ["<all>"]:
                results.append({"table": name, "target": tgt, "log_status": "error", "message": msg})
        emit(); return

    with open(log_path, errors="replace") as f:
        lines = f.readlines()

    success_re = re.compile("|".join(SUCCESS_PATTERNS), re.IGNORECASE)
    error_re = re.compile("|".join(ERROR_PATTERNS))
    process_failed = meta.get("exit_code", 1) != 0

    for name, tbl in zip(names, tables):
        ident_bits = [b for b in (name, tbl.get("tableBasePath")) if b]
        # Lines mentioning this table (by name or base path)
        mentions = [(i, ln) for i, ln in enumerate(lines) if any(b in ln for b in ident_bits)]
        for tgt in targets:
            status, message = "unknown", None
            # error lines mentioning the table, or table mentions inside a stack-trace window
            for i, ln in mentions:
                if error_re.search(ln):
                    status, message = "error", ln.strip()
                    break
            if status == "unknown":
                for i, ln in mentions:
                    if success_re.search(ln) and (tgt.lower() in ln.lower() or len(targets) == 1):
                        status = "ok"
                        break
                else:
                    # success line mentioning the table but not a specific target
                    if any(success_re.search(ln) for _, ln in mentions):
                        status = "ok"
            if process_failed and status != "error":
                # whole process died; nothing can be trusted as ok from the log
                status = "error" if status == "unknown" else status
                message = message or f"process exit_code={meta.get('exit_code')}"
                if status == "ok":
                    status, message = "unknown", f"log showed success but process exit_code={meta.get('exit_code')}"
            results.append({"table": name, "target": tgt, "log_status": status, "message": message})

    emit()


if __name__ == "__main__":
    main()
