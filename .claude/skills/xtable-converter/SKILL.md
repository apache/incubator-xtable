---
name: xtable-converter
description: Convert lakehouse tables between Apache Hudi, Apache Iceberg, and Delta Lake formats using Apache XTable (incubator-xtable) driven by natural language instead of hand-written YAML. Use this skill whenever the user asks to convert, sync, translate, or expose a table in another format — e.g. "convert my Hudi table to Iceberg", "make this Delta table queryable from Snowflake", "sync s3://bucket/table to Delta and Iceberg", "run xtable on these tables" — or mentions XTable, table format interoperability, or datasetConfig YAML. Every conversion MUST end with an explicit per-table SUCCESS / FAILED / UNVERIFIED verdict; never report an implied or assumed success.
---

# XTable Converter

Turn a natural-language request like *"convert my Hudi table at s3://lake/orders to Iceberg"* into a validated XTable `datasetConfig` YAML, run the conversion, and report a verified per-table verdict. XTable converts **metadata only** — it never copies or rewrites data files, so conversions are cheap and non-destructive.

## Non-negotiable rules

1. **No silent failures.** Every run ends with the Conversion Report (format below) showing SUCCESS, FAILED, or UNVERIFIED per (table, target format) pair. The XTable jar catches per-table exceptions and can exit 0 even when a table failed — exit code alone is NEVER proof of success.
2. **No guessed paths or credentials.** If the user hasn't given a table path, source format, or required partition spec, ask. Never invent S3/ADLS/GCS paths or credentials.
3. **Run immediately when slots are complete.** If the request contains all required slots, write the YAML and run without asking for confirmation — show the config inline as part of reporting, not as a blocker. Only pause when a required slot is missing or ambiguous.
4. **SUCCESS requires ground-truth verification.** "The log said it worked" is only UNVERIFIED until target metadata is confirmed on storage.

## Workflow

### Step 1 — Extract conversion slots from the user's request

| Slot | Required? | Notes |
|---|---|---|
| `sourceFormat` | Yes | One of HUDI, ICEBERG, DELTA. Infer from phrases like "my Hudi table". Ask if ambiguous. |
| `targetFormats` | Yes | One or more of the other two formats. |
| `tableBasePath` | Yes (per table) | s3://, gs://, abfs://, hdfs://, or local file path. Never guess. |
| `tableName` | Yes (per table) | Default to the last path segment if user doesn't name it; mention the assumption. |
| `tableDataPath` | Required for ICEBERG sources | Usually `<basePath>/data`. Optional otherwise. |
| `partitionSpec` | Required for partitioned HUDI sources | Format `path:type[:format]`, comma-separated for multi-partition. See `references/config-schema.md`. |
| `namespace` | Optional | Only when syncing to a catalog. |
| Catalog / hadoop / converter configs | Optional | `--icebergCatalogConfig`, `--hadoopConfig`, `--convertersConfig`. See `references/config-schema.md`. |

If the source is HUDI and you don't know whether the table is partitioned, ask: "Is the table partitioned? If so, which column(s) and are they string/date-string values or numeric timestamps?" — a wrong partitionSpec is the #1 cause of failed Hudi conversions. Use `VALUE` for string/date-string partition columns (identity transform); use `DAY`/`MONTH`/`YEAR`/`HOUR` only for numeric timestamp or long epoch columns.

### Step 2 — Build and validate the YAML, then write it to disk

Construct the config directly. Validate before writing:
- `sourceFormat` and each entry in `targetFormats` must be HUDI, ICEBERG, or DELTA (uppercase).
- `targetFormats` must not include `sourceFormat`, and must have no duplicates.
- Each dataset must have `tableBasePath` (valid scheme) and `tableName`.
- ICEBERG sources must have `tableDataPath`.
- `partitionSpec` is only valid for HUDI sources; each entry must be `path:type` or `path:type:format`, where type is VALUE, YEAR, MONTH, DAY, or HOUR, and date types require a format string.

If any slot is invalid, report the reason and ask the user to clarify — do not write a bad config.

Example output for *"convert my partitioned Hudi table at /tmp/orders to Iceberg and Delta"* where `order_date` is a string column:

```yaml
sourceFormat: HUDI
targetFormats:
  - ICEBERG
  - DELTA
datasets:
  - tableBasePath: /tmp/orders
    tableName: orders
    partitionSpec: order_date:VALUE
```

Write it to disk immediately — do not wait for confirmation:

```
RUN_DIR=/tmp/xtable_run
mkdir -p $RUN_DIR
# write config.yaml to $RUN_DIR/config.yaml
```

Show the YAML in the same message that reports the conversion result.

### Step 3 — Run the conversion

```bash
bash scripts/run_xtable.sh /tmp/xtable_run/config.yaml \
  [--hadoopConfig path] [--icebergCatalogConfig path] [--convertersConfig path]
```

The wrapper locates the bundled jar (env var `XTABLE_JAR`, or searches `xtable-utilities*/target` and the current dir), records the run start timestamp, streams the jar output to `/tmp/xtable_run/run_<ts>.log`, and writes `/tmp/xtable_run/run_meta.json` (exit code, log path, start epoch). It requires Java 11+. A non-zero exit code means the whole run FAILED — still continue to Step 4 to extract the reason.

**Known issue — `jol-core` missing from bundled jar:** If the run fails with `NoClassDefFoundError: org/openjdk/jol/info/GraphLayout`, bypass `run_xtable.sh` and invoke the jar directly with `jol-core` on the classpath:

```bash
JOL_JAR=$(find ~/.m2/repository/org/openjdk/jol -name "jol-core-*.jar" | grep -v sources | head -1)
XTABLE_JAR=<path-to-bundled.jar>
TS=$(date +%s)
LOG=/tmp/xtable_run/run_${TS}.log
java -cp "$XTABLE_JAR:$JOL_JAR" org.apache.xtable.utilities.RunSync \
  --datasetConfig /tmp/xtable_run/config.yaml > "$LOG" 2>&1
EXIT=$?
printf '{"exit_code": %s, "log": "%s", "start_epoch": %s, "config": "/tmp/xtable_run/config.yaml"}\n' \
  "$EXIT" "$LOG" "$TS" > /tmp/xtable_run/run_meta.json
```

### Step 4 — Parse the log for per-table outcomes

```bash
python3 scripts/parse_result.py /tmp/xtable_run/run_meta.json --config /tmp/xtable_run/config.yaml
```

Outputs `log_results.json`: one record per (table, target) with `log_status` of `ok`, `error`, or `unknown`, plus the captured error message. Log markers are version-dependent — if you see many `unknown` statuses, read `references/troubleshooting.md` (section "Pinning log markers") and rely on Step 5 as ground truth.

### Step 5 — Verify against storage and produce the final verdict

```bash
python3 scripts/verify.py /tmp/xtable_run/run_meta.json \
  --log-results /tmp/xtable_run/log_results.json \
  --config /tmp/xtable_run/config.yaml
```

For each (table, target) the script checks that the expected metadata exists at the target path **and is newer than the run start**: `_delta_log/*.json` for DELTA, `metadata/*.metadata.json` for ICEBERG, `.hoodie` timeline for HUDI. Local paths are checked directly; `s3://` via aws CLI; other schemes via `hadoop fs` if available. It merges with the log results into `final_report.json` using this verdict matrix:

| Log says | Storage check | Final verdict |
|---|---|---|
| ok | fresh metadata found | **SUCCESS** |
| ok | metadata missing or stale | **FAILED** (log/storage mismatch — investigate) |
| ok | check not possible (no access/tooling) | **UNVERIFIED** |
| error | anything | **FAILED** (with error message) |
| unknown | fresh metadata found | **SUCCESS** (storage is ground truth) |
| unknown | missing/stale or not possible | **FAILED** / **UNVERIFIED** |

### Step 6 — Report. ALWAYS use this exact template

```
## Conversion Report — <date time> (run log: <path>)

| Table | Target | Verdict | Evidence |
|---|---|---|---|
| orders | ICEBERG | ✅ SUCCESS | metadata/v3.metadata.json written <time> |
| orders | DELTA   | ❌ FAILED  | <first line of error> |
| sales  | DELTA   | ⚠️ UNVERIFIED | log reports success; no read access to verify s3://... |

<one line per FAILED row: probable cause + suggested fix (see references/troubleshooting.md)>
<for UNVERIFIED rows: exactly what the user can check manually>
```

Never summarize a run as "done" or "completed" without this table. If the process itself failed before any table was attempted (bad jar path, Java missing, malformed YAML), report a single FAILED row with the cause.

## Bundled resources

- `references/config-schema.md` — full datasetConfig schema, partitionSpec grammar, catalog/converter/hadoop config formats. Read when building a non-trivial config (multi-partition, catalogs, custom converters).
- `references/troubleshooting.md` — common failures mapped to fixes, and how to pin log markers to your XTable version. Read whenever any row is FAILED or many rows are `unknown`.
- `scripts/run_xtable.sh` — locates the jar, runs it, captures exit code and log path into `run_meta.json`.
- `scripts/parse_result.py` — parses the run log into per-(table, target) `log_status`.
- `scripts/verify.py` — checks target metadata on storage and produces the final SUCCESS/FAILED/UNVERIFIED verdict.

## Environment assumptions (state these to the user on first run)

- Java 11+ on PATH (`java -version`).
- The XTable bundled jar available; set `XTABLE_JAR=/path/to/xtable-utilities_*-bundled.jar` or place it where the wrapper can find it. Built via `./mvnw install -DskipTests` or downloaded from a release.
- Read access to target storage paths from this machine for verification (aws CLI for s3://, or hadoop fs for other schemes). Without it, the best possible verdict is UNVERIFIED — say so up front, not after the run.