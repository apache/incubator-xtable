#!/usr/bin/env bash
# Run the XTable bundled jar with full capture. Never swallows the exit code.
# Usage: run_xtable.sh <datasetConfig.yaml> [extra java -jar args: --hadoopConfig X --icebergCatalogConfig Y --convertersConfig Z]
set -u

CONFIG="${1:?Usage: run_xtable.sh <datasetConfig.yaml> [extra args]}"
shift || true
RUN_DIR="$(cd "$(dirname "$CONFIG")" && pwd)"
TS="$(date +%s)"
LOG="$RUN_DIR/run_${TS}.log"
META="$RUN_DIR/run_meta.json"

err() { echo "RUN FAILED: $*" >&2; }

# --- preflight: java ---
if ! command -v java >/dev/null 2>&1; then
  err "java not found on PATH (Java 11+ required)"
  printf '{"exit_code": 127, "log": null, "start_epoch": %s, "config": "%s", "error": "java not found on PATH"}\n' "$TS" "$CONFIG" > "$META"
  echo "Wrote $META"; exit 127
fi
JAVA_MAJOR="$(java -version 2>&1 | head -n1 | sed -E 's/.*version "([0-9]+).*/\1/')"
if [ "${JAVA_MAJOR:-0}" = "1" ]; then JAVA_MAJOR=8; fi
if [ "${JAVA_MAJOR:-0}" -lt 11 ] 2>/dev/null; then
  err "Java 11+ required, found major version: ${JAVA_MAJOR:-unknown}"
  printf '{"exit_code": 1, "log": null, "start_epoch": %s, "config": "%s", "error": "Java 11+ required, found %s"}\n' "$TS" "$CONFIG" "${JAVA_MAJOR:-unknown}" > "$META"
  echo "Wrote $META"; exit 1
fi

# --- preflight: locate jar ---
JAR="${XTABLE_JAR:-}"
if [ -z "$JAR" ]; then
  JAR="$(ls -1 ./xtable-utilities*-bundled.jar ./*xtable*bundled*.jar \
         xtable-utilities/target/xtable-utilities*-bundled.jar 2>/dev/null | head -n1 || true)"
fi
if [ -z "$JAR" ] || [ ! -f "$JAR" ]; then
  err "XTable bundled jar not found. Set XTABLE_JAR=/path/to/xtable-utilities_*-bundled.jar"
  printf '{"exit_code": 2, "log": null, "start_epoch": %s, "config": "%s", "error": "bundled jar not found; set XTABLE_JAR"}\n' "$TS" "$CONFIG" > "$META"
  echo "Wrote $META"; exit 2
fi

echo "Running XTable: jar=$JAR config=$CONFIG extra_args=$*"
echo "Log: $LOG"

# --- run (never abort on failure; we must record the exit code) ---
java -jar "$JAR" --datasetConfig "$CONFIG" "$@" >"$LOG" 2>&1
EXIT=$?

printf '{"exit_code": %s, "log": "%s", "start_epoch": %s, "config": "%s", "jar": "%s"}\n' \
  "$EXIT" "$LOG" "$TS" "$CONFIG" "$JAR" > "$META"

echo "Exit code: $EXIT (reminder: exit 0 does NOT guarantee per-table success — run parse_result.py and verify.py)"
echo "Wrote $META"
exit "$EXIT"
