# Troubleshooting XTable conversions

Read this when any report row is FAILED, or when parse_result.py returns many `unknown` statuses.

## Common failures → fixes

| Symptom in log | Probable cause | Fix |
|---|---|---|
| `UnsupportedClassVersionError` / `class file version 55.0` | Running with Java 8 | Use Java 11+ (`java -version`; switch with jenv/sdkman or JAVA_HOME) |
| `NoSuchMethodError` / `ClassNotFoundException` on hadoop/parquet classes | Classpath clash — extra jars on CLASSPATH alongside the bundled jar | Run the bundled jar alone with `java -jar`; unset CLASSPATH |
| `AccessDenied` / `403` on s3 paths | Missing/wrong AWS credentials or region | Provide credentials via env/instance profile, or pass `--hadoopConfig` with fs.s3a settings; verify with `aws s3 ls <path>` |
| `AuthorizationPermissionMismatch` / abfs auth errors | ADLS auth not configured | Pass `--hadoopConfig` with the user's existing core-site for abfs |
| `Unable to infer partition` / partition values null or wrong in target | Missing or incorrect `partitionSpec` for a Hudi source | Confirm partition column, transform type, and date format exactly as it appears in file paths (e.g. `yyyy/MM/dd` vs `yyyy-MM-dd`) |
| `Cannot find table` / empty metadata on ICEBERG source | `tableDataPath` not set to the `/data` directory | Add `tableDataPath: <basePath>/data` |
| Iceberg catalog errors (`NoSuchNamespaceException`, Glue/HMS auth) | Catalog config missing or namespace doesn't exist | Pass `--icebergCatalogConfig`; create the namespace; check `namespace` field |
| Spark session errors on Delta target | Converter Spark conf | Pass `--convertersConfig` setting `spark.master: local[2]` for the DELTA target |
| Schema evolution / incompatible type errors | Source has types unsupported by target snapshot | Check XTable GitHub issues for the specific type; sometimes fixed in newer release |
| Process succeeded for table A, failed for table B, exit code 0 | Expected XTable behavior: per-table exceptions are caught so the batch continues | This is exactly why verify.py exists — trust the per-table verdict, not the exit code |

## Log says ok but storage check FAILED (mismatch)

Possible causes, in order of likelihood:
1. verify.py checked the wrong path — confirm tableBasePath has no trailing-slash/scheme differences (s3 vs s3a) between config and check.
2. Eventual consistency or a listing cache on the object store — re-run verify.py after ~30s before concluding.
3. The sync genuinely wrote nothing (e.g. no new commits since last sync — XTable is incremental). If source had no new commits, target metadata mtimes may legitimately predate run start. Ask the user whether the source changed since the last sync; if not, an unchanged target is correct behavior, report SUCCESS with note "no new commits to sync".

## UNVERIFIED rows

Means the tooling couldn't read target storage. Tell the user exactly what to check manually, e.g.:
- DELTA: `aws s3 ls s3://lake/orders/_delta_log/ | tail` — look for a new `.json` commit at the run time.
- ICEBERG: `aws s3 ls s3://lake/orders/metadata/ | tail` — look for a new `vN.metadata.json`.
- HUDI: list `.hoodie/` (or `.hoodie/timeline/`) for a new instant file.

To make future runs verifiable: install/configure aws CLI for s3 paths, or ensure `hadoop` is on PATH with the same core-site used for the conversion.

## Pinning log markers to your XTable version

parse_result.py matches generic markers (lines containing `ERROR`, `Exception`, and success phrases like `Completed sync`/`completed successfully` near a table name). Log wording can change between releases. To pin:

1. Run one known-good conversion and one deliberately broken one (bad path).
2. `grep -i -E "sync|complete|error|exception" run_<ts>.log` and note the exact per-table success and failure lines.
3. Update `SUCCESS_PATTERNS` / `ERROR_PATTERNS` at the top of `scripts/parse_result.py`.

Even unpinned, the final verdict stays trustworthy because verify.py's storage check is ground truth.
