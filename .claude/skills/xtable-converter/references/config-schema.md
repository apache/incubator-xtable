# XTable datasetConfig schema reference

Verified against apache/incubator-xtable README (release 0.3.0-incubating). Re-check against the user's version if conversions behave unexpectedly.

## Top-level datasetConfig YAML

```yaml
sourceFormat: HUDI            # HUDI | ICEBERG | DELTA
targetFormats:
  - DELTA
  - ICEBERG
datasets:
  - tableBasePath: s3://tpc-ds-datasets/1GB/hudi/call_center
    tableDataPath: s3://tpc-ds-datasets/1GB/hudi/call_center/data   # optional; REQUIRED for Iceberg sources
    tableName: call_center
    namespace: my.db          # optional; used when syncing to a catalog
  - tableBasePath: s3://tpc-ds-datasets/1GB/hudi/catalog_sales
    tableName: catalog_sales
    partitionSpec: cs_sold_date_sk:VALUE          # Hudi sources only
  - tableBasePath: s3://hudi/multi-partition-dataset
    tableName: multi_partition_dataset
    partitionSpec: time_millis:DAY:yyyy-MM-dd,type:VALUE
```

## Field rules

- **sourceFormat / targetFormats**: uppercase HUDI, ICEBERG, DELTA. targetFormats must not include sourceFormat.
- **tableBasePath**: base path of the table. Supported schemes: s3://, s3a://, gs://, abfs://, abfss://, hdfs://, file://, or a plain local path.
- **tableDataPath**: where the data files live. For ICEBERG sources you must point at the `/data` directory. If omitted, basePath is used.
- **namespace**: dot-separated (e.g. `my.db`). Only meaningful with a catalog sync.
- **partitionSpec** (HUDI sources only):
  - Comma-separated list of `path:type[:format]`.
  - `path` = dot-separated path to the partition field (nested fields allowed: `meta.event_date`).
  - `type` = one of:
    - `VALUE` — identity transform (no format).
    - `YEAR` | `MONTH` | `DAY` | `HOUR` — date-derived partitioning; **format is required**, e.g. `yyyy-MM-dd` or `yyyy/MM/dd`, matching how the date string appears in file paths.
  - Unpartitioned Hudi table → omit partitionSpec entirely.
  - DELTA and ICEBERG sources never need partitionSpec (partitioning is read from their own metadata).

## Optional companion configs (CLI flags)

### --icebergCatalogConfig catalog.yaml
```yaml
catalogImpl: io.my.CatalogImpl     # e.g. org.apache.iceberg.aws.glue.GlueCatalog
catalogName: name
catalogOptions:                    # passed through as a map
  key1: value1
  warehouse: s3://bucket/warehouse
```

### --convertersConfig converters.yaml
Replace default converter implementations:
```yaml
tableFormatConverters:
  HUDI:
    conversionSourceProviderClass: org.apache.xtable.hudi.HudiConversionSourceProvider
  DELTA:
    conversionTargetProviderClass: org.apache.xtable.delta.DeltaConversionTarget
    configuration:
      spark.master: local[2]
      spark.app.name: xtable
```

### --hadoopConfig hadoop.xml
Standard Hadoop XML for storage credentials/endpoints (S3 keys, ADLS auth, GCS service accounts). The bundled jar already includes AWS, Azure, and GCP hadoop dependencies and ships defaults in `xtable-hadoop-defaults.xml`; a custom file overrides them. Never fabricate credential values — ask the user for their existing core-site/hadoop config.

## Run command

```bash
java -jar xtable-utilities_2.12-<version>-bundled.jar \
  --datasetConfig config.yaml \
  [--hadoopConfig hdfs-site.xml] \
  [--convertersConfig converters.yaml] \
  [--icebergCatalogConfig catalog.yaml]
```

Java 11 is the supported build/runtime version.

## What metadata appears on success (used by verify.py)

| Target | Expected at tableBasePath | Notes |
|---|---|---|
| DELTA | `_delta_log/` containing `NNNNNNNNNNNNNNNNNNNN.json` commits | new commit file per sync |
| ICEBERG | `metadata/` containing `vN.metadata.json` (+ snapshots/manifests) | new metadata version per sync |
| HUDI | `.hoodie/` with `hoodie.properties` and timeline files (`*.commit`, `*.deltacommit`, or under `.hoodie/timeline/`) | new timeline instant per sync |
