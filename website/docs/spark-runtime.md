---
title: "Run XTable on Apache Spark"
---

# Running Apache XTable™ (Incubating) on Apache Spark

The `xtable-spark-runtime` module publishes a self-contained runtime jar that runs an
XTable metadata sync with `spark-submit` on an existing Apache Spark cluster. It is the
`spark-submit` equivalent of the `RunSync` utility: no data is rewritten, only the target table
format metadata is generated alongside the existing data files.

Use this when you already run Spark (EMR, Dataproc, HDInsight, Databricks, or a local install) and
want to add interoperability without standing up a separate process.

## Build the runtime jar

From the project root:

```shell md title="shell"
./mvnw clean package -pl xtable-spark-runtime -am -DskipTests
```

This produces the runtime jar at
`xtable-spark-runtime/target/xtable-spark-runtime_2.12-<version>.jar`.

## Quick start

Sync an existing Hudi table to Iceberg and Delta:

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  --master 'local[*]' \
  xtable-spark-runtime_2.12-<version>.jar \
  --basepath /path/to/hudi_table \
  --sourceformat HUDI \
  --targets ICEBERG,DELTA
```

Sync an Iceberg table to Hudi:

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  xtable-spark-runtime_2.12-<version>.jar \
  --basepath /path/to/iceberg_table \
  --sourceformat ICEBERG \
  --targets HUDI
```

Sync a partitioned Hudi table (pass the source partition spec):

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  xtable-spark-runtime_2.12-<version>.jar \
  --basepath /path/to/hudi_table \
  --sourceformat HUDI \
  --targets DELTA \
  --partitionspec level:VALUE
```

The engine libraries (Hudi, Iceberg, Delta, Avro, Parquet) are `provided`. The runtime jar expects
them on the Spark runtime classpath, which is the case on a standard Spark install with the relevant
format support.

## Adding to an existing Spark job

A common setup is a Spark job that already writes a table in one format (for example a job that
writes Hudi) where the same table should also be readable as Iceberg or Delta. Because the runtime
jar's engine libraries are `provided`, it reuses the Hudi, Iceberg, and Delta libraries already
present on the Spark runtime that job uses; the only additional artifact is this jar. Add it to that
Spark runtime's classpath and run the sync, with no separate installation required.

Run `XTableSparkSync` on the same Spark cluster, pointed at the table the job writes, as a follow-on
step after the write completes. Set `--sourceformat` to the format the job writes and list the
formats to add in `--targets`:

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  xtable-spark-runtime_2.12-<version>.jar \
  --basepath s3://example-warehouse/db/orders \
  --sourceformat HUDI \
  --targets ICEBERG,DELTA
```

The sync reads the table's existing source-format metadata and writes the Iceberg and Delta metadata
alongside the data files the job already produced; no data is rewritten.

## Sync multiple tables

To sync more than one table in a single submit, pass `--datasetconfig` with a YAML file instead of
the per-table `--basepath`/`--sourceformat`/`--targets` flags (the two modes are mutually
exclusive). `sourceFormat` and `targetFormats` apply to every table; each entry under `datasets`
needs only a `tableBasePath`, with `tableName`, `tableDataPath`, `namespace`, and `partitionSpec`
optional. The config path may be local or on cloud storage, and each table is synced in turn.

```yaml md title="dataset.yaml"
sourceFormat: HUDI
targetFormats:
  - ICEBERG
  - DELTA
datasets:
  - tableBasePath: s3://tpcds-datasets/100GB/store_sales
  - tableBasePath: s3://tpcds-datasets/100GB/store_returns
  - tableBasePath: s3://tpcds-datasets/100GB/item
```

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  xtable-spark-runtime_2.12-<version>.jar \
  --datasetconfig dataset.yaml
```

A complete example covering the 24 tables of the TPC-DS schema is provided at
[`xtable-spark-runtime/examples/tpcds-dataset.yaml`](https://github.com/apache/incubator-xtable/blob/main/xtable-spark-runtime/examples/tpcds-dataset.yaml).

## Command-line options

One of `--basepath` (single table) or `--datasetconfig` (multiple tables) is required; the two are
mutually exclusive.

| Option | Required | Description |
| --- | --- | --- |
| `--basepath` | yes\* | Base path of the source table. Required unless `--datasetconfig` is given. |
| `--sourceformat` | yes\* | Source table format: `HUDI`, `ICEBERG`, `DELTA`, `PAIMON`, or `PARQUET`. Required unless `--datasetconfig` is given. |
| `--targets` | yes\* | Comma-separated target formats, e.g. `ICEBERG,DELTA`. Required unless `--datasetconfig` is given. |
| `--datasetconfig` | yes\* | Path (local or cloud) to a YAML dataset config for syncing multiple tables. Mutually exclusive with `--basepath`/`--sourceformat`/`--targets`. |
| `--datapath` | no | Path to the data files if different from the base path (e.g. Iceberg keeps data under `<basePath>/data`). |
| `--tablename` | no | Table name; defaults to the last segment of the base path. |
| `--namespace` | no | Dot-separated table namespace. |
| `--partitionspec` | no | Hudi source partition field spec, e.g. `level:VALUE`. |
| `--usedeltakernel` | no | Force the Spark-free Delta Kernel for the Delta source/target. Auto-enabled on Spark 3.5+ (see below). |
| `--help` | no | Print usage. |

## Supported formats

Paimon and Parquet are read-only sources (there is no corresponding write target).

| Source ↓ / Target → | Hudi | Iceberg | Delta |
| --- | :---: | :---: | :---: |
| **Hudi** | – | ✅ | ✅ |
| **Iceberg** | ✅ | – | ✅ |
| **Delta** | ✅ | ✅ | – |
| **Paimon** | ✅ | ✅ | ✅ |
| **Parquet** | ✅ | ✅ | ✅ |

## Spark version compatibility

Hudi and Iceberg conversion use only Spark-free core classes, so they run on any of the Spark lines
below. Delta is the only Spark-version-sensitive engine, and the runtime jar picks the right
implementation automatically:

| Spark version | Hudi / Iceberg | Delta implementation |
| --- | :---: | --- |
| 3.4.x | ✅ | Delta Standalone (`delta-core`) |
| 3.5.x and newer | ✅ | Delta Kernel (Spark-free), selected automatically |

On Spark 3.5+, the `delta-core` in the runtime jar does not run, so a Delta source or target is
routed through the Spark-free [Delta Kernel](https://docs.delta.io/latest/delta-kernel.html)
implementation automatically, with no flag needed. To force Kernel on any Spark version (e.g. Spark
3.4), pass `--usedeltakernel`.

## Next steps

- See [Installation](/docs/setup) for building the project.
- See the [Quickstart](/docs/how-to) for an end-to-end interoperability walkthrough.
- To query a synced table from Spark, see [Apache Spark](/docs/spark).
