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

## Get the runtime jar

The runtime jar is published to Maven Central under the coordinates
`org.apache.xtable:xtable-spark-runtime_2.12:0.4.0-incubating`. Every engine dependency is
`provided`, so the jar carries no transitive downloads.

`spark-submit` needs the jar as its application jar, so download it first:

```shell md title="shell"
curl -O https://repo1.maven.org/maven2/org/apache/xtable/xtable-spark-runtime_2.12/0.4.0-incubating/xtable-spark-runtime_2.12-0.4.0-incubating.jar
```

See the [Downloads](/releases/downloads) page for the full list of releases.

## Quick start

Sync an existing Hudi table to Iceberg and Delta:

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  --master 'local[*]' \
  xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --basepath /path/to/hudi_table \
  --sourceformat HUDI \
  --targets ICEBERG,DELTA
```

Sync an Iceberg table to Hudi:

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --basepath /path/to/iceberg_table \
  --sourceformat ICEBERG \
  --targets HUDI
```

Sync a partitioned Hudi table (pass the source partition spec):

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  xtable-spark-runtime_2.12-0.4.0-incubating.jar \
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
  xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --basepath s3://example-warehouse/db/orders \
  --sourceformat HUDI \
  --targets ICEBERG,DELTA
```

The sync reads the table's existing source-format metadata and writes the Iceberg and Delta metadata
alongside the data files the job already produced; no data is rewritten.

### Run the sync inside the job

If the job already submits its own application jar, add the runtime to that job's classpath with
`--packages` instead of downloading it:

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --packages org.apache.xtable:xtable-spark-runtime_2.12:0.4.0-incubating \
  --class com.example.YourJob \
  your-job.jar
```

The job can then call `XTableSyncService` directly after its write, so the table is readable in the
other formats without a second submit. Build one `TableSyncSpec` per table and pass the session's
Hadoop configuration:

#### A job that writes Hudi, kept readable as Iceberg and Delta

```java md title="OrdersJob.java"
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.xtable.spark.TableSyncSpec;
import org.apache.xtable.spark.XTableSyncService;

String basePath = "s3://example-warehouse/db/orders";

// the write the job already does
df.write().format("hudi").options(hudiOptions).mode("append").save(basePath);

// keep Iceberg and Delta metadata in step with it
Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
new XTableSyncService()
    .sync(
        TableSyncSpec.builder()
            .key("orders")
            .basePath(basePath)
            .sourceFormat("HUDI")
            .targets(Arrays.asList("ICEBERG", "DELTA"))
            .build(),
        hadoopConf);
```

#### A job that writes Iceberg, kept readable as Hudi and Delta

An Iceberg table keeps its data files under `<basePath>/data`, and the targets write their metadata
alongside those files, so set `dataPath` as well:

```java md title="OrdersIcebergJob.java"
String basePath = "s3://example-warehouse/db/orders";

// the write the job already does
df.writeTo("catalog.db.orders").append();

Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
new XTableSyncService()
    .sync(
        TableSyncSpec.builder()
            .key("orders")
            .basePath(basePath)
            .dataPath(basePath + "/data")
            .sourceFormat("ICEBERG")
            .targets(Arrays.asList("HUDI", "DELTA"))
            .build(),
        hadoopConf);
```

`sync` runs in `INCREMENTAL` mode and stores its watermark in the target's sync metadata. It falls
back to a full snapshot when an incremental sync is not safe, such as the first run, so calling it
after every write is idempotent. On Spark 3.4 add `.useDeltaKernel(true)` to force the Spark-free
Delta Kernel writer; on Spark 3.5 and newer it is selected automatically.

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
  xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --datasetconfig dataset.yaml
```

The same shape scales to a whole schema: list every table under `datasets` and keep one
`sourceFormat` and `targetFormats` block for all of them.

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
