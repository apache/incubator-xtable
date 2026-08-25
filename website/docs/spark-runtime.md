---
sidebar_position: 3
title: "Run a sync on Apache Spark"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Run a sync on Apache Spark

`xtable-spark-runtime` is a runtime jar that runs an Apache XTable™ (Incubating) sync on an
Apache Spark cluster. The sync reads the source table metadata and writes the target format
metadata next to the data files that already exist. It rewrites no data.

Add the jar to a Spark job you already run. You need no separate service and no separate cluster.

## Get the jar

The jar is on Maven Central under `org.apache.xtable:xtable-spark-runtime_2.12:0.4.0-incubating`.
Download it once:

```shell md title="shell"
curl -O https://repo1.maven.org/maven2/org/apache/xtable/xtable-spark-runtime_2.12/0.4.0-incubating/xtable-spark-runtime_2.12-0.4.0-incubating.jar
```

The [Downloads](/releases/downloads) page lists every release.

## Add the sync to a Spark job

This is the main use case. Your job already writes a table in one format. You want the same
table to be readable in the other formats when the write completes.

Pass the jar to your existing `spark-submit` with `--jars`, next to the application jar you
already submit:

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --jars xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --class com.example.OrdersJob \
  orders-job.jar
```

Your job then calls `XTableSyncService` after the write. Build one `TableSyncSpec` for the
table and pass the Hadoop configuration of the Spark session:

<Tabs
groupId="language"
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Java', value: 'java', },
]}
>
<TabItem value="scala">

```scala md title="OrdersJob.scala"
import java.util.{Arrays => JArrays}
import org.apache.xtable.spark.{TableSyncSpec, XTableSyncService}

val basePath = "s3://example-warehouse/db/orders"

// the write your job already does
df.write.format("hudi").options(hudiOptions).mode("append").save(basePath)

// the one call you add
new XTableSyncService().sync(
  TableSyncSpec.builder()
    .key("orders")
    .basePath(basePath)
    .sourceFormat("HUDI")
    .targets(JArrays.asList("ICEBERG", "DELTA"))
    .build(),
  spark.sparkContext.hadoopConfiguration)
```

</TabItem>
<TabItem value="java">

```java md title="OrdersJob.java"
import java.util.Arrays;
import org.apache.xtable.spark.TableSyncSpec;
import org.apache.xtable.spark.XTableSyncService;

String basePath = "s3://example-warehouse/db/orders";

// the write your job already does
df.write().format("hudi").options(hudiOptions).mode("append").save(basePath);

// the one call you add
new XTableSyncService()
    .sync(
        TableSyncSpec.builder()
            .key("orders")
            .basePath(basePath)
            .sourceFormat("HUDI")
            .targets(Arrays.asList("ICEBERG", "DELTA"))
            .build(),
        spark.sparkContext().hadoopConfiguration());
```

</TabItem>
</Tabs>

To compile against the classes, add the same coordinates to your build with `provided` scope.

`sync` runs in incremental mode and keeps its watermark in the target sync metadata. It falls
back to a full snapshot when an incremental sync is not safe, such as the first run. A call
after every write is therefore safe to repeat.

For an Iceberg source, also set `dataPath` to `<basePath>/data`. Iceberg keeps the data files
there, and each target writes its metadata next to those files.

:::note Runnable example
[`demo/spark-runtime`](https://github.com/apache/incubator-xtable/tree/main/demo/spark-runtime)
holds a complete job that runs both directions and checks the row counts.
:::

## Why `--jars` and not `--packages`

The runtime jar carries only XTable code plus a few relocated helper libraries. It declares
Spark, Hadoop, Hudi, Iceberg, Delta, Avro and Parquet as `provided`, so it uses the copies your
cluster already supplies.

Use `--jars`. It takes the file you downloaded and needs no network access at submit time.

`--packages` also works. Every dependency in the published POM is `provided` or `test` scoped,
so Ivy downloads this one jar and no transitive jars. It costs you a Maven Central lookup on
each submit, which is the only reason to prefer `--jars`.

:::danger Do not add the engines with `--packages`
Add Hudi, Iceberg or Delta with `--packages` only when your cluster does not already supply
them. `--packages` and `--jars` load classes in a child class loader. A second copy of Avro or
Parquet there breaks casts across the class loader boundary. Put engine jars on the flat
classpath with `spark.driver.extraClassPath` and `spark.executor.extraClassPath` instead.
:::

## Run a sync without a job

The jar also ships a `spark-submit` entry point. Use it when you want the sync as a separate
step. Pass the jar as the application jar, not with `--jars`:

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  --master 'local[*]' \
  xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --basepath /path/to/hudi_table \
  --sourceformat HUDI \
  --targets ICEBERG,DELTA
```

To sync many tables in one submit, pass `--datasetconfig` with a YAML file. `sourceFormat` and
`targetFormats` apply to every table. Each entry under `datasets` needs only a `tableBasePath`.
The config path can be local or on cloud storage.

```yaml md title="dataset.yaml"
sourceFormat: HUDI
targetFormats:
  - ICEBERG
  - DELTA
datasets:
  - tableBasePath: s3://example-warehouse/db/store_sales
  - tableBasePath: s3://example-warehouse/db/store_returns
  - tableBasePath: s3://example-warehouse/db/item
```

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --datasetconfig dataset.yaml
```

`--datasetconfig` and `--basepath` are mutually exclusive. Pass one of the two.

| Option | Description |
| --- | --- |
| `--basepath` | Base path of the source table. |
| `--sourceformat` | Source format: `HUDI`, `ICEBERG`, `DELTA`, `PAIMON` or `PARQUET`. |
| `--targets` | Comma-separated target formats, such as `ICEBERG,DELTA`. |
| `--datasetconfig` | Path to a YAML config that lists many tables. |
| `--datapath` | Path to the data files when it differs from the base path. |
| `--tablename` | Table name. Defaults to the last segment of the base path. |
| `--namespace` | Dot-separated table namespace. |
| `--partitionspec` | Hudi source partition field spec, such as `level:VALUE`. |
| `--usedeltakernel` | Force the Spark-free Delta Kernel for a Delta source or target. |
| `--help` | Print the usage text. |

## Supported formats

Paimon and Parquet are read-only sources. XTable writes no Paimon or Parquet target.

| Source ↓ / Target → | Hudi | Iceberg | Delta |
| --- | :---: | :---: | :---: |
| **Hudi** | – | ✅ | ✅ |
| **Iceberg** | ✅ | – | ✅ |
| **Delta** | ✅ | ✅ | – |
| **Paimon** | ✅ | ✅ | ✅ |
| **Parquet** | ✅ | ✅ | ✅ |

## Spark versions

Hudi and Iceberg conversion use Spark-free core classes only, so they run on every Spark line
below. Delta is the one engine that depends on the Spark version, and the jar picks the right
implementation for you.

| Spark version | Hudi and Iceberg | Delta implementation |
| --- | :---: | --- |
| 3.4.x | ✅ | Delta Standalone (`delta-core`) |
| 3.5.x and newer | ✅ | [Delta Kernel](https://docs.delta.io/latest/delta-kernel.html), selected for you |

On Spark 3.5 and newer, `delta-core` does not run. A Delta source or target goes through the
Spark-free Delta Kernel instead, with no flag. To force Kernel on Spark 3.4, pass
`--usedeltakernel`, or set `.useDeltaKernel(true)` on the `TableSyncSpec`.

## Read a Hudi target

XTable records the file listing of a Hudi target in the Hudi metadata table. Enable that table
in the reader. Without the option, the sync reports success and the read returns zero rows.

```scala md title="scala"
spark.read
  .option("hoodie.metadata.enable", "true")
  .option("hoodie.datasource.read.extract.partition.values.from.path", "true")
  .format("hudi")
  .load(basePath)
```

Iceberg and Delta targets need no extra read option. One exception: the vectorized Arrow reader
of Iceberg mis-casts timestamp columns on a table that another engine wrote. Pass
`vectorization-enabled=false` when you see that.

## Next steps

- Read the [Quickstart](/docs/how-to) for a walkthrough of interoperability.
- Read [Apache Spark](/docs/spark) to query a synced table from Spark.
- Read [Installation](/docs/setup) to build the project from source.
