---
sidebar_position: 3
title: "Run an XTable sync on Apache Spark"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Run an XTable sync on Apache Spark

`xtable-spark-runtime` is a runtime jar that runs an Apache XTable™ (Incubating) sync on an Apache
Spark cluster. As with any XTable sync, no data files are rewritten — the sync reads the source
table's metadata and writes the target format's metadata alongside the data that is already there.

The jar is meant to be dropped into a Spark job you already run, so you don't need a separate
process or a separate cluster to keep a table interoperable across formats.

## Getting the jar

The jar is published to Maven Central as
`org.apache.xtable:xtable-spark-runtime_2.12:0.4.0-incubating`. Download it once:

```shell md title="shell"
curl -O https://repo1.maven.org/maven2/org/apache/xtable/xtable-spark-runtime_2.12/0.4.0-incubating/xtable-spark-runtime_2.12-0.4.0-incubating.jar
```

Every engine dependency is `provided`, so the jar is about 4 MB and reuses the Hudi, Iceberg and
Delta libraries your cluster already has. See the [Downloads](/releases/downloads) page for the
full list of releases.

## Adding the sync to a Spark job

This is what the jar is for. Your job already writes a table in one format, and you want that same
table to be interoperable with the others as soon as the write finishes.

Add the jar to the `spark-submit` you already use, alongside your application jar:

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --jars xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --class com.example.OrdersJob \
  orders-job.jar
```

Then call `XTableSyncService` after your write. You describe the table with a `TableSyncSpec` and
hand it the session's Hadoop configuration:

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

To compile against these classes, add the same Maven coordinates to your build with `provided`
scope.

The sync runs incrementally and tracks its own watermark in the target's sync metadata, falling
back to a full snapshot whenever an incremental sync isn't safe — the first run, for example. That
makes it safe to call after every write.

:::note Tables are addressed by path
The runtime jar identifies tables by path rather than through a catalog, so there is no equivalent
of the `RunSync` Iceberg catalog config (`-i`) yet. For an Iceberg source, point `basePath` at the
table root and set `dataPath` to `<basePath>/data`, since that is where Iceberg keeps its data
files and where each target writes its metadata.
:::

:::tip Runnable example
[`demo/spark-runtime`](https://github.com/apache/incubator-xtable/tree/main/demo/spark-runtime) is
a complete job that syncs both directions and verifies the row counts.
:::

## Running a sync as its own job

The jar also ships a `spark-submit` entry point, which is the equivalent of `RunSync` for this
bundle. Pass the jar as the application jar rather than with `--jars`:

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  --master 'local[*]' \
  xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --basepath /path/to/hudi_table \
  --sourceformat HUDI \
  --targets ICEBERG,DELTA
```

To sync several tables in one submit, use `--datasetconfig`. It takes the same YAML that `RunSync`
uses, so an existing config works unchanged, and unlike `RunSync` the config itself may live on
cloud storage:

```yaml md title="my_config.yaml"
sourceFormat: HUDI
targetFormats:
  - DELTA
  - ICEBERG
datasets:
  -
    tableBasePath: s3://tpc-ds-datasets/1GB/hudi/call_center
    tableDataPath: s3://tpc-ds-datasets/1GB/hudi/call_center/data
    tableName: call_center
    namespace: my.db
  -
    tableBasePath: s3://tpc-ds-datasets/1GB/hudi/catalog_sales
    tableName: catalog_sales
    partitionSpec: cs_sold_date_sk:VALUE
  -
    tableBasePath: s3://hudi/multi-partition-dataset
    tableName: multi_partition_dataset
    partitionSpec: time_millis:DAY:yyyy-MM-dd,type:VALUE
```

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --datasetconfig my_config.yaml
```

`--datasetconfig` and `--basepath` are mutually exclusive — pass one or the other.

| Option | Description |
| --- | --- |
| `--basepath` | Base path of the source table. |
| `--sourceformat` | Source format: `HUDI`, `ICEBERG`, `DELTA`, `PAIMON` or `PARQUET`. |
| `--targets` | Comma-separated target formats, for example `ICEBERG,DELTA`. |
| `--datasetconfig` | Path to a YAML config listing several tables. May be local or on cloud storage. |
| `--datapath` | Path to the data files, when it differs from the base path. |
| `--tablename` | Table name. Defaults to the last segment of the base path. |
| `--namespace` | Dot-separated table namespace. |
| `--partitionspec` | Hudi source partition field spec, for example `level:VALUE`. |
| `--usedeltakernel` | Force Delta Kernel for a Delta source or target. |
| `--help` | Print the usage text. |

## Supported formats

Paimon and Parquet are read-only sources; XTable does not write either format as a target.

| Source ↓ / Target → | Hudi | Iceberg | Delta |
| --- | :---: | :---: | :---: |
| **Hudi** | – | ✅ | ✅ |
| **Iceberg** | ✅ | – | ✅ |
| **Delta** | ✅ | ✅ | – |
| **Paimon** | ✅ | ✅ | ✅ |
| **Parquet** | ✅ | ✅ | ✅ |

## Spark version support

Converting to and from Hudi and Iceberg doesn't require Spark at all, so those run on any of the
Spark versions below. Delta is the only engine whose implementation depends on the Spark version,
and the jar chooses the right one automatically:

| Spark version | Hudi and Iceberg | Delta implementation |
| --- | :---: | --- |
| 3.4.x | ✅ | Delta Standalone |
| 3.5.x and newer | ✅ | [Delta Kernel](https://docs.delta.io/latest/delta-kernel.html), selected automatically |

Delta Standalone doesn't run on Spark 3.5, so on 3.5 and newer a Delta source or target is routed
through Delta Kernel with no flag needed. If you want Kernel on Spark 3.4 as well, pass
`--usedeltakernel`, or set `.useDeltaKernel(true)` on the `TableSyncSpec`.

## Next steps

- See the [Quickstart](/docs/how-to) for an end-to-end interoperability walkthrough.
- See [Apache Spark](/docs/spark) for the options each format needs when you query a synced table.
- See [Installation](/docs/setup) if you'd rather build the project from source.
