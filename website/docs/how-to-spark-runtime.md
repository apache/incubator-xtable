---
sidebar_position: 3
title: "Run an XTable sync on Apache Spark"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Run an XTable sync on Apache Spark

`xtable-spark-runtime` is a runtime jar that runs an Apache XTable™ (Incubating) sync on an Apache
Spark cluster. As with any XTable sync, no data files are rewritten. The sync reads the source
table's metadata and writes the target format's metadata alongside the data that is already there.

There are two ways to use it, and this page covers them in that order:

1. **As its own job**, with the `XTableSparkSync` entry point. This is the quickest way to try a
   sync, and it is the equivalent of `RunSync` for this bundle.
2. **Inside a Spark job you already run**, by calling `XTableSyncService` right after your write,
   so a table stays interoperable without a separate process or cluster.

## Getting the jar

The jar is published to Maven Central as
`org.apache.xtable:xtable-spark-runtime_2.12:0.4.0-incubating`. Download it once:

```shell md title="shell"
curl -O https://repo1.maven.org/maven2/org/apache/xtable/xtable-spark-runtime_2.12/0.4.0-incubating/xtable-spark-runtime_2.12-0.4.0-incubating.jar
```

Every engine dependency is `provided`, so the jar is about 4 MB and reuses the Hudi, Iceberg and
Delta libraries your cluster already has. See the [Downloads](/releases/downloads) page for the
full list of releases.

## Prerequisites

XTable converts the metadata of a table that already exists, so you need a source table before you
can sync anything. If you don't have one, follow
[Creating your first interoperable table](/docs/how-to) to create the Hudi table `people` under
`file:///tmp/hudi-dataset`. Every example on this page syncs that table.

You also need a Spark 3.4.x or 3.5.x installation with `$SPARK_HOME` set. See
[Spark version support](#spark-version-support) for what each line provides.

## Running a sync as its own job

The jar ships a `spark-submit` entry point, `org.apache.xtable.spark.XTableSparkSync`. Pass the jar
as the application jar rather than with `--jars`. It runs in one of two modes.

### Syncing a single table

Describe the table with command line options. This is the shortest path from a source table to an
interoperable one:

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  --master 'local[*]' \
  xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --basepath file:///tmp/hudi-dataset/people \
  --sourceformat HUDI \
  --targets ICEBERG,DELTA
```

After this finishes, `/tmp/hudi-dataset/people` carries Iceberg and Delta metadata next to the Hudi
data files, and all three formats read the same rows.

### Syncing several tables from a config file

To sync more than one table in a single submit, list them in a YAML file and pass
`--datasetconfig`. It takes the same config that `RunSync` uses, so an existing file works
unchanged:

```yaml md title="my_config.yaml"
sourceFormat: HUDI
targetFormats:
  - DELTA
  - ICEBERG
datasets:
  -
    tableBasePath: file:///tmp/hudi-dataset/people
    tableName: people
  -
    tableBasePath: file:///tmp/hudi-dataset/orders
    tableName: orders
    partitionSpec: order_date:VALUE
  -
    tableBasePath: file:///tmp/hudi-dataset/events
    tableName: events
    namespace: analytics.raw
    partitionSpec: event_ts:DAY:yyyy-MM-dd
```

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  --master 'local[*]' \
  xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --datasetconfig my_config.yaml
```

Unlike `RunSync`, which reads the file from the local filesystem, `XTableSparkSync` reads it
through the Spark Hadoop configuration, so the config itself may live on S3, GCS or ABFS.

`--datasetconfig` and `--basepath` are mutually exclusive. Pass one or the other.

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

## Adding the sync to a Spark job

Running the sync as its own job means a second submit every time the table changes. If your job
already writes the table, you can keep it interoperable in the same run by calling the sync
directly after your write.

### Writing the job

Call `XTableSyncService` after your write. You describe the table with a `TableSyncSpec` and hand
it the session's Hadoop configuration:

<Tabs
groupId="language"
defaultValue="scala"
values={[
{ label: 'Scala', value: 'scala', },
{ label: 'Java', value: 'java', },
]}
>
<TabItem value="scala">

```scala md title="PeopleJob.scala"
import java.util.{Arrays => JArrays}
import org.apache.xtable.spark.{TableSyncSpec, XTableSyncService}

val basePath = "file:///tmp/hudi-dataset/people"

// the write your job already does
df.write.format("hudi").options(hudiOptions).mode("append").save(basePath)

// the one call you add
new XTableSyncService().sync(
  TableSyncSpec.builder()
    .key("people")
    .basePath(basePath)
    .sourceFormat("HUDI")
    .targets(JArrays.asList("ICEBERG", "DELTA"))
    .build(),
  spark.sparkContext.hadoopConfiguration)
```

</TabItem>
<TabItem value="java">

```java md title="PeopleJob.java"
import java.util.Arrays;
import org.apache.xtable.spark.TableSyncSpec;
import org.apache.xtable.spark.XTableSyncService;

String basePath = "file:///tmp/hudi-dataset/people";

// the write your job already does
df.write().format("hudi").options(hudiOptions).mode("append").save(basePath);

// the one call you add
new XTableSyncService()
    .sync(
        TableSyncSpec.builder()
            .key("people")
            .basePath(basePath)
            .sourceFormat("HUDI")
            .targets(Arrays.asList("ICEBERG", "DELTA"))
            .build(),
        spark.sparkContext().hadoopConfiguration());
```

</TabItem>
</Tabs>

The sync runs incrementally and tracks its own watermark in the target's sync metadata, falling
back to a full snapshot whenever an incremental sync isn't safe, the first run being the obvious
case. That makes it safe to call after every write.

### Building the job

Add the same Maven coordinates you downloaded above to your build with `provided` scope, so the
classes compile but are not packaged into your application jar:

```xml md title="pom.xml"
<dependency>
  <groupId>org.apache.xtable</groupId>
  <artifactId>xtable-spark-runtime_2.12</artifactId>
  <version>0.4.0-incubating</version>
  <scope>provided</scope>
</dependency>
```

Then build your application jar as usual:

```shell md title="shell"
mvn clean package
```

### Running the job

Add the runtime jar to the `spark-submit` you already use, alongside your application jar:

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --jars xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --class com.example.PeopleJob \
  --master 'local[*]' \
  people-job.jar
```

:::note Tables are addressed by path
The runtime jar identifies tables by path rather than through a catalog, so there is no equivalent
of the `RunSync` Iceberg catalog config (`-i`) yet. When a table's data files don't sit directly
under `basePath`, set `dataPath` to wherever they do, because that's the location each target
writes its metadata to. Iceberg tables often end up at `<basePath>/data`, but nothing in Iceberg
requires it. `write.data.path` and object-storage layouts can put data files anywhere, so check
where your table actually writes.
:::

:::tip Runnable example
[`demo/spark-runtime`](https://github.com/apache/incubator-xtable/tree/main/demo/spark-runtime) is
a complete job that syncs both directions and verifies the row counts.
:::

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

`xtable-spark-runtime` 0.4.0-incubating supports Spark 3.4.x and 3.5.x, on Scala 2.12.

Converting to and from Hudi and Iceberg doesn't require Spark at all, so those run on both lines.
Delta is the only engine whose implementation depends on the Spark version, and the jar chooses
the right one automatically:

| Spark version | Hudi and Iceberg | Delta implementation |
| --- | :---: | --- |
| 3.4.x | ✅ | Delta Standalone |
| 3.5.x | ✅ | [Delta Kernel](https://docs.delta.io/latest/delta-kernel.html), selected automatically |

:::note Which Delta implementation you get
Delta Standalone doesn't run on Spark 3.5, so on 3.5.x a Delta source or target is routed through
Delta Kernel with no flag needed. If you want Kernel on Spark 3.4 as well, pass
`--usedeltakernel`, or set `.useDeltaKernel(true)` on the `TableSyncSpec`.
:::

## Next steps

- See the [Quickstart](/docs/how-to) for an end-to-end interoperability walkthrough.
- See [Apache Spark](/docs/spark) for the options each format needs when you query a synced table.
- See [Installation](/docs/setup) if you'd rather build the project from source.
