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

## Prerequisites

- **A source table.** XTable converts the metadata of a table that already exists, so you need one
  before you can sync anything. If you don't have one, follow
  [Creating your first interoperable table](/docs/how-to) to create the Hudi table `people` under
  `file:///tmp/hudi-dataset`, partitioned by `city`. Every example on this page syncs that table.
- **A Spark 3.4.x or 3.5.x installation** with `$SPARK_HOME` set. See
  [Spark version support](#spark-version-support) for what each line provides.
- **Maven**, to resolve the engine libraries in
  [Getting the engine libraries](#getting-the-engine-libraries).

:::caution Write the source table with Hudi 0.14.0
`xtable-spark-runtime` `0.4.0-incubating` is built against Hudi `0.14.0`, which reads Hudi table
version 6. The Quickstart's shell uses `hudi-spark3.4-bundle_2.12:1.2.0`, which writes table
version 9, and the sync then fails in `HoodieTableMetaClient` with an `IllegalArgumentException`
from `TimelineLayoutVersion` before it reads any data. Launch that shell with
`--packages org.apache.hudi:hudi-spark3.4-bundle_2.12:0.14.0` instead.
:::

## Getting the jar

The jar is published to Maven Central as
`org.apache.xtable:xtable-spark-runtime_2.12:0.4.0-incubating`. Download it once:

```shell md title="shell"
curl -O https://repo1.maven.org/maven2/org/apache/xtable/xtable-spark-runtime_2.12/0.4.0-incubating/xtable-spark-runtime_2.12-0.4.0-incubating.jar
```

Every engine dependency is `provided`, so the jar is about 4 MB: XTable's own code plus the few
libraries it relocates for internal use. See the [Downloads](/releases/downloads) page for the full
list of releases.

## Getting the engine libraries

Because Hudi, Iceberg and Delta are `provided`, the runtime jar does not carry them. A cluster that
already runs those engines supplies them and there is nothing more to do. A plain Spark
distribution does not, so resolve them once from the published POM:

```shell md title="shell"
BASE=https://repo1.maven.org/maven2/org/apache/xtable/xtable-spark-runtime_2.12/0.4.0-incubating
curl -O $BASE/xtable-spark-runtime_2.12-0.4.0-incubating.pom

mvn -q -f xtable-spark-runtime_2.12-0.4.0-incubating.pom dependency:build-classpath \
  -DincludeScope=provided -Dmdep.outputFile=engine-classpath-raw.txt
```

That also resolves Spark and Hadoop, which the distribution already has. Keep the engine jars
only:

```shell md title="shell"
tr ':' '\n' < engine-classpath-raw.txt \
  | grep -E '/(hudi-|iceberg-|delta-|avro-|parquet-|jol-core)' \
  | grep -v 'avro-mapred\|avro-ipc' \
  | paste -sd: - > engine-classpath.txt
```

Keep every `parquet-*` jar, `parquet-format-structures` included. If you drop it, the older copy in
the Spark distribution wins and the read fails with `NoSuchFieldError: size_statistics`.

Every `spark-submit` below passes that file through `spark.driver.extraClassPath` and
`spark.executor.extraClassPath`. On a cluster that already supplies the engines, drop those two
settings.

:::note Use a flat classpath, not `--packages`
The engines have to go on the flat classpath rather than through `--packages` or `--jars`. Those
load them in a child classloader, separate from the Avro that Spark itself supplies, which breaks
casts across the boundary such as Iceberg's `PartitionData` to Avro's `IndexedRecord`.
:::

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
  --conf spark.driver.extraClassPath="$(cat engine-classpath.txt)" \
  --conf spark.executor.extraClassPath="$(cat engine-classpath.txt)" \
  xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --basepath file:///tmp/hudi-dataset/people \
  --sourceformat HUDI \
  --targets ICEBERG,DELTA \
  --partitionspec city:VALUE
```

`--partitionspec` is what carries the source partitioning across. The Quickstart writes `people`
partitioned by `city`, and without the flag XTable reads an empty partition-field list and creates
the Iceberg and Delta metadata as unpartitioned.

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
    partitionSpec: city:VALUE
```

```shell md title="shell"
$SPARK_HOME/bin/spark-submit \
  --class org.apache.xtable.spark.XTableSparkSync \
  --master 'local[*]' \
  --conf spark.driver.extraClassPath="$(cat engine-classpath.txt)" \
  --conf spark.executor.extraClassPath="$(cat engine-classpath.txt)" \
  xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --datasetconfig my_config.yaml
```

Add one entry per table. Every entry has to point at a table that already exists:
`XTableSparkSync` logs each table that fails, carries on with the rest, then exits nonzero, so one
missing path fails the whole submit. `datasets` above lists only `people` because that is the one
table the [Prerequisites](#prerequisites) create. An entry for a table partitioned by a timestamp,
in its own namespace, looks like this:

```yaml md title="my_config.yaml, a second datasets entry"
  -
    tableBasePath: file:///tmp/hudi-dataset/events
    tableName: events
    namespace: analytics.raw
    partitionSpec: event_ts:DAY:yyyy-MM-dd
```

All the tables in one file share `sourceFormat` and `targetFormats`, so a run covers one source
format at a time.

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
| `--partitionspec` | Hudi source partition field spec, for example `city:VALUE`. |
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
    .partitionSpec("city:VALUE")
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
            .partitionSpec("city:VALUE")
            .targets(Arrays.asList("ICEBERG", "DELTA"))
            .build(),
        spark.sparkContext().hadoopConfiguration());
```

</TabItem>
</Tabs>

`partitionSpec` mirrors the `--partitionspec` flag above and carries the source partitioning into
the targets. Leave it out for an unpartitioned source. Leaving it out for a partitioned one creates
the targets as unpartitioned.

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
  --master 'local[*]' \
  --conf spark.driver.extraClassPath="$(cat engine-classpath.txt)" \
  --conf spark.executor.extraClassPath="$(cat engine-classpath.txt)" \
  --jars xtable-spark-runtime_2.12-0.4.0-incubating.jar \
  --class com.example.PeopleJob \
  people-job.jar
```

The runtime jar goes on `--jars`, the way you add it to a job you already run, while the engines go
on the flat classpath, the way a cluster supplies them. On a cluster that already runs Hudi,
Iceberg or Delta, drop the two `extraClassPath` settings.

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
