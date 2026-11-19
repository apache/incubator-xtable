---
sidebar_position: 5
title: "Write Hudi tables as Iceberg tables"
---

# Write Hudi tables as Iceberg tables

Apache Hudi 1.1 added pluggable table formats ([RFC-93](https://github.com/apache/hudi/blob/master/rfc/rfc-93/rfc-93.md)):
a Hudi writer can keep the metadata of another table format next to its own, in the same commit.
Apache XTable™ (Incubating) provides the Apache Iceberg implementation in the
`xtable-iceberg-pluggable-tf` module. With it, every Hudi write, clean, rollback, savepoint and
archive also commits an Iceberg snapshot in the table folder, so the table reads as a standard
Iceberg table right after the write, without a separate sync job.

Iceberg decides which Hudi commits are visible: a Hudi commit whose Iceberg snapshot was not
committed is treated as pending and is rolled back by the next write, and a failed Iceberg commit
fails the Hudi operation.

## Setting up Spark

The module needs Hudi 1.2.x. Until a release includes it, build the jars from source:

```shell md title="shell"
./mvnw -pl xtable-hudi-support/xtable-iceberg-pluggable-tf -am package -DskipTests
```

A Hudi writer needs four XTable jars next to its own Hudi Spark bundle. Add the Iceberg Spark runtime
to read the table as Iceberg from the same session. The versions below are for Spark 3.5; use the
bundles that match your Spark version.

```shell md title="shell"
XTABLE_JARS=xtable-hudi-support/xtable-iceberg-pluggable-tf/target/xtable-iceberg-pluggable-tf_2.12-<version>.jar,\
xtable-core/target/xtable-core_2.12-<version>.jar,\
xtable-api/target/xtable-api-<version>.jar,\
xtable-hudi-support/xtable-hudi-support-utils/target/xtable-hudi-support-utils-<version>.jar

spark-shell \
  --jars $XTABLE_JARS,hudi-spark3.5-bundle_2.12-1.2.1.jar,iceberg-spark-runtime-3.5_2.12-1.9.2.jar \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar
```

`spark-submit` takes the same `--jars` and `--conf` options.

Each commit can log a warning, `Skipping a ConversionTarget whose engine library is not on the
classpath`, for the Delta targets of `xtable-core`. The Iceberg format does not use them, so you
can ignore it.

:::caution Every writer needs the jars
When the module is not on the classpath, Hudi writes the table in its native format without an
error. Iceberg then does not see the commits of that writer, and the next writer that has the jars
rolls them back. Put the jars on every job that writes the table, including table services such as
clean and clustering that run in separate jobs.
:::

## Creating a table

Set `hoodie.table.format=ICEBERG` on the first write. Hudi stores the format in
`hoodie.properties`, so later writes do not repeat it. The Hudi metadata table must stay off on
every write (see [Limitations](#limitations)).

```scala md title="scala"
val basePath = "s3://example-warehouse/db/trips"

df.write.format("hudi")
  .option("hoodie.table.name", "trips")
  .option("hoodie.datasource.write.recordkey.field", "id")
  .option("hoodie.datasource.write.precombine.field", "ts")
  .option("hoodie.datasource.write.partitionpath.field", "city")
  .option("hoodie.table.format", "ICEBERG")
  .option("hoodie.metadata.enable", "false")
  .mode("append")
  .save(basePath)
```

Upserts, deletes, insert overwrite, clean, clustering, rollbacks and savepoints work as on any Hudi
table. Pass `hoodie.metadata.enable=false` on every write, and in SQL sessions set it before you
run `UPDATE`, `DELETE`, `MERGE INTO` or a procedure.

## Reading the table

The same folder reads as both formats:

```scala md title="scala"
spark.read.format("hudi").load(basePath)
spark.read.format("iceberg").load(basePath)
```

The Iceberg metadata is in the `metadata/` folder under the base path, so any engine that reads
Iceberg tables by path, or a catalog that registers the latest `metadata.json` file, reads the
table without Hudi.

## Limitations

- Copy-on-write tables only. Hudi refuses to load a merge-on-read table with this format.
- The Hudi metadata table must stay off (`hoodie.metadata.enable=false`). A commit on a table that
  has a metadata table fails, and the next write rolls it back.
- Identity partitioning only. Timestamp-based and custom key generators with typed partition
  fields are refused.
- Restore is refused before it changes the table.
- One writer at a time.
- The Iceberg table uses format version 2.
- Write the table only through Hudi. Other engines read it as an Iceberg table, but Hudi does not
  see snapshots that an Iceberg writer commits.
