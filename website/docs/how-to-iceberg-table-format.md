---
sidebar_position: 5
title: "Write Hudi tables as Iceberg tables"
---

# Write Hudi tables as Iceberg tables

Apache Hudi 1.1 added pluggable table formats ([RFC-93](https://github.com/apache/hudi/blob/master/rfc/rfc-93/rfc-93.md)):
a Hudi writer can keep the metadata of another table format next to its own, in the same commit.
Apache XTable™ (Incubating) provides the Apache Iceberg implementation in the
`xtable-iceberg-pluggable-tf` module. With it, every Hudi write, clean, rollback, savepoint and
archive also commits an Iceberg snapshot under the table base path, so the table reads as a
standard Iceberg table right after the write, without a separate sync job.

The Iceberg metadata is the source of truth for the table. A Hudi commit is visible only after its
Iceberg snapshot is committed: a Hudi commit without an Iceberg snapshot is treated as pending and
is rolled back by the next write, and a failed Iceberg commit fails the Hudi operation.

## Setting up Spark

The Iceberg table format requires Apache Hudi 1.2.x. In addition to the Hudi Spark bundle, the
Spark job needs the `xtable-iceberg-pluggable-tf` jar, its XTable dependencies (`xtable-core`,
`xtable-api` and `xtable-hudi-support-utils`) and the Iceberg Spark runtime. The XTable jars are
available on Maven Central from 0.5.0:

```shell md title="shell"
REPO=https://repo1.maven.org/maven2/org/apache/xtable
VERSION=0.5.0-incubating
curl -O $REPO/xtable-iceberg-pluggable-tf_2.12/$VERSION/xtable-iceberg-pluggable-tf_2.12-$VERSION.jar
curl -O $REPO/xtable-core_2.12/$VERSION/xtable-core_2.12-$VERSION.jar
curl -O $REPO/xtable-api/$VERSION/xtable-api-$VERSION.jar
curl -O $REPO/xtable-hudi-support-utils/$VERSION/xtable-hudi-support-utils-$VERSION.jar
```

The XTable jars do not bundle Iceberg, so the Iceberg Spark runtime is required. The Hudi writer
uses it to commit each snapshot, and the same Spark session can read the table as Iceberg. The
example below is for Spark 3.5; use the Hudi and Iceberg bundles that match the Spark version.

```shell md title="shell"
XTABLE_JARS=xtable-iceberg-pluggable-tf_2.12-$VERSION.jar,\
xtable-core_2.12-$VERSION.jar,\
xtable-api-$VERSION.jar,\
xtable-hudi-support-utils-$VERSION.jar

spark-shell \
  --jars $XTABLE_JARS,hudi-spark3.5-bundle_2.12-1.2.1.jar,iceberg-spark-runtime-3.5_2.12-1.9.2.jar \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension \
  --conf spark.kryo.registrator=org.apache.spark.HoodieSparkKryoRegistrar
```

`spark-submit` takes the same `--jars` and `--conf` options.

:::caution Every writer needs the jars
Hudi loads the table format through the Java `ServiceLoader`. When the
`xtable-iceberg-pluggable-tf` jar is not on the classpath, Hudi does not fail: it falls back to its
native format, even when `hoodie.table.format` is `ICEBERG`. The commits of that writer have no
Iceberg snapshot, so the next writer that has the jars rolls them back. Add the jars to every job
that writes the table, including table services such as clean and clustering that run in separate
jobs.
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
table.

`hoodie.metadata.enable` is a writer setting, and Hudi enables the metadata table on every write
that does not set it to `false`, even when earlier writes of the table did. Such a write builds
the metadata table and its commit fails; the next write with the setting off removes the metadata
table and rolls the failed commit back. A DataFrame write passes the setting as an option, as in
the example above. A Spark SQL session sets it once, before any `UPDATE`, `DELETE`, `MERGE INTO` or
procedure call:

```sql md title="sql"
SET hoodie.metadata.enable=false;
```

## Reading the table

The same base path reads as both formats:

```scala md title="scala"
spark.read.format("hudi").load(basePath)
spark.read.format("iceberg").load(basePath)
```

The Iceberg metadata is under `metadata/` in the base path, so any engine that reads
Iceberg tables by path, or a catalog that registers the latest `metadata.json` file, reads the
table without Hudi.

## Limitations

- Copy-on-write tables only. Hudi refuses to load a merge-on-read table with this format.
- The Hudi metadata table must stay off (`hoodie.metadata.enable=false`). A commit on a table that
  has a metadata table fails, and the next write rolls it back.
- Identity partitioning only. Timestamp-based and custom key generators with typed partition
  fields are refused.
- Restore is refused before it changes the table.
- Spark SQL `UPDATE` fails on a table whose record key column is `NOT NULL`, for example one
  created from a Scala DataFrame with a primitive key column, with `Cannot add field id as an
  identifier field: not a required field`. `DELETE`, `MERGE INTO` and DataFrame upserts work on
  such a table, and `UPDATE` works on a table created with `CREATE TABLE` in Spark SQL.
- Multi-writer is not supported. Only one job can write the table at a time, including table
  services.
- The Iceberg table uses format version 2.
- Hudi is the only supported writer. Any engine can read the table as Iceberg, but writes must go
  through Hudi. A snapshot that an Iceberg writer commits directly is not recorded on the Hudi
  timeline, so Hudi readers and writers do not see its data.
