---
sidebar_position: 4
title: "Index Iceberg tables"
---

# Index Iceberg tables

Apache XTable™ (Incubating) can keep a secondary index on a column of an Apache Iceberg table. The
index maps each value of the column to the data files that hold it, and is stored as an Apache Hudi
metadata table under `.hoodie/` in the data folder of the table. Using an index takes two parts:

1. An XTable sync job [builds the index and keeps it current](#building-the-index-with-a-sync-job).
2. Spark sessions with the XTable Spark SQL extensions [use the index](#querying-with-the-index) for
   point lookups, in SQL queries and DataFrame reads alike. Queries do not change.

## Building the index with a sync job

Any XTable sync of the table builds the index when it has a Hudi target with the index settings:
`RunSync` from the utilities jar, `XTableSparkSync` from the
[Spark runtime jar](how-to-spark-runtime), or a Spark job that calls the XTable sync API. Add the
Hudi target to the `--datasetconfig` of the job:

```yaml
sourceFormat: ICEBERG
targetFormats:
  - HUDI
datasets:
  - tableBasePath: s3://bucket/warehouse/db.db/customer
    tableDataPath: s3://bucket/warehouse/db.db/customer/data
    tableName: customer
    namespace: db
    targetProperties:
      HUDI:
        xtable.hudi.target.table_version: '9'
        xtable.hudi.target.execution.engine: spark
        xtable.hudi.target.secondary.index.column: c_email_address
```

The first sync builds the index from the current snapshot of the table, and each later sync adds
the changes of the new snapshots.

- An index covers one top level column of type `string`, `int` or `bigint`, and its column does not
  change. A sync with another column fails before it commits.
- Other `xtable.hudi.target.*` settings tune the index, for example the
  `xtable.hudi.target.metadata.record.index.*` file group counts.
- To remove the index, remove the Hudi target from the sync job and delete the `.hoodie/` folder in
  the data folder of the table. The data files of the table are not changed. A later sync with
  another column builds a new index.

## Querying with the index

### Setting up Spark

The extensions need Spark 3.x with Scala 2.12, the Iceberg Spark runtime and the Hudi Spark bundle.
They ship in the [XTable Spark runtime jar](how-to-spark-runtime), so no other XTable jar is needed.
Until a release includes them, build the jar from source:

```shell
./mvnw -pl xtable-spark-runtime -am package -DskipTests
```

Put these jars on the classpath of the driver and the executors. Use
`spark.driver.extraClassPath` and `spark.executor.extraClassPath` rather than `--jars`, so that
XTable and Hudi are loaded by the same class loader. The versions below are for Spark 3.5; use the
bundles that match your Spark version.

- `xtable-spark-runtime/target/xtable-spark-runtime_2.12-<version>.jar`
- `org.apache.hudi:hudi-spark3.5-bundle_2.12:1.2.1`
- `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2`

`spark.sql.extensions` must list the Iceberg extensions first and the XTable extensions second,
and Hudi needs the Kryo serializer:

```properties
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.xtable.spark.extensions.XTableSparkSessionExtensions
spark.serializer=org.apache.spark.serializer.KryoSerializer
```

The extensions work with any Iceberg catalog that Spark is configured with.

### Running queries

The extensions find the index by themselves: when a query filters an Iceberg table, they look for
the Hudi metadata in the data folder of the table. When a filter compares an indexed column with
literals, for example `email = 'x'` or `email IN ('x', 'y')`, possibly combined with other filters
by `AND`, the extensions look the values up in the index and read only the files that hold them.
The query returns the same rows as without the index.

```sql
SELECT * FROM prod.db.customer WHERE c_email_address IN ('a@example.com', 'b@example.com');
```

DataFrame reads use the index the same way:

```python
from pyspark.sql.functions import col

spark.table("prod.db.customer").where(col("c_email_address") == "a@example.com")
spark.read.format("iceberg").load("prod.db.customer").where(col("c_email_address") == "a@example.com")
```

The driver log shows for each query whether the index was used and how many files it selected.

### When the index is not used

The query reads the table as usual, so results stay correct, when:

- the index was not synced with the current snapshot of the table. The driver log has a warning
  that names the snapshot of the index and of the table; run the sync job to use the index again.
- a sync, a rollback or a rebuild changes the index while the query looks keys up in it.
- a schema of the table gives the name of the indexed column to another field, renames the
  indexed field or changes its type. The driver log has a warning. Old schemas stay in the table
  metadata, so a rebuild of the index does not make it usable again.
- the query reads an older snapshot (`VERSION AS OF`, `TIMESTAMP AS OF`, `snapshot-id`,
  `as-of-timestamp`), a branch, a tag, or the changes between snapshots (`start-snapshot-id`,
  `end-snapshot-id`).
- the scan is small. A lookup runs a Spark job, which costs more than reading a few files. The
  index is used only when Iceberg's own partition and min/max pruning leave at least
  `spark.xtable.index.pruning.minCandidateFiles` files or `spark.xtable.index.pruning.minCandidateBytes`
  bytes to read.
- the filter has more than `spark.xtable.index.pruning.maxKeys` values.

### Settings

| Setting | Default | Meaning |
|---|---|---|
| `spark.xtable.index.pruning.enabled` | `true` | Use indexes for queries |
| `spark.xtable.index.pruning.minCandidateFiles` | `32` | Minimum files a scan reads before an index is used |
| `spark.xtable.index.pruning.minCandidateBytes` | `1073741824` | Minimum bytes a scan reads before an index is used |
| `spark.xtable.index.pruning.maxKeys` | `10000` | Maximum values of a filter that is looked up |

To read tables without their index, for example while an index is repaired, turn the index off for
a session without removing the jar:

```sql
SET spark.xtable.index.pruning.enabled=false;
```

## Limitations

- Only Spark sessions with the extensions use the index. Other engines read the table as usual.
- Joins, subqueries, `MERGE`, `UPDATE` and `DELETE` do not use the index.
