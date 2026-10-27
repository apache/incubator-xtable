---
sidebar_position: 4
title: "Index Iceberg tables with Spark SQL"
---

# Index Iceberg tables with Spark SQL

Apache XTable™ (Incubating) can keep a secondary index on a column of an Apache Iceberg table. The index maps each value
of the column to the data files that hold it, and is stored as an Apache Hudi metadata table next to
the table's data. The XTable Spark SQL extensions let you manage the index with SQL, and they use it
automatically for point lookups:

```sql
CREATE INDEX email_idx ON prod.db.customer USING xtable (email);

-- reads only the files that hold these emails
SELECT * FROM prod.db.customer WHERE email IN ('a@example.com', 'b@example.com');
```

## Setting up Spark

The extensions need Spark 3.4 with Scala 2.12, the Iceberg Spark runtime and the Hudi Spark bundle.
Build the XTable jars from source:

```shell
./mvnw -pl xtable-spark-extensions -am package -DskipTests
```

Put these jars on the classpath of the driver and the executors. Use
`spark.driver.extraClassPath` and `spark.executor.extraClassPath` rather than `--jars`, so that
XTable and Hudi are loaded by the same class loader:

- `xtable-spark-extensions/target/xtable-spark-extensions_2.12-<version>.jar`
- `xtable-core/target/xtable-core_2.12-<version>.jar`
- `xtable-api/target/xtable-api-<version>.jar`
- `xtable-hudi-support/xtable-hudi-support-utils/target/xtable-hudi-support-utils-<version>.jar`
- `org.apache.hudi:hudi-spark3.4-bundle_2.12:1.2.1`
- `org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.9.2`

Then add the extensions after Iceberg's, and use Kryo, which Hudi needs:

```properties
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.apache.xtable.spark.extensions.XTableSparkSessionExtensions
spark.serializer=org.apache.spark.serializer.KryoSerializer
```

The extensions work with any Iceberg catalog that Spark is configured with.

## Managing indexes

```sql
-- builds the index from the current snapshot of the table
CREATE INDEX [IF NOT EXISTS] email_idx ON prod.db.customer USING xtable (email)
  [OPTIONS ('xtable.hudi.target.metadata.record.index.min.filegroup.count' = '64')];

-- syncs the index with the current snapshot of the table
REFRESH INDEX email_idx ON prod.db.customer;

DROP INDEX [IF EXISTS] email_idx ON prod.db.customer;
```

- An index covers one top level column of type `string`, `int` or `bigint`. A table can have one
  index, and its column does not change. To index another column, drop the index and create a new
  one.
- The index definition is stored in the table properties as `xtable.index.<name>.column`, and
  options as `xtable.index.<name>.option.<key>`. `SHOW TBLPROPERTIES` lists them.
- Options are passed to the Hudi target that stores the index, for example the
  `xtable.hudi.target.metadata.record.index.*` file group counts.
- The index files are stored under `.hoodie/` in the data folder of the table. Dropping the index
  deletes that folder and keeps the data files.

## Querying with an index

Queries do not change. When a filter compares an indexed column with literals, for example
`email = 'x'` or `email IN ('x', 'y')`, possibly combined with other filters by `AND`, the extensions
look the values up in the index and read only the files that hold them. The query returns the same
rows as without the index.

The index is not used when:

- the table has new snapshots that the index was not refreshed with. The query then reads the table
  as usual, so results stay correct; run `REFRESH INDEX` to use the index again.
- the query reads an older snapshot (`VERSION AS OF`, `TIMESTAMP AS OF`) or a branch.
- the scan is small. A lookup runs a Spark job, which costs more than reading a few files. The
  index is used only when Iceberg's own partition and min/max pruning leave at least
  `spark.xtable.index.pruning.minCandidateFiles` files or `spark.xtable.index.pruning.minCandidateBytes`
  bytes to read.
- the filter has more than `spark.xtable.index.pruning.maxKeys` values.

The driver log shows for each query whether the index was used and how many files it selected.

| Setting | Default | Meaning |
|---|---|---|
| `spark.xtable.index.pruning.enabled` | `true` | Use indexes for queries |
| `spark.xtable.index.pruning.minCandidateFiles` | `32` | Minimum files a scan reads before an index is used |
| `spark.xtable.index.pruning.minCandidateBytes` | `1073741824` | Minimum bytes a scan reads before an index is used |
| `spark.xtable.index.pruning.maxKeys` | `10000` | Maximum values of a filter that is looked up |

## Limitations

- Only Spark sessions with the extensions use the index. Other engines read the table as usual.
- Joins, subqueries, `MERGE`, `UPDATE` and `DELETE` do not use the index.
- Indexes are kept up to date by `REFRESH INDEX`, or by a `RunSync` job with a Hudi target whose
  `targetProperties` set `xtable.hudi.target.secondary.index.column` to the indexed column.
