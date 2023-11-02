---
sidebar_position: 1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Hive Metastore
This document walks through the steps to register a Onetable synced table on Hive Metastore (HMS).

## Pre-requisites
1. Source table(s) (Hudi/Delta/Iceberg) already written to your local storage or external storage locations like S3/GCS. 
   If you don't have the source table written in place already,
   you can follow the steps in [this](https://onetable.dev/docs/how-to#create-dataset) tutorial to set it up.
2. A compute instance where you can run Apache Spark. This can be your local machine, docker,
   or a distributed system like Amazon EMR, Cloud Dataproc etc.
   This is a required step to register the table in HMS using a Spark client.
3. Clone the Onetable [repository](https://github.com/onetable-io/onetable) and create the
   `utilities-0.1.0-SNAPSHOT-bundled.jar` by following the steps on the [Installation page](https://onetable.dev/docs/setup) 
4. This guide also assumes that you have configured the Hive Metastore locally or on EMR/Cloud Dataproc
   and is already running.

## Steps
### Running sync
Create `my_config.yaml` in the cloned Onetable directory.

<Tabs
groupId="table-format"
defaultValue="hudi"
values={[
{ label: 'targetFormat: HUDI', value: 'hudi', },
{ label: 'targetFormat: DELTA', value: 'delta', },
{ label: 'targetFormat: ICEBERG', value: 'iceberg', },
]}
>
<TabItem value="hudi">

```yaml md title="yaml"
sourceFormat: DELTA|ICEBERG # choose only one
targetFormats:
  - HUDI
datasets:
  -
    tableBasePath: file:///path/to/source/data
    tableName: table_name
    partitionSpec: partitionpath:VALUE
```

</TabItem>
<TabItem value="delta">

```yaml md title="yaml"
sourceFormat: HUDI|ICEBERG # choose only one
targetFormats:
  - DELTA
datasets:
  -
    tableBasePath: file:///path/to/source/data
    tableName: table_name
    partitionSpec: partitionpath:VALUE
```

</TabItem>
<TabItem value="iceberg">

```yaml md title="yaml"
sourceFormat: HUDI|DELTA # choose only one
targetFormats:
  - ICEBERG
datasets:
  -
    tableBasePath: file:///path/to/source/data
    tableName: table_name
    partitionSpec: partitionpath:VALUE
```

</TabItem>
</Tabs>

:::danger Note:
1. Replace with appropriate values for `sourceFormat`, `tableBasePath` and `tableName` fields
2. If your running sync in your local file system, you should use `file:///path/to/source/data` and not `/path/to/source/data`
:::

:::tip Note:
Replace `file:///path/to/source/data` to appropriate source data path
if you have your source table in S3/GCS i.e. `s3://path/to/source/data` or `gs://path/to/source/data`.
:::

From your terminal under the cloned Onetable directory, run the sync process using the below command.
```shell md title="shell"
java -jar utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar -datasetConfig my_config.yaml
```

:::tip Note:
At this point, if you check your bucket path, you will be able to see `.hoodie`/`_delta_log`/`metadata` directory with
relevant metadata files that helps query engines to interpret the data as a Hudi/Delta/Iceberg table.
:::

### Register the target table in Hive Metastore 
Now you need to register the Onetable synced target table in Hive Metastore.  

<Tabs
groupId="table-format"
defaultValue="hudi"
values={[
{ label: 'targetFormat: HUDI', value: 'hudi', },
{ label: 'targetFormat: DELTA', value: 'delta', },
{ label: 'targetFormat: ICEBERG', value: 'iceberg', },
]}
>
<TabItem value="hudi">

A Hudi table can directly be synced to the Hive Metastore using Hive Sync Tool 
and subsequently be queried by different query engines. For more information on the Hive Sync Tool, check 
[Hudi Hive Metastore](https://hudi.apache.org/docs/syncing_metastore) docs.

</TabItem>
<TabItem value="delta">

```shell md title="shell"
spark-sql --packages io.delta:delta-core_2.12:2.0.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--conf "spark.sql.catalogImplementation=hive"
```

:::tip Note:
If you instead want to write your table to Amazon S3 or Google Cloud Storage,
your spark session will need additional configurations
* For Amazon S3, follow the configurations specified [here](https://docs.delta.io/latest/delta-storage.html#quickstart-s3-single-cluster)
* For Google Cloud Storage, follow the configurations specified [here](https://docs.delta.io/latest/delta-storage.html#requirements-gcs)
:::

</TabItem>
<TabItem value="iceberg">

```shell md title="shell"
spark-sql --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.2.1 \
--conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
--conf "spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog" \
--conf "spark.sql.catalog.spark_catalog.type=hive" \
--conf "spark.sql.catalog.hive_prod=org.apache.iceberg.spark.SparkCatalog" \
--conf "spark.sql.catalog.hive_prod.type=hive"
```

:::tip Note:
If you instead want to write your table to Amazon S3 or Google Cloud Storage,
your spark session will need additional configurations
* For Amazon S3, follow the configurations specified [here](https://docs.delta.io/latest/delta-storage.html#quickstart-s3-single-cluster)
* For Google Cloud Storage, follow the configurations specified [here](https://docs.delta.io/latest/delta-storage.html#requirements-gcs)
:::

</TabItem>
</Tabs>

<Tabs
groupId="table-format"
defaultValue="hudi"
values={[
{ label: 'targetFormat: HUDI', value: 'hudi', },
{ label: 'targetFormat: DELTA', value: 'delta', },
{ label: 'targetFormat: ICEBERG', value: 'iceberg', },
]}
>
<TabItem value="hudi">

```shell md title="shell"
cd $HUDI_HOME/hudi-sync/hudi-hive-sync

./run_sync_tool.sh  \
--jdbc-url <jdbc_url> \
--user <username> \
--pass <password> \
--partitioned-by <partition_field> \
--base-path <'/path/to/synced/hudi/table'> \
--database <database_name> \
--table <tableName>
```

:::tip Note:
Replace the dataset path while creating a dataframe to appropriate data path if you have your table
in S3/GCS i.e. `s3://path/to/synced/hudi/table` or `gs://path/to/synced/hudi/table`.
:::

Now you will be able to query the created table directly as a Hudi table from the same `spark` session or
using query engines like `Presto` and/or `Trino`. Check out the guides for querying the Onetable synced tables on
[Presto](https://link/to/presto) or [Trino](https://link/to/trino) query engines for more information.

```sql md title="sql"
SELECT * FROM <database_name>.<table_name>;
```

</TabItem>
<TabItem value="delta">

In the `spark-sql` shell, you need to create a schema and table like below.

```sql md title="sql"
CREATE SCHEMA delta_db;

CREATE TABLE delta_db.<table_name> USING DELTA LOCATION '/path/to/synced/delta/table';
```

:::tip Note:
Replace the dataset path while creating a dataframe to appropriate data path if you have your table
in S3/GCS i.e. `s3://path/to/synced/delta/table` or `gs://path/to/synced/delta/table`.
:::

Now you will be able to query the created table directly as a Delta table from the same `spark` session or
using query engines like `Presto` and/or `Trino`. Check out the guides for querying the Onetable synced tables on
[Presto](https://link/to/presto) or [Trino](https://link/to/trino) query engines for more information.

```sql md title="sql"
SELECT * FROM delta_db.<table_name>;
```

</TabItem>
<TabItem value="iceberg">

In the `spark-sql` shell, you need to create a schema and table like below.

```sql md title="sql"
CREATE SCHEMA iceberg_db;

CALL hive_prod.system.register_table(
   table => 'hive_prod.iceberg_db.<table_name>',
   metadata_file => '/path/to/synced/iceberg/table/metadata/*.metadata.json'
);

```

:::tip Note:
Replace the dataset path while creating a dataframe to appropriate data path if you have your table
in S3/GCS i.e. `s3://path/to/synced/iceberg/table` or `gs://path/to/synced/iceberg/table` and `*.metadata.json` 
with the appropriate metadata file.
:::

Now you will be able to query the created table directly as an Iceberg table from the same `spark` session or
using query engines like `Presto` and/or `Trino`. Check out the guides for querying the Onetable synced tables on 
[Presto](https://onetable.dev/docs/presto) or [Trino](https://onetable.dev/docs/trino) query engines for more information.

```sql md title="sql"
SELECT * FROM iceberg_db.<table_name>;
```

</TabItem>
</Tabs>

## Conclusion
In this guide we saw how to,
1. sync a source table to create metadata for the desired target table formats using Onetable
2. catalog the data in the target table format in Hive Metastore
3. query the target table using Spark
