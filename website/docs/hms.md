---
sidebar_position: 1
title: "Hive Metastore"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Syncing to Hive Metastore
This document walks through the steps to register an Apache XTable™ (Incubating) synced table on Hive Metastore (HMS).

## Pre-requisites
1. Source table(s) (Hudi/Delta/Iceberg) already written to your local storage or external storage locations like S3/GCS/ADLS. 
   If you don't have the source table written in place already,
   you can follow the steps in [this](/docs/how-to#create-dataset) tutorial to set it up.
2. A compute instance where you can run Apache Spark. This can be your local machine, docker,
   or a distributed system like Amazon EMR, Google Cloud's Dataproc, Azure HDInsight etc.
   This is a required step to register the table in HMS using a Spark client.
3. Clone the XTable™ (Incubating) [repository](https://github.com/apache/incubator-xtable) and create the
   `utilities-0.1.0-SNAPSHOT-bundled.jar` by following the steps on the [Installation page](/docs/setup) 
4. This guide also assumes that you have configured the Hive Metastore locally or on EMR/Dataproc/HDInsight
   and is already running.

## Steps
### Running sync
Create `my_config.yaml` in the cloned Apache XTable™ (Incubating) directory.

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
    partitionSpec: partitionpath:VALUE # you only need to specify partitionSpec for HUDI sourceFormat
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
    partitionSpec: partitionpath:VALUE # you only need to specify partitionSpec for HUDI sourceFormat
```

</TabItem>
</Tabs>

:::note Note:
1. Replace with appropriate values for `sourceFormat`, `tableBasePath` and `tableName` fields.
2. Replace `file:///path/to/source/data` to appropriate source data path
   if you have your source table in S3/GCS/ADLS i.e. 
    * S3 - `s3://path/to/source/data` 
    * GCS - `gs://path/to/source/data` or
    * ADLS - `abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>`
:::

From your terminal under the cloned Apache XTable™ (Incubating) directory, run the sync process using the below command.
```shell md title="shell"
java -jar utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar --datasetConfig my_config.yaml
```

:::tip Note:
At this point, if you check your bucket path, you will be able to see `.hoodie` or `_delta_log` or `metadata`
directory with relevant metadata files that helps query engines to interpret the data as a Hudi/Delta/Iceberg table.
:::

### Register the target table in Hive Metastore 
Now you need to register the Apache XTable™ (Incubating) synced target table in Hive Metastore.  

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

:::note Note:
Replace `file:///path/to/source/data` to appropriate source data path
if you have your source table in S3/GCS/ADLS i.e.
* S3 - `s3://path/to/source/data`
* GCS - `gs://path/to/source/data` or
* ADLS - `abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>`
:::


Now you will be able to query the created table directly as a Hudi table from the same `spark` session or
using query engines like `Presto` and/or `Trino`. Check out the guides for querying the Apache XTable™ (Incubating) synced tables on
[Presto](/docs/presto) or [Trino](/docs/trino) query engines for more information.

```sql md title="sql"
SELECT * FROM <database_name>.<table_name>;
```

</TabItem>
<TabItem value="delta">

```shell md title="shell"
spark-sql --packages io.delta:delta-core_2.12:2.0.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--conf "spark.sql.catalogImplementation=hive"
```

In the `spark-sql` shell, you need to create a schema and table like below.

```sql md title="sql"
CREATE SCHEMA delta_db;

CREATE TABLE delta_db.<table_name> USING DELTA LOCATION '/path/to/synced/delta/table';
```

:::note Note:
Replace `file:///path/to/source/data` to appropriate source data path
if you have your source table in S3/GCS/ADLS i.e. 
* S3 - `s3://path/to/source/data`
* GCS - `gs://path/to/source/data` or
* ADLS - `abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>`
:::

Now you will be able to query the created table directly as a Delta table from the same `spark` session or
using query engines like `Presto` and/or `Trino`. Check out the guides for querying the Apache XTable™ (Incubating) synced tables on
[Presto](/docs/presto) or [Trino](/docs/trino) query engines for more information.

```sql md title="sql"
SELECT * FROM delta_db.<table_name>;
```

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

In the `spark-sql` shell, you need to create a schema and table like below.

```sql md title="sql"
CREATE SCHEMA iceberg_db;

CALL hive_prod.system.register_table(
   table => 'hive_prod.iceberg_db.<table_name>',
   metadata_file => '/path/to/synced/iceberg/table/metadata/<VERSION>.metadata.json'
);

```

:::note Note:
Replace the dataset path while creating a dataframe to appropriate data path if you have your table
in S3/GCS/ADLS i.e. 
* S3 - `s3://path/to/source/data`
* GCS - `gs://path/to/source/data` or
* ADLS - `abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>`
:::

Now you will be able to query the created table directly as an Iceberg table from the same `spark` session or
using query engines like `Presto` and/or `Trino`. Check out the guides for querying the Apache XTable™ (Incubating) synced tables on
[Presto](/docs/presto) or [Trino](/docs/trino) query engines for more information.

```sql md title="sql"
SELECT * FROM iceberg_db.<table_name>;
```

</TabItem>
</Tabs>

## Conclusion
In this guide we saw how to,
1. sync a source table to create metadata for the desired target table formats using Apache XTable™ (Incubating) 
2. catalog the data in the target table format in Hive Metastore
3. query the target table using Spark
