---
sidebar_position: 1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Hive Metastore
This document walks through the steps to create a Onetable synced table on Hive Metastore.

## Pre-requisites
1. Source table(s) (Hudi/Delta/Iceberg) already written to your local storage or external storage locations like S3/GCS. 
   If you don't have the source table written in place already,
   you can follow the steps in [this](https://link-to-how-to/create-dataset.md) tutorial to set it up.
2. A compute instance where you can run Apache Spark. This can be your local machine, docker,
   or a distributed system like Amazon EMR, Cloud Dataproc etc.
3. Clone the onetable github [repository](https://github.com/onetable-io/onetable) and create the `utilities-0.1.0-SNAPSHOT-bundled.jar` 
   by following the steps here. 
4. This guide also assumes that you have configured the Hive Metastore locally or on EMR/Cloud Dataproc
   and is already running.

## Steps
### Running sync
Create `my_config.yaml` in the cloned onetable directory.

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
    tableBasePath: /path/to/source/data
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
    tableBasePath: /path/to/source/data
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
    tableBasePath: /path/to/source/data
    tableName: table_name
    partitionSpec: partitionpath:VALUE
```

</TabItem>
</Tabs>

:::tip Note:
Replace `/path/to/source/data` to appropriate source data path
if you have your source table in S3/GCS i.e. `s3:///path/to/source/data` or `gs:///path/to/source/data`.
:::

From your terminal under the cloned onetable directory, run the sync process using the below command.
```shell md title="shell"
java -jar utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar -datasetConfig my_config.yaml
```

:::tip Note:
At this point, if you check your bucket path, you will be able to see `.hoodie`, `_delta_log`, `metadata` directory with
with relevant metadata files that helps query engines to interpret the data as a hudi/delta/iceberg table.
:::

### Register the target table in Hive Metastore 
Now you need to register the synced target table in Hive Metastore. 
Let’s sync the target table to HMS using Spark client. 

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
pyspark --packages org.apache.hudi:hudi-spark3.2-bundle_2.12:0.14.0 \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.hudi.catalog.HoodieCatalog" \
--conf "spark.sql.extensions=org.apache.spark.sql.hudi.HoodieSparkSessionExtension"
```

</TabItem>
<TabItem value="delta">

```shell md title="shell"
pyspark --packages io.delta:delta-core_2.12:2.0.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog" \
--conf "spark.sql.catalogImplementation=hive"
```

</TabItem>
<TabItem value="iceberg">

```shell md title="shell"
pyspark --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.2.1 \
--conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
--conf "spark.sql.catalog.hive_prod=org.apache.iceberg.spark.SparkCatalog" \              
--conf "spark.sql.catalog.hive_prod.type=hive" \                                          
--conf "spark.sql.catalog.hive_prod.uri=thrift://localhost:9083" \
--conf "spark.sql.defaultCatalog=hive_prod"
```

</TabItem>
</Tabs>

:::tip Note:
If you instead want to write your table to Amazon S3 or Google Cloud Storage, 
your spark session will need additional configurations
* For Amazon S3, follow the configurations specified [here](https://docs.delta.io/latest/delta-storage.html#quickstart-s3-single-cluster)
* For Google Cloud Storage, follow the configurations specified [here](https://docs.delta.io/latest/delta-storage.html#requirements-gcs)
:::

In the `pyspark` shell, you need to create a schema and table like below.

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

```python md title="python"
spark.sql("CREATE SCHEMA hudi_db;")

df = spark.read.format("hudi").load("/path/to/synced/hudi/table")

df.write.format("hudi").saveAsTable("hudi_db.table_name")
```

:::tip Note:
Replace the dataset path while creating a dataframe to appropriate data path if you have your table
in S3/GCS i.e. `s3://path/to/synced/hudi/table` or `gs://path/to/synced/hudi/table`.
:::

Now you will be able to query the created table directly as an Iceberg table from the same `spark-sql` session or
using query engines like `Presto` and/or `Trino`. Check out the guides for querying the Onetable synced tables on
[Presto](https://link/to/presto) or [Trino](https://link/to/trino) query engines.

```sql md title="sql"
SELECT * FROM hudi.hudi_db.table_name;
```

</TabItem>
<TabItem value="delta">

```python md title="python"
spark.sql("CREATE SCHEMA delta_db;")

df = spark.read.format("delta").load("/path/to/synced/delta/table")

df.write.format("delta").saveAsTable("delta_db.table_name")
```

:::tip Note:
Replace the dataset path while creating a dataframe to appropriate data path if you have your table
in S3/GCS i.e. `s3://path/to/synced/delta/table` or `gs://path/to/synced/delta/table`.
:::

Now you will be able to query the created table directly as an Iceberg table from the same `spark-sql` session or
using query engines like `Presto` and/or `Trino`. Check out the guides for querying the Onetable synced tables on
[Presto](https://link/to/presto) or [Trino](https://link/to/trino) query engines.

```sql md title="sql"
SELECT * FROM delta.delta_db.table_name;
```

</TabItem>
<TabItem value="iceberg">

```python md title="python"
spark.sql("CREATE SCHEMA iceberg_db;")

df = spark.read.format("iceberg").load("/path/to/synced/iceberg/table")

df.write.format("iceberg").saveAsTable("iceberg_db.table_name")
```

:::tip Note:
Replace the dataset path while creating a dataframe to appropriate data path if you have your table
in S3/GCS i.e. `s3://path/to/synced/iceberg/table` or `gs://path/to/synced/iceberg/table`.
:::

Now you will be able to query the created table directly as an Iceberg table from the same `spark-sql` session or
using query engines like `Presto` and/or `Trino`. Check out the guides for querying the Onetable synced tables on 
[Presto](https://link/to/presto) or [Trino](https://link/to/trino) query engines.

```sql md title="sql"
SELECT * FROM iceberg.iceberg_db.table_name;
```

</TabItem>
</Tabs>

## Conclusion
In this guide we saw how to,
1. sync a source table to create metadata for target table formats with Onetable
2. catalog the data in the target table format in Hive Metastore
3. query the target table using Spark SQL
