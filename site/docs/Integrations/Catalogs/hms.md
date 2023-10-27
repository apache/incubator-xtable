---
sidebar_position: 1
---

# Hive Metastore
This document walks through the steps to create a Onetable synced Delta table on Hive Metastore.

## Pre-requisites:
1. Hudi table(s) already written to your local storage or external storage locations like S3/GCS. 
   If you don't have the Hudi table written in place already,
   you can follow the steps in [this](https://link-to-how-to.md) tutorial to set it up.
2. A compute instance where you can run Apache Spark. This can be your local machine, docker,
   or a distributed system like Amazon EMR, Cloud Dataproc etc.
3. Clone the onetable github [repository](https://github.com/onetable-io/onetable) and create the `utilities-0.1.0-SNAPSHOT-bundled.jar` 
   by following the steps here. 
4. This guide also assumes that you have configured the Hive Metastore locally or on EMR/Cloud Dataproc
   and is already running.

## Steps:
Create `my_config.yaml` in the cloned onetable directory.
```yaml md title="yaml"
sourceFormat: HUDI
targetFormats:
  - DELTA
datasets:
  -
    tableBasePath: /path/to/trips_data
    tableName: trips_data
    partitionSpec: partitionpath:VALUE
```
:::tip Note:
Replace `/path/to/trips_data` to appropriate source data path
if you have your source table in S3/GCS i.e. `s3://path/to/trips_data` or `gs://path/to/trips_data`.
:::

From your terminal under the cloned onetable directory, run the sync process using the below command.
```shell md title="shell"
java -jar utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar -datasetConfig my_config.yaml
```

:::tip Note:
At this point, if you check your bucket path, you will be able to see `_delta_log` directory with
00000000000000000000.json which contains the log that helps query engines to interpret the data as a delta table.
:::

Now you need to register the Delta table in Hive Metastore. 
Let’s sync the Delta table to HMS using Spark client. You can also optionally use the Hive client to sync the
data in Hive Metastore.

```yaml md title="shell"
spark-sql \
--packages io.delta:delta-core_2.12:2.0.0 \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
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

In the `spark-sql` shell, you need to create a schema and table like below.

```sql md title="sql"
CREATE SCHEMA IF NOT EXISTS onetable_synced_db;

CREATE EXTERNAL TABLE onetable_synced_db.trips_data 
USING delta LOCATION '/path/to/trips_data';
```
:::tip Note:
Replace `/path/to/trips_data` to appropriate source data path
if you have your source table in S3/GCS i.e. `s3://path/to/trips_data` or `gs://path/to/trips_data`.
:::

Now you will be able to query the created table directly as a Delta table from the same `spark-sql` session or
using query engines like `Presto` and/or `Trino`.

```sql md title="sql"
SELECT * FROM onetable_synced_db.trips_data;
```

## Conclusion:
In this guide we saw how to, 
1. sync a hudi table to create delta log with Onetable 
2. catalog the data as a Delta table in Hive Metastore.
3. query the Delta table using Spark SQL