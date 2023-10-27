---
sidebar_position: 3
---

# Unity Catalog
This document walks through the steps to create a Onetable synced Delta table on Unity Catalog on Databricks.

## Pre-requisites
1. Hudi table(s) already written to external storage locations like S3/GCS.
   If you don't have a Hudi table written in S3/GCS,
   you can follow the steps in [this](https://link-to-how-to.md) tutorial to set it up.
2. A compute instance where you can run Apache Spark.
   This can be your local machine, docker, or a distributed system like Amazon EMR, Cloud Dataproc etc.
3. Setup connection to external storage locations from Databricks.
   * Follow the steps outlined [here](https://docs.databricks.com/en/storage/amazon-s3.html) for Amazon S3
   * Follow the steps outlined [here](https://docs.databricks.com/en/storage/gcs.html) for Google Cloud Storage
4. Create a Unity Catalog metastore in Databricks as outlined [here](https://docs.gcp.databricks.com/data-governance/unity-catalog/create-metastore.html#create-a-unity-catalog-metastore).
5. Create an external location in Databricks as outlined [here](https://github.com/onetable-io/onetable#onetable).
6. Clone the onetable github [repository](https://github.com/onetable-io/onetable) 
   and create the `utilities-0.1.0-SNAPSHOT-bundled.jar` by following the steps here.

## Steps

### Running sync
Create `my_config.yaml` in the cloned onetable directory.

```yaml md title="yaml"
sourceFormat: HUDI
targetFormats:
  - DELTA
datasets:
  -
    tableBasePath: s3://path/to/trips/data
    tableName: trips_data
    partitionSpec: partitionpath:VALUE
```
:::tip Note:
Replace `s3://path/to/trips/data` to `gs://path/to/trips/data` if you have your source table in GCS. 
:::

From your terminal under the cloned onetable directory, run the sync process using the below command.

```shell md title="shell"
java -jar utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar -datasetConfig my_config.yaml
```

:::tip Note: 
At this point, if you check your bucket path, you will be able to see `_delta_log` directory with 
00000000000000000000.json which contains the log that helps query engines to interpret the data as a delta table.
:::

### Register the target table in Unity Catalog 
In your Databricks workspace, under SQL editor, run the following queries.

```sql md title="SQL"
CREATE CATALOG onetable;

CREATE SCHEMA onetable.synced_delta_schema;

CREATE TABLE onetable.synced_delta_schema.trips_data
USING DELTA
LOCATION 's3://path/to/trips/data';
```
:::tip Note:
Replace `s3://path/to/trips/data` to `gs://path/to/trips/data` if you have your source table in GCS.
:::


You can now see the created delta table in **Unity Catalog** under **Catalog** as `trips_data` under
`synced_delta_schema`

## Conclusion
In this guide we saw how to 
1. sync a Hudi table to create Iceberg metadata with Onetable
2. catalog the data as a Delta table in Unity Catalog on Databricks
