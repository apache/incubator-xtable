---
sidebar_position: 3
title: "Unity Catalog"
---

# Syncing to Unity Catalog
This document walks through the steps to register an Apache XTable™ (Incubating) synced Delta table in Unity Catalog on Databricks and open-source Unity Catalog.

## Pre-requisites (for Databricks Unity Catalog)
1. Source table(s) (Hudi/Iceberg) already written to external storage locations like S3/GCS/ADLS.
   If you don't have a source table written in S3/GCS/ADLS,
   you can follow the steps in [this](/docs/hms) tutorial to set it up.
2. Setup connection to external storage locations from Databricks.
   * Follow the steps outlined [here](https://docs.databricks.com/en/storage/amazon-s3.html) for Amazon S3
   * Follow the steps outlined [here](https://docs.databricks.com/en/storage/gcs.html) for Google Cloud Storage
   * Follow the steps outlined [here](https://docs.databricks.com/en/storage/azure-storage.html) for Azure Data Lake Storage Gen2 and Blob Storage.
3. Create a Unity Catalog metastore in Databricks as outlined [here](https://docs.gcp.databricks.com/data-governance/unity-catalog/create-metastore.html#create-a-unity-catalog-metastore).
4. Create an external location in Databricks as outlined [here](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-location.html).
5. Clone the Apache XTable™ (Incubating) [repository](https://github.com/apache/incubator-xtable) and create the
   `xtable-utilities_2.12-0.2.0-SNAPSHOT-bundled.jar` by following the steps on the [Installation page](/docs/setup)

## Pre-requisites (for open-source Unity Catalog)
1. Source table(s) (Hudi/Iceberg) already written to external storage locations like S3/GCS/ADLS or local.
   In this guide, we will use the local file system. 
   But for S3/GCS/ADLS, you must add additional properties related to the respective cloud object storage system you're working with as mentioned [here](https://github.com/unitycatalog/unitycatalog/blob/main/docs/server.md)
2. Clone the Unity Catalog repository from [here](https://github.com/unitycatalog/unitycatalog) and build the project by following the steps outlined [here](https://github.com/unitycatalog/unitycatalog?tab=readme-ov-file#prerequisites)

## Steps
### Running sync
Create `my_config.yaml` in the cloned Apache XTable™ (Incubating) directory.

```yaml md title="yaml"
sourceFormat: HUDI|ICEBERG # choose only one
targetFormats:
  - DELTA
datasets:
  -
    tableBasePath: s3://path/to/source/data
    tableName: table_name
    partitionSpec: partitionpath:VALUE # you only need to specify partitionSpec for HUDI sourceFormat
```
:::note Note:
1. Replace `s3://path/to/source/data` to `gs://path/to/source/data` if you have your source table in GCS
   and `abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>` if you have your source table in ADLS.
2. And replace with appropriate values for `sourceFormat`, and `tableName` fields. 
:::

From your terminal under the cloned Apache XTable™ (Incubating) directory, run the sync process using the below command.

```shell md title="shell"
java -jar xtable-utilities/target/xtable-utilities_2.12-0.2.0-SNAPSHOT-bundled.jar --datasetConfig my_config.yaml
```

:::tip Note: 
At this point, if you check your bucket path, you will be able to see `_delta_log` directory with 
00000000000000000000.json which contains the logs that helps query engines to interpret the source table as a Delta table.
:::

### Register the target table in Databricks Unity Catalog
(After making sure you complete the pre-requisites mentioned for Databricks Unity Catalog above) In your Databricks workspace, under SQL editor, run the following queries.

```sql md title="SQL"
CREATE CATALOG xtable;

CREATE SCHEMA xtable.synced_delta_schema;

CREATE TABLE xtable.synced_delta_schema.<table_name>
USING DELTA
LOCATION 's3://path/to/source/data';
```
:::note Note:
Replace `s3://path/to/source/data` to `gs://path/to/source/data` if you have your source table in GCS
and `abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>` if you have your source table in ADLS.
:::

### Validating the results
You can now see the created delta table in **Unity Catalog** under **Catalog** as `<table_name>` under
`synced_delta_schema` and also query the table in the SQL editor:

```sql
SELECT * FROM xtable.synced_delta_schema.<table_name>;
```

### Register the target table in open-source Unity Catalog using the CLI
(After making sure you complete the pre-requisites mentioned for open-source Unity Catalog above) In your terminal start the UC server by following the steps outlined [here](https://github.com/unitycatalog/unitycatalog/tree/main?tab=readme-ov-file#quickstart---hello-uc)

In a different terminal, run the following commands to register the target table in Unity Catalog.

```shell md title="shell"
bin/uc table create --full_name unity.default.people --columns "id INT, name STRING, age INT, city STRING, create_ts STRING" --storage_location /tmp/delta-dataset/people
```

### Validating the results
You can now read the table registered in Unity Catalog using the below command.

```shell md title="shell"
bin/uc table read --full_name unity.default.people
```

## Conclusion
In this guide we saw how to,
1. sync a source table to create metadata for the desired target table formats using Apache XTable™ (Incubating)
2. catalog the data in Delta format in Unity Catalog on Databricks, and also open-source Unity Catalog
3. query the Delta table using Databricks SQL editor, and open-source Unity Catalog CLI.
