---
sidebar_position: 3
---

# Unity Catalog
This document walks through the steps to register a OneTable synced Delta table in Unity Catalog on Databricks.

## Pre-requisites
1. Source table(s) (Hudi/Iceberg) already written to external storage locations like S3/GCS.
   If you don't have a source table written in S3/GCS,
   you can follow the steps in [this](https://onetable.dev/docs/hms) tutorial to set it up.
2. Setup connection to external storage locations from Databricks.
   * Follow the steps outlined [here](https://docs.databricks.com/en/storage/amazon-s3.html) for Amazon S3
   * Follow the steps outlined [here](https://docs.databricks.com/en/storage/gcs.html) for Google Cloud Storage
3. Create a Unity Catalog metastore in Databricks as outlined [here](https://docs.gcp.databricks.com/data-governance/unity-catalog/create-metastore.html#create-a-unity-catalog-metastore).
4. Create an external location in Databricks as outlined [here](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-location.html).
5. Clone the OneTable [repository](https://github.com/onetable-io/onetable) and create the
   `utilities-0.1.0-SNAPSHOT-bundled.jar` by following the steps on the [Installation page](https://onetable.dev/docs/setup)

## Steps
### Running sync
Create `my_config.yaml` in the cloned OneTable directory.

```yaml md title="yaml"
sourceFormat: HUDI|ICEBERG # choose only one
targetFormats:
  - DELTA
datasets:
  -
    tableBasePath: s3://path/to/source/data
    tableName: table_name
    partitionSpec: partitionpath:VALUE
```
:::tip Note:
Replace `s3://path/to/source/data` to `gs://path/to/source/data` if you have your source table in GCS. 
And replace with appropriate values for `sourceFormat`, and `tableName` fields. 
:::

From your terminal under the cloned OneTable directory, run the sync process using the below command.

```shell md title="shell"
java -jar utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar -datasetConfig my_config.yaml
```

:::tip Note: 
At this point, if you check your bucket path, you will be able to see `_delta_log` directory with 
00000000000000000000.json which contains the logs that helps query engines to interpret the source table as a Delta table.
:::

### Register the target table in Unity Catalog 
In your Databricks workspace, under SQL editor, run the following queries.

```sql md title="SQL"
CREATE CATALOG onetable;

CREATE SCHEMA onetable.synced_delta_schema;

CREATE TABLE onetable.synced_delta_schema.<table_name>
USING DELTA
LOCATION 's3://path/to/source/data';
```
:::tip Note:
Replace `s3://path/to/source/data` to `gs://path/to/source/data` if you have your source table in GCS.
:::

### Validating the results
You can now see the created delta table in **Unity Catalog** under **Catalog** as `<table_name>` under
`synced_delta_schema` and also query the table in the SQL editor:

```sql
SELECT * FROM onetable.synced_delta_schema.<table_name>;
```

## Conclusion
In this guide we saw how to,
1. sync a source table to create metadata for the desired target table formats using OneTable
2. catalog the data in Delta format in Unity Catalog on Databricks
3. query the Delta table using Databricks SQL editor