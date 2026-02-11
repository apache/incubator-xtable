---
sidebar_position: 3
title: "Unity Catalog"
---

# Syncing to Unity Catalog

This page covers **Databricks Unity Catalog** and **open-source Unity Catalog**. They are different systems:
Databricks Unity Catalog is a managed service in Databricks, while open-source Unity Catalog is a standalone server.
Both support **Delta external tables only** as Unity Catalog targets; Iceberg/Hudi are not supported as UC targets.

## Pre-requisites (for Databricks Unity Catalog)

1. Source table(s) (Hudi/Iceberg) already written to external storage locations like S3/GCS/ADLS.
   If you don't have a source table written in S3/GCS/ADLS,
   you can follow the steps in [this](/docs/hms) tutorial to set it up.
2. Setup connection to external storage locations from Databricks.
   - Follow the steps outlined [here](https://docs.databricks.com/en/storage/amazon-s3.html) for Amazon S3
   - Follow the steps outlined [here](https://docs.databricks.com/en/storage/gcs.html) for Google Cloud Storage
   - Follow the steps outlined [here](https://docs.databricks.com/en/storage/azure-storage.html) for Azure Data Lake Storage Gen2 and Blob Storage.
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
  - tableBasePath: s3://path/to/source/data
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

### Databricks Unity Catalog: manual registration (SQL)

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

### Validating the results (Databricks)

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

### Databricks Unity Catalog: built-in catalog sync (XTable)

XTable can also register the Delta table directly in Databricks Unity Catalog using the catalog
sync configuration. This uses the Databricks Java SDK and issues DDL against a SQL Warehouse.

```yaml md title="yaml"
sourceCatalog:
  catalogId: source
  catalogType: STORAGE
  catalogProperties: {}
targetCatalogs:
  - catalogId: uc
    catalogType: DATABRICKS_UC
    catalogProperties:
      externalCatalog.uc.host: https://<workspace>
      externalCatalog.uc.warehouseId: <sql-warehouse-id>
      # OAuth M2M (recommended)
      externalCatalog.uc.authType: oauth-m2m
      externalCatalog.uc.clientId: <client-id>
      externalCatalog.uc.clientSecret: <client-secret>
datasets:
  - sourceCatalogTableIdentifier:
      tableIdentifier:
        hierarchicalId: <db>.<table>
        partitionSpec: partitionpath:VALUE
      storageIdentifier:
        tableFormat: HUDI
        tableBasePath: s3://path/to/source/data
        tableDataPath: s3://path/to/source/data
        tableName: <table>
        partitionSpec: partitionpath:VALUE
        namespace: <db>
    targetCatalogTableIdentifiers:
      - catalogId: uc
        tableFormat: DELTA
        tableIdentifier:
          hierarchicalId: <catalog>.<schema>.<table>
```

### Authentication (Databricks UC)

**Supported now**

- OAuth M2M via:
  - `externalCatalog.uc.authType: oauth-m2m`
  - `externalCatalog.uc.clientId`
  - `externalCatalog.uc.clientSecret`

**Not supported yet**

- PAT/token-based auth is intentionally not wired in the current XTable UC integration.

**Possible later**

- PAT or other auth flows could be added by extending the UC config and SDK wiring,
  but they are out of scope for now.

### Implementation details (Databricks UC)

- XTable uses the Databricks SQL Statement Execution API (`StatementExecutionAPI`) to run DDL
  against a SQL Warehouse.
- The built-in sync registers **external Delta tables** only:
  - `CREATE TABLE IF NOT EXISTS <catalog>.<schema>.<table> USING DELTA LOCATION '<path>'`
- Schema evolution currently uses **drop + recreate** of the UC table metadata (not data).
  See the limitations section below.

### Schema evolution limitations (Databricks UC)

Unity Catalog does not provide a catalog-only schema evolution API for external tables.
While `ALTER TABLE ...` can update the catalog, it also assumes control over the Delta
transaction log. For external tables managed outside Databricks, this can be unsafe.

To avoid mutating the Delta log, XTable currently:

1. Detects any schema differences (new columns, dropped columns, type/comment changes).
2. Drops the UC table metadata.
3. Recreates the table at the same location.

This approach **does not delete data** and typically preserves metadata such as statistics,
usage, and lineage because Unity Catalog keeps that information even after a drop/recreate.
However, it causes a short period (seconds) where the table is not accessible.
Schema evolution is usually rare in production pipelines, so this trade-off is considered acceptable.

### Databricks UC limitations and requirements

- Unity Catalog enforces **unique external locations**. A location used by another
  table/volume cannot be reused. This means you cannot register multiple external tables
  (e.g., Iceberg/Delta via Glue federation and Databricks UC external) pointing to the same location.
- Ensure your Unity Catalog storage credential has **write** access to the Delta
  `_delta_log` directory at the table location.

## Conclusion

In this guide we saw how to,

1. sync a source table to create metadata for the desired target table formats using Apache XTable™ (Incubating)
2. catalog the data in Delta format in Unity Catalog on Databricks, and also open-source Unity Catalog
3. query the Delta table using Databricks SQL editor, and open-source Unity Catalog CLI.
