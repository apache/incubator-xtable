---
sidebar_position: 4
---

# BigLake Metastore
This document walks through the steps to create a Onetable synced Iceberg table in BigLake Metastore on GCP.

## Pre-requisites
1. Hudi table(s) already written to Google Cloud Storage.
   If you don't have a Hudi table written in GCS,
   you can follow the steps in [this](https://link-to-how-to.md) tutorial to set it up.
2. To ensure that the BigLake API's caller (your service account used by Onetable) has the
   necessary permissions to create a BigLake table, ask your administrator to grant the service account
   the following IAM roles on the project:
   * [BigLake Admin](https://cloud.google.com/iam/docs/understanding-roles#biglake.admin) (roles/bigquery.admin)
3. To ensure that the Storage Account API's caller (your service account used by Onetable) has the
   necessary permissions to write log/metadata files in GCS, ask your administrator to grant the service account
   the following IAM roles on the project:
   * [Storage Object User](https://cloud.google.com/storage/docs/access-control/iam-roles) 
   (roles/storage.objectUser)
4. If you're running Onetable outside of GCP, you need to provide the machine access to interact with BigLake and GCS.
   To do so, store the permissions key for your service account in your machine using 
   ```shell
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service_account_key.json
   ```
5. Clone the onetable github [repository](https://github.com/onetable-io/onetable).

## Steps
:::danger Important:
Currently BigLake Metastore is only accessible through Google's 
[BigLake Rest APIs](https://cloud.google.com/bigquery/docs/reference/biglake/rest), and as such
Onetable requires you to setup the below items prior to running sync on your source dataset.
   * BigLake Catalog
   * BigLake Database
:::

### Create BigLake Catalog
Use the `Try this method` on Google's REST reference docs for
[`projects.locations.catalogs.create`](https://cloud.google.com/bigquery/docs/reference/biglake/rest/v1/projects.locations.catalogs/create)
method to create a catalog.

In this tutorial we'll use `us-west1` region.
```rest md title="parent"
projects/<yourProjectName>/locations/us-west1/catalogs
```
```rest md title="catalogId"
onetable
```

### Create BigLake Database
Use the `Try this method` on Google's REST reference docs for
[projects.locations.catalogs.databases.create](https://cloud.google.com/bigquery/docs/reference/biglake/rest/v1/projects.locations.catalogs/create)
page for [`projects.locations.catalogs.create`] method to create a catalog.
```rest md title="parent"
projects/<yourProjectName>/locations/us-west1/catalogs/onetable/databases
```
```rest md title="databaseId"
onetable_synced_db
```

:::danger Limitation: 
The current implementation of Onetable requires you to set the below options in the
[Java object](https://github.com/onetable-io/onetable/blob/47806329fddba55a15c6af317b9f323bd0147f46/core/src/main/java/io/onetable/iceberg/IcebergClient.java#L78C3-L78C16).

  * `catalogImpl`
  * `catalogName`
  * `gcp_project`
  * `gcp_location`
  * `warehouse`

Then proceed to creating the `utilities-0.1.0-SNAPSHOT-bundled.jar` by following the steps
[here](https://github.com/onetable-io/onetable#building-the-project-and-running-tests)
:::

:::tip
The improvement to let users pass these options through the `my_config.yaml` 
is being tracked under [this](https://github.com/onetable-io/onetable/issues/107) github issue.
:::

### Running sync

```yaml md title="yaml"
sourceFormat: HUDI
targetFormats:
  - ICEBERG
datasets:
  -
    tableBasePath: gs://path/to/trips_data
    tableName: trips_data
    namespace: onetable_synced_db
    partitionSpec: partitionpath:VALUE
```

From your terminal under the cloned onetable directory, run the sync process using the below command.

```shell md title="shell"
java -jar utilities/target/utilities-0.1.0-SNAPSHOT-bundled.jar -datasetConfig my_config.yaml
```

:::tip Note:
At this point, if you check your bucket path, you will be able to see the `metadata` directory
with metadata files which contains the information that helps query engines
to interpret the data as an iceberg table.
:::

### Validating the results
Once the sync succeeds, Onetable would have written the table directly to BigLake Metastore.
We can use `Try this method` option on Google's REST reference docs for
[`projects.locations.catalogs.databases.tables.get`](https://cloud.google.com/bigquery/docs/reference/biglake/rest/v1/projects.locations.catalogs.databases.tables/get)
method to view the created table.
```rest md title="name"
projects/<yourProjectName>/locations/us-west1/catalogs/onetable/databases/onetable_synced_db/tables/trips_data
```

## Conclusion
In this guide we saw how to,
1. sync a hudi table to create Iceberg metadata with Onetable
2. catalog the data as an Iceberg table in BigLake Metastore
3. validate the table creation using `projects.locations.catalogs.databases.tables.get` method
