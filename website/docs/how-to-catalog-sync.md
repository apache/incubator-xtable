---
sidebar_position: 1
title: "Registering your interoperable tables across multiple catalogs"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Registering your interoperable tables across multiple catalogs

:::danger Important
Using Apache XTable™ (Incubating) to sync your source tables across multiple target catalogs involves running catalog sync on your
current dataset using a bundled jar. You can create this bundled jar by following the instructions
on the [Installation page](/docs/setup). Read through Apache XTable™'s
[GitHub page](https://github.com/apache/incubator-xtable#building-the-project-and-running-tests) for more information.
:::

In this tutorial, we’ll show you how to use Apache XTable™ (Incubating) to enable interoperability between catalogs. 
For example, you can expose a Hudi, Iceberg, or Delta table in Hive Metastore (HMS) and make it available in the AWS Glue Data Catalog—without manually registering each table. 
Additionally, Apache XTable™ (Incubating) allows you to convert the table format metadata. For instance, a Delta table in HMS can be exposed as an Iceberg table in Glue.


## Pre-requisites
1. Source table(s) (Hudi/Delta/Iceberg) already written to your local storage or external storage locations like S3/GCS/ADLS. 
   If you don't have the source table written in place already, you can follow the steps in this [tutorial](/docs/how-to#create-dataset) to set it up.
2. Clone the Apache XTable™ (Incubating) [repository](https://github.com/apache/incubator-xtable) and create the
   `xtable-utilities_2.12-0.2.0-SNAPSHOT-bundled.jar` by following the steps on the [Installation page](/docs/setup)
3. Hive Metastore is configured and running—either locally or on platforms like EMR, Dataproc, or HDInsight.
4. Setup access to interact with AWS APIs from the command line.
   If you haven’t installed AWSCLIv2, you do so by following the steps outlined in
   [AWS docs](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) and
   also set up access credentials by following the steps
   [here](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-quickstart.html)

In the next steps, we’ll walk through how to run Apache XTable™ (Incubating) locally to sync your tables from a source catalog to multiple target catalogs.

## Steps

### Running catalog sync

Create `my_config_catalog.yaml` in the cloned xtable directory.
<Tabs
groupId="source-catalog"
defaultValue="hms"
values={[
{ label: 'sourceCatalog: HMS', value: 'hms', },
{ label: 'sourceCatalog: GLUE', value: 'glue', },
{ label: 'sourceCatalog: STORAGE', value: 'storage', },
]}
>
<TabItem value="hms">

```yaml md title="yaml"
sourceCatalog:
   catalogId: "source-catalog-id"
   catalogType: "HMS"
   catalogProperties:
      # Ex: thrift://localhost:9083
      # Checkout org.apache.xtable.hms.HMSCatalogConfig for advanced configs.
      externalCatalog.hms.serverUrl: "hms-server-url"

targetCatalogs:
   - catalogId: "target-catalog-id-glue"
     catalogSyncClientImpl: "org.apache.xtable.glue.GlueCatalogSyncClient"
     catalogProperties:
        # Checkout org.apache.xtable.glue.GlueCatalogConfig for advanced configs. 
        externalCatalog.glue.region: "aws-region"

datasets:
   - sourceCatalogTableIdentifier:
        tableIdentifier:
           hierarchicalId: "db.hudi_table"
           # you only need to specify partitionSpec for HUDI sourceFormat
           partitionSpec: "cs_sold_date_sk:VALUE"
     targetCatalogTableIdentifiers:
        - catalogId: "target-catalog-id-hms"
          tableFormat: "ICEBERG"
          tableIdentifier:
             hierarchicalId: "db.iceberg_table"
        - catalogId: "target-catalog-id-hms"
          tableFormat: "DELTA"
          tableIdentifier:
             hierarchicalId: "db.delta_table"
```

</TabItem>
<TabItem value="glue">

```yaml md title="yaml"
sourceCatalog:
  catalogId: "source-catalog-id"
  catalogType: "GLUE"
  catalogProperties:
    # Checkout org.apache.xtable.glue.GlueCatalogConfig for advanced configs.
    externalCatalog.glue.region: "aws-region"

targetCatalogs:
  - catalogId: "target-catalog-id-hms"
    catalogSyncClientImpl: "org.apache.xtable.hms.HMSCatalogSyncClient"
    catalogProperties:
      # Ex: thrift://localhost:9083
      # Checkout org.apache.xtable.hms.HMSCatalogConfig for advanced configs.
      externalCatalog.hms.serverUrl: "hms-server-url"

datasets:
  - sourceCatalogTableIdentifier:
      tableIdentifier:
        hierarchicalId: "db.iceberg_table"
    targetCatalogTableIdentifiers:
      - catalogId: "target-catalog-id-hms"
        tableFormat: "DELTA"
        tableIdentifier:
          hierarchicalId: "db.delta_table"
      - catalogId: "target-catalog-id-hms"
        tableFormat: "HUDI"
        tableIdentifier:
          hierarchicalId: "db.hudi_table"
```

</TabItem>
<TabItem value="storage">

```yaml md title="yaml"
sourceCatalog:
   catalogId: "source-catalog-id"
   catalogType: "STORAGE"
   catalogProperties: {}

targetCatalogs:
   - catalogId: "target-catalog-id-glue"
     catalogSyncClientImpl: "org.apache.xtable.glue.GlueCatalogSyncClient"
     catalogProperties:
        # Checkout org.apache.xtable.glue.GlueCatalogConfig for advanced configs.
        externalCatalog.glue.region: "aws-region"

   - catalogId: "target-catalog-id-hms"
     catalogSyncClientImpl: "org.apache.xtable.hms.HMSCatalogSyncClient"
     catalogProperties:
        # Ex: thrift://localhost:9083
        # Checkout org.apache.xtable.hms.HMSCatalogConfig for advanced configs.
        externalCatalog.hms.serverUrl: "hms-server-url" 

datasets:
   - sourceCatalogTableIdentifier:
        storageIdentifier:
           tableBasePath: file:///path/to/hudi/source/data
           tableName: table_name
           # you only need to specify partitionSpec for HUDI sourceFormat
           partitionSpec: partitionpath:VALUE
           tableFormat: "HUDI"

     targetCatalogTableIdentifiers:
        - catalogId: "target-catalog-id-glue"
          tableFormat: "DELTA"
          tableIdentifier:
             hierarchicalId: "db.delta_table"

        - catalogId: "target-catalog-id-hms"
          tableFormat: "DELTA"
          tableIdentifier:
             hierarchicalId: "db.delta_table"

        - catalogId: "target-catalog-id-glue"
          tableFormat: "ICEBERG"
          tableIdentifier:
             hierarchicalId: "db.iceberg_table"

        - catalogId: "target-catalog-id-hms"
          tableFormat: "ICEBERG"
          tableIdentifier:
             hierarchicalId: "db.iceberg_table"
```

</TabItem>
</Tabs>

:::note Note:
1. `catalogId` is a user defined unique identifier for each catalog, useful if you want to sync a table to multiple glue/hms catalogs. 
2. Replace with appropriate values for `hierarchicalId`, a 2-part or 3-part tableIdentifier. 
3. For storage catalog, replace `file:///path/to/source/data` to appropriate `tableBasePath`
   if you have your source table in S3/GCS/ADLS i.e.
   * S3 - `s3://path/to/source/data`
   * GCS - `gs://path/to/source/data` or
   * ADLS - `abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/<path-to-data>`
4. For advanced configurations checkout java docs for [**GlueCatalogConfig**](https://github.com/apache/incubator-xtable/blob/main/xtable-aws/src/main/java/org/apache/xtable/glue/GlueCatalogConfig.java) and [**HMSCatalogConfig**](https://github.com/apache/incubator-xtable/blob/main/xtable-hive-metastore/src/main/java/org/apache/xtable/hms/HMSCatalogConfig.java).
:::

:::note Note:
Authentication for AWS is done with `com.amazonaws.auth.DefaultAWSCredentialsProviderChain`.
To override this setting, specify a different implementation with the `--awsCredentialsProvider` option.
:::

In your terminal under the cloned Apache XTable™ (Incubating) directory, run the below command.

```shell md title="shell"
java -cp xtable-utilities/target/xtable-utilities_2.12-0.2.0-SNAPSHOT-bundled.jar org.apache.xtable.utilities.RunCatalogSync --catalogSyncConfig my_config_catalog.yaml
```

**Optional:**
Now, if you check your target catalog, you'll see the external tables have been created and are ready to be queried.


## Conclusion
In this tutorial, we explored how to set up Apache XTable™ (Incubating) to automatically read tables from a source catalog and create interoperable external tables in target catalogs.
Once synced, these tables are immediately queryable from the target catalog without manual registration.