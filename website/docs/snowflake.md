---
sidebar_position: 7
title: "Snowflake"
---

# Querying from Snowflake

Currently, Snowflake supports [Iceberg tables through External Tables](https://www.snowflake.com/blog/expanding-the-data-cloud-with-apache-iceberg/)
and also [Native Iceberg Tables](https://www.snowflake.com/blog/iceberg-tables-powering-open-standards-with-snowflake-innovations/).

## Steps:
These are high level steps to help you integrate Apache XTable™ (Incubating) synced Iceberg tables on Snowflake. For more additional information
refer to the [Getting started with Iceberg tables](https://docs.snowflake.com/LIMITEDACCESS/iceberg-2023/tables-iceberg-getting-started).

### Create an external volume
Iceberg tables on Snowflake uses user-supplied storage. The first step to create an Iceberg table is by [creating an
external volume in Snowflake](https://docs.snowflake.com/LIMITEDACCESS/iceberg-2023/tables-external-volume-s3#step-4-creating-an-external-volume-in-snowflake)
to hold the Iceberg table data and metadata

```sql md title="sql"
-- Create an External Volume to hold Parquet and Iceberg data
CREATE OR REPLACE EXTERNAL VOLUME <volume_name>
STORAGE_LOCATIONS = 
(
  (
    NAME = <'my-s3-us-west-2'>
    STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = 's3://<bucket_name>/'
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<accountId>:role/<roleName>'
  )
);
```

### Create a catalog integration for Iceberg files in object storage
You can skip this step if you are using Snowflake as the catalog. 
You can also use [AWS Glue as the catalog source](https://docs.snowflake.com/LIMITEDACCESS/iceberg-2023/create-catalog-integration#examples).

```sql md title="sql"
CREATE OR REPLACE CATALOG INTEGRATION <catalog_name>
CATALOG_SOURCE=OBJECT_STORE
TABLE_FORMAT=ICEBERG
ENABLED=TRUE;
```

### Method 1: Create an Iceberg table from Iceberg metadata in object storage
Refer to additional [examples](https://docs.snowflake.com/LIMITEDACCESS/iceberg-2023/create-iceberg-table#examples) 
in the Snowflake Create Iceberg Table guide for more information.

```sql md title="sql"
CREATE ICEBERG TABLE myIcebergTable
EXTERNAL_VOLUME='<volume_name>'
CATALOG=<catalog_name>
METADATA_FILE_PATH='path/to/metadata/<VERSION>.metadata.json';
```

Once the table creation succeeds you can start using the Iceberg table as any other table in Snowflake.

### Method 2: Using XTable APIs to sync with Snowflake Catalog directly

#### Pre-requisites:

* Build Apache XTable™ (Incubating) from [source](https://github.com/apache/incubator-xtable)
* Download `iceberg-aws-X.X.X.jar` from the [Maven repository](https://mvnrepository.com/artifact/org.apache.iceberg/iceberg-aws)
* Download `bundle-X.X.X.jar` from the [Maven repository](https://mvnrepository.com/artifact/software.amazon.awssdk/bundle)
* Download `iceberg-spark-runtime-3.X_2.12/X.X.X.jar` from [here](https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.2_2.12/1.4.2/)
* Download `snowflake-jdbc-X.X.X.jar` from the [Maven repository](https://mvnrepository.com/artifact/net.snowflake/snowflake-jdbc)

Create a `snowflake-sync-config.yaml` file:

```yaml md title="yaml"
sourceFormat: DELTA
targetFormats:
  - ICEBERG
datasets:
  -
    tableBasePath: s3://path/to/table
    tableName: <table_name>
    namespace: <db_name>.<schema_name>
```

Create a `snowflake-sync-catalog.yaml` file:

```yaml md title="yaml"
catalogImpl: org.apache.iceberg.snowflake.SnowflakeCatalog
catalogName: <catalog_name>
catalogOptions:
  io-impl: org.apache.iceberg.aws.s3.S3FileIO
  warehouse: s3://path/to/table
  uri: jdbc:snowflake://<account-identifier>.snowflakecomputing.com
  jdbc.user: <snowflake-username>
  jdbc.password: <snowflake-password>
```

Sample command to sync the table with Snowflake:
```shell md title="shell"
java -cp /path/to/iceberg-spark-runtime-3.2_2.12-1.4.2.jar:/path/to/xtable-utilities-0.2.0-SNAPSHOT-bundled.jar:/path/to/snowflake-jdbc-3.13.28.jar:/path/to/iceberg-aws-1.4.2.jar:/Users/sagarl/Downloads/bundle-2.23.9.jar org.apache.xtable.utilities.RunSync  --datasetConfig snowflake-sync-config.yaml --icebergCatalogConfig snowflake-sync-catalog.yaml
```
