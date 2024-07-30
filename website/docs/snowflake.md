---
sidebar_position: 7
title: "Snowflake"
---

# Querying from Snowflake

Currently, Snowflake supports [Iceberg tables through External Tables](https://www.snowflake.com/blog/expanding-the-data-cloud-with-apache-iceberg/)
and also [Native Iceberg Tables](https://www.snowflake.com/blog/iceberg-tables-powering-open-standards-with-snowflake-innovations/).

:::note NOTE:
Iceberg on Snowflake is currently supported in
[public preview](https://www.snowflake.com/blog/build-open-data-lakehouse-iceberg-tables/)
:::

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

### Create an Iceberg table from Iceberg metadata in object storage
Refer to additional [examples](https://docs.snowflake.com/LIMITEDACCESS/iceberg-2023/create-iceberg-table#examples) 
in the Snowflake Create Iceberg Table guide for more information.

```sql md title="sql"
CREATE ICEBERG TABLE myIcebergTable
EXTERNAL_VOLUME='<volume_name>'
CATALOG=<catalog_name>
METADATA_FILE_PATH='path/to/metadata/<VERSION>.metadata.json';
```

Once the table creation succeeds you can start using the Iceberg table as any other table in Snowflake.