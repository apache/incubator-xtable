---
sidebar_position: 6
---

# Snowflake

Currently, Snowflake supports [Iceberg tables through External Tables](https://www.snowflake.com/blog/expanding-the-data-cloud-with-apache-iceberg/)
and also [Native Iceberg Tables](https://www.snowflake.com/blog/iceberg-tables-powering-open-standards-with-snowflake-innovations/).

:::danger LIMITATION:
Iceberg on Snowflake is currently supported in
[private preview](https://www.snowflake.com/guides/what-are-apache-iceberg-tables/#:~:text=Apache%20Iceberg%20is%20currently%20supported,with%20customer%2Dmanaged%20cloud%20storage)
:::

#### Steps:
1. Iceberg tables on Snowflake uses user-supplied storage. The first step to create an Iceberg table is by creating
   `EXTERNAL VOLUME` to hold the Iceberg table data and metadata.

```sql md title="SQL"
-- Create an External Volume to hold Parquet and Iceberg data
CREATE OR REPLACE EXTERNAL VOLUME my_ext_vol
STORAGE_LOCATIONS = 
(
  (
    NAME = 'my-s3-us-east-1'
    STORAGE_PROVIDER = 'S3'
    STORAGE_BASE_URL = 's3://my-s3-bucket/data/snowflake_extvol/'
    STORAGE_AWS_ROLE_ARN = '****'
  )
);
```

Once done, you can proceed with creating the table using the external volume.

```sql md title="SQL"
-- Create an Iceberg Table using my External Volume
CREATE OR REPLACE ICEBERG TABLE my_iceberg_table
WITH EXTERNAL_VOLUME = 'my_ext_vol'
AS SELECT id, date, first_name, last_name, address, region, order_number, invoice_amount FROM sales;
```

Once the table creation succeeds you can start using the Iceberg table as any other table in Snowflake.