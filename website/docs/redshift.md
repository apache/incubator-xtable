---
sidebar_position: 2
title: "Amazon Redshift Spectrum"
---

# Querying from Redshift Spectrum
To read an Apache XTable™ (Incubating) synced target table (regardless of the table format) in Amazon Redshift,
users have to create an external schema and refer to the external data catalog that contains the table.
Redshift infers the table's schema and format from the external catalog/database directly.
For more information on creating external schemas, refer to
[Redshift docs](https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-external-schemas.html#c-spectrum-external-catalogs).

### Hudi and Iceberg tables
The following query creates an external schema `xtable_synced_schema` using the Glue database `glue_database_name`

```sql md title="sql"
CREATE EXTERNAL SCHEMA xtable_synced_schema
FROM DATA CATALOG
DATABASE <glue_database_name>
IAM_ROLE 'arn:aws:iam::<accountId>:role/<roleName>'
CREATE EXTERNAL DATABASE IF NOT EXISTS;
```

:::danger Note:
The IAM role needs to have minimum access to Amazon S3 and AWS Glue Data Catalog. For more information refer to
[AWS docs](https://docs.aws.amazon.com/redshift/latest/dg/c-spectrum-iam-policies.html#spectrum-iam-policies-s3).
:::

Redshift can infer the tables present in the Glue database automatically. You can then query the tables using:

```sql md title="sql"
SELECT *
FROM xtable_synced_schema.<table_name>;
```

### Delta Lake table
For Delta Lake, steps slightly vary because Redshift Spectrum relies on Delta Lake's manifest file - a text
file containing the list of data files to read for querying a Delta table.

You have two options to create and query Delta tables in Redshift Spectrum:
1. Follow the steps in
   [this](https://docs.delta.io/latest/redshift-spectrum-integration.html#set-up-a-redshift-spectrum-to-delta-lake-integration-and-query-delta-tables) 
   article to set up a Redshift Spectrum to Delta Lake integration and query Delta tables directly from Amazon S3.
2. While creating the Glue Crawler to crawl the Apache XTable™ (Incubating) synced Delta table, choose the `Create Symlink tables`
   option in `Add data source` pop-up window. This will add `_symlink_format_manifest` folder with manifest files in the table
   root path.

You can then use a similar approach to query the Hudi and Iceberg tables mentioned above.

```sql md title="sql"
CREATE EXTERNAL SCHEMA xtable_synced_schema_delta
FROM DATA CATALOG
DATABASE <delta_glue_database_name>
IAM_ROLE 'arn:aws:iam::<accountId>:role/<roleName>'
CREATE EXTERNAL DATABASE IF NOT EXISTS;
```

```sql md title="sql"
SELECT *
FROM xtable_synced_schema_delta.<table_name>;
```
