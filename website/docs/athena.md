---
sidebar_position: 1
---

# Amazon Athena
To read a OneTable synced target table (regardless of the table format) in Amazon Athena,
you can create the table either by:
* Using a DDL statement as mentioned in the following AWS docs:
    * [Example](https://docs.aws.amazon.com/athena/latest/ug/querying-hudi.html#querying-hudi-in-athena-creating-hudi-tables) for Hudi
    * [Example](https://docs.aws.amazon.com/athena/latest/ug/delta-lake-tables.html#delta-lake-tables-getting-started) for Delta Lake
    * [Example](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html#querying-iceberg-creating-tables-query-editor) for Iceberg
* Or maintain the tables in Glue Data Catalog

For an end to end tutorial that walks through S3, Glue Data Catalog and Athena to query a OneTable synced table,
you can refer to the OneTable [Glue Data Catalog guide](https://onetable.dev/docs/glue-catalog).