---
sidebar_position: 1
title: "Amazon Athena"
---

# Querying from Amazon Athena
To read an Apache XTable™ (Incubating) synced target table (regardless of the table format) in Amazon Athena,
you can create the table either by:
* Using a DDL statement as mentioned in the following AWS docs:
    * [Example](https://docs.aws.amazon.com/athena/latest/ug/querying-hudi.html#querying-hudi-in-athena-creating-hudi-tables) for Hudi
    * [Example](https://docs.aws.amazon.com/athena/latest/ug/delta-lake-tables.html#delta-lake-tables-getting-started) for Delta Lake
    * [Example](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-creating-tables.html#querying-iceberg-creating-tables-query-editor) for Iceberg
* Or maintain the tables in Glue Data Catalog

For an end to end tutorial that walks through S3, Glue Data Catalog and Athena to query an Apache XTable™ (Incubating) synced table,
you can refer to the Apache XTable™ (Incubating) [Glue Data Catalog Guide](/docs/glue-catalog).

:::danger LIMITATION for Hudi target format:
To validate the Hudi targetFormat table results, you need to ensure that the query engine that you're using
supports Hudi version 0.14.0 as mentioned [here](/docs/features-and-limitations#hudi). 
Currently, Athena [only supports 0.12.2](https://docs.aws.amazon.com/athena/latest/ug/querying-hudi.html) 
in Athena engine version 3, so querying Hudi targetFormat tables from Athena will not work. 
:::