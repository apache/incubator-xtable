---
sidebar_position: 4
---

# Google BigQuery

### Iceberg tables
To read a Onetable synced [Iceberg table from BigQuery](https://cloud.google.com/bigquery/docs/iceberg-tables),
you have two options:

#### [Using Iceberg JSON metadata file to create the Iceberg BigLake tables](https://cloud.google.com/bigquery/docs/iceberg-tables#create-using-metadata-file):
Onetable outputs metadata files for Iceberg target format syncs which can be used by BigQuery
to read the BigLake tables.

```sql md title="sql"
CREATE EXTERNAL TABLE onetable_synced_iceberg_table
WITH CONNECTION `myproject.mylocation.myconnection`
OPTIONS (
     format = 'ICEBERG',
     uris = ["gs://mybucket/mydata/mytable/metadata/iceberg.metadata.json"]
 )
```
:::danger Note:
This method requires you to keep the table updated when there are table updates and hence Google
[does not recommend]((https://cloud.google.com/bigquery/docs/iceberg-tables#create-using-metadata-file)) this approach.
:::

:::danger Important: For Hudi source format to Iceberg target format use cases
The Hudi extensions provide the ability to add field IDs to the parquet schema when writing with Hudi.
This is a requirement for some engines, like BigQuery and Snowflake, when reading an Iceberg table.
If you are not planning on using Iceberg, then you do not need to add these to your Hudi writers.
:::

#### Steps to add additional configurations to the Hudi writers:
1. Add the extensions jar (`hudi-extensions-0.1.0-SNAPSHOT-bundled.jar`) to your class path  
   For example, if you're using the Hudi [quick-start guide](https://hudi.apache.org/docs/quick-start-guide#spark-shellsql)
   for spark you can just add `--jars hudi-extensions-0.1.0-SNAPSHOT-bundled.jar` to the end of the command.
2. Set the following configurations in your writer options:  
   `hoodie.avro.write.support.class: io.onetable.hudi.extensions.HoodieAvroWriteSupportWithFieldIds`
   `hoodie.client.init.callback.classes: io.onetable.hudi.extensions.AddFieldIdsClientInitCallback`
3. Run your existing code that use Hudi writers


#### [Using BigLake Metastore to create the Iceberg BigLake tables](https://cloud.google.com/bigquery/docs/iceberg-tables#create-using-biglake-metastore):
You can use two options to register Onetable synced Iceberg tables to BigLake Metastore:
* To directly register the Onetable synced Iceberg table to BigLake Metastore,
  follow the [Onetable guide to integrate with BigLake Metastore](https://onetable.dev/docs/biglake-metastore)
* Use [stored procedures for Spark](https://cloud.google.com/bigquery/docs/spark-procedures)
  on BigQuery to register the table in BigLake Metastore and query the tables from BigQuery.

### Hudi and Delta tables
[This document](https://cloud.google.com/bigquery/docs/query-open-table-format-using-manifest-files)
explains how to query Hudi and Delta table formats through the use of manifest files.
