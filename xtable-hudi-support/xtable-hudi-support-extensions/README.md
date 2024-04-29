# Hudi Extensions
## Writer Extensions
### When should you use them?
The Hudi extensions provide the ability to add field IDs to the parquet schema when writing with Hudi. This is a requirement for some engines, like BigQuery and Snowflake, when reading an Iceberg table. If you are not planning on using Iceberg, then you do not need to add these to your Hudi writers.
### How do you use them?
1. Add the extensions jar (`hudi-extensions-0.1.0-SNAPSHOT-bundled.jar`) to your class path.  
For example, if you're using the Hudi [quick-start guide](https://hudi.apache.org/docs/quick-start-guide#spark-shellsql) for spark you can just add `--jars hudi-extensions-0.1.0-SNAPSHOT-bundled.jar` to the end of the command. 
2. Set the following configurations in your writer options:  
   `hoodie.avro.write.support.class: org.apache.xtable.hudi.extensions.HoodieAvroWriteSupportWithFieldIds`  
   `hoodie.client.init.callback.classes: org.apache.xtable.hudi.extensions.AddFieldIdsClientInitCallback`  
   `hoodie.datasource.write.row.writer.enable : false` (RowWriter support is coming soon)  
3. Run your existing code that use Hudi writers

## Hudi Streamer Extensions
### When should you use them?
If you want to use XTable with Hudi [streaming ingestion](https://hudi.apache.org/docs/hoodie_streaming_ingestion) to sync each commit into other table formats.
### How do you use them?
1. Add the extensions jar (`hudi-extensions-0.1.0-SNAPSHOT-bundled.jar`) to your class path.
2. Add `org.apache.xtable.hudi.sync.XTableSyncTool` to your list of sync classes
3. Set the following configurations based on your preferences:
   `hoodie.xtable.formats.to.sync: "ICEBERG,DELTA"` (or simply use one format)
   `hoodie.xtable.target.metadata.retention.hr: 168` (default retention for target format metadata is 168 hours)