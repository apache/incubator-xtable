# Hudi Extensions
## When should you use them?
The Hudi extensions provide the ability to add field IDs to the parquet schema when writing with Hudi. This is a requirement for some engines, like BigQuery and Snowflake, when reading an Iceberg table. If you are not planning on using Iceberg, then you do not need to add these to your Hudi writers.
## How do you use them?
1. Add the extensions jar (`hudi-extensions-0.1.0-SNAPSHOT-bundled.jar`) to your class path.  
For example, if you're using the Hudi [quick-start guide](https://hudi.apache.org/docs/quick-start-guide#spark-shellsql) for spark you can just add `--jars hudi-extensions-0.1.0-SNAPSHOT-bundled.jar` to the end of the command. 
2. Set the following configurations in your writer options:  
   `hoodie.avro.write.support.class: io.onetable.hudi.extensions.HoodieAvroWriteSupportWithFieldIds`  
   `hoodie.client.init.callback.classes: io.onetable.hudi.extensions.AddFieldIdsClientInitCallback`  
   `hoodie.datasource.write.row.writer.enable : false` (RowWriter support is coming soon)  
3. Run your existing code that use Hudi writers