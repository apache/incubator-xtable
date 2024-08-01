<!--
 - Licensed to the Apache Software Foundation (ASF) under one
 - or more contributor license agreements.  See the NOTICE file
 - distributed with this work for additional information
 - regarding copyright ownership.  The ASF licenses this file
 - to you under the Apache License, Version 2.0 (the
 - "License"); you may not use this file except in compliance
 - with the License.  You may obtain a copy of the License at
 - 
 -     http://www.apache.org/licenses/LICENSE-2.0
 - 
 - Unless required by applicable law or agreed to in writing, software
 - distributed under the License is distributed on an "AS IS" BASIS,
 - WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 - See the License for the specific language governing permissions and 
 - limitations under the License.
-->

# Hudi Extensions
## Writer Extensions
### When should you use them?
The Hudi extensions provide the ability to add field IDs to the parquet schema when writing with Hudi. This is a requirement for some engines, like BigQuery and Snowflake, when reading an Iceberg table. If you are not planning on using Iceberg, then you do not need to add these to your Hudi writers.
### How do you use them?
1. Add the extensions jar (`incubator-xtable-hudi-extensions-0.1.0-SNAPSHOT-bundled.jar`) to your class path.  
For example, if you're using the Hudi [quick-start guide](https://hudi.apache.org/docs/quick-start-guide#spark-shellsql) for spark you can just add `--jars incubator-xtable-hudi-extensions-0.1.0-SNAPSHOT-bundled.jar` to the end of the command. 
2. Set the following configurations in your writer options:  
   `hoodie.avro.write.support.class: org.apache.xtable.hudi.extensions.HoodieAvroWriteSupportWithFieldIds`  
   `hoodie.client.init.callback.classes: org.apache.xtable.hudi.extensions.AddFieldIdsClientInitCallback`  
   `hoodie.datasource.write.row.writer.enable : false` (RowWriter support is coming soon)  
3. Run your existing code that use Hudi writers

## Hudi Streamer Extensions
### When should you use them?
If you want to use XTable with Hudi [streaming ingestion](https://hudi.apache.org/docs/hoodie_streaming_ingestion) to sync each commit into other table formats.
### How do you use them?
1. Add the extensions jar (`incubator-xtable-hudi-extensions-0.1.0-SNAPSHOT-bundled.jar`) to your class path.
2. Add `org.apache.xtable.hudi.sync.XTableSyncTool` to your list of sync classes
3. Set the following configurations based on your preferences:
   `hoodie.xtable.formats.to.sync: "ICEBERG,DELTA"` (or simply use one format)
   `hoodie.xtable.target.metadata.retention.hr: 168` (default retention for target format metadata is 168 hours)