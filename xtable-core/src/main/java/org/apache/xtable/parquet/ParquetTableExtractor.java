/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.xtable.parquet;

import lombok.Builder;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.xtable.avro.AvroSchemaConverter;
import org.apache.xtable.delta.DeltaPartitionExtractor;
import org.apache.xtable.delta.DeltaSchemaExtractor;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.storage.TableFormat;
import scala.Option;

import java.io.IOException;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * Extracts {@link InternalTable} canonical representation of a table at a point in time for Delta.
 */
@Builder
public class ParquetTableExtractor {
  @Builder.Default
  private static final AvroSchemaConverter schemaExtractor = AvroSchemaConverter.getInstance();

  public InternalTable table(Configuration conf, String basePath, String tableName, Long latestFileTimeStamp) throws IOException {

    ParquetMetadata metadata = ParquetFileReader.readFooter(conf,new Path(basePath));
    MessageType  messageType = metadata.getFileMetaData().getSchema();
    org.apache.parquet.avro.AvroSchemaConverter avroSchemaConverter = new org.apache.parquet.avro.AvroSchemaConverter();
    Schema avroSchema = avroSchemaConverter.convert(messageType);
    InternalSchema schema = schemaExtractor.toInternalSchema(avroSchema);
    List<InternalPartitionField> partitionFields =Collections.emptyList();
    DataLayoutStrategy dataLayoutStrategy = DataLayoutStrategy.FLAT;
    return InternalTable.builder()
        .tableFormat(TableFormat.PARQUET)
        .basePath(basePath)
        .name(tableName)
        .layoutStrategy(dataLayoutStrategy)
        .partitioningFields(partitionFields)
        .readSchema(schema)
        .latestCommitTime(Instant.ofEpochMilli(latestFileTimeStamp))
        .build();
  }
}
