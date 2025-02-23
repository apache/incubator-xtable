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
 
package org.apache.xtable.delta;

import java.time.Instant;
import java.util.List;

import lombok.Builder;

import org.apache.parquet.hadoop.ParquetFileReader;

import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.storage.TableFormat;

/**
 * Extracts {@link InternalTable} canonical representation of a table at a point in time for Delta.
 */
@Builder
public class ParquetTableExtractor {
  @Builder.Default
  private static final ParquetSchemaExtractor schemaExtractor = ParquetTableExtractor.getInstance();
  @Builder.Default
  private static final ParquetMetadataExtractor parquetMetadataExtractor =
          ParquetMetadataExtractor.getInstance();
  private Map<String, List<String>> initPartitionInfo() {
    return getPartitionFromDirectoryStructure(hadoopConf, basePath, Collections.emptyMap());
  }

  public InternalTable table(String tableName, Long version) {
    ParquetMetadata footer =
            parquetMetadataExtractor.readParquetMetadata(conf, path, ParquetMetadataConverter.NO_FILTER);
    MessageType schema = parquetMetadataExtractor.getSchema(footer);
    InternalSchema schema = schemaExtractor.toInternalSchema(schema);
    Set<String> partitionKeys = initPartitionInfo().keySet();
    List<InternalPartitionField> partitionFields =
        ParquetPartitionExtractor.getInstance()
            .convertFromParquetPartitionFormat(partitionKeys,schema);
    DataLayoutStrategy dataLayoutStrategy =
        !partitionFields.isEmpty()
            ? DataLayoutStrategy.HIVE_STYLE_PARTITION
            : DataLayoutStrategy.FLAT;
    return InternalTable.builder()
        .tableFormat(TableFormat.APACHE_PARQUET)
        .basePath(snapshot.deltaLog().dataPath().toString())
        .name(tableName)
        .layoutStrategy(dataLayoutStrategy)
        .partitioningFields(partitionFields)
        .readSchema(schema)
        .latestCommitTime(Instant.ofEpochMilli(snapshot.timestamp()))
        .build();
  }
}
