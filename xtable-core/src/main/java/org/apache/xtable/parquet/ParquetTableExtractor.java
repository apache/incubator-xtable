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
  private static final ParquetPartitionExtractor partitionExtractor =
      ParquetPartitionExtractor.getInstance();

  @Builder.Default
  private static final ParquetPartitionValueExtractor partitionValueExtractor =
          ParquetPartitionValueExtractor.getInstance();

  @Builder.Default
  private static final ParquetConversionSource parquetConversionSource =
          ParquetConversionSource.getInstance();

  @Builder.Default
  private static final ParquetMetadataExtractor parquetMetadataExtractor =
      ParquetMetadataExtractor.getInstance();

 /* private Map<String, List<String>> initPartitionInfo() {
    return getPartitionFromDirectoryStructure(hadoopConf, basePath, Collections.emptyMap());
  }*/
  public String getBasePathFromLastModifiedTable(){
    InternalTable table = parquetConversionSource.getTable(-1L);
    return table.getBasePath();
  }

  public InternalTable table(String tableName, Set<String> partitionKeys) {
    ParquetMetadata footer = parquetMetadataExtractor.readParquetMetadata(conf, path);
    MessageType schema = parquetMetadataExtractor.getSchema(footer);
    InternalSchema internalSchema = schemaExtractor.toInternalSchema(schema);
    List<InternalPartitionField> partitionFields =
            parquetConversionSource.initPartitionInfo().getPartitions();
    InternalTable snapshot = parquetConversionSource.getTable(-1L);
    // Assuming InternalTable.java has its getters
    Instant lastCommit = snapshot.latestCommitTime();
    DataLayoutStrategy dataLayoutStrategy =
        !partitionFields.isEmpty()
            ? DataLayoutStrategy.HIVE_STYLE_PARTITION
            : DataLayoutStrategy.FLAT;
    return InternalTable.builder()
        .tableFormat(TableFormat.APACHE_PARQUET)
        .basePath(getBasePathFromLastModifiedTable())
        .name(tableName)
        .layoutStrategy(dataLayoutStrategy)
        .partitioningFields(partitionFields)
        .readSchema(internalSchema)
        .latestCommitTime(Instant.ofEpochMilli(lastCommit))
        .build();
  }
}
