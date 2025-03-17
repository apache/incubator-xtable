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

import java.time.Instant;
import java.util.List;

import lombok.Builder;
import java.util.Set;
import java.util.Map;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.config.InputPartitionFields;
import org.apache.xtable.model.config.InputPartitionField;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

/**
 * Extracts {@link InternalTable} canonical representation of a table at a point in time for Delta.
 */
@Builder
public class ParquetTableExtractor {
  private static final InputPartitionFields partitions=null;

  private static final ParquetTableExtractor INSTANCE =
          new ParquetTableExtractor();
  public static ParquetTableExtractor getInstance() {
    return INSTANCE;
  }
  @Builder.Default
  private static final ParquetTableExtractor tableExtractor = ParquetTableExtractor.getInstance();
  private static final ParquetSchemaExtractor schemaExtractor = ParquetSchemaExtractor.getInstance();


  @Builder.Default
  private static final ParquetPartitionValueExtractor partitionValueExtractor =
          ParquetPartitionValueExtractor.getInstance();

 /* @Builder.Default
  private static final ParquetConversionSource parquetConversionSource =
          ParquetConversionSource.getInstance();*/

  @Builder.Default
  private static final ParquetMetadataExtractor parquetMetadataExtractor =
      ParquetMetadataExtractor.getInstance();

   private InputPartitionFields initPartitionInfo() {
    return partitions;
  }
 /* public String getBasePathFromLastModifiedTable(){
    InternalTable table = parquetConversionSource.getTable(-1L);
    return table.getBasePath();
  }*/

  /*public InternalTable table(String tableName, Set<String> partitionKeys,MessageType schema) {
    InternalSchema internalSchema = schemaExtractor.toInternalSchema(schema);
    List<InputPartitionField> partitionFields =
            parquetConversionSource.initPartitionInfo().getPartitions();
    List<InternalPartitionField> convertedPartitionFields = partitionValueExtractor.getInternalPartitionFields(partitionFields);
    InternalTable snapshot = parquetConversionSource.getTable(-1L);
    // Assuming InternalTable.java has its getters
    Instant lastCommit = snapshot.latestCommitTime();
    DataLayoutStrategy dataLayoutStrategy =
        !partitionFields.isEmpty()
            ? DataLayoutStrategy.HIVE_STYLE_PARTITION
            : DataLayoutStrategy.FLAT;
    return InternalTable.builder()
        .tableFormat(TableFormat.PARQUET)
        .basePath(getBasePathFromLastModifiedTable())
        .name(tableName)
        .layoutStrategy(dataLayoutStrategy)
        .partitioningFields(convertedPartitionFields)
        .readSchema(internalSchema)
        .latestCommitTime(lastCommit)
        .build();
  }*/
}
