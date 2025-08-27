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
 
package org.apache.xtable.kernel;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

import lombok.Builder;

import io.delta.kernel.*;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.storage.TableFormat;

/**
 * Extracts {@link InternalTable} canonical representation of a table at a point in time for Delta.
 */
@Builder
public class DeltaKernelTableExtractor {
  @Builder.Default
  private static final DeltaKernelSchemaExtractor schemaExtractor =
      DeltaKernelSchemaExtractor.getInstance();

  private final String basePath;

  public InternalTable table(
      Table deltaKernelTable, Snapshot snapshot, Engine engine, String tableName, String basePath) {
    try {
      // Get schema from Delta Kernel's snapshot
      io.delta.kernel.types.StructType schema = snapshot.getSchema();
      InternalSchema internalSchema = schemaExtractor.toInternalSchema(schema);
      // Get partition columns);
      StructType fullSchema = snapshot.getSchema(); // The full table schema
      List<String> partitionColumns = snapshot.getPartitionColumnNames(); // List<String>

      List<StructField> partitionFields_strfld =
          fullSchema.fields().stream()
              .filter(field -> partitionColumns.contains(field.getName()))
              .collect(Collectors.toList());

      StructType partitionSchema = new StructType(partitionFields_strfld);

      List<InternalPartitionField> partitionFields =
          DeltaKernelPartitionExtractor.getInstance()
              .convertFromDeltaPartitionFormat(internalSchema, partitionSchema);

      DataLayoutStrategy dataLayoutStrategy =
          !partitionFields.isEmpty()
              ? DataLayoutStrategy.HIVE_STYLE_PARTITION
              : DataLayoutStrategy.FLAT;

      // Get the timestamp
      long timestamp = snapshot.getTimestamp(engine) * 1000; // Convert to milliseconds
      return InternalTable.builder()
          .tableFormat(TableFormat.DELTA)
          .basePath(basePath)
          .name(tableName)
          .layoutStrategy(dataLayoutStrategy)
          .partitioningFields(partitionFields)
          .readSchema(internalSchema)
          .latestCommitTime(Instant.ofEpochMilli(timestamp))
          .latestMetadataPath(basePath + "/_delta_log")
          .build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to extract table information using Delta Kernel", e);
    }
  }
}
