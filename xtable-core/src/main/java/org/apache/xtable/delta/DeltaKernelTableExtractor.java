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
import java.util.ArrayList;
import java.util.List;

import lombok.Builder;

import io.delta.kernel.*;
import io.delta.kernel.engine.Engine;

import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
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

      System.out.println("Kernelschema: " + schema);

      InternalSchema internalSchema = schemaExtractor.toInternalSchema_v2(schema);
      //      io.delta.kernel.types.StructType schema = snapshot.getSchema();
      ////      InternalSchema internalSchema = schemaExtractor.toInternalSchema_v2(schema);
      //      InternalSchema internalSchema =
      // schemaExtractor.toInternalSchema(snapshot.getSchema());

      // Get partition columns
      System.out.println("Partition columns: " + internalSchema);
      List<String> partitionColumnNames = snapshot.getPartitionColumnNames();
      List<InternalPartitionField> partitionFields = new ArrayList<>();
      for (String columnName : partitionColumnNames) {
        InternalField sourceField =
            InternalField.builder()
                .name(columnName)
                .schema(
                    InternalSchema.builder()
                        .name(columnName)
                        .dataType(InternalType.STRING) // Assuming string type for partition columns
                        .build())
                .build();

        // Create the partition field with the source field
        partitionFields.add(InternalPartitionField.builder().sourceField(sourceField).build());
      }

      DataLayoutStrategy dataLayoutStrategy =
          partitionFields.isEmpty()
              ? DataLayoutStrategy.FLAT
              : DataLayoutStrategy.HIVE_STYLE_PARTITION;

      // Get the timestamp
      long timestamp = snapshot.getTimestamp(engine) * 1000; // Convert to milliseconds
      System.out.println("InternalTable basepath" + basePath);
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
