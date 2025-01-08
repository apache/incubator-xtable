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

import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;

import scala.Option;

import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.storage.TableFormat;

/**
 * Extracts {@link InternalTable} canonical representation of a table at a point in time for Delta.
 */
@Builder
public class DeltaTableExtractor {
  @Builder.Default
  private static final DeltaSchemaExtractor schemaExtractor = DeltaSchemaExtractor.getInstance();

  public InternalTable table(DeltaLog deltaLog, String tableName, Long version) {
    Snapshot snapshot = deltaLog.getSnapshotAt(version, Option.empty());
    InternalSchema schema = schemaExtractor.toInternalSchema(snapshot.metadata().schema());
    List<InternalPartitionField> partitionFields =
        DeltaPartitionExtractor.getInstance()
            .convertFromDeltaPartitionFormat(schema, snapshot.metadata().partitionSchema());
    // Delta follows Hive Style partitioning layout
    // (https://delta.io/blog/2023-01-18-add-remove-partition-delta-lake/)
    DataLayoutStrategy dataLayoutStrategy =
        !partitionFields.isEmpty()
            ? DataLayoutStrategy.HIVE_STYLE_PARTITION
            : DataLayoutStrategy.FLAT;
    return InternalTable.builder()
        .tableFormat(TableFormat.DELTA)
        .basePath(snapshot.deltaLog().dataPath().toString())
        .name(tableName)
        .layoutStrategy(dataLayoutStrategy)
        .partitioningFields(partitionFields)
        .readSchema(schema)
        .latestCommitTime(Instant.ofEpochMilli(snapshot.timestamp()))
        .build();
  }
}
