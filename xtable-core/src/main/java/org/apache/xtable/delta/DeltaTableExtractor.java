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
import org.apache.spark.sql.delta.actions.Metadata;

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
    Snapshot snapshot = deltaLog.getSnapshotAt(version, Option.empty(), Option.empty());
    return table(snapshot, tableName);
  }

  /**
   * Builds an {@link InternalTable} from a pre-resolved Delta {@link Snapshot}.
   *
   * @param snapshot a valid, non-null snapshot with metadata and schema present
   * @param tableName name of the table
   * @return the internal representation of the table at the snapshot's version
   * @throws IllegalStateException if the snapshot or its metadata/schema is null
   */
  public InternalTable table(Snapshot snapshot, String tableName) {
    requireMetadata(snapshot, tableName);
    return table(
        snapshot.metadata(),
        snapshot.deltaLog(),
        tableName,
        Instant.ofEpochMilli(snapshot.timestamp()));
  }

  /**
   * Builds an {@link InternalTable} from the table's {@link Metadata} directly, without a resolved
   * {@link Snapshot}. Everything an InternalTable carries is either constant for the table (paths)
   * or derived from the metadata, so callers that already know the metadata at a version — e.g. an
   * incremental backlog reusing it across commits — can avoid reconstructing the snapshot.
   *
   * @param metadata the table metadata in effect at the commit
   * @param deltaLog the delta log, used only for the table and log paths
   * @param tableName name of the table
   * @param commitTimestamp the commit-file modification time of the commit, used as the
   *     incremental-sync watermark
   * @return the internal representation of the table at the commit
   */
  public InternalTable table(
      Metadata metadata, DeltaLog deltaLog, String tableName, Instant commitTimestamp) {
    if (metadata == null || metadata.schema() == null) {
      throw new IllegalStateException("Missing metadata/schema for table: " + tableName);
    }
    InternalSchema schema = schemaExtractor.toInternalSchema(metadata.schema());
    List<InternalPartitionField> partitionFields =
        DeltaPartitionExtractor.getInstance()
            .convertFromDeltaPartitionFormat(schema, metadata.partitionSchema());
    // Delta follows Hive Style partitioning layout
    // (https://delta.io/blog/2023-01-18-add-remove-partition-delta-lake/)
    DataLayoutStrategy dataLayoutStrategy =
        !partitionFields.isEmpty()
            ? DataLayoutStrategy.HIVE_STYLE_PARTITION
            : DataLayoutStrategy.FLAT;
    return InternalTable.builder()
        .tableFormat(TableFormat.DELTA)
        .basePath(deltaLog.dataPath().toString())
        .name(tableName)
        .layoutStrategy(dataLayoutStrategy)
        .partitioningFields(partitionFields)
        .readSchema(schema)
        .latestCommitTime(commitTimestamp)
        .latestMetadataPath(deltaLog.logPath().toString())
        .build();
  }

  private static void requireMetadata(Snapshot snapshot, String tableName) {
    if (snapshot == null || snapshot.metadata() == null || snapshot.metadata().schema() == null) {
      throw new IllegalStateException(
          "Missing metadata/schema for table: "
              + tableName
              + " at version: "
              + (snapshot != null ? snapshot.version() : "unknown"));
    }
  }
}
