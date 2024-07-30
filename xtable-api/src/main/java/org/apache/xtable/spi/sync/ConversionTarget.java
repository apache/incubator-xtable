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
 
package org.apache.xtable.spi.sync;

import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;

import org.apache.xtable.conversion.PerTableConfig;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.DataFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;

/** A client that provides the major functionality for syncing changes to a target system. */
public interface ConversionTarget {

  /**
   * Syncs the current schema state.
   *
   * @param schema the current schema
   */
  void syncSchema(InternalSchema schema);

  /**
   * Syncs the current partition spec.
   *
   * @param partitionSpec the current partition spec
   */
  void syncPartitionSpec(List<InternalPartitionField> partitionSpec);

  /**
   * Syncs the {@link TableSyncMetadata} to the target for tracking metadata between runs. This is
   * required for incremental sync.
   *
   * @param metadata the current metadata
   */
  void syncMetadata(TableSyncMetadata metadata);

  /**
   * Syncs the provided snapshot files to the target system. This method is required to both add and
   * remove files.
   *
   * @param partitionedDataFiles the files to sync, grouped by partition
   */
  void syncFilesForSnapshot(List<PartitionFileGroup> partitionedDataFiles);

  /**
   * Syncs the changes in files to the target system. This method is required to both add and remove
   * files.
   *
   * @param dataFilesDiff the diff that needs to be synced
   */
  void syncFilesForDiff(DataFilesDiff dataFilesDiff);

  /**
   * Starts the sync and performs any initialization required
   *
   * @param table the table that will be synced
   */
  void beginSync(InternalTable table);

  /** Completes the sync and performs any cleanup required. */
  void completeSync();

  /** Returns the xtable metadata persisted in the target */
  Optional<TableSyncMetadata> getTableMetadata();

  /** Returns the TableFormat name the client syncs to */
  String getTableFormat();

  /** Initializes the client with provided configuration */
  void init(PerTableConfig perTableConfig, Configuration configuration);
}
