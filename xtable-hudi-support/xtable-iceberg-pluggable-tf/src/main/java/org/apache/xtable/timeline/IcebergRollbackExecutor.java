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
 
package org.apache.xtable.timeline;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.table.timeline.dto.InstantDTO;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.xtable.iceberg.IcebergConversionTarget;
import org.apache.xtable.iceberg.IcebergTableManager;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;

@Log4j2
public class IcebergRollbackExecutor {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
          .setSerializationInclusion(JsonInclude.Include.NON_NULL);

  private final HoodieTableMetaClient metaClient;
  private final IcebergConversionTarget target;
  private final IcebergTableManager tableManager;

  public IcebergRollbackExecutor(HoodieTableMetaClient metaClient, IcebergConversionTarget target) {
    this.metaClient = metaClient;
    this.target = target;
    this.tableManager =
        IcebergTableManager.of(
            (org.apache.hadoop.conf.Configuration) metaClient.getStorageConf().unwrap());
  }

  @SneakyThrows
  public void rollbackSnapshot(InternalTable internalTable, HoodieInstant instantToRollback) {
    TableIdentifier tableIdentifier =
        TableIdentifier.of(metaClient.getTableConfig().getTableName());
    if (tableManager.tableExists(null, tableIdentifier, metaClient.getBasePath().toString())) {
      Table table =
          tableManager.getTable(null, tableIdentifier, metaClient.getBasePath().toString());
      TableSyncMetadata syncMetadata =
          TableSyncMetadata.fromJson(
                  table.currentSnapshot().summary().get(TableSyncMetadata.XTABLE_METADATA))
              .get();
      HoodieInstant latestHoodieInstantInIceberg =
          InstantDTO.toInstant(
              MAPPER.readValue(syncMetadata.getLatestTableOperationId(), InstantDTO.class),
              metaClient.getInstantGenerator());
      if (latestHoodieInstantInIceberg.equals(instantToRollback)) {
        // The instant to rollback is committed in iceberg, so rollback to previous snapshot.
        // NOTE: This is equivalent to hudi restore and should be performed by killing all active
        // writers.
        target.beginSync(internalTable);
        target.rollbackToSnapshotId(table.currentSnapshot().snapshotId());
      } else if (InstantComparison.compareTimestamps(
          latestHoodieInstantInIceberg.getCompletionTime(),
          InstantComparison.LESSER_THAN,
          instantToRollback.getCompletionTime())) {
        // In this case, instantToRollback was not committed in iceberg, so we can will be ignoring
        // it.
        log.info(
            "Ignoring rollback to instant {}' because it is not committed in Iceberg. Latest committed instant in Iceberg {}'",
            instantToRollback,
            latestHoodieInstantInIceberg);
      } else {
        throw new IllegalArgumentException(
            String.format(
                "Cannot rollback to instant '%s' because it is older than the latest committed Hudi instant in Iceberg '%s'. "
                    + "Rolling back would create an inconsistent state.",
                instantToRollback, latestHoodieInstantInIceberg));
      }
    }
  }
}
