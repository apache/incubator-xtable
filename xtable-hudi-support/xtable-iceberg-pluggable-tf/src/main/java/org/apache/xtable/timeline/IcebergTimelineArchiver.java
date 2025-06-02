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

import java.util.ArrayList;
import java.util.List;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.dto.InstantDTO;

import org.apache.iceberg.Snapshot;
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

public class IcebergTimelineArchiver {
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
          .setSerializationInclusion(JsonInclude.Include.NON_NULL);

  private final HoodieTableMetaClient metaClient;
  private final IcebergConversionTarget target;
  private final IcebergTableManager tableManager;

  public IcebergTimelineArchiver(HoodieTableMetaClient metaClient, IcebergConversionTarget target) {
    this.metaClient = metaClient;
    this.target = target;
    this.tableManager =
        IcebergTableManager.of((Configuration) metaClient.getStorageConf().unwrap());
  }

  @SneakyThrows
  public void archiveInstants(InternalTable internalTable, List<HoodieInstant> archivedInstants) {
    TableIdentifier tableIdentifier =
        TableIdentifier.of(metaClient.getTableConfig().getTableName());
    if (tableManager.tableExists(null, tableIdentifier, metaClient.getBasePath().toString())) {
      Table table =
          tableManager.getTable(null, tableIdentifier, metaClient.getBasePath().toString());
      List<Long> expireSnapshots = new ArrayList<>();
      for (Snapshot snapshot : table.snapshots()) {
        TableSyncMetadata syncMetadata =
            TableSyncMetadata.fromJson(snapshot.summary().get(TableSyncMetadata.XTABLE_METADATA))
                .get();
        HoodieInstant hoodieInstant =
            InstantDTO.toInstant(
                MAPPER.readValue(syncMetadata.getLatestTableOperationId(), InstantDTO.class),
                metaClient.getInstantGenerator());
        if (archivedInstants.contains(hoodieInstant)) {
          expireSnapshots.add(snapshot.snapshotId());
        }
      }
      target.beginSync(internalTable);
      target.expireSnapshotIds(expireSnapshots);
    }
  }
}
