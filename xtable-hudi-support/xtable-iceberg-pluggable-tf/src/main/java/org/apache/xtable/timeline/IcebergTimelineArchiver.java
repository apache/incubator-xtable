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
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

import org.apache.xtable.iceberg.IcebergConversionTarget;
import org.apache.xtable.iceberg.IcebergTableManager;
import org.apache.xtable.model.InternalTable;

/**
 * Expires the Iceberg snapshots whose instants Hudi has archived. Snapshots are expired only from
 * the oldest end of the current snapshot's ancestry: removing one from the middle would cut the
 * parent chain and hide every older snapshot from the reconstructed timeline.
 */
@Log4j2
public class IcebergTimelineArchiver {
  private final HoodieTableMetaClient metaClient;
  private final IcebergConversionTarget target;
  private final IcebergTableManager tableManager;

  public IcebergTimelineArchiver(HoodieTableMetaClient metaClient, IcebergConversionTarget target) {
    this.metaClient = metaClient;
    this.target = target;
    this.tableManager =
        IcebergTableManager.of((Configuration) metaClient.getStorageConf().unwrap());
  }

  public void archiveInstants(InternalTable internalTable, List<HoodieInstant> archivedInstants) {
    TableIdentifier tableIdentifier =
        TableIdentifier.of(metaClient.getTableConfig().getTableName());
    if (!tableManager.tableExists(null, tableIdentifier, metaClient.getBasePath().toString())) {
      return;
    }
    Table table = tableManager.getTable(null, tableIdentifier, metaClient.getBasePath().toString());
    Set<String> archivedInstantKeys =
        archivedInstants.stream()
            .map(IcebergActiveTimeline::instantKey)
            .collect(Collectors.toSet());
    Set<String> activeInstantKeys =
        metaClient
            .reloadActiveTimeline()
            .getInstantsAsStream()
            .map(IcebergActiveTimeline::instantKey)
            .collect(Collectors.toSet());
    List<Long> expireSnapshots = new ArrayList<>();
    for (Snapshot snapshot : IcebergSnapshotInstants.ancestorsOldestFirst(table)) {
      HoodieInstant hoodieInstant =
          IcebergSnapshotInstants.recordedInstant(snapshot, metaClient.getInstantGenerator());
      if (HoodieTimeline.SAVEPOINT_ACTION.equals(hoodieInstant.getAction())) {
        log.info("Skipping expiring next set of snapshots because of savepoint {}", hoodieInstant);
        break;
      }
      String instantKey = IcebergActiveTimeline.instantKey(hoodieInstant);
      // A snapshot whose instant is neither being archived nor already gone from the timeline (a
      // rolled-back commit, or an instant archived behind a savepoint earlier) has to stay, and so
      // does everything newer.
      if (!archivedInstantKeys.contains(instantKey) && activeInstantKeys.contains(instantKey)) {
        break;
      }
      expireSnapshots.add(snapshot.snapshotId());
    }
    target.beginSync(internalTable);
    target.expireSnapshotIds(expireSnapshots);
  }
}
