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
 
package io.onetable.spi.sync;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import lombok.AllArgsConstructor;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import io.onetable.model.IncrementalTableChanges;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.OneTableMetadata;
import io.onetable.model.TableChange;
import io.onetable.model.sync.SyncMode;
import io.onetable.model.sync.SyncResult;

/** Provides the functionality to sync from the OneTable format to the target format. */
@AllArgsConstructor(staticName = "of")
public class TableFormatSync {
  private static final Logger LOG = LogManager.getLogger(TableFormatSync.class);
  private final TargetClient client;

  /**
   * Syncs the provided snapshot to the target table format.
   *
   * @param snapshot the snapshot to sync
   * @return the result of the sync process
   */
  public SyncResult syncSnapshot(OneSnapshot snapshot) {
    Instant startTime = Instant.now();
    try {
      OneTable oneTable = snapshot.getTable();
      return getSyncResult(
          SyncMode.FULL,
          oneTable,
          client -> client.syncFilesForSnapshot(snapshot.getDataFiles()),
          startTime);
    } catch (Exception e) {
      LOG.error("Failed to sync snapshot", e);
      return buildResultForError(SyncMode.FULL, startTime, e);
    }
  }

  /**
   * Syncs a set of changes to the target table format.
   *
   * @param changes the changes from the source table format that need to be applied
   * @return the results of trying to sync each change
   */
  public List<SyncResult> syncChanges(IncrementalTableChanges changes) {
    List<SyncResult> results = new ArrayList<>();
    for (TableChange change : changes.getTableChanges()) {
      Instant startTime = Instant.now();
      try {
        results.add(
            getSyncResult(
                SyncMode.INCREMENTAL,
                change.getCurrentTableState(),
                client -> client.syncFilesForDiff(change.getFilesDiff()),
                startTime));
      } catch (Exception e) {
        // Fallback to a sync where table changes are from changes.getInstant() to latest, write a
        // test case for this.
        // (OR) Progress with empty col stats to mirror the timeline.
        LOG.error("Failed to sync table changes", e);
        results.add(buildResultForError(SyncMode.INCREMENTAL, startTime, e));
        break;
      }
    }
    return results;
  }

  public Optional<Instant> getLastSyncInstant() {
    return client.getTableMetadata().map(OneTableMetadata::getLastInstantSynced);
  }

  public List<Instant> getPendingInstantsToConsiderForNextSync() {
    return client
        .getTableMetadata()
        .map(OneTableMetadata::getInstantsToConsiderForNextSync)
        .orElse(Collections.emptyList());
  }

  private SyncResult getSyncResult(
      SyncMode mode, OneTable tableState, SyncFiles fileSyncMethod, Instant startTime) {
    // initialize the sync
    client.beginSync(tableState);
    // sync schema updates
    client.syncSchema(tableState.getReadSchema());
    // sync partition updates
    client.syncPartitionSpec(tableState.getPartitioningFields());
    // Update the files in the target table
    fileSyncMethod.sync(client);
    // Persist the latest commit time in table properties for incremental syncs.
    OneTableMetadata latestState =
        OneTableMetadata.of(tableState.getLatestCommitTime(), tableState.getPendingCommits());
    client.syncMetadata(latestState);
    client.completeSync();

    return SyncResult.builder()
        .mode(mode)
        .status(
            SyncResult.SyncStatus.builder().statusCode(SyncResult.SyncStatusCode.SUCCESS).build())
        .syncStartTime(startTime)
        .syncDuration(Duration.between(startTime, Instant.now()))
        .lastInstantSynced(tableState.getLatestCommitTime())
        .pendingCommits(tableState.getPendingCommits())
        .build();
  }

  @FunctionalInterface
  private interface SyncFiles {
    void sync(TargetClient client);
  }

  private SyncResult buildResultForError(SyncMode mode, Instant startTime, Exception e) {
    return SyncResult.builder()
        .mode(mode)
        .status(
            SyncResult.SyncStatus.builder()
                .statusCode(SyncResult.SyncStatusCode.ERROR)
                .errorMessage(e.getMessage())
                .errorDescription("Failed to sync " + mode.name())
                .canRetryOnFailure(true)
                .build())
        .syncStartTime(startTime)
        .syncDuration(Duration.between(startTime, Instant.now()))
        .build();
  }
}
