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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.xtable.model.IncrementalTableChanges;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.sync.SyncMode;
import org.apache.xtable.model.sync.SyncResult;

/** Provides the functionality to sync from the InternalTable format to the target format. */
@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TableFormatSync {
  private static final TableFormatSync INSTANCE = new TableFormatSync();

  public static TableFormatSync getInstance() {
    return INSTANCE;
  }

  /**
   * Syncs the provided snapshot to the target table formats.
   *
   * @param conversionTargets the targets to sync with the snapshot
   * @param snapshot the snapshot to sync
   * @return the result of the sync process
   */
  public Map<String, SyncResult> syncSnapshot(
      Collection<ConversionTarget> conversionTargets, InternalSnapshot snapshot) {
    Instant startTime = Instant.now();
    Map<String, SyncResult> results = new HashMap<>();
    for (ConversionTarget conversionTarget : conversionTargets) {
      try {
        InternalTable internalTable = snapshot.getTable();
        results.put(
            conversionTarget.getTableFormat(),
            getSyncResult(
                conversionTarget,
                SyncMode.FULL,
                internalTable,
                target -> target.syncFilesForSnapshot(snapshot.getPartitionedDataFiles()),
                startTime,
                snapshot.getPendingCommits()));
      } catch (Exception e) {
        log.error("Failed to sync snapshot", e);
        results.put(
            conversionTarget.getTableFormat(), buildResultForError(SyncMode.FULL, startTime, e));
      }
    }
    return results;
  }

  /**
   * Syncs a set of changes to the target table formats.
   *
   * @param conversionTargetWithMetadata a map of conversion targets to their last sync metadata
   * @param changes the changes from the source table format that need to be applied
   * @return the results of trying to sync each change
   */
  public Map<String, List<SyncResult>> syncChanges(
      Map<ConversionTarget, TableSyncMetadata> conversionTargetWithMetadata,
      IncrementalTableChanges changes) {
    Map<String, List<SyncResult>> results = new HashMap<>();
    Set<ConversionTarget> conversionTargetsWithFailures = new HashSet<>();
    while (changes.getTableChanges().hasNext()) {
      TableChange change = changes.getTableChanges().next();
      Collection<ConversionTarget> conversionTargetsToSync =
          conversionTargetWithMetadata.entrySet().stream()
              .filter(
                  entry -> {
                    TableSyncMetadata metadata = entry.getValue();
                    return isChangeApplicableForLastSyncMetadata(change, metadata);
                  })
              .map(Map.Entry::getKey)
              .collect(Collectors.toList());
      for (ConversionTarget conversionTarget : conversionTargetsToSync) {
        if (conversionTargetsWithFailures.contains(conversionTarget)) {
          continue;
        }
        Instant startTime = Instant.now();
        List<SyncResult> resultsForFormat =
            results.computeIfAbsent(conversionTarget.getTableFormat(), key -> new ArrayList<>());
        try {
          resultsForFormat.add(
              getSyncResult(
                  conversionTarget,
                  SyncMode.INCREMENTAL,
                  change.getTableAsOfChange(),
                  target -> target.syncFilesForDiff(change.getFilesDiff()),
                  startTime,
                  changes.getPendingCommits()));
        } catch (Exception e) {
          log.error("Failed to sync table changes", e);
          resultsForFormat.add(buildResultForError(SyncMode.INCREMENTAL, startTime, e));
          conversionTargetsWithFailures.add(conversionTarget);
        }
      }
    }
    return results;
  }

  private static boolean isChangeApplicableForLastSyncMetadata(
      TableChange change, TableSyncMetadata metadata) {
    return change
            .getTableAsOfChange()
            .getLatestCommitTime()
            .isAfter(metadata.getLastInstantSynced())
        || metadata
            .getInstantsToConsiderForNextSync()
            .contains(change.getTableAsOfChange().getLatestCommitTime());
  }

  private SyncResult getSyncResult(
      ConversionTarget conversionTarget,
      SyncMode mode,
      InternalTable tableState,
      SyncFiles fileSyncMethod,
      Instant startTime,
      List<Instant> pendingCommits) {
    // initialize the sync
    conversionTarget.beginSync(tableState);
    // sync schema updates
    conversionTarget.syncSchema(tableState.getReadSchema());
    // sync partition updates
    conversionTarget.syncPartitionSpec(tableState.getPartitioningFields());
    // Update the files in the target table
    fileSyncMethod.sync(conversionTarget);
    // Persist the latest commit time in table properties for incremental syncs.
    TableSyncMetadata latestState =
        TableSyncMetadata.of(tableState.getLatestCommitTime(), pendingCommits);
    conversionTarget.syncMetadata(latestState);
    conversionTarget.completeSync();

    return SyncResult.builder()
        .mode(mode)
        .status(SyncResult.SyncStatus.SUCCESS)
        .syncStartTime(startTime)
        .syncDuration(Duration.between(startTime, Instant.now()))
        .lastInstantSynced(tableState.getLatestCommitTime())
        .build();
  }

  @FunctionalInterface
  private interface SyncFiles {
    void sync(ConversionTarget conversionTarget);
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
