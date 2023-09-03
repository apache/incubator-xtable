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
 
package io.onetable.client;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Value;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import io.onetable.exception.OneIOException;
import io.onetable.hudi.HudiSourceClientProvider;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.TableChange;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.TableFormat;
import io.onetable.model.sync.SyncMode;
import io.onetable.model.sync.SyncResult;
import io.onetable.persistence.OneTableState;
import io.onetable.persistence.OneTableStateStorage;
import io.onetable.spi.extractor.ExtractFromSource;
import io.onetable.spi.extractor.SourceClient;
import io.onetable.spi.sync.TableFormatSync;

/**
 * Responsible for completing the entire lifecycle of the sync process given {@link PerTableConfig}.
 * This is done in three steps,
 *
 * <ul>
 *   <li>1. Extracting snapshot {@link OneSnapshot} from the source table format.
 *   <li>2. Syncing the snapshot to the table formats provided in the config.
 *   <li>3. Storing the sync results generated in step
 * </ul>
 */
public class OneTableClient {
  private static final Logger LOG = LogManager.getLogger(OneTableClient.class);
  private final Configuration conf;
  private final OneTableStateStorage storage;

  public OneTableClient(Configuration conf) {
    this.conf = conf;
    // Disable writing archive until there is a clear purpose for it
    this.storage = new OneTableStateStorage(conf, false);
  }

  /**
   * Runs a sync for the given source table configuration in PerTableConfig.
   *
   * @param config A per table level config containing tableBasePath, partitionFieldSpecConfig,
   *     targetTableFormats and syncMode
   * @return Returns a map containing the table format, and it's sync result. Run sync for a table
   *     with the provided per table level configuration.
   */
  public Map<TableFormat, SyncResult> sync(PerTableConfig config) {
    return sync(config, new HudiSourceClientProvider());
  }

  public <COMMIT> Map<TableFormat, SyncResult> sync(
      PerTableConfig config, SourceClientProvider<COMMIT> sourceClientProvider) {
    if (config.getTargetTableFormats().isEmpty()) {
      throw new IllegalArgumentException("Please provide at-least one format to sync");
    }

    SourceClient<COMMIT> sourceClient = sourceClientProvider.getSourceClientInstance(config);
    ExtractFromSource<COMMIT> source = ExtractFromSource.of(sourceClient);

    Optional<OneTableState> currentState = getSyncState(config.getTableBasePath());

    SyncResultForTableFormats result;
    Map<TableFormat, TableFormatSync> syncClientByFormat =
        config.getTargetTableFormats().stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    tableFormat ->
                        TableFormatClientFactory.createForFormat(tableFormat, config, conf)));
    if (config.getSyncMode() == SyncMode.FULL) {
      result = syncSnapshot(syncClientByFormat, source, currentState);
    } else {
      result = syncIncrementalChanges(syncClientByFormat, source, currentState);
    }
    OneTableState newState =
        OneTableState.builder()
            .table(result.getSyncedTable())
            .lastSyncResult(result.getLastSyncResult())
            .lastSuccessfulSyncResult(result.getLastSuccessfulSyncResult())
            .tableFormatsToSync(config.getTargetTableFormats())
            .build();
    persistOneTableState(newState);
    LOG.info(
        "OneTable Sync is successful for the following formats " + config.getTargetTableFormats());
    return newState.getLastSyncResult();
  }

  private <COMMIT> SyncResultForTableFormats syncSnapshot(
      Map<TableFormat, TableFormatSync> syncClientByFormat,
      ExtractFromSource<COMMIT> source,
      Optional<OneTableState> currentState) {
    Map<TableFormat, SyncResult> syncResultsByFormat = new HashMap<>();
    Map<TableFormat, SyncResult> lastSuccessfulSyncResultMap =
        currentState.map(OneTableState::getLastSuccessfulSyncResult).orElseGet(HashMap::new);
    OneSnapshot snapshot = source.extractSnapshot();
    syncClientByFormat.forEach(
        (tableFormat, tableFormatSync) -> {
          SyncResult syncResult = tableFormatSync.syncSnapshot(snapshot);
          syncResultsByFormat.put(tableFormat, syncResult);
          if (syncResult.getStatus().getStatusCode().equals(SyncResult.SyncStatusCode.SUCCESS)) {
            lastSuccessfulSyncResultMap.put(tableFormat, syncResult);
          }
        });
    return SyncResultForTableFormats.builder()
        .lastSyncResult(syncResultsByFormat)
        .lastSuccessfulSyncResult(lastSuccessfulSyncResultMap)
        .syncedTable(snapshot.getTable())
        .build();
  }

  private <COMMIT> SyncResultForTableFormats syncIncrementalChanges(
      Map<TableFormat, TableFormatSync> syncClientByFormat,
      ExtractFromSource<COMMIT> source,
      Optional<OneTableState> currentStateOptional) {
    if (!currentStateOptional.isPresent()) {
      // Fallback to snapshot if current state doesn't exist or corrupted.
      return syncSnapshot(syncClientByFormat, source, currentStateOptional);
    }
    OneTableState currentState = currentStateOptional.get();
    Map<TableFormat, SyncResult> syncResultsByFormat =
        new HashMap<>(currentState.getLastSuccessfulSyncResult());
    Map<TableFormat, SyncResult> lastSuccessfulSyncResultMap =
        currentState.getLastSuccessfulSyncResult();
    OneTable syncedTable = null;
    // State for each TableFormat
    Map<TableFormat, Optional<Instant>> lastSyncInstantByFormat =
        syncClientByFormat.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, entry -> entry.getValue().getLastSyncInstant()));

    // Fallback to snapshot sync if lastSyncInstant doesn't exist or no longer present in the
    // active timeline.
    Map<TableFormat, SyncMode> requiredSyncModeByFormat =
        getRequiredSyncModes(
            source, currentState, syncClientByFormat.keySet(), lastSyncInstantByFormat);
    Map<TableFormat, TableFormatSync> formatsForIncrementalSync =
        getFormatsForSyncMode(syncClientByFormat, requiredSyncModeByFormat, SyncMode.INCREMENTAL);
    Map<TableFormat, TableFormatSync> formatsForFullSync =
        getFormatsForSyncMode(syncClientByFormat, requiredSyncModeByFormat, SyncMode.FULL);

    if (!formatsForFullSync.isEmpty()) {
      SyncResultForTableFormats result =
          syncSnapshot(formatsForFullSync, source, Optional.of(currentState));
      syncResultsByFormat.putAll(result.getLastSyncResult());
      syncResultsByFormat.putAll(result.getLastSuccessfulSyncResult());
      syncedTable = result.getSyncedTable();
    }

    if (!formatsForIncrementalSync.isEmpty()) {
      Map.Entry<TableFormat, Optional<Instant>> mostOutOfSync =
          getMostOutOfSyncInstant(
              // Filter to formats using incremental sync
              lastSyncInstantByFormat.entrySet().stream()
                  .filter(entry -> formatsForIncrementalSync.containsKey(entry.getKey()))
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
      List<TableChange> allTableChanges =
          source.extractTableChanges(
              currentState
                  .getLastSuccessfulSyncResult()
                  .get(mostOutOfSync.getKey())
                  .getLastSyncedDataFiles(),
              mostOutOfSync.getValue().get());
      for (Map.Entry<TableFormat, TableFormatSync> entry : formatsForIncrementalSync.entrySet()) {
        TableFormat tableFormat = entry.getKey();
        TableFormatSync tableFormatSync = entry.getValue();
        // extract the changes that happened since this format was last synced
        List<TableChange> tableChanges =
            allTableChanges.stream()
                .filter(
                    change ->
                        change
                            .getCurrentTableState()
                            .getLatestCommitTime()
                            .isAfter(lastSyncInstantByFormat.get(tableFormat).get()))
                .collect(Collectors.toList());
        List<SyncResult> syncResultList = tableFormatSync.syncChanges(tableChanges);

        syncedTable = tableChanges.get(tableChanges.size() - 1).getCurrentTableState();
        SyncResult latestSyncResult = syncResultList.get(syncResultList.size() - 1);
        syncResultsByFormat.put(tableFormat, latestSyncResult);
        Optional<SyncResult> latestSuccessfulSyncResult =
            syncResultList.stream()
                .filter(
                    s -> s.getStatus().getStatusCode().equals(SyncResult.SyncStatusCode.SUCCESS))
                .max(Comparator.comparing(SyncResult::getLastInstantSynced));
        latestSuccessfulSyncResult.ifPresent(
            syncResult -> lastSuccessfulSyncResultMap.put(tableFormat, syncResult));
      }
    }
    return SyncResultForTableFormats.builder()
        .lastSyncResult(syncResultsByFormat)
        .lastSuccessfulSyncResult(lastSuccessfulSyncResultMap)
        .syncedTable(syncedTable)
        .build();
  }

  private <COMMIT> Map<TableFormat, SyncMode> getRequiredSyncModes(
      ExtractFromSource<COMMIT> source,
      OneTableState currentState,
      Collection<TableFormat> tableFormats,
      Map<TableFormat, Optional<Instant>> lastSyncInstantByFormat) {
    return tableFormats.stream()
        .collect(
            Collectors.toMap(
                Function.identity(),
                format -> {
                  SyncResult lastSyncResult =
                      currentState.getLastSuccessfulSyncResult().get(format);
                  OneDataFiles lastSyncedDataFiles =
                      lastSyncResult == null ? null : lastSyncResult.getLastSyncedDataFiles();
                  Optional<Instant> lastSyncInstant = lastSyncInstantByFormat.get(format);
                  return lastSyncedDataFiles != null
                          && checkIfLastSyncInstantExists(source, lastSyncInstant)
                      ? SyncMode.INCREMENTAL
                      : SyncMode.FULL;
                }));
  }

  private Map<TableFormat, TableFormatSync> getFormatsForSyncMode(
      Map<TableFormat, TableFormatSync> tableFormatSyncMap,
      Map<TableFormat, SyncMode> requiredSyncModeByFormat,
      SyncMode syncMode) {
    return tableFormatSyncMap.entrySet().stream()
        .filter(entry -> requiredSyncModeByFormat.get(entry.getKey()) == syncMode)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private Map.Entry<TableFormat, Optional<Instant>> getMostOutOfSyncInstant(
      Map<TableFormat, Optional<Instant>> lastSyncInstantByFormat) {
    Map.Entry<TableFormat, Optional<Instant>> mostOutOfSync = null;
    for (Map.Entry<TableFormat, Optional<Instant>> lastSyncInstant :
        lastSyncInstantByFormat.entrySet()) {
      if (mostOutOfSync == null && lastSyncInstant.getValue().isPresent()) {
        mostOutOfSync = lastSyncInstant;
      } else if (mostOutOfSync != null
          && lastSyncInstant.getValue().isPresent()
          && lastSyncInstant.getValue().get().isBefore(mostOutOfSync.getValue().get())) {
        mostOutOfSync = lastSyncInstant;
      }
    }
    return mostOutOfSync;
  }

  private void persistOneTableState(OneTableState state) {
    try {
      storage.write(state);
    } catch (IOException e) {
      throw new OneIOException("Failed to persist sync result", e);
    }
  }

  private Optional<OneTableState> getSyncState(String tableBasePath) {
    try {
      return storage.read(tableBasePath);
    } catch (IOException e) {
      throw new OneIOException("Failed to get one table state", e);
    }
  }

  private <COMMIT> boolean checkIfLastSyncInstantExists(
      ExtractFromSource<COMMIT> source, Optional<Instant> lastSyncInstant) {
    if (!lastSyncInstant.isPresent()) {
      return false;
    }

    // TODO This check is not generic and should be moved to HudiClient
    // TODO hardcoding the return value to true for now
    //    COMMIT lastSyncHoodieInstant = source.getLastSyncCommit(lastSyncInstant.get());
    //    return HudiClient.parseFromInstantTime(lastSyncHoodieInstant.getTimestamp())
    //        .equals(lastSyncInstant.get());
    return true;
  }

  @Value
  @Builder
  private static class SyncResultForTableFormats {
    Map<TableFormat, SyncResult> lastSyncResult;
    Map<TableFormat, SyncResult> lastSuccessfulSyncResult;
    OneTable syncedTable;
  }
}
