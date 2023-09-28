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

import java.time.Instant;
import java.util.Collection;
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

import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.TableChange;
import io.onetable.model.storage.TableFormat;
import io.onetable.model.sync.SyncMode;
import io.onetable.model.sync.SyncResult;
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

  public OneTableClient(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Runs a sync for the given source table configuration in PerTableConfig.
   *
   * @param config A per table level config containing tableBasePath, partitionFieldSpecConfig,
   *     targetTableFormats and syncMode
   * @param sourceClientProvider A provider for the source client instance, {@link
   *     SourceClientProvider#init(Configuration, Map)} must be called before calling this method.
   * @return Returns a map containing the table format, and it's sync result. Run sync for a table
   *     with the provided per table level configuration.
   */
  public <COMMIT> Map<TableFormat, SyncResult> sync(
      PerTableConfig config, SourceClientProvider<COMMIT> sourceClientProvider) {
    if (config.getTargetTableFormats().isEmpty()) {
      throw new IllegalArgumentException("Please provide at-least one format to sync");
    }

    SourceClient<COMMIT> sourceClient = sourceClientProvider.getSourceClientInstance(config);
    ExtractFromSource<COMMIT> source = ExtractFromSource.of(sourceClient);

    SyncResultForTableFormats result;
    Map<TableFormat, TableFormatSync> syncClientByFormat =
        config.getTargetTableFormats().stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    tableFormat ->
                        TableFormatClientFactory.createForFormat(tableFormat, config, conf)));
    if (config.getSyncMode() == SyncMode.FULL) {
      result = syncSnapshot(syncClientByFormat, source);
    } else {
      result = syncIncrementalChanges(syncClientByFormat, source);
    }
    LOG.info(
        "OneTable Sync is successful for the following formats " + config.getTargetTableFormats());
    return result.getLastSyncResult();
  }

  private <COMMIT> SyncResultForTableFormats syncSnapshot(
      Map<TableFormat, TableFormatSync> syncClientByFormat, ExtractFromSource<COMMIT> source) {
    Map<TableFormat, SyncResult> syncResultsByFormat = new HashMap<>();
    OneSnapshot snapshot = source.extractSnapshot();
    syncClientByFormat.forEach(
        (tableFormat, tableFormatSync) -> {
          SyncResult syncResult = tableFormatSync.syncSnapshot(snapshot);
          syncResultsByFormat.put(tableFormat, syncResult);
        });
    return SyncResultForTableFormats.builder()
        .lastSyncResult(syncResultsByFormat)
        .syncedTable(snapshot.getTable())
        .build();
  }

  private <COMMIT> SyncResultForTableFormats syncIncrementalChanges(
      Map<TableFormat, TableFormatSync> syncClientByFormat, ExtractFromSource<COMMIT> source) {
    Map<TableFormat, SyncResult> syncResultsByFormat = new HashMap<>();
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
        getRequiredSyncModes(source, syncClientByFormat.keySet(), lastSyncInstantByFormat);
    Map<TableFormat, TableFormatSync> formatsForIncrementalSync =
        getFormatsForSyncMode(syncClientByFormat, requiredSyncModeByFormat, SyncMode.INCREMENTAL);
    Map<TableFormat, TableFormatSync> formatsForFullSync =
        getFormatsForSyncMode(syncClientByFormat, requiredSyncModeByFormat, SyncMode.FULL);

    if (!formatsForFullSync.isEmpty()) {
      SyncResultForTableFormats result = syncSnapshot(formatsForFullSync, source);
      syncResultsByFormat.putAll(result.getLastSyncResult());
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
          source.extractTableChanges(mostOutOfSync.getValue().get());
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
      }
    }
    return SyncResultForTableFormats.builder()
        .lastSyncResult(syncResultsByFormat)
        .syncedTable(syncedTable)
        .build();
  }

  private <COMMIT> Map<TableFormat, SyncMode> getRequiredSyncModes(
      ExtractFromSource<COMMIT> source,
      Collection<TableFormat> tableFormats,
      Map<TableFormat, Optional<Instant>> lastSyncInstantByFormat) {
    return tableFormats.stream()
        .collect(
            Collectors.toMap(
                Function.identity(),
                format -> {
                  Optional<Instant> lastSyncInstant = lastSyncInstantByFormat.get(format);
                  return checkIfLastSyncInstantExists(source, lastSyncInstant)
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
    OneTable syncedTable;
  }
}
