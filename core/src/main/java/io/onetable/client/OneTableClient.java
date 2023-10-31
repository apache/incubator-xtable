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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import io.onetable.model.IncrementalTableChanges;
import io.onetable.model.InstantsForIncrementalSync;
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
@Log4j2
public class OneTableClient {
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
    log.info(
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
    // TODO: Simplify consolidation of storing lastInstant and inFlightInstants together.
    Map<TableFormat, List<Instant>> pendingInstantsToConsiderForNextSyncByFormat =
        syncClientByFormat.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> entry.getValue().getPendingInstantsToConsiderForNextSync()));

    // Fallback to snapshot sync if lastSyncInstant doesn't exist or no longer present in the
    // active timeline.
    Map<TableFormat, SyncMode> requiredSyncModeByFormat =
        getRequiredSyncModes(
            source,
            syncClientByFormat.keySet(),
            lastSyncInstantByFormat,
            pendingInstantsToConsiderForNextSyncByFormat);
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
      InstantsForIncrementalSync instantsForIncrementalSync =
          getMostOutOfSyncCommitAndPendingCommits(
              // Filter to formats using incremental sync
              lastSyncInstantByFormat.entrySet().stream()
                  .filter(entry -> formatsForIncrementalSync.containsKey(entry.getKey()))
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)),
              pendingInstantsToConsiderForNextSyncByFormat);
      IncrementalTableChanges incrementalTableChanges =
          source.extractTableChanges(instantsForIncrementalSync);
      for (Map.Entry<TableFormat, TableFormatSync> entry : formatsForIncrementalSync.entrySet()) {
        TableFormat tableFormat = entry.getKey();
        TableFormatSync tableFormatSync = entry.getValue();
        List<Instant> pendingSyncsByFormat =
            pendingInstantsToConsiderForNextSyncByFormat.getOrDefault(
                tableFormat, Collections.emptyList());
        Set<Instant> pendingInstantsSet = new HashSet<>(pendingSyncsByFormat);
        // extract the changes that happened since this format was last synced
        List<TableChange> filteredTableChanges =
            incrementalTableChanges.getTableChanges().stream()
                .filter(
                    change ->
                        change
                                .getTableAsOfChange()
                                .getLatestCommitTime()
                                .isAfter(lastSyncInstantByFormat.get(tableFormat).get())
                            || pendingInstantsSet.contains(
                                change.getTableAsOfChange().getLatestCommitTime()))
                .collect(Collectors.toList());
        IncrementalTableChanges tableFormatIncrementalChanges =
            IncrementalTableChanges.builder()
                .tableChanges(filteredTableChanges)
                .pendingCommits(incrementalTableChanges.getPendingCommits())
                .build();
        List<SyncResult> syncResultList =
            tableFormatSync.syncChanges(tableFormatIncrementalChanges);

        syncedTable =
            filteredTableChanges.get(filteredTableChanges.size() - 1).getTableAsOfChange();
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
      Map<TableFormat, Optional<Instant>> lastSyncInstantByFormat,
      Map<TableFormat, List<Instant>> pendingInstantsToConsiderForNextSyncByFormat) {
    return tableFormats.stream()
        .collect(
            Collectors.toMap(
                Function.identity(),
                format -> {
                  Optional<Instant> lastSyncInstant = lastSyncInstantByFormat.get(format);
                  List<Instant> pendingInstantsToConsiderForNextSync =
                      pendingInstantsToConsiderForNextSyncByFormat.get(format);
                  return isIncrementalSyncSufficient(
                          source, lastSyncInstant, pendingInstantsToConsiderForNextSync)
                      ? SyncMode.INCREMENTAL
                      : SyncMode.FULL;
                }));
  }

  private <COMMIT> boolean isIncrementalSyncSufficient(
      ExtractFromSource<COMMIT> source,
      Optional<Instant> lastSyncInstant,
      List<Instant> pendingInstants) {
    Optional<Instant> earliestInstant;
    Stream<Instant> pendingInstantsStream =
        (pendingInstants == null) ? Stream.empty() : pendingInstants.stream();
    if (lastSyncInstant.isPresent()) {
      earliestInstant =
          Stream.concat(Stream.of(lastSyncInstant.get()), pendingInstantsStream)
              .min(Instant::compareTo);
    } else {
      earliestInstant = pendingInstantsStream.min(Instant::compareTo);
    }
    boolean isEarliestInstantExists = doesInstantExists(source, earliestInstant);
    if (!isEarliestInstantExists) {
      log.info(
          "Earliest instant {} doesn't exist in the source table. Falling back to full sync.",
          earliestInstant);
      return false;
    }
    boolean isEarliestInstantAffectedByCleaned =
        source.getSourceClient().isAffectedByClean(earliestInstant.get());
    if (isEarliestInstantAffectedByCleaned) {
      log.info(
          "Earliest instant {} is affected by clean. Falling back to full sync.", earliestInstant);
      return false;
    }
    return true;
  }

  private Map<TableFormat, TableFormatSync> getFormatsForSyncMode(
      Map<TableFormat, TableFormatSync> tableFormatSyncMap,
      Map<TableFormat, SyncMode> requiredSyncModeByFormat,
      SyncMode syncMode) {
    return tableFormatSyncMap.entrySet().stream()
        .filter(entry -> requiredSyncModeByFormat.get(entry.getKey()) == syncMode)
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private InstantsForIncrementalSync getMostOutOfSyncCommitAndPendingCommits(
      Map<TableFormat, Optional<Instant>> lastSyncInstantByFormat,
      Map<TableFormat, List<Instant>> pendingInstantsToConsiderByFormat) {
    Optional<Instant> mostOutOfSyncCommit =
        lastSyncInstantByFormat.values().stream()
            .filter(Optional::isPresent)
            .map(Optional::get)
            .sorted()
            .findFirst();
    List<Instant> allPendingInstants =
        pendingInstantsToConsiderByFormat.values().stream()
            .flatMap(Collection::stream)
            .distinct()
            .sorted()
            .collect(Collectors.toList());
    return InstantsForIncrementalSync.builder()
        .lastSyncInstant(mostOutOfSyncCommit.get())
        .pendingCommits(allPendingInstants)
        .build();
  }

  private <COMMIT> boolean doesInstantExists(
      ExtractFromSource<COMMIT> source, Optional<Instant> instantToCheck) {
    if (!instantToCheck.isPresent()) {
      return false;
    }
    return source.getSourceClient().doesCommitForInstantExists(instantToCheck.get());
  }

  @Value
  @Builder
  private static class SyncResultForTableFormats {
    Map<TableFormat, SyncResult> lastSyncResult;
    OneTable syncedTable;
  }
}
