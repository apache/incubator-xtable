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

import io.onetable.spi.sync.TargetClient;
import lombok.Builder;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

import io.onetable.model.IncrementalTableChanges;
import io.onetable.model.InstantsForIncrementalSync;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.OneTableMetadata;
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
  private final TableFormatClientFactory tableFormatClientFactory;

  public OneTableClient(Configuration conf) {
    this.conf = conf;
    this.tableFormatClientFactory = TableFormatClientFactory.getInstance();
  }

  @VisibleForTesting
  OneTableClient(Configuration conf, TableFormatClientFactory tableFormatClientFactory) {
    this.conf = conf;
    this.tableFormatClientFactory = tableFormatClientFactory;
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

    Map<TableFormat, TableFormatSync> syncClientByFormat =
        config.getTargetTableFormats().stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    tableFormat ->
                        tableFormatClientFactory.createForFormat(tableFormat, config, conf)));
    // State for each TableFormat
    Map<TableFormat, Optional<OneTableMetadata>> lastSyncMetadataByFormat =
        syncClientByFormat.entrySet().stream()
            .collect(
                Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getTableMetadata()));
    Map<TableFormat, TableFormatSync> formatsToSyncIncrementally =
        getFormatsToSyncIncrementally(
            config, syncClientByFormat, lastSyncMetadataByFormat, source.getSourceClient());
    Map<TableFormat, TableFormatSync> formatsToSyncBySnapshot =
        syncClientByFormat.entrySet().stream()
            .filter(entry -> !formatsToSyncIncrementally.containsKey(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    SyncResultForTableFormats syncResultForSnapshotSync =
        formatsToSyncBySnapshot.isEmpty()
            ? SyncResultForTableFormats.builder().build()
            : syncSnapshot(formatsToSyncBySnapshot, source);
    SyncResultForTableFormats syncResultForIncrementalSync =
        formatsToSyncIncrementally.isEmpty()
            ? SyncResultForTableFormats.builder().build()
            : syncIncrementalChanges(formatsToSyncIncrementally, lastSyncMetadataByFormat, source);
    log.info(
        "OneTable Sync is successful for the following formats " + config.getTargetTableFormats());
    Map<TableFormat, SyncResult> syncResultsMerged =
        new HashMap<>(syncResultForIncrementalSync.getLastSyncResult());
    syncResultsMerged.putAll(syncResultForSnapshotSync.getLastSyncResult());
    return syncResultsMerged;
  }

  private <COMMIT> Map<TableFormat, TableFormatSync> getFormatsToSyncIncrementally(
      PerTableConfig perTableConfig,
      Map<TableFormat, TableFormatSync> syncClientByFormat,
      Map<TableFormat, Optional<OneTableMetadata>> lastSyncMetadataByFormat,
      SourceClient<COMMIT> sourceClient) {
    if (perTableConfig.getSyncMode() == SyncMode.FULL) {
      // Full sync requested by config, hence no incremental sync.
      return Collections.emptyMap();
    }
    return syncClientByFormat.entrySet().stream()
        .filter(
            entry -> {
              Optional<Instant> lastSyncInstant =
                  lastSyncMetadataByFormat
                      .get(entry.getKey())
                      .map(OneTableMetadata::getLastInstantSynced);
              List<Instant> pendingInstants =
                  lastSyncMetadataByFormat
                      .get(entry.getKey())
                      .map(OneTableMetadata::getInstantsToConsiderForNextSync)
                      .orElse(Collections.emptyList());
              return isIncrementalSyncSufficient(sourceClient, lastSyncInstant, pendingInstants);
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
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
      Map<TableFormat, TargetClient> syncClientByFormat,
      Map<TableFormat, Optional<OneTableMetadata>> lastSyncMetadataByFormat,
      ExtractFromSource<COMMIT> source) {
    Map<TableFormat, SyncResult> syncResultsByFormat = new HashMap<>();
    OneTable syncedTable = null;

    Map<TableFormat, Optional<OneTableMetadata>> filteredSyncMetadataByFormat =
        lastSyncMetadataByFormat.entrySet().stream().filter(entry -> syncClientByFormat.containsKey(entry.getKey())).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    InstantsForIncrementalSync instantsForIncrementalSync =
        getMostOutOfSyncCommitAndPendingCommits(filteredSyncMetadataByFormat);
    IncrementalTableChanges incrementalTableChanges =
        source.extractTableChanges(instantsForIncrementalSync);
    if (!incrementalTableChanges.getTableChanges().isEmpty()) {
      for (Map.Entry<TableFormat, TableFormatSync> entry : syncClientByFormat.entrySet()) {
        TableFormat tableFormat = entry.getKey();
        TableFormatSync tableFormatSync = entry.getValue();
        List<Instant> pendingSyncsByFormat =
            filteredSyncMetadataByFormat
                .get(tableFormat)
                .map(OneTableMetadata::getInstantsToConsiderForNextSync)
                .orElse(Collections.emptyList());
        Set<Instant> pendingInstantsSet = new HashSet<>(pendingSyncsByFormat);
        // extract the changes that happened since this format was last synced
        List<TableChange> filteredTableChanges =
            incrementalTableChanges.getTableChanges().stream()
                .filter(
                    change ->
                        change
                                .getTableAsOfChange()
                                .getLatestCommitTime()
                                .isAfter(
                                    filteredSyncMetadataByFormat
                                        .get(tableFormat)
                                        .map(OneTableMetadata::getLastInstantSynced)
                                        .get())
                            || pendingInstantsSet.contains(
                                change.getTableAsOfChange().getLatestCommitTime()))
                .collect(Collectors.toList());
        if (filteredTableChanges.isEmpty()) {
          // no updates to sync
          continue;
        }
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

  /**
   * Checks if incremental sync is sufficient for a target table format.
   *
   * @param sourceClient {@link SourceClient}
   * @param lastSyncInstant the last instant at which the target table format was synced
   * @param pendingInstants the list of pending instants for the target table format to consider for
   *     next sync
   * @return true if incremental sync is sufficient, false otherwise.
   */
  private <COMMIT> boolean isIncrementalSyncSufficient(
      SourceClient<COMMIT> sourceClient,
      Optional<Instant> lastSyncInstant,
      List<Instant> pendingInstants) {
    Stream<Instant> pendingInstantsStream =
        (pendingInstants == null) ? Stream.empty() : pendingInstants.stream();
    Optional<Instant> earliestInstant =
        lastSyncInstant
            .map(
                instant ->
                    Stream.concat(Stream.of(instant), pendingInstantsStream)
                        .min(Instant::compareTo))
            .orElseGet(() -> pendingInstantsStream.min(Instant::compareTo));
    if (!earliestInstant.isPresent()) {
      log.info("No previous OneTable sync for target. Falling back to snapshot sync.");
      return false;
    }
    boolean isIncrementalSafeFromInstant =
        sourceClient.isIncrementalSyncSafeFrom(earliestInstant.get());
    if (!isIncrementalSafeFromInstant) {
      log.info(
          "Incremental sync is not safe from instant {}. Falling back to snapshot sync.",
          earliestInstant);
      return false;
    }
    return true;
  }

  private InstantsForIncrementalSync getMostOutOfSyncCommitAndPendingCommits(
      Map<TableFormat, Optional<OneTableMetadata>> lastSyncMetadataByFormat) {
    Optional<Instant> mostOutOfSyncCommit =
        lastSyncMetadataByFormat.values().stream()
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(OneTableMetadata::getLastInstantSynced)
            .sorted()
            .findFirst();
    List<Instant> allPendingInstants =
        lastSyncMetadataByFormat.values().stream()
            .map(
                oneTableMetadata ->
                    oneTableMetadata
                        .map(OneTableMetadata::getInstantsToConsiderForNextSync)
                        .orElse(Collections.emptyList()))
            .flatMap(Collection::stream)
            .distinct()
            .sorted()
            .collect(Collectors.toList());
    return InstantsForIncrementalSync.builder()
        .lastSyncInstant(mostOutOfSyncCommit.get())
        .pendingCommits(allPendingInstants)
        .build();
  }

  @Value
  @Builder
  private static class SyncResultForTableFormats {
    @Builder.Default Map<TableFormat, SyncResult> lastSyncResult = Collections.emptyMap();
    @Builder.Default OneTable syncedTable = null;
  }
}
