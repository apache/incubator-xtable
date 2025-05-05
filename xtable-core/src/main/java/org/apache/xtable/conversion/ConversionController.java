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
 
package org.apache.xtable.conversion;

import static org.apache.xtable.conversion.ConversionUtils.convertToSourceTable;

import java.io.IOException;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.xtable.catalog.CatalogConversionFactory;
import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.IncrementalTableChanges;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.sync.SyncMode;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.model.sync.SyncStatusCode;
import org.apache.xtable.spi.extractor.ConversionSource;
import org.apache.xtable.spi.extractor.ExtractFromSource;
import org.apache.xtable.spi.sync.CatalogSync;
import org.apache.xtable.spi.sync.CatalogSyncClient;
import org.apache.xtable.spi.sync.ConversionTarget;
import org.apache.xtable.spi.sync.TableFormatSync;

/**
 * Responsible for completing the entire lifecycle of the sync process given {@link
 * ConversionConfig}. This is done in three steps,
 *
 * <ul>
 *   <li>1. Extracting snapshot {@link InternalSnapshot} from the source table format.
 *   <li>2. Syncing the snapshot to the table formats provided in the config.
 *   <li>3. Storing the sync results generated in step
 * </ul>
 */
@Log4j2
@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
public class ConversionController {
  private final Configuration conf;
  private final ConversionTargetFactory conversionTargetFactory;
  private final CatalogConversionFactory catalogConversionFactory;
  private final TableFormatSync tableFormatSync;
  private final CatalogSync catalogSync;

  public ConversionController(Configuration conf) {
    this(
        conf,
        ConversionTargetFactory.getInstance(),
        CatalogConversionFactory.getInstance(),
        TableFormatSync.getInstance(),
        CatalogSync.getInstance());
  }

  /**
   * Runs a sync for the given source table configuration in ConversionConfig.
   *
   * @param config A per table level config containing tableBasePath, partitionFieldSpecConfig,
   *     targetTableFormats and syncMode
   * @param conversionSourceProvider A provider for the {@link ConversionSource} instance, {@link
   *     ConversionSourceProvider#init(Configuration)} must be called before calling this method.
   * @return Returns a map containing the table format, and it's sync result. Run sync for a table
   *     with the provided per table level configuration.
   */
  public <COMMIT> Map<String, SyncResult> sync(
      ConversionConfig config, ConversionSourceProvider<COMMIT> conversionSourceProvider) {
    if (config.getTargetTables() == null || config.getTargetTables().isEmpty()) {
      throw new IllegalArgumentException("Please provide at-least one format to sync");
    }

    try (ConversionSource<COMMIT> conversionSource =
        conversionSourceProvider.getConversionSourceInstance(config.getSourceTable())) {
      ExtractFromSource<COMMIT> source = ExtractFromSource.of(conversionSource);
      return syncTableFormats(config, source, config.getSyncMode());
    } catch (IOException ioException) {
      throw new ReadException("Failed to close source converter", ioException);
    }
  }

  /**
   * Synchronizes the source table in conversion config to multiple target catalogs. If the
   * configuration for the target table uses a different table format, synchronizes the table format
   * first before syncing it to target catalog
   *
   * @param config A per table level config containing source table, target tables, target catalogs
   *     and syncMode.
   * @param conversionSourceProvider A provider for the {@link ConversionSource} instance for each
   *     tableFormat, {@link ConversionSourceProvider#init(Configuration)} must be called before
   *     calling this method.
   * @return Returns a map containing the table format, and it's sync result. Run sync for a table *
   *     with the provided per table level configuration.
   */
  public Map<String, SyncResult> syncTableAcrossCatalogs(
      ConversionConfig config, Map<String, ConversionSourceProvider> conversionSourceProvider) {
    if (config.getTargetTables() == null || config.getTargetTables().isEmpty()) {
      throw new IllegalArgumentException("Please provide at-least one format to sync");
    }
    try (ConversionSource conversionSource =
        conversionSourceProvider
            .get(config.getSourceTable().getFormatName())
            .getConversionSourceInstance(config.getSourceTable())) {
      ExtractFromSource source = ExtractFromSource.of(conversionSource);
      Map<String, SyncResult> tableFormatSyncResults =
          syncTableFormats(config, source, config.getSyncMode());
      Map<String, SyncResult> catalogSyncResults = new HashMap<>();
      for (TargetTable targetTable : config.getTargetTables()) {
        Map<CatalogTableIdentifier, CatalogSyncClient> catalogSyncClients =
            config.getTargetCatalogs().get(targetTable).stream()
                .collect(
                    Collectors.toMap(
                        TargetCatalogConfig::getCatalogTableIdentifier,
                        targetCatalog ->
                            catalogConversionFactory.createCatalogSyncClient(
                                targetCatalog.getCatalogConfig(),
                                targetTable.getFormatName(),
                                conf)));
        catalogSyncResults.put(
            targetTable.getFormatName(),
            syncCatalogsForTargetTable(
                targetTable,
                catalogSyncClients,
                conversionSourceProvider.get(targetTable.getFormatName())));
      }
      mergeSyncResults(tableFormatSyncResults, catalogSyncResults);
      return tableFormatSyncResults;
    } catch (IOException ioException) {
      throw new ReadException("Failed to close source converter", ioException);
    }
  }

  /**
   * Synchronizes the given source table format metadata in ConversionConfig to multiple target
   * formats.
   *
   * @param config A per table level config containing tableBasePath, partitionFieldSpecConfig,
   *     targetTableFormats and syncMode.
   * @param source An extractor class for {@link ConversionSource} and allows fetching current
   *     snapshot or incremental table changes.
   * @param syncMode sync mode is either FULL or INCREMENTAL.
   * @return Returns a map containing the table format, and it's sync result.
   */
  private <COMMIT> Map<String, SyncResult> syncTableFormats(
      ConversionConfig config, ExtractFromSource<COMMIT> source, SyncMode syncMode) {
    Map<String, ConversionTarget> conversionTargetByFormat =
        config.getTargetTables().stream()
            .filter(
                targetTable ->
                    !targetTable.getFormatName().equals(config.getSourceTable().getFormatName()))
            .collect(
                Collectors.toMap(
                    TargetTable::getFormatName,
                    targetTable -> conversionTargetFactory.createForFormat(targetTable, conf)));

    Map<String, Optional<TableSyncMetadata>> lastSyncMetadataByFormat =
        conversionTargetByFormat.entrySet().stream()
            .collect(
                Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().getTableMetadata()));
    Map<String, ConversionTarget> formatsToSyncIncrementally =
        getFormatsToSyncIncrementally(
            syncMode,
            conversionTargetByFormat,
            lastSyncMetadataByFormat,
            source.getConversionSource());
    Map<String, ConversionTarget> formatsToSyncBySnapshot =
        conversionTargetByFormat.entrySet().stream()
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
    Map<String, SyncResult> syncResultsMerged =
        new HashMap<>(syncResultForIncrementalSync.getLastSyncResult());
    syncResultsMerged.putAll(syncResultForSnapshotSync.getLastSyncResult());
    String successfulSyncs = getFormatsWithStatusCode(syncResultsMerged, SyncStatusCode.SUCCESS);
    if (!successfulSyncs.isEmpty()) {
      log.info("Sync is successful for the following formats {}", successfulSyncs);
    }
    String failedSyncs = getFormatsWithStatusCode(syncResultsMerged, SyncStatusCode.ERROR);
    if (!failedSyncs.isEmpty()) {
      log.error("Sync failed for the following formats {}", failedSyncs);
    }
    return syncResultsMerged;
  }

  /**
   * Synchronizes the target table to multiple target catalogs.
   *
   * @param targetTable target table that needs to synced.
   * @param catalogSyncClients Collection of catalog sync clients along with their table identifiers
   *     for each target catalog.
   * @param conversionSourceProvider A provider for the {@link ConversionSource} instance for the
   *     table format of targetTable.
   */
  private SyncResult syncCatalogsForTargetTable(
      TargetTable targetTable,
      Map<CatalogTableIdentifier, CatalogSyncClient> catalogSyncClients,
      ConversionSourceProvider conversionSourceProvider) {
    return catalogSync.syncTable(
        catalogSyncClients,
        // We get the latest state of InternalTable for TargetTable
        // and then synchronize it to catalogSyncClients.
        conversionSourceProvider
            .getConversionSourceInstance(convertToSourceTable(targetTable))
            .getCurrentTable());
  }

  private static String getFormatsWithStatusCode(
      Map<String, SyncResult> syncResultsMerged, SyncStatusCode statusCode) {
    return syncResultsMerged.entrySet().stream()
        .filter(entry -> entry.getValue().getTableFormatSyncStatus().getStatusCode() == statusCode)
        .map(Map.Entry::getKey)
        .collect(Collectors.joining(","));
  }

  private <COMMIT> Map<String, ConversionTarget> getFormatsToSyncIncrementally(
      SyncMode syncMode,
      Map<String, ConversionTarget> conversionTargetByFormat,
      Map<String, Optional<TableSyncMetadata>> lastSyncMetadataByFormat,
      ConversionSource<COMMIT> conversionSource) {
    if (syncMode == SyncMode.FULL) {
      // Full sync requested by config, hence no incremental sync.
      return Collections.emptyMap();
    }
    return conversionTargetByFormat.entrySet().stream()
        .filter(
            entry -> {
              Optional<Instant> lastSyncInstant =
                  lastSyncMetadataByFormat
                      .get(entry.getKey())
                      .map(TableSyncMetadata::getLastInstantSynced);
              List<Instant> pendingInstants =
                  lastSyncMetadataByFormat
                      .get(entry.getKey())
                      .map(TableSyncMetadata::getInstantsToConsiderForNextSync)
                      .orElse(Collections.emptyList());
              return isIncrementalSyncSufficient(
                  conversionSource, lastSyncInstant, pendingInstants);
            })
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private <COMMIT> SyncResultForTableFormats syncSnapshot(
      Map<String, ConversionTarget> conversionTargetByFormat, ExtractFromSource<COMMIT> source) {
    InternalSnapshot snapshot = source.extractSnapshot();
    Map<String, SyncResult> syncResultsByFormat =
        tableFormatSync.syncSnapshot(conversionTargetByFormat.values(), snapshot);
    return SyncResultForTableFormats.builder().lastSyncResult(syncResultsByFormat).build();
  }

  private <COMMIT> SyncResultForTableFormats syncIncrementalChanges(
      Map<String, ConversionTarget> conversionTargetByFormat,
      Map<String, Optional<TableSyncMetadata>> lastSyncMetadataByFormat,
      ExtractFromSource<COMMIT> source) {
    Map<String, SyncResult> syncResultsByFormat = Collections.emptyMap();
    Map<ConversionTarget, TableSyncMetadata> filteredSyncMetadataByFormat =
        lastSyncMetadataByFormat.entrySet().stream()
            .filter(entry -> conversionTargetByFormat.containsKey(entry.getKey()))
            .collect(
                Collectors.toMap(
                    entry -> conversionTargetByFormat.get(entry.getKey()),
                    entry -> entry.getValue().get()));

    InstantsForIncrementalSync instantsForIncrementalSync =
        getMostOutOfSyncCommitAndPendingCommits(filteredSyncMetadataByFormat);
    IncrementalTableChanges incrementalTableChanges =
        source.extractTableChanges(instantsForIncrementalSync);
    Map<String, List<SyncResult>> allResults =
        tableFormatSync.syncChanges(filteredSyncMetadataByFormat, incrementalTableChanges);
    // return only the last sync result in the list of results for each format
    syncResultsByFormat =
        allResults.entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey, entry -> entry.getValue().get(entry.getValue().size() - 1)));
    return SyncResultForTableFormats.builder().lastSyncResult(syncResultsByFormat).build();
  }

  /**
   * Checks if incremental sync is sufficient for a target table format.
   *
   * @param conversionSource {@link ConversionSource}
   * @param lastSyncInstant the last instant at which the target table format was synced
   * @param pendingInstants the list of pending instants for the target table format to consider for
   *     next sync
   * @return true if incremental sync is sufficient, false otherwise.
   */
  private <COMMIT> boolean isIncrementalSyncSufficient(
      ConversionSource<COMMIT> conversionSource,
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
      log.info("No previous InternalTable sync for target. Falling back to snapshot sync.");
      return false;
    }
    boolean isIncrementalSafeFromInstant =
        conversionSource.isIncrementalSyncSafeFrom(earliestInstant.get());
    if (!isIncrementalSafeFromInstant) {
      log.info(
          "Incremental sync is not safe from instant {}. Falling back to snapshot sync.",
          earliestInstant);
      return false;
    }
    return true;
  }

  private InstantsForIncrementalSync getMostOutOfSyncCommitAndPendingCommits(
      Map<ConversionTarget, TableSyncMetadata> lastSyncMetadataByFormat) {
    Instant mostOutOfSyncCommit =
        lastSyncMetadataByFormat.values().stream()
            .map(TableSyncMetadata::getLastInstantSynced)
            .sorted()
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException("No existing commits found for incremental sync"));
    List<Instant> allPendingInstants =
        lastSyncMetadataByFormat.values().stream()
            .map(TableSyncMetadata::getInstantsToConsiderForNextSync)
            .flatMap(Collection::stream)
            .distinct()
            .sorted()
            .collect(Collectors.toList());
    return InstantsForIncrementalSync.builder()
        .lastSyncInstant(mostOutOfSyncCommit)
        .pendingCommits(allPendingInstants)
        .build();
  }

  private void mergeSyncResults(
      Map<String, SyncResult> syncResultsMerged, Map<String, SyncResult> catalogSyncResults) {
    catalogSyncResults.forEach(
        (tableFormat, catalogSyncResult) -> {
          syncResultsMerged.computeIfPresent(
              tableFormat,
              (k, syncResult) ->
                  syncResult.toBuilder()
                      .syncDuration(
                          syncResult.getSyncDuration().plus(catalogSyncResult.getSyncDuration()))
                      .catalogSyncStatusList(catalogSyncResult.getCatalogSyncStatusList())
                      .build());
          syncResultsMerged.computeIfAbsent(tableFormat, k -> catalogSyncResult);
        });
  }

  @Value
  @Builder
  private static class SyncResultForTableFormats {
    @Builder.Default Map<String, SyncResult> lastSyncResult = Collections.emptyMap();
  }
}
