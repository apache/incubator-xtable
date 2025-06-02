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
 
package org.apache.xtable.hudi.catalog;

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import org.apache.xtable.catalog.CatalogPartition;
import org.apache.xtable.catalog.CatalogPartitionEvent;
import org.apache.xtable.catalog.CatalogPartitionSyncOperations;
import org.apache.xtable.catalog.CatalogPartitionSyncTool;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.hudi.HudiTableManager;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;

@Log4j2
public class HudiCatalogPartitionSyncTool implements CatalogPartitionSyncTool {

  private final CatalogPartitionSyncOperations catalogClient;
  private final HudiTableManager hudiTableManager;
  private final PartitionValueExtractor partitionValuesExtractor;
  private final Configuration configuration;

  public static final String LAST_COMMIT_TIME_SYNC = "last_commit_time_sync";
  public static final String LAST_COMMIT_COMPLETION_TIME_SYNC = "last_commit_completion_time_sync";

  public HudiCatalogPartitionSyncTool(
      CatalogPartitionSyncOperations catalogClient,
      PartitionValueExtractor partitionValueExtractor,
      Configuration configuration) {
    this.catalogClient = catalogClient;
    this.hudiTableManager = HudiTableManager.of(configuration);
    this.partitionValuesExtractor = partitionValueExtractor;
    this.configuration = configuration;
  }

  @VisibleForTesting
  HudiCatalogPartitionSyncTool(
      CatalogPartitionSyncOperations catalogClient,
      HudiTableManager hudiTableManager,
      PartitionValueExtractor partitionValueExtractor,
      Configuration configuration) {
    this.catalogClient = catalogClient;
    this.hudiTableManager = hudiTableManager;
    this.partitionValuesExtractor = partitionValueExtractor;
    this.configuration = configuration;
  }

  HoodieTableMetaClient getMetaClient(String basePath) {
    Optional<HoodieTableMetaClient> metaClientOpt =
        hudiTableManager.loadTableMetaClientIfExists(basePath);

    if (!metaClientOpt.isPresent()) {
      throw new CatalogSyncException(
          "failed to get meta client since table is not present in the base path " + basePath);
    }

    return metaClientOpt.get();
  }

  /**
   * Syncs all partitions on storage to the catalog, by only making incremental changes.
   *
   * @param tableIdentifier The table in the catalog.
   * @return {@code true} if one or more partition(s) are changed in the catalog; {@code false}
   *     otherwise.
   */
  private boolean syncAllPartitions(
      HoodieTableMetaClient metaClient,
      InternalTable internalTable,
      CatalogTableIdentifier tableIdentifier) {
    try {
      if (internalTable.getPartitioningFields().isEmpty()) {
        return false;
      }

      List<CatalogPartition> allPartitionsInCatalog =
          catalogClient.getAllPartitions(tableIdentifier);
      List<String> allPartitionsOnStorage =
          getAllPartitionPathsOnStorage(internalTable.getBasePath());
      boolean partitionsChanged =
          syncPartitions(
              metaClient,
              tableIdentifier,
              getPartitionEvents(metaClient, allPartitionsInCatalog, allPartitionsOnStorage));
      if (partitionsChanged) {
        updateLastCommitTimeSynced(metaClient, tableIdentifier);
      }
      return partitionsChanged;
    } catch (Exception e) {
      throw new CatalogSyncException(
          "Failed to sync partitions for table " + tableIdentifier.getId(), e);
    }
  }

  /**
   * Syncs all partitions on storage to the catalog, by only making incremental changes.
   *
   * @param tableIdentifier The table in the catalog.
   * @return {@code true} if one or more partition(s) are changed in the catalog; {@code false}
   *     otherwise.
   */
  @Override
  public boolean syncPartitions(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    Map<String, String> lastCommitTimeSyncedProperties =
        catalogClient.getTableProperties(
            tableIdentifier,
            Arrays.asList(LAST_COMMIT_TIME_SYNC, LAST_COMMIT_COMPLETION_TIME_SYNC));
    Option<String> lastCommitTimeSynced =
        Option.ofNullable(lastCommitTimeSyncedProperties.get(LAST_COMMIT_TIME_SYNC));
    Option<String> lastCommitCompletionTimeSynced =
        Option.ofNullable(lastCommitTimeSyncedProperties.get(LAST_COMMIT_COMPLETION_TIME_SYNC));
    HoodieTableMetaClient metaClient = getMetaClient(table.getBasePath());
    if (!lastCommitTimeSynced.isPresent()
        || metaClient.getActiveTimeline().isBeforeTimelineStarts(lastCommitTimeSynced.get())) {
      // If the last commit time synced is before the start of the active timeline,
      // the Hive sync falls back to list all partitions on storage, instead of
      // reading active and archived timelines for written partitions.
      log.info(
          "Sync all partitions given the last commit time synced is empty or "
              + "before the start of the active timeline. Listing all partitions in "
              + table.getBasePath());
      return syncAllPartitions(metaClient, table, tableIdentifier);
    } else {
      List<String> writtenPartitionsSince =
          getWrittenPartitionsSince(
              metaClient,
              Option.ofNullable(lastCommitTimeSynced.get()),
              Option.ofNullable(lastCommitCompletionTimeSynced.get()));
      log.info("Storage partitions scan complete. Found " + writtenPartitionsSince.size());

      // Sync the partitions if needed
      // find dropped partitions, if any, in the latest commit
      Set<String> droppedPartitions =
          getDroppedPartitionsSince(
              metaClient,
              Option.ofNullable(lastCommitTimeSynced.get()),
              Option.of(lastCommitCompletionTimeSynced.get()));
      boolean partitionsChanged =
          syncPartitions(metaClient, tableIdentifier, writtenPartitionsSince, droppedPartitions);
      if (partitionsChanged) {
        updateLastCommitTimeSynced(metaClient, tableIdentifier);
      }
      return partitionsChanged;
    }
  }

  private void updateLastCommitTimeSynced(
      HoodieTableMetaClient metaClient, CatalogTableIdentifier tableIdentifier) {
    HoodieTimeline activeTimeline = metaClient.getActiveTimeline();
    Option<String> lastCommitSynced =
        activeTimeline.lastInstant().map(HoodieInstant::requestedTime);
    Option<String> lastCommitCompletionSynced =
        activeTimeline
            .getInstantsOrderedByCompletionTime()
            .skip(activeTimeline.countInstants() - 1L)
            .findFirst()
            .map(i -> Option.of(i.getCompletionTime()))
            .orElse(Option.empty());

    if (lastCommitSynced.isPresent()) {
      Map<String, String> lastSyncedProperties = new HashMap<>();
      lastSyncedProperties.put(LAST_COMMIT_TIME_SYNC, lastCommitSynced.get());
      lastSyncedProperties.put(LAST_COMMIT_COMPLETION_TIME_SYNC, lastCommitCompletionSynced.get());
      catalogClient.updateTableProperties(tableIdentifier, lastSyncedProperties);
    }
  }

  /**
   * Gets all relative partitions paths in the Hudi table on storage.
   *
   * @return All relative partitions paths.
   */
  public List<String> getAllPartitionPathsOnStorage(String basePath) {
    HoodieLocalEngineContext engineContext =
        new HoodieLocalEngineContext(getStorageConf(configuration));
    // ToDo - if we need to config to validate assumeDatePartitioning
    return FSUtils.getAllPartitionPaths(
        engineContext,
        hudiTableManager.loadTableMetaClientIfExists(basePath).get().getStorage(),
        new StoragePath(basePath),
        true);
  }

  public List<String> getWrittenPartitionsSince(
      HoodieTableMetaClient metaClient,
      Option<String> lastCommitTimeSynced,
      Option<String> lastCommitCompletionTimeSynced) {
    if (!lastCommitTimeSynced.isPresent()) {
      String basePath = metaClient.getBasePath().toUri().toString();
      log.info("Last commit time synced is not known, listing all partitions in " + basePath);
      return getAllPartitionPathsOnStorage(basePath);
    } else {
      log.info(
          "Last commit time synced is "
              + lastCommitTimeSynced.get()
              + ", Getting commits since then");
      return TimelineUtils.getWrittenPartitions(
          TimelineUtils.getCommitsTimelineAfter(
              metaClient, lastCommitTimeSynced.get(), lastCommitCompletionTimeSynced));
    }
  }

  /**
   * Get the set of dropped partitions since the last synced commit. If last sync time is not known
   * then consider only active timeline. Going through archive timeline is a costly operation, and
   * it should be avoided unless some start time is given.
   */
  private Set<String> getDroppedPartitionsSince(
      HoodieTableMetaClient metaClient,
      Option<String> lastCommitTimeSynced,
      Option<String> lastCommitCompletionTimeSynced) {
    return new HashSet<>(
        TimelineUtils.getDroppedPartitions(
            metaClient, lastCommitTimeSynced, lastCommitCompletionTimeSynced));
  }

  /**
   * Syncs added, updated, and dropped partitions to the catalog.
   *
   * @param tableIdentifier The table in the catalog.
   * @param partitionEventList The partition change event list.
   * @return {@code true} if one or more partition(s) are changed in the catalog; {@code false}
   *     otherwise.
   */
  private boolean syncPartitions(
      HoodieTableMetaClient metaClient,
      CatalogTableIdentifier tableIdentifier,
      List<CatalogPartitionEvent> partitionEventList) {
    List<CatalogPartition> newPartitions =
        filterPartitions(
            HadoopFSUtils.convertToHadoopPath(metaClient.getBasePath()),
            partitionEventList,
            CatalogPartitionEvent.PartitionEventType.ADD);
    if (!newPartitions.isEmpty()) {
      log.info("New Partitions " + newPartitions);
      catalogClient.addPartitionsToTable(tableIdentifier, newPartitions);
    }

    List<CatalogPartition> updatePartitions =
        filterPartitions(
            HadoopFSUtils.convertToHadoopPath(metaClient.getBasePath()),
            partitionEventList,
            CatalogPartitionEvent.PartitionEventType.UPDATE);
    if (!updatePartitions.isEmpty()) {
      log.info("Changed Partitions " + updatePartitions);
      catalogClient.updatePartitionsToTable(tableIdentifier, updatePartitions);
    }

    List<CatalogPartition> dropPartitions =
        filterPartitions(
            HadoopFSUtils.convertToHadoopPath(metaClient.getBasePath()),
            partitionEventList,
            CatalogPartitionEvent.PartitionEventType.DROP);
    if (!dropPartitions.isEmpty()) {
      log.info("Drop Partitions " + dropPartitions);
      catalogClient.dropPartitions(tableIdentifier, dropPartitions);
    }

    return !updatePartitions.isEmpty() || !newPartitions.isEmpty() || !dropPartitions.isEmpty();
  }

  private List<CatalogPartition> filterPartitions(
      Path basePath,
      List<CatalogPartitionEvent> events,
      CatalogPartitionEvent.PartitionEventType eventType) {
    return events.stream()
        .filter(s -> s.eventType == eventType)
        .map(
            s ->
                new CatalogPartition(
                    partitionValuesExtractor.extractPartitionValuesInPath(s.storagePartition),
                    new Path(basePath, s.storagePartition).toUri().toString()))
        .collect(Collectors.toList());
  }

  /**
   * Syncs the list of storage partitions passed in (checks if the partition is in hive, if not adds
   * it or if the partition path does not match, it updates the partition path).
   *
   * @param tableIdentifier The table name in the catalog.
   * @param writtenPartitionsSince Partitions has been added, updated, or dropped since last synced.
   * @param droppedPartitions Partitions that are dropped since last sync.
   * @return {@code true} if one or more partition(s) are changed in the catalog; {@code false}
   *     otherwise.
   */
  private boolean syncPartitions(
      HoodieTableMetaClient metaClient,
      CatalogTableIdentifier tableIdentifier,
      List<String> writtenPartitionsSince,
      Set<String> droppedPartitions) {
    try {
      if (writtenPartitionsSince.isEmpty()) {
        return false;
      }

      List<CatalogPartition> hivePartitions = getTablePartitions(tableIdentifier);
      return syncPartitions(
          metaClient,
          tableIdentifier,
          getPartitionEvents(
              metaClient, hivePartitions, writtenPartitionsSince, droppedPartitions));
    } catch (Exception e) {
      throw new CatalogSyncException(
          "Failed to sync partitions for table " + tableIdentifier.getId(), e);
    }
  }

  /**
   * Fetch partitions from meta service, will try to push down more filters to avoid fetching too
   * many unnecessary partitions.
   */
  private List<CatalogPartition> getTablePartitions(CatalogTableIdentifier tableIdentifier) {
    return catalogClient.getAllPartitions(tableIdentifier);
  }

  /**
   * Gets the partition events for changed partitions.
   *
   * <p>This compares the list of all partitions of a table stored in the catalog and on the
   * storage: (1) Partitions exist in the catalog, but NOT the storage: drops them in the catalog;
   * (2) Partitions exist on the storage, but NOT the catalog: adds them to the catalog; (3)
   * Partitions exist in both, but the partition path is different: update them in the catalog.
   *
   * @param allPartitionsInCatalog All partitions of a table stored in the catalog.
   * @param allPartitionsOnStorage All partitions of a table stored on the storage.
   * @return partition events for changed partitions.
   */
  private List<CatalogPartitionEvent> getPartitionEvents(
      HoodieTableMetaClient metaClient,
      List<CatalogPartition> allPartitionsInCatalog,
      List<String> allPartitionsOnStorage) {
    Map<String, String> paths = getPartitionValuesToPathMapping(allPartitionsInCatalog);
    Set<String> partitionsToDrop = new HashSet<>(paths.keySet());

    List<CatalogPartitionEvent> events = new ArrayList<>();
    for (String storagePartition : allPartitionsOnStorage) {
      Path storagePartitionPath =
          HadoopFSUtils.convertToHadoopPath(
              FSUtils.constructAbsolutePath(metaClient.getBasePath(), storagePartition));
      String fullStoragePartitionPath =
          Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
      List<String> storagePartitionValues =
          partitionValuesExtractor.extractPartitionValuesInPath(storagePartition);

      if (!storagePartitionValues.isEmpty()) {
        String storageValue = String.join(", ", storagePartitionValues);
        // Remove partitions that exist on storage from the `partitionsToDrop` set,
        // so the remaining partitions that exist in the catalog should be dropped
        partitionsToDrop.remove(storageValue);
        if (!paths.containsKey(storageValue)) {
          events.add(CatalogPartitionEvent.newPartitionAddEvent(storagePartition));
        } else if (!paths.get(storageValue).equals(fullStoragePartitionPath)) {
          events.add(CatalogPartitionEvent.newPartitionUpdateEvent(storagePartition));
        }
      }
    }

    partitionsToDrop.forEach(
        storageValue -> {
          String storagePath = paths.get(storageValue);
          try {
            String relativePath =
                FSUtils.getRelativePartitionPath(
                    metaClient.getBasePath(), new StoragePath(storagePath));
            events.add(CatalogPartitionEvent.newPartitionDropEvent(relativePath));
          } catch (IllegalArgumentException e) {
            log.error(
                "Cannot parse the path stored in the catalog, ignoring it for "
                    + "generating DROP partition event: \""
                    + storagePath
                    + "\".",
                e);
          }
        });
    return events;
  }

  /**
   * Iterate over the storage partitions and find if there are any new partitions that need to be
   * added or updated. Generate a list of PartitionEvent based on the changes required.
   */
  public List<CatalogPartitionEvent> getPartitionEvents(
      HoodieTableMetaClient metaClient,
      List<CatalogPartition> partitionsInCatalog,
      List<String> writtenPartitionsOnStorage,
      Set<String> droppedPartitionsOnStorage) {
    Map<String, String> paths = getPartitionValuesToPathMapping(partitionsInCatalog);

    List<CatalogPartitionEvent> events = new ArrayList<>();
    for (String storagePartition : writtenPartitionsOnStorage) {
      Path storagePartitionPath =
          HadoopFSUtils.convertToHadoopPath(
              FSUtils.constructAbsolutePath(metaClient.getBasePath(), storagePartition));
      String fullStoragePartitionPath =
          Path.getPathWithoutSchemeAndAuthority(storagePartitionPath).toUri().getPath();
      List<String> storagePartitionValues =
          partitionValuesExtractor.extractPartitionValuesInPath(storagePartition);

      if (droppedPartitionsOnStorage.contains(storagePartition)) {
        events.add(CatalogPartitionEvent.newPartitionDropEvent(storagePartition));
      } else {
        if (!storagePartitionValues.isEmpty()) {
          String storageValue = String.join(", ", storagePartitionValues);
          if (!paths.containsKey(storageValue)) {
            events.add(CatalogPartitionEvent.newPartitionAddEvent(storagePartition));
          } else if (!paths.get(storageValue).equals(fullStoragePartitionPath)) {
            events.add(CatalogPartitionEvent.newPartitionUpdateEvent(storagePartition));
          }
        }
      }
    }
    return events;
  }

  /**
   * Gets the partition values to the absolute path mapping based on the partition information from
   * the catalog.
   *
   * @param partitionsInCatalog Partitions in the catalog.
   * @return The partition values to the absolute path mapping.
   */
  private Map<String, String> getPartitionValuesToPathMapping(
      List<CatalogPartition> partitionsInCatalog) {
    Map<String, String> paths = new HashMap<>();
    for (CatalogPartition tablePartition : partitionsInCatalog) {
      List<String> hivePartitionValues = tablePartition.getValues();
      String fullTablePartitionPath =
          Path.getPathWithoutSchemeAndAuthority(new Path(tablePartition.getStorageLocation()))
              .toUri()
              .getPath();
      paths.put(String.join(", ", hivePartitionValues), fullTablePartitionPath);
    }
    return paths;
  }
}
