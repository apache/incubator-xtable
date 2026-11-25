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
 
package org.apache.xtable;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.FormatMetaColumn;
import org.apache.hudi.common.HoodieTableFormat;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.TableMetadataFactory;
import org.apache.hudi.storage.StoragePath;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import org.apache.xtable.conversion.ConversionTargetFactory;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.exception.ReadException;
import org.apache.xtable.exception.UpdateException;
import org.apache.xtable.hudi.HudiDataFileExtractor;
import org.apache.xtable.hudi.HudiFileStatsExtractor;
import org.apache.xtable.hudi.HudiIncrementalTableChangeExtractor;
import org.apache.xtable.hudi.HudiInstantUtils;
import org.apache.xtable.hudi.HudiSchemaExtractor;
import org.apache.xtable.hudi.HudiSourceConfig;
import org.apache.xtable.hudi.HudiTableExtractor;
import org.apache.xtable.hudi.PathBasedPartitionSpecExtractor;
import org.apache.xtable.hudi.PathBasedPartitionValuesExtractor;
import org.apache.xtable.iceberg.IcebergConversionSource;
import org.apache.xtable.iceberg.IcebergConversionTarget;
import org.apache.xtable.iceberg.IcebergTableManager;
import org.apache.xtable.metadata.IcebergMetadataFactory;
import org.apache.xtable.model.IncrementalTableChanges;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFile;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.model.sync.SyncStatusCode;
import org.apache.xtable.spi.sync.TableFormatSync;
import org.apache.xtable.timeline.IcebergActiveTimeline;
import org.apache.xtable.timeline.IcebergSnapshotInstants;
import org.apache.xtable.timeline.IcebergTimelineArchiver;
import org.apache.xtable.timeline.IcebergTimelineFactory;

@Log4j2
public class IcebergTableFormat implements HoodieTableFormat {
  /**
   * The partition type a custom key generator records for a field whose value is the partition path
   * as is. See {@code CustomAvroKeyGenerator.PartitionKeyType} in Hudi.
   */
  private static final String CUSTOM_KEY_GENERATOR_SIMPLE_PARTITION_TYPE = "SIMPLE";

  private transient TableFormatSync tableFormatSync;

  /** Whether rewritten base files keep the Iceberg row lineage, see {@link IcebergRowLineage}. */
  private boolean rowLineage;

  public IcebergTableFormat() {}

  /**
   * Hudi passes the table's {@code hoodie.properties} here on every meta client load, which is the
   * earliest point an unsupported table shape or invalid Iceberg settings can be refused: before
   * any instant exists.
   */
  @Override
  public void init(Properties properties) {
    HoodieConfig tableConfig = new HoodieConfig(TypedProperties.copy(properties));
    requireTableVersionEightOrLater(tableConfig);
    IcebergFormatConfig formatConfig = IcebergFormatConfig.of(properties, null);
    requireDeletionVectorsForMergeOnRead(tableConfig, formatConfig);
    this.rowLineage = formatConfig.getFormatVersion() >= 3;
    this.tableFormatSync = TableFormatSync.getInstance();
  }

  /**
   * At format version 3 the row lineage columns, which Hudi then stores in every base file it
   * rewrites, see {@link IcebergRowLineage}. The version is the one the table config sets, as that
   * is all a table format sees here; when the Iceberg table is at version 2 the columns stay null.
   */
  @Override
  public List<FormatMetaColumn> getFormatMetaColumns() {
    return rowLineage ? IcebergRowLineage.columns() : Collections.emptyList();
  }

  @Override
  public String getName() {
    return org.apache.xtable.model.storage.TableFormat.ICEBERG;
  }

  @Override
  public void commit(
      HoodieCommitMetadata commitMetadata,
      HoodieInstant completedInstant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
    IcebergRowLineage.requireRowLineageInRewrittenBaseFiles(
        commitMetadata, completedInstant, engineContext, metaClient, rowLineage);
    HudiIncrementalTableChangeExtractor hudiTableExtractor =
        getHudiTableExtractor(metaClient, viewManager);
    IncrementalTableChanges changes =
        hudiTableExtractor.extractTableChanges(commitMetadata, completedInstant);
    boolean rewrite =
        rewritesWithoutChangingRows(commitMetadata, completedInstant, metaClient.getTableConfig());
    boolean deleteOperation =
        !rewrite && WriteOperationType.isDelete(commitMetadata.getOperationType());
    HudiPositionalDeleteExtractor positionalDeletes =
        new HudiPositionalDeleteExtractor(metaClient, viewManager);
    completeInstant(
        metaClient,
        changes,
        isMergeOnRead(metaClient.getTableConfig())
            ? positionalDeletes.positionsDeletedBy(commitMetadata, completedInstant)
            : null,
        false,
        deleteOperation,
        positionalDeletes::holdsNoLiveRows,
        rewrite);
  }

  /**
   * Whether the instant rewrites base files without changing any row: a clustering, or a
   * compaction, which on a merge-on-read table is the only instant that completes as a commit, see
   * {@link HudiPositionalDeleteExtractor#positionsDeletedBy}. Iceberg publishes such a rewrite as a
   * replace, which streaming readers skip. An insert that Hudi packs into a small base file also
   * rewrites the file, but adds rows.
   */
  static boolean rewritesWithoutChangingRows(
      HoodieCommitMetadata commitMetadata, HoodieInstant instant, HoodieConfig tableConfig) {
    return HoodieTimeline.CLUSTERING_ACTION.equals(instant.getAction())
        || commitMetadata.getOperationType() == WriteOperationType.CLUSTER
        || (isMergeOnRead(tableConfig) && HoodieTimeline.COMMIT_ACTION.equals(instant.getAction()));
  }

  @Override
  public void clean(
      HoodieCleanMetadata cleanMetadata,
      HoodieInstant completedInstant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
    HudiIncrementalTableChangeExtractor hudiTableExtractor =
        getHudiTableExtractor(metaClient, viewManager);
    completeInstant(metaClient, hudiTableExtractor.extractTableChanges(completedInstant));
  }

  @Override
  public void archive(
      Supplier<List<HoodieInstant>> archivedInstants,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
    HudiIncrementalTableChangeExtractor hudiTableExtractor =
        getHudiTableExtractor(metaClient, viewManager);
    InternalTable internalTable =
        hudiTableExtractor
            .getTableExtractor()
            .table(
                metaClient,
                metaClient.getActiveTimeline().filterCompletedInstants().lastInstant().get());
    List<HoodieInstant> archived = archivedInstants.get();
    archiveInstants(metaClient, internalTable, archived);
    warnIfArchivedAheadOfMetadataTableCompaction(engineContext, metaClient, archived);
  }

  /**
   * Hudi's archiver keeps a commit active until the metadata table has compacted its log block, but
   * applies that bound to the native format only, so under this format the archiver can run ahead
   * of the compaction. The instants are already archived when this hook runs, and the write has
   * committed in Hudi and Iceberg, so this only warns: failing here would fail a committed write. A
   * metadata table without a compaction yet, such as a freshly built one, is not reported, and
   * neither is one that cannot be read, as when this write turned it off.
   */
  static void warnIfArchivedAheadOfMetadataTableCompaction(
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      List<HoodieInstant> archivedInstants) {
    if (!metaClient.getTableConfig().isMetadataTableAvailable()) {
      return;
    }
    Optional<HoodieInstant> latestArchivedCommit =
        archivedInstants.stream()
            .filter(HoodieInstant::isCompleted)
            .filter(instant -> !HoodieTimeline.CLEAN_ACTION.equals(instant.getAction()))
            .filter(instant -> !HoodieTimeline.SAVEPOINT_ACTION.equals(instant.getAction()))
            .max(Comparator.comparing(HoodieInstant::requestedTime));
    if (!latestArchivedCommit.isPresent()) {
      return;
    }
    Option<String> latestCompaction;
    try (HoodieTableMetadata metadata =
        IcebergMetadataFactory.getInstance()
            .create(
                engineContext,
                metaClient.getStorage(),
                HoodieMetadataConfig.newBuilder().enable(true).build(),
                metaClient.getBasePath().toString(),
                false)) {
      latestCompaction = metadata.getLatestCompactionTime();
    } catch (Exception e) {
      log.warn("Unable to read the latest compaction of the Hudi metadata table", e);
      return;
    }
    if (latestCompaction.isPresent()
        && InstantComparison.compareTimestamps(
            latestArchivedCommit.get().requestedTime(),
            InstantComparison.GREATER_THAN_OR_EQUALS,
            latestCompaction.get())) {
      log.warn(
          "Table {} archived {} before the Hudi metadata table compacted it (latest metadata table"
              + " compaction: {}); keep {} above {} so the metadata table listing stays complete",
          metaClient.getTableConfig().getTableName(),
          latestArchivedCommit.get(),
          latestCompaction.get(),
          HoodieArchivalConfig.MIN_COMMITS_TO_KEEP.key(),
          HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key());
    }
  }

  /**
   * Iceberg is updated in {@link #completedRollback} instead, once Hudi has deleted the files and
   * recorded the rollback. A rollback is represented the way Hudi records it, as a forward instant
   * whose snapshot removes the rolled-back files and restores the previous file versions, rather
   * than by rewinding snapshot history, which would also hide the clean and rollback instants
   * recorded after the commit and make them pending again.
   *
   * <p>A rollback that belongs to a restore is refused here, before Hudi deletes any data file:
   * Hudi runs those rollbacks without publishing them, so {@link #completedRollback} would never
   * fire and Iceberg would keep pointing at the deleted files.
   */
  @Override
  public void rollback(
      HoodieInstant completedInstant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
    if (!metaClient
        .reloadActiveTimeline()
        .getRestoreTimeline()
        .filterInflightsAndRequested()
        .empty()) {
      throw IcebergActiveTimeline.restoreNotSupported(metaClient.getTableConfig());
    }
  }

  /**
   * Reached only when a restore rolled back nothing that Iceberg had recorded; see {@link
   * IcebergActiveTimeline#restoreNotSupported}. Hudi has already completed the restore instant by
   * now, and without a snapshot the reconstructed timeline reports it pending, so the instant has
   * to be removed from the timeline by hand before the table accepts writes again.
   */
  @Override
  public void restore(
      HoodieInstant restoreCompletedInstant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
    throw IcebergActiveTimeline.restoreNotSupported(metaClient.getTableConfig());
  }

  @Override
  public void completedRollback(
      HoodieInstant rollbackInstant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
    metaClient.reloadActiveTimeline();
    if (!icebergTableExists(metaClient)) {
      // The rollback undid a first commit whose snapshot never landed. Iceberg recorded nothing to
      // remove and has no table to record the rollback in; the reconstructed timeline reports the
      // rollback completed until the first snapshot lands, and as history after that.
      return;
    }
    HoodieRollbackMetadata rollbackMetadata;
    try {
      rollbackMetadata = metaClient.getActiveTimeline().readRollbackMetadata(rollbackInstant);
    } catch (IOException e) {
      throw new ReadException("Unable to read rollback metadata for " + rollbackInstant, e);
    }
    HudiIncrementalTableChangeExtractor hudiTableExtractor =
        getHudiTableExtractor(metaClient, viewManager);
    Table table = icebergTable(metaClient);
    List<Snapshot> ancestors = IcebergSnapshotInstants.ancestorsOldestFirst(table);
    Set<String> publishedCommitTimes = publishedCommitTimes(ancestors, metaClient);
    List<String> filesRemovedByRolledBackCommits =
        filesRemovedByCommits(
            table, ancestors, metaClient, new HashSet<>(rollbackMetadata.getCommitsRollback()));
    IncrementalTableChanges changes;
    if (metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().empty()) {
      // Every commit has been rolled back, so Hudi has no commit to take the schema from. The
      // table itself is unchanged by the rollback, so describe it from the Iceberg table.
      changes =
          hudiTableExtractor.extractTableChanges(
              rollbackMetadata,
              rollbackInstant,
              publishedCommitTimes,
              filesRemovedByRolledBackCommits,
              tableAsRecordedInIceberg(metaClient, rollbackInstant));
    } else {
      changes =
          hudiTableExtractor.extractTableChanges(
              rollbackMetadata,
              rollbackInstant,
              publishedCommitTimes,
              filesRemovedByRolledBackCommits);
    }
    if (isMergeOnRead(metaClient.getTableConfig())) {
      // The log files the rollback deleted no longer delete anything, and a base file it restored
      // is deleted from by the log files its file group still has.
      HudiPositionalDeleteExtractor positionalDeletes =
          new HudiPositionalDeleteExtractor(metaClient, viewManager);
      completeInstant(
          metaClient,
          changes,
          positionalDeletes.positionsDeletedAfter(
              rollbackMetadata, publishedCommitTimes, filesRemovedByRolledBackCommits),
          true,
          false,
          positionalDeletes::holdsNoLiveRows,
          false);
    } else {
      completeInstant(metaClient, changes);
    }
  }

  private InternalTable tableAsRecordedInIceberg(
      HoodieTableMetaClient metaClient, HoodieInstant instant) {
    IcebergFormatConfig config = IcebergFormatConfig.of(metaClient);
    SourceTable icebergTable =
        SourceTable.builder()
            .name(metaClient.getTableConfig().getTableName())
            .formatName(org.apache.xtable.model.storage.TableFormat.ICEBERG)
            .basePath(metaClient.getBasePath().toString())
            .namespace(config.getNamespace())
            .catalogConfig(config.getCatalogConfig())
            .build();
    InternalTable current =
        IcebergConversionSource.builder()
            .hadoopConf((Configuration) metaClient.getStorageConf().unwrap())
            .sourceTableConfig(icebergTable)
            .build()
            .getCurrentTable();
    return current.toBuilder()
        .tableFormat(org.apache.xtable.model.storage.TableFormat.HUDI)
        .layoutStrategy(
            current.getPartitioningFields().isEmpty()
                ? DataLayoutStrategy.FLAT
                : DataLayoutStrategy.DIR_HIERARCHY_PARTITION_VALUES)
        .latestMetadataPath(metaClient.getMetaPath().toString())
        .latestCommitTime(HudiInstantUtils.getSyncInstant(metaClient, instant))
        .latestTableOperationIdentifier(HudiTableExtractor.tableOperationIdentifier(instant))
        .build();
  }

  private static boolean icebergTableExists(HoodieTableMetaClient metaClient) {
    return IcebergFormatConfig.of(metaClient)
        .tableExists(
            IcebergTableManager.of((Configuration) metaClient.getStorageConf().unwrap()),
            metaClient);
  }

  static Table icebergTable(HoodieTableMetaClient metaClient) {
    return IcebergFormatConfig.of(metaClient)
        .getTable(
            IcebergTableManager.of((Configuration) metaClient.getStorageConf().unwrap()),
            metaClient);
  }

  /**
   * Requested times of the instants recorded in the current Iceberg snapshot's ancestry. A commit
   * completed in Hudi but never recorded in Iceberg, because the writer died in between, has no
   * files in Iceberg for its rollback to remove.
   */
  private static Set<String> publishedCommitTimes(
      List<Snapshot> ancestors, HoodieTableMetaClient metaClient) {
    return ancestors.stream()
        .map(
            snapshot ->
                IcebergSnapshotInstants.recordedInstant(snapshot, metaClient.getInstantGenerator())
                    .requestedTime())
        .collect(Collectors.toSet());
  }

  /**
   * Paths of the data files the snapshots recording the given commits removed: the versions an
   * upsert superseded and the file groups a clustering, an insert overwrite or a delete partition
   * replaced. A savepoint shares its commit's requested time and removes nothing, so it is skipped.
   */
  private static List<String> filesRemovedByCommits(
      Table table,
      List<Snapshot> ancestors,
      HoodieTableMetaClient metaClient,
      Set<String> commitTimes) {
    List<String> removedFiles = new ArrayList<>();
    for (Snapshot snapshot : ancestors) {
      HoodieInstant recorded =
          IcebergSnapshotInstants.recordedInstant(snapshot, metaClient.getInstantGenerator());
      if (commitTimes.contains(recorded.requestedTime())
          && !HoodieTimeline.SAVEPOINT_ACTION.equals(recorded.getAction())) {
        snapshot.removedDataFiles(table.io()).forEach(file -> removedFiles.add(file.location()));
      }
    }
    return removedFiles;
  }

  @Override
  public void savepoint(
      HoodieInstant instant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
    HudiIncrementalTableChangeExtractor hudiTableExtractor =
        getHudiTableExtractor(metaClient, viewManager);
    completeInstant(metaClient, hudiTableExtractor.extractTableChanges(instant));
  }

  @Override
  public TimelineFactory getTimelineFactory() {
    return new IcebergTimelineFactory(new HoodieConfig());
  }

  @Override
  public TableMetadataFactory getMetadataFactory() {
    return IcebergMetadataFactory.getInstance();
  }

  private void completeInstant(HoodieTableMetaClient metaClient, IncrementalTableChanges changes) {
    completeInstant(metaClient, changes, null, false, false, null, false);
  }

  /**
   * @param deletedPositionsByBaseFile row positions deleted from base files, to record as deletion
   *     vectors in the snapshot of the instant; null for an instant of a table without any
   * @param replaceDeletionVectors whether the positions replace the current deletion vector of each
   *     base file rather than being merged into it
   * @param deleteOperation whether the instant is a Hudi delete, which Iceberg labels a delete even
   *     when it matched no rows
   * @param withoutLiveRows whether no row of the base file at a path is live, for a base file
   *     Iceberg does not hold since it holds no rows or a delete removed all of them; null when
   *     there is none
   * @param rewrite whether the instant rewrites base files without changing any row, which Iceberg
   *     publishes as a replace
   */
  private void completeInstant(
      HoodieTableMetaClient metaClient,
      IncrementalTableChanges changes,
      Map<String, Set<Long>> deletedPositionsByBaseFile,
      boolean replaceDeletionVectors,
      boolean deleteOperation,
      Predicate<String> withoutLiveRows,
      boolean rewrite) {
    IcebergConversionTarget target = getIcebergConversionTarget(metaClient);
    if (withoutLiveRows != null) {
      target.stageDataFilesWithoutLiveRows(withoutLiveRows);
    }
    if (rewrite) {
      target.stageRewrite();
    }
    if (deletedPositionsByBaseFile != null) {
      List<TableChange> tableChanges = new ArrayList<>();
      changes.getTableChanges().forEachRemaining(tableChanges::add);
      Set<List<PartitionValue>> partitions = new LinkedHashSet<>();
      for (TableChange change : tableChanges) {
        partitions.addAll(
            partitionsOf(
                metaClient,
                change.getTableAsOfChange().getPartitioningFields(),
                deletedPositionsByBaseFile.keySet()));
      }
      target.stageDeletionVectors(deletedPositionsByBaseFile, partitions, replaceDeletionVectors);
      changes =
          IncrementalTableChanges.builder()
              .tableChanges(tableChanges.iterator())
              .pendingCommits(changes.getPendingCommits())
              .build();
    } else {
      changes = withoutAddedFilesWithoutRows(changes);
    }
    if (deleteOperation) {
      target.stageDeleteOperation();
    }
    TableSyncMetadata tableSyncMetadata =
        target
            .getTableMetadata()
            .orElse(TableSyncMetadata.of(Instant.MIN, Collections.emptyList()));
    Map<String, List<SyncResult>> results;
    try {
      results =
          tableFormatSync.syncChanges(Collections.singletonMap(target, tableSyncMetadata), changes);
    } catch (Exception e) {
      throw new UpdateException("Failed to update iceberg metadata", e);
    }
    failUnlessSynced(results);
  }

  /**
   * Leaves the data files without rows out of the files an instant adds. A copy-on-write delete of
   * every row of a file group writes a base file without rows, where Iceberg removes the data file,
   * so the snapshot is a delete as Iceberg's own writer publishes it. A later instant that replaces
   * such a file then removes a path Iceberg does not hold, which an overwrite ignores; a row delta
   * fails on it, which is why only the diffs published without deletion vectors are filtered.
   */
  private static IncrementalTableChanges withoutAddedFilesWithoutRows(
      IncrementalTableChanges changes) {
    List<TableChange> tableChanges = new ArrayList<>();
    changes
        .getTableChanges()
        .forEachRemaining(change -> tableChanges.add(withoutAddedFilesWithoutRows(change)));
    return IncrementalTableChanges.builder()
        .tableChanges(tableChanges.iterator())
        .pendingCommits(changes.getPendingCommits())
        .build();
  }

  private static TableChange withoutAddedFilesWithoutRows(TableChange change) {
    List<InternalFile> filesAdded =
        change.getFilesDiff().getFilesAdded().stream()
            .filter(
                file ->
                    !(file instanceof InternalDataFile)
                        || ((InternalDataFile) file).getRecordCount() > 0)
            .collect(Collectors.toList());
    return change.toBuilder()
        .filesDiff(
            InternalFilesDiff.builder()
                .filesAdded(filesAdded)
                .filesRemoved(change.getFilesDiff().getFilesRemoved())
                .build())
        .build();
  }

  /**
   * {@link TableFormatSync} reports a failed sync in its result rather than throwing. The Hudi
   * instant is already complete when a hook runs, so a failure that is not surfaced would leave the
   * instant without a snapshot, and the next writer would roll it back as if the write had never
   * succeeded. Failing the hook fails the Hudi operation instead, so the user sees the error and
   * retries.
   */
  static void failUnlessSynced(Map<String, List<SyncResult>> results) {
    List<SyncResult> icebergResults =
        results.get(org.apache.xtable.model.storage.TableFormat.ICEBERG);
    if (icebergResults == null || icebergResults.isEmpty()) {
      throw new UpdateException(
          "The Iceberg sync produced no result, so the instant is not recorded in Iceberg");
    }
    for (SyncResult result : icebergResults) {
      SyncResult.SyncStatus status = result.getTableFormatSyncStatus();
      if (status == null || status.getStatusCode() != SyncStatusCode.SUCCESS) {
        String detail =
            status == null || status.getErrorDetails() == null
                ? "no error details"
                : status.getErrorDetails().getErrorMessage();
        throw new UpdateException("Failed to record the instant in Iceberg: " + detail);
      }
    }
  }

  private void archiveInstants(
      HoodieTableMetaClient metaClient,
      InternalTable internalTable,
      List<HoodieInstant> archivedInstants) {
    IcebergConversionTarget target = getIcebergConversionTarget(metaClient);
    IcebergTimelineArchiver timelineArchiver = new IcebergTimelineArchiver(metaClient, target);
    timelineArchiver.archiveInstants(internalTable, archivedInstants);
  }

  /**
   * A merge-on-read table is published as base files with deletion vectors, which Iceberg has from
   * format version 3 on. The table type and the configured version are known before any instant
   * exists; whether the writer keeps its log files to positional deletes is not, and is checked as
   * each deltacommit is published, see {@link HudiPositionalDeleteExtractor}.
   *
   * @throws UnsupportedOperationException when the table is merge-on-read and set to an Iceberg
   *     format version below 3
   */
  static void requireDeletionVectorsForMergeOnRead(
      HoodieConfig tableConfig, IcebergFormatConfig formatConfig) {
    if (isMergeOnRead(tableConfig) && formatConfig.getFormatVersion() < 3) {
      throw new UnsupportedOperationException(
          String.format(
              "The Iceberg table format publishes a %s table with deletion vectors, which need"
                  + " Iceberg format version 3, but table %s is set to format version %d",
              HoodieTableType.MERGE_ON_READ,
              tableConfig.getStringOrDefault(HoodieTableConfig.NAME, ""),
              formatConfig.getFormatVersion()));
    }
  }

  private static boolean isMergeOnRead(HoodieConfig tableConfig) {
    return HoodieTableType.MERGE_ON_READ
        .name()
        .equals(tableConfig.getStringOrDefault(HoodieTableConfig.TYPE));
  }

  /**
   * The reconstructed timeline is built on the version 2 timeline layout, which table versions
   * below 8 do not have, and on version 6 a savepoint, which shares its commit's requested time,
   * would never count as newer than the last synced instant.
   *
   * @throws UnsupportedOperationException when the table version is below 8
   */
  static void requireTableVersionEightOrLater(HoodieConfig tableConfig) {
    HoodieTableVersion version = HoodieTableConfig.getTableVersion(tableConfig);
    if (version.lesserThan(HoodieTableVersion.EIGHT)) {
      throw new UnsupportedOperationException(
          String.format(
              "The Iceberg table format requires table version %d or later, but table %s is at"
                  + " version %d",
              HoodieTableVersion.EIGHT.versionCode(),
              tableConfig.getStringOrDefault(HoodieTableConfig.NAME, ""),
              version.versionCode()));
    }
  }

  /**
   * Maps the table's Hudi partition fields to the XTable partition spec. Only identity partitioning
   * is supported: each field becomes a {@link PartitionTransformType#VALUE} Iceberg partition field
   * of the source column's type. A timestamp-based key generator, or a custom key generator with a
   * timestamp-typed field, derives the partition path from a formatted timestamp, and the format
   * needed to map it back lives in the writer's key generator configuration rather than in {@code
   * hoodie.properties}, so such tables are rejected rather than exposed with a partition field
   * whose values do not match the source column.
   *
   * @return the spec in the {@code field:VALUE,...} form that {@link HudiSourceConfig} parses, or
   *     null for an unpartitioned table
   * @throws UnsupportedOperationException when a partition field is not identity-partitioned
   */
  static String partitionFieldSpec(HoodieTableConfig tableConfig) {
    Option<String[]> partitionFields = tableConfig.getPartitionFields();
    if (!partitionFields.isPresent() || partitionFields.get().length == 0) {
      return null;
    }
    String keyGeneratorClassName = tableConfig.getKeyGeneratorClassName();
    KeyGeneratorType keyGeneratorType =
        keyGeneratorClassName == null
            ? null
            : KeyGeneratorType.fromClassName(keyGeneratorClassName);
    if (keyGeneratorType == KeyGeneratorType.TIMESTAMP
        || keyGeneratorType == KeyGeneratorType.TIMESTAMP_AVRO) {
      throw new UnsupportedOperationException(
          String.format(
              "The Iceberg table format only supports identity partitioning, but table %s uses the "
                  + "timestamp-based key generator %s",
              tableConfig.getTableName(), keyGeneratorClassName));
    }
    if (keyGeneratorType == KeyGeneratorType.CUSTOM
        || keyGeneratorType == KeyGeneratorType.CUSTOM_AVRO) {
      for (String fieldWithType :
          tableConfig.getString(HoodieTableConfig.PARTITION_FIELDS).split(",")) {
        String[] parts = fieldWithType.trim().split(":");
        if (parts.length > 1
            && !CUSTOM_KEY_GENERATOR_SIMPLE_PARTITION_TYPE.equalsIgnoreCase(parts[1])) {
          throw new UnsupportedOperationException(
              String.format(
                  "The Iceberg table format only supports identity partitioning, but partition "
                      + "field %s of table %s has partition type %s",
                  parts[0], tableConfig.getTableName(), parts[1]));
        }
      }
    }
    return Arrays.stream(partitionFields.get())
        .map(field -> field + ":" + PartitionTransformType.VALUE)
        .collect(Collectors.joining(","));
  }

  private HudiIncrementalTableChangeExtractor getHudiTableExtractor(
      HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    // The Hadoop configuration can also set the format version, which init does not see.
    requireDeletionVectorsForMergeOnRead(
        metaClient.getTableConfig(), IcebergFormatConfig.of(metaClient));
    PathBasedPartitionSpecExtractor sourcePartitionSpecExtractor =
        sourcePartitionSpecExtractor(metaClient.getTableConfig());
    return new HudiIncrementalTableChangeExtractor(
        metaClient,
        // The Iceberg schema is the table's own columns. The base files also store Hudi's meta
        // columns, which Iceberg readers skip, as they resolve columns through the name mapping.
        new HudiTableExtractor(new HudiSchemaExtractor(), sourcePartitionSpecExtractor, false),
        new HudiDataFileExtractor(
            metaClient,
            new PathBasedPartitionValuesExtractor(
                sourcePartitionSpecExtractor.getPathToPartitionFieldFormat()),
            new HudiFileStatsExtractor(metaClient),
            viewManager));
  }

  static PathBasedPartitionSpecExtractor sourcePartitionSpecExtractor(
      HoodieTableConfig tableConfig) {
    return HudiSourceConfig.fromPartitionFieldSpecConfig(partitionFieldSpec(tableConfig))
        .loadSourcePartitionSpecExtractor();
  }

  /**
   * The spelling of a base file's path in Iceberg, qualified with the storage scheme and authority
   * the way {@code HudiDataFileExtractor} adds the file.
   */
  static String qualifiedPath(HoodieTableMetaClient metaClient, HoodieBaseFile baseFile) {
    return new StoragePath(baseFile.getPath())
        .makeQualified(metaClient.getStorage().getUri())
        .toString();
  }

  /** The partition values of the partitions holding the given base files. */
  private static Set<List<PartitionValue>> partitionsOf(
      HoodieTableMetaClient metaClient,
      List<InternalPartitionField> partitionFields,
      Collection<String> baseFiles) {
    PathBasedPartitionValuesExtractor partitionValuesExtractor =
        new PathBasedPartitionValuesExtractor(
            sourcePartitionSpecExtractor(metaClient.getTableConfig())
                .getPathToPartitionFieldFormat());
    return baseFiles.stream()
        .map(
            baseFile ->
                FSUtils.getRelativePartitionPath(
                    metaClient.getBasePath(), new StoragePath(baseFile).getParent()))
        .distinct()
        .map(
            partitionPath ->
                partitionValuesExtractor.extractPartitionValues(partitionFields, partitionPath))
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  private IcebergConversionTarget getIcebergConversionTarget(HoodieTableMetaClient metaClient) {
    IcebergFormatConfig config = IcebergFormatConfig.of(metaClient);
    TargetTable targetTable =
        targetTable(metaClient.getTableConfig().getTableName(), metaClient.getBasePath().toString())
            .toBuilder()
            .namespace(config.getNamespace())
            .catalogConfig(config.getCatalogConfig())
            .build();
    // A copy, since the meta client's configuration is shared with the rest of the writer.
    Configuration conf = new Configuration((Configuration) metaClient.getStorageConf().unwrap());
    conf.setInt(IcebergTableManager.ICEBERG_FORMAT_VERSION, config.getFormatVersion());
    conf.setBoolean(IcebergTableManager.ICEBERG_REMOVE_PLACEHOLDER_SCHEMA, true);
    return (IcebergConversionTarget)
        ConversionTargetFactory.getInstance().createForFormat(targetTable, conf);
  }

  /**
   * Iceberg snapshots are expired only as Hudi archives the instants they record, since the
   * reconstructed timeline treats a completed instant without a snapshot as inflight; time-based
   * expiry would otherwise remove snapshots the active timeline still needs.
   */
  static TargetTable targetTable(String tableName, String basePath) {
    return TargetTable.builder()
        .name(tableName)
        .formatName(org.apache.xtable.model.storage.TableFormat.ICEBERG)
        .basePath(basePath)
        .metadataRetention(TargetTable.NO_METADATA_EXPIRY)
        .build();
  }
}
