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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.HoodieTableFormat;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.keygen.constant.KeyGeneratorType;
import org.apache.hudi.metadata.TableMetadataFactory;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

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
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.model.sync.SyncStatusCode;
import org.apache.xtable.spi.sync.TableFormatSync;
import org.apache.xtable.timeline.IcebergActiveTimeline;
import org.apache.xtable.timeline.IcebergSnapshotInstants;
import org.apache.xtable.timeline.IcebergTimelineArchiver;
import org.apache.xtable.timeline.IcebergTimelineFactory;

public class IcebergTableFormat implements HoodieTableFormat {
  /**
   * The partition type a custom key generator records for a field whose value is the partition path
   * as is. See {@code CustomAvroKeyGenerator.PartitionKeyType} in Hudi.
   */
  private static final String CUSTOM_KEY_GENERATOR_SIMPLE_PARTITION_TYPE = "SIMPLE";

  private transient TableFormatSync tableFormatSync;

  public IcebergTableFormat() {}

  /**
   * Hudi passes the table's {@code hoodie.properties} here on every meta client load, which is the
   * earliest point an unsupported table shape can be refused: before any instant exists.
   */
  @Override
  public void init(Properties properties) {
    requireCopyOnWrite(new HoodieConfig(TypedProperties.copy(properties)));
    this.tableFormatSync = TableFormatSync.getInstance();
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
    HudiIncrementalTableChangeExtractor hudiTableExtractor =
        getHudiTableExtractor(metaClient, viewManager);
    completeInstant(
        metaClient, hudiTableExtractor.extractTableChanges(commitMetadata, completedInstant));
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
    archiveInstants(metaClient, internalTable, archivedInstants.get());
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
    HoodieRollbackMetadata rollbackMetadata;
    try {
      rollbackMetadata = metaClient.getActiveTimeline().readRollbackMetadata(rollbackInstant);
    } catch (IOException e) {
      throw new ReadException("Unable to read rollback metadata for " + rollbackInstant, e);
    }
    HudiIncrementalTableChangeExtractor hudiTableExtractor =
        getHudiTableExtractor(metaClient, viewManager);
    Set<String> publishedCommitTimes = publishedCommitTimes(metaClient);
    IncrementalTableChanges changes;
    if (metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().empty()) {
      // Every commit has been rolled back, so Hudi has no commit to take the schema from. The
      // table itself is unchanged by the rollback, so describe it from the Iceberg table.
      changes =
          hudiTableExtractor.extractTableChanges(
              rollbackMetadata,
              rollbackInstant,
              publishedCommitTimes,
              tableAsRecordedInIceberg(metaClient, rollbackInstant));
    } else {
      changes =
          hudiTableExtractor.extractTableChanges(
              rollbackMetadata, rollbackInstant, publishedCommitTimes);
    }
    completeInstant(metaClient, changes);
  }

  private InternalTable tableAsRecordedInIceberg(
      HoodieTableMetaClient metaClient, HoodieInstant instant) {
    SourceTable icebergTable =
        SourceTable.builder()
            .name(metaClient.getTableConfig().getTableName())
            .formatName(org.apache.xtable.model.storage.TableFormat.ICEBERG)
            .basePath(metaClient.getBasePath().toString())
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

  /**
   * Requested times of the commits recorded in the current Iceberg snapshot's ancestry. A commit
   * completed in Hudi but never recorded in Iceberg, because the writer died in between, has no
   * files in Iceberg for its rollback to remove.
   */
  private Set<String> publishedCommitTimes(HoodieTableMetaClient metaClient) {
    IcebergTableManager tableManager =
        IcebergTableManager.of((Configuration) metaClient.getStorageConf().unwrap());
    TableIdentifier tableIdentifier =
        TableIdentifier.of(metaClient.getTableConfig().getTableName());
    String basePath = metaClient.getBasePath().toString();
    if (!tableManager.tableExists(null, tableIdentifier, basePath)) {
      return Collections.emptySet();
    }
    Table table = tableManager.getTable(null, tableIdentifier, basePath);
    return IcebergSnapshotInstants.ancestorsOldestFirst(table).stream()
        .map(
            snapshot ->
                IcebergSnapshotInstants.recordedInstant(snapshot, metaClient.getInstantGenerator())
                    .requestedTime())
        .collect(Collectors.toSet());
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
    IcebergConversionTarget target = getIcebergConversionTarget(metaClient);
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
   * Only copy-on-write tables are supported. {@link HudiDataFileExtractor} skips log files, so a
   * deltacommit that writes only log files would publish an empty snapshot and Iceberg readers
   * would silently miss every update and delete in it.
   *
   * @throws UnsupportedOperationException when the table is merge-on-read
   */
  static void requireCopyOnWrite(HoodieConfig tableConfig) {
    String tableType = tableConfig.getStringOrDefault(HoodieTableConfig.TYPE);
    if (!HoodieTableType.COPY_ON_WRITE.name().equals(tableType)) {
      throw new UnsupportedOperationException(
          String.format(
              "The Iceberg table format only supports %s tables, but table %s is %s",
              HoodieTableType.COPY_ON_WRITE,
              tableConfig.getStringOrDefault(HoodieTableConfig.NAME, ""),
              tableType));
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
    requireCopyOnWrite(metaClient.getTableConfig());
    String partitionSpec = partitionFieldSpec(metaClient.getTableConfig());
    final PathBasedPartitionSpecExtractor sourcePartitionSpecExtractor =
        HudiSourceConfig.fromPartitionFieldSpecConfig(partitionSpec)
            .loadSourcePartitionSpecExtractor();
    return new HudiIncrementalTableChangeExtractor(
        metaClient,
        new HudiTableExtractor(new HudiSchemaExtractor(), sourcePartitionSpecExtractor),
        new HudiDataFileExtractor(
            metaClient,
            new PathBasedPartitionValuesExtractor(
                sourcePartitionSpecExtractor.getPathToPartitionFieldFormat()),
            new HudiFileStatsExtractor(metaClient),
            viewManager));
  }

  private IcebergConversionTarget getIcebergConversionTarget(HoodieTableMetaClient metaClient) {
    // TODO: Add iceberg catalog config through user inputs.
    TargetTable targetTable =
        targetTable(
            metaClient.getTableConfig().getTableName(), metaClient.getBasePath().toString());
    return (IcebergConversionTarget)
        ConversionTargetFactory.getInstance()
            .createForFormat(targetTable, (Configuration) metaClient.getStorageConf().unwrap());
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
