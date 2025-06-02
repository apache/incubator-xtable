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

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.TableFormat;
import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineFactory;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.metadata.TableMetadataFactory;

import com.google.common.collect.ImmutableMap;

import org.apache.xtable.conversion.ConversionTargetFactory;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.exception.UpdateException;
import org.apache.xtable.hudi.HudiDataFileExtractor;
import org.apache.xtable.hudi.HudiFileStatsExtractor;
import org.apache.xtable.hudi.HudiIncrementalTableChangeExtractor;
import org.apache.xtable.hudi.HudiPartitionValuesExtractor;
import org.apache.xtable.hudi.HudiSchemaExtractor;
import org.apache.xtable.hudi.HudiSourceConfig;
import org.apache.xtable.hudi.HudiSourcePartitionSpecExtractor;
import org.apache.xtable.hudi.HudiTableExtractor;
import org.apache.xtable.iceberg.IcebergConversionTarget;
import org.apache.xtable.metadata.IcebergMetadataFactory;
import org.apache.xtable.model.IncrementalTableChanges;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.spi.sync.TableFormatSync;
import org.apache.xtable.timeline.IcebergTimelineArchiver;
import org.apache.xtable.timeline.IcebergTimelineFactory;

public class IcebergTableFormat implements TableFormat {
  private transient TableFormatSync tableFormatSync;
  private transient ExecutorService executorService;

  public IcebergTableFormat() {}

  @Override
  public void init(Properties properties) {
    this.tableFormatSync = TableFormatSync.getInstance();
    this.executorService = Executors.newSingleThreadExecutor();
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
      List<HoodieInstant> archivedInstants,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
    HudiIncrementalTableChangeExtractor hudiTableExtractor =
        getHudiTableExtractor(metaClient, viewManager);
    InternalTable internalTable =
        hudiTableExtractor
            .getTableExtractor()
            .table(metaClient, metaClient.getActiveTimeline().lastInstant().get());
    archiveInstants(metaClient, internalTable, archivedInstants);
  }

  @Override
  public void rollback(
      HoodieInstant completedInstant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
    throw new UnsupportedOperationException("Rollback not supported yet");
  }

  @Override
  public void savepoint(
      HoodieInstant instant,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
    throw new UnsupportedOperationException("Savepoint not supported yet");
  }

  @Override
  public void restore(
      HoodieInstant savepoint,
      HoodieEngineContext engineContext,
      HoodieTableMetaClient metaClient,
      FileSystemViewManager viewManager) {
    throw new UnsupportedOperationException("Restore not supported yet");
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
    try {
      tableFormatSync.syncChanges(ImmutableMap.of(target, tableSyncMetadata), changes);
    } catch (Exception e) {
      throw new UpdateException("Failed to update iceberg metadata", e);
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

  private HudiIncrementalTableChangeExtractor getHudiTableExtractor(
      HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    String partitionSpec =
        metaClient
            .getTableConfig()
            .getPartitionFields()
            .map(
                partitionPaths ->
                    Arrays.stream(partitionPaths)
                        .map(p -> String.format("%s:VALUE", p))
                        .collect(Collectors.joining(",")))
            .orElse(null);
    final HudiSourcePartitionSpecExtractor sourcePartitionSpecExtractor =
        HudiSourceConfig.fromPartitionFieldSpecConfig(partitionSpec)
            .loadSourcePartitionSpecExtractor();
    return new HudiIncrementalTableChangeExtractor(
        metaClient,
        new HudiTableExtractor(new HudiSchemaExtractor(), sourcePartitionSpecExtractor),
        new HudiDataFileExtractor(
            metaClient,
            new HudiPartitionValuesExtractor(
                sourcePartitionSpecExtractor.getPathToPartitionFieldFormat()),
            new HudiFileStatsExtractor(metaClient),
            viewManager));
  }

  private IcebergConversionTarget getIcebergConversionTarget(HoodieTableMetaClient metaClient) {
    // TODO: Add iceberg catalog config through user inputs.
    TargetTable targetTable =
        TargetTable.builder()
            .name(metaClient.getTableConfig().getTableName())
            .formatName(org.apache.xtable.model.storage.TableFormat.ICEBERG)
            .basePath(metaClient.getBasePath().toString())
            .build();
    return (IcebergConversionTarget)
        ConversionTargetFactory.getInstance()
            .createForFormat(targetTable, (Configuration) metaClient.getStorageConf().unwrap());
  }
}
