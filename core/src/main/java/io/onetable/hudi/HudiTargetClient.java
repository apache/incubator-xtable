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
 
package io.onetable.hudi;

import static org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.Value;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanFileInfo;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.client.BaseHoodieWriteClient;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.action.clean.CleanPlanner;

import io.onetable.avro.AvroSchemaConverter;
import io.onetable.client.PerTableConfig;
import io.onetable.exception.NotSupportedException;
import io.onetable.exception.OneIOException;
import io.onetable.model.OneTable;
import io.onetable.model.OneTableMetadata;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.spi.sync.TargetClient;

public class HudiTargetClient implements TargetClient {
  private static final ZoneId UTC = ZoneId.of("UTC");
  private final BaseFileUpdatesExtractor baseFileUpdatesExtractor;
  private final AvroSchemaConverter avroSchemaConverter;
  private final HudiTableManager hudiTableManager;
  private final CommitStateCreator commitStateCreator;
  private final int timelineRetentionInHours;
  private final int maxNumDeltaCommitsBeforeCompaction;
  private HoodieTableMetaClient metaClient;
  private CommitState commitState;

  public HudiTargetClient(PerTableConfig perTableConfig, Configuration configuration) {
    this(
        perTableConfig.getTableBasePath(),
        perTableConfig.getTargetMetadataRetentionInHours(),
        HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.defaultValue(),
        BaseFileUpdatesExtractor.of(
            new HoodieJavaEngineContext(configuration), perTableConfig.getTableBasePath()),
        AvroSchemaConverter.getInstance(),
        HudiTableManager.of(configuration),
        CommitState::new);
  }

  @VisibleForTesting
  HudiTargetClient(
      PerTableConfig perTableConfig,
      Configuration configuration,
      int maxNumDeltaCommitsBeforeCompaction) {
    this(
        perTableConfig.getTableBasePath(),
        perTableConfig.getTargetMetadataRetentionInHours(),
        maxNumDeltaCommitsBeforeCompaction,
        BaseFileUpdatesExtractor.of(
            new HoodieJavaEngineContext(configuration), perTableConfig.getTableBasePath()),
        AvroSchemaConverter.getInstance(),
        HudiTableManager.of(configuration),
        CommitState::new);
  }

  @VisibleForTesting
  HudiTargetClient(
      String basePath,
      int timelineRetentionInHours,
      int maxNumDeltaCommitsBeforeCompaction,
      BaseFileUpdatesExtractor baseFileUpdatesExtractor,
      AvroSchemaConverter avroSchemaConverter,
      HudiTableManager hudiTableManager,
      CommitStateCreator commitStateCreator) {
    this.baseFileUpdatesExtractor = baseFileUpdatesExtractor;
    this.timelineRetentionInHours = timelineRetentionInHours;
    this.maxNumDeltaCommitsBeforeCompaction = maxNumDeltaCommitsBeforeCompaction;
    this.avroSchemaConverter = avroSchemaConverter;
    this.hudiTableManager = hudiTableManager;
    // create meta client if table already exists
    this.metaClient = hudiTableManager.loadTableIfExists(basePath);
    this.commitStateCreator = commitStateCreator;
  }

  @FunctionalInterface
  interface CommitStateCreator {
    CommitState create(
        HoodieTableMetaClient metaClient,
        String instantTime,
        int timelineRetentionInHours,
        int maxNumDeltaCommitsBeforeCompaction);
  }

  @Override
  public void syncSchema(OneSchema schema) {
    commitState.setSchema(avroSchemaConverter.fromOneSchema(schema));
  }

  @Override
  public void syncPartitionSpec(List<OnePartitionField> partitionSpec) {
    List<String> existingPartitionFields =
        metaClient
            .getTableConfig()
            .getPartitionFields()
            .map(Arrays::asList)
            .orElse(Collections.emptyList());
    List<String> newPartitionFields =
        partitionSpec.stream()
            .map(OnePartitionField::getSourceField)
            .map(OneField::getPath)
            .collect(Collectors.toList());
    if (!existingPartitionFields.equals(newPartitionFields)) {
      throw new NotSupportedException("Partition spec changes are not supported for Hudi targets");
    }
  }

  @Override
  public void syncMetadata(OneTableMetadata metadata) {
    commitState.setOneTableMetadata(metadata);
  }

  @Override
  public void syncFilesForSnapshot(OneDataFiles snapshotFiles) {
    BaseFileUpdatesExtractor.ReplaceMetadata replaceMetadata =
        baseFileUpdatesExtractor.extractSnapshotChanges(
            snapshotFiles, metaClient, commitState.getInstantTime());
    commitState.setReplaceMetadata(replaceMetadata);
  }

  @Override
  public void syncFilesForDiff(OneDataFilesDiff oneDataFilesDiff) {
    BaseFileUpdatesExtractor.ReplaceMetadata replaceMetadata =
        baseFileUpdatesExtractor.convertDiff(oneDataFilesDiff, commitState.getInstantTime());
    commitState.setReplaceMetadata(replaceMetadata);
  }

  @Override
  public void beginSync(OneTable table) {
    if (metaClient == null) {
      metaClient = hudiTableManager.initializeHudiTable(table);
    } else {
      // make sure meta client has up-to-date view of the timeline
      metaClient.reloadActiveTimeline();
    }
    String instant = convertInstantToCommit(table.getLatestCommitTime());
    this.commitState =
        commitStateCreator.create(
            metaClient, instant, timelineRetentionInHours, maxNumDeltaCommitsBeforeCompaction);
  }

  // TODO make util class for this and reverse calculation?
  static String convertInstantToCommit(Instant instant) {
    LocalDateTime instantTime = instant.atZone(UTC).toLocalDateTime();
    return HoodieInstantTimeGenerator.getInstantFromTemporalAccessor(instantTime);
  }

  @Override
  public void completeSync() {
    commitState.commit();
    commitState = null;
  }

  @Override
  public Optional<OneTableMetadata> getTableMetadata() {
    if (metaClient != null) {
      return metaClient
          .reloadActiveTimeline()
          .getCommitsTimeline()
          .filterCompletedInstants()
          .lastInstant()
          .toJavaOptional()
          .map(
              instant -> {
                try {
                  if (instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION)) {
                    return HoodieReplaceCommitMetadata.fromBytes(
                            metaClient.getActiveTimeline().getInstantDetails(instant).get(),
                            HoodieReplaceCommitMetadata.class)
                        .getExtraMetadata();
                  } else {
                    return HoodieCommitMetadata.fromBytes(
                            metaClient.getActiveTimeline().getInstantDetails(instant).get(),
                            HoodieCommitMetadata.class)
                        .getExtraMetadata();
                  }
                } catch (IOException ex) {
                  throw new OneIOException("Unable to read Hudi commit metadata", ex);
                }
              })
          .flatMap(OneTableMetadata::fromMap);
    }
    return Optional.empty();
  }

  static class CommitState {
    private final int maxNumDeltaCommitsBeforeCompaction;
    private final HoodieTableMetaClient metaClient;
    @Getter private final String instantTime;
    private final int timelineRetentionInHours;
    private List<WriteStatus> writeStatuses;
    @Setter private Schema schema;
    @Setter private OneTableMetadata oneTableMetadata;
    private Map<String, List<String>> partitionToReplacedFileIds;

    private CommitState(
        HoodieTableMetaClient metaClient,
        String instantTime,
        int timelineRetentionInHours,
        int maxNumDeltaCommitsBeforeCompaction) {
      this.metaClient = metaClient;
      this.instantTime = instantTime;
      this.timelineRetentionInHours = timelineRetentionInHours;
      this.maxNumDeltaCommitsBeforeCompaction = maxNumDeltaCommitsBeforeCompaction;
      this.schema = null;
      this.writeStatuses = Collections.emptyList();
      this.oneTableMetadata = null;
      this.partitionToReplacedFileIds = Collections.emptyMap();
    }

    public void setReplaceMetadata(BaseFileUpdatesExtractor.ReplaceMetadata replaceMetadata) {
      if (!writeStatuses.isEmpty() || !partitionToReplacedFileIds.isEmpty()) {
        throw new IllegalArgumentException("Replace metadata can only be set once");
      }
      this.writeStatuses = replaceMetadata.getWriteStatuses();
      this.partitionToReplacedFileIds = replaceMetadata.getPartitionToReplacedFileIds();
    }

    public void commit() {
      if (schema == null) {
        try {
          // reuse existing table schema if no schema is provided as part of this commit
          schema = new TableSchemaResolver(metaClient).getTableAvroSchema();
        } catch (Exception ex) {
          throw new OneIOException("Unable to read Hudi table schema", ex);
        }
      }
      InstantsToArchiveAndRetain instantsToArchiveAndRetain = getInstantsToArchiveAndRetain();
      HoodieWriteConfig writeConfig =
          getWriteConfig(
              schema,
              instantsToArchiveAndRetain.getNumInstantsToRetain(),
              maxNumDeltaCommitsBeforeCompaction);
      HoodieEngineContext engineContext = new HoodieJavaEngineContext(metaClient.getHadoopConf());
      try (HoodieJavaWriteClient<?> writeClient =
          new HoodieJavaWriteClient<>(engineContext, writeConfig)) {
        writeClient.startCommitWithTime(instantTime, HoodieTimeline.REPLACE_COMMIT_ACTION);
        metaClient
            .getActiveTimeline()
            .transitionReplaceRequestedToInflight(
                new HoodieInstant(
                    HoodieInstant.State.REQUESTED,
                    HoodieTimeline.REPLACE_COMMIT_ACTION,
                    instantTime),
                Option.empty());
        writeClient.commit(
            instantTime,
            writeStatuses,
            getExtraMetadata(),
            HoodieTimeline.REPLACE_COMMIT_ACTION,
            partitionToReplacedFileIds);
        if (instantsToArchiveAndRetain.canRunCleanAndArchive()) {
          // clean up old commits and archive them
          HoodieInstant completedReplaceCommitInstant =
              new HoodieInstant(
                  HoodieInstant.State.COMPLETED, HoodieTimeline.REPLACE_COMMIT_ACTION, instantTime);
          cleanAndArchive(
              engineContext,
              writeClient,
              instantsToArchiveAndRetain,
              completedReplaceCommitInstant);
        }
      }
    }

    private InstantsToArchiveAndRetain getInstantsToArchiveAndRetain() {
      String commitCutoff =
          convertInstantToCommit(
              HudiClient.parseFromInstantTime(instantTime)
                  .minus(timelineRetentionInHours, ChronoUnit.HOURS));
      HoodieTimeline activeTimeline = metaClient.getActiveTimeline();
      List<HoodieInstant> instantsToArchive =
          activeTimeline.findInstantsBeforeOrEquals(commitCutoff).getInstants();
      List<HoodieInstant> instantsAfterCutoff =
          activeTimeline.findInstantsAfter(commitCutoff).getInstants();
      return InstantsToArchiveAndRetain.builder()
          .instantsToArchive(instantsToArchive)
          .lastInstantToRetain(
              instantsAfterCutoff.isEmpty()
                  ? Optional.empty()
                  : Optional.of(instantsAfterCutoff.get(0)))
          .numInstantsToRetain(instantsAfterCutoff.size())
          .build();
    }

    @Builder
    @Value
    private static class InstantsToArchiveAndRetain {
      @NonNull List<HoodieInstant> instantsToArchive;
      @NonNull Optional<HoodieInstant> lastInstantToRetain;
      int numInstantsToRetain;

      boolean canRunCleanAndArchive() {
        return !instantsToArchive.isEmpty();
      }
    }

    private void cleanAndArchive(
        HoodieEngineContext engineContext,
        BaseHoodieWriteClient<?, ?, ?, ?> writeClient,
        InstantsToArchiveAndRetain instantsToArchiveAndRetain,
        HoodieInstant completedReplaceCommit) {
      HoodieInstant earliestInstantToRetain =
          instantsToArchiveAndRetain.getLastInstantToRetain().orElse(completedReplaceCommit);
      List<HoodieInstant> replaceCommitsToCleanAndArchive =
          instantsToArchiveAndRetain.getInstantsToArchive().stream()
              .filter(instant -> instant.getAction().equals(HoodieTimeline.REPLACE_COMMIT_ACTION))
              .collect(Collectors.toList());
      HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
      // find all removed file groups in replace commits from before the earliestInstantToRetain
      Set<String> affectedPartitions =
          replaceCommitsToCleanAndArchive.stream()
              .map(
                  replaceCommit -> {
                    try {
                      return HoodieReplaceCommitMetadata.fromBytes(
                          activeTimeline.getInstantDetails(replaceCommit).get(),
                          HoodieReplaceCommitMetadata.class);
                    } catch (IOException ex) {
                      throw new OneIOException("Unable to read Hudi commit metadata", ex);
                    }
                  })
              .flatMap(
                  metadata ->
                      Stream.concat(
                          metadata.getPartitionToReplaceFileIds().keySet().stream(),
                          metadata.getPartitionToWriteStats().keySet().stream()))
              .collect(Collectors.toSet());
      if (affectedPartitions.isEmpty()) {
        return;
      }
      String cleanTime =
          convertInstantToCommit(
              HudiClient.parseFromInstantTime(instantTime).plus(1, ChronoUnit.SECONDS));
      HoodieTableFileSystemView fsView =
          new HoodieMetadataFileSystemView(
              engineContext,
              metaClient,
              activeTimeline,
              writeClient.getConfig().getMetadataConfig());
      try (HoodieTableMetadataWriter hoodieTableMetadataWriter =
          (HoodieTableMetadataWriter)
              writeClient
                  .initTable(WriteOperationType.UPSERT, Option.of(cleanTime))
                  .getMetadataWriter(cleanTime)
                  .get()) {
        // find all file paths for the removed file groups
        Map<String, List<HoodieCleanFileInfo>> filePathsToCleanPerPartition =
            affectedPartitions.stream()
                .collect(
                    Collectors.toMap(
                        Function.identity(),
                        partitionPath ->
                            fsView
                                .getReplacedFileGroupsBeforeOrOn(
                                    earliestInstantToRetain.getTimestamp(), partitionPath)
                                .flatMap(HoodieFileGroup::getAllBaseFiles)
                                .map(
                                    baseFile ->
                                        new HoodieCleanFileInfo(
                                            ExternalFilePathUtil
                                                .appendCommitTimeAndExternalFileMarker(
                                                    baseFile.getFileName(),
                                                    baseFile.getCommitTime()),
                                            false))
                                .collect(Collectors.toList())));
        if (filePathsToCleanPerPartition.isEmpty()) {
          return;
        }
        HoodieCleanerPlan cleanerPlan =
            new HoodieCleanerPlan(
                new HoodieActionInstant(
                    earliestInstantToRetain.getTimestamp(),
                    earliestInstantToRetain.getAction(),
                    earliestInstantToRetain.getState().name()),
                instantTime,
                writeClient.getConfig().getCleanerPolicy().name(),
                Collections.emptyMap(),
                CleanPlanner.LATEST_CLEAN_PLAN_VERSION,
                filePathsToCleanPerPartition,
                Collections.emptyList());
        HoodieInstant requestedCleanInstant =
            new HoodieInstant(
                HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, cleanTime);
        activeTimeline.saveToCleanRequested(
            requestedCleanInstant, TimelineMetadataUtils.serializeCleanerPlan(cleanerPlan));
        HoodieInstant inflightClean =
            activeTimeline.transitionCleanRequestedToInflight(
                requestedCleanInstant, Option.empty());
        List<HoodieCleanStat> cleanStats =
            filePathsToCleanPerPartition.entrySet().stream()
                .map(
                    entry -> {
                      String partitionPath = entry.getKey();
                      List<String> deletePaths =
                          entry.getValue().stream()
                              .map(HoodieCleanFileInfo::getFilePath)
                              .collect(Collectors.toList());
                      return new HoodieCleanStat(
                          HoodieCleaningPolicy.KEEP_LATEST_COMMITS,
                          partitionPath,
                          deletePaths,
                          deletePaths,
                          Collections.emptyList(),
                          earliestInstantToRetain.getTimestamp(),
                          instantTime);
                    })
                .collect(Collectors.toList());
        HoodieCleanMetadata cleanMetadata =
            CleanerUtils.convertCleanMetadata(cleanTime, Option.empty(), cleanStats);

        hoodieTableMetadataWriter.update(cleanMetadata, cleanTime);
        activeTimeline.transitionCleanInflightToComplete(
            inflightClean, TimelineMetadataUtils.serializeCleanMetadata(cleanMetadata));
        // trigger archiver manually
        writeClient.archive();
      } catch (Exception ex) {
        throw new OneIOException("Unable to clean and/or archive Hudi timeline", ex);
      } finally {
        fsView.close();
      }
    }

    private Option<Map<String, String>> getExtraMetadata() {
      Map<String, String> extraMetadata = new HashMap<>(oneTableMetadata.asMap());
      return Option.of(extraMetadata);
    }

    private HoodieWriteConfig getWriteConfig(
        Schema schema, int numCommitsToKeep, int maxNumDeltaCommitsBeforeCompaction) {
      Properties properties = new Properties();
      properties.setProperty(HoodieMetadataConfig.AUTO_INITIALIZE.key(), "false");
      return HoodieWriteConfig.newBuilder()
          .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(INMEMORY).build())
          .withPath(metaClient.getBasePathV2().toString())
          .withPopulateMetaFields(metaClient.getTableConfig().populateMetaFields())
          .withEmbeddedTimelineServerEnabled(false)
          .withSchema(schema == null ? "" : schema.toString())
          .withArchivalConfig(
              HoodieArchivalConfig.newBuilder()
                  .archiveCommitsWith(
                      Math.max(0, numCommitsToKeep - 1), Math.max(1, numCommitsToKeep))
                  .withAutoArchive(false)
                  .build())
          .withCleanConfig(
              HoodieCleanConfig.newBuilder()
                  // set this to avoid warnings but clean plan is manually generated
                  .retainCommits(Math.max(0, numCommitsToKeep - 2))
                  .withAutoClean(false)
                  .build())
          .withMetadataConfig(
              HoodieMetadataConfig.newBuilder()
                  .enable(true)
                  .withProperties(properties)
                  .withMetadataIndexColumnStats(true)
                  .withMaxNumDeltaCommitsBeforeCompaction(maxNumDeltaCommitsBeforeCompaction)
                  .build())
          .build();
    }
  }
}
