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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Getter;
import lombok.Setter;

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
import org.apache.hudi.common.config.HoodieStorageConfig;
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
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodiePayloadConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;
import org.apache.hudi.metadata.HoodieTableMetadataWriter;
import org.apache.hudi.table.action.clean.CleanPlanner;

import com.google.common.collect.Streams;

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
  private HoodieTableMetaClient metaClient;
  private CommitState commitState;

  public HudiTargetClient(PerTableConfig perTableConfig, Configuration configuration) {
    this(
        perTableConfig.getTableBasePath(),
        perTableConfig.getTargetMetadataRetentionInHours(),
        BaseFileUpdatesExtractor.of(
            new HoodieJavaEngineContext(configuration), perTableConfig.getTableBasePath()),
        AvroSchemaConverter.getInstance(),
        HudiTableManager.of(configuration),
        CommitState::new);
  }

  HudiTargetClient(
      String basePath,
      int timelineRetentionInHours,
      BaseFileUpdatesExtractor baseFileUpdatesExtractor,
      AvroSchemaConverter avroSchemaConverter,
      HudiTableManager hudiTableManager,
      CommitStateCreator commitStateCreator) {
    this.baseFileUpdatesExtractor = baseFileUpdatesExtractor;
    this.timelineRetentionInHours = timelineRetentionInHours;
    this.avroSchemaConverter = avroSchemaConverter;
    this.hudiTableManager = hudiTableManager;
    // create meta client if table already exists
    this.metaClient = hudiTableManager.loadTableIfExists(basePath);
    this.commitStateCreator = commitStateCreator;
  }

  @FunctionalInterface
  interface CommitStateCreator {
    CommitState create(
        HoodieTableMetaClient metaClient, String instantTime, int timelineRetentionInHours);
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
    this.commitState = commitStateCreator.create(metaClient, instant, timelineRetentionInHours);
  }

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
    private final HoodieTableMetaClient metaClient;
    @Getter private final String instantTime;
    private final int timelineRetentionInHours;
    private List<WriteStatus> writeStatuses;
    @Setter private Schema schema;
    @Setter private OneTableMetadata oneTableMetadata;
    private Map<String, List<String>> partitionToReplacedFileIds;

    private CommitState(
        HoodieTableMetaClient metaClient, String instantTime, int timelineRetentionInHours) {
      this.metaClient = metaClient;
      this.instantTime = instantTime;
      this.timelineRetentionInHours = timelineRetentionInHours;
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
      List<HoodieInstant> instantsToRetain = getInstantsToRetain();
      HoodieWriteConfig writeConfig = getWriteConfig(schema);
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
        if (!instantsToRetain.isEmpty()) {
          // clean up old commits and archive them
          //cleanAndArchive(engineContext, writeClient, instantsToRetain.get(0));
        }
      }
    }

    private List<HoodieInstant> getInstantsToRetain() {
      String commitCutoff =
          convertInstantToCommit(
              HudiClient.parseFromInstantTime(instantTime)
                  .minus(timelineRetentionInHours, ChronoUnit.HOURS));
      return metaClient.getActiveTimeline().findInstantsAfter(commitCutoff).getInstants();
    }

    private void cleanAndArchive(
        HoodieEngineContext engineContext,
        BaseHoodieWriteClient writeClient,
        HoodieInstant earliestInstantToRetain) {
      List<HoodieInstant> replaceCommitsToArchive =
          metaClient
              .getActiveTimeline()
              .findInstantsBefore(earliestInstantToRetain.getTimestamp())
              .getCompletedReplaceTimeline()
              .getInstants();
      if (replaceCommitsToArchive.isEmpty()) {
        return;
      }
      String cleanTime = HoodieActiveTimeline.createNewInstantTime();
      try (HoodieTableMetadataWriter hoodieTableMetadataWriter =
          (HoodieTableMetadataWriter)
              writeClient
                  .initTable(WriteOperationType.UPSERT, Option.of(cleanTime))
                  .getMetadataWriter(cleanTime)
                  .get()) {
        // find all removed file groups in replace commits from before the earliestInstantToRetain
        Map<String, Stream<String>> partitionToRemovedFileIds =
            replaceCommitsToArchive.stream()
                .map(
                    replaceCommit -> {
                      try {
                        return HoodieReplaceCommitMetadata.fromBytes(
                            metaClient.getActiveTimeline().getInstantDetails(replaceCommit).get(),
                            HoodieReplaceCommitMetadata.class);
                      } catch (IOException ex) {
                        throw new OneIOException("Unable to read Hudi commit metadata", ex);
                      }
                    })
                .flatMap(metadata -> metadata.getPartitionToReplaceFileIds().entrySet().stream())
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey, entry -> entry.getValue().stream(), Streams::concat));

        // find all file paths for the removed file groups
        HoodieTableFileSystemView fsView =
            new HoodieMetadataFileSystemView(
                engineContext,
                metaClient,
                metaClient.getActiveTimeline(),
                writeClient.getConfig().getMetadataConfig());
        Map<String, List<HoodieCleanFileInfo>> filePathsToCleanPerPartition =
            partitionToRemovedFileIds.entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> {
                          String partitionPath = entry.getKey();
                          Set<String> fileIds = entry.getValue().collect(Collectors.toSet());
                          return fsView
                              .getAllFileGroups(partitionPath)
                              .filter(
                                  hoodieFileGroup ->
                                      fileIds.contains(
                                          hoodieFileGroup.getFileGroupId().getFileId()))
                              .flatMap(HoodieFileGroup::getAllBaseFiles)
                              .map(
                                  baseFile -> {
                                    String path =
                                        partitionPath.isEmpty()
                                            ? baseFile.getFileName()
                                            : partitionPath + "/" + baseFile.getFileName();
                                    return new HoodieCleanFileInfo(path, false);
                                  })
                              .collect(Collectors.toList());
                        }));
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
        metaClient
            .getActiveTimeline()
            .saveToCleanRequested(
                new HoodieInstant(
                    HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, cleanTime),
                TimelineMetadataUtils.serializeCleanerPlan(cleanerPlan));
        HoodieInstant inflightClean =
            metaClient
                .getActiveTimeline()
                .transitionCleanRequestedToInflight(
                    new HoodieInstant(
                        HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, cleanTime),
                    Option.empty());
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
        metaClient
            .getActiveTimeline()
            .transitionCleanInflightToComplete(
                inflightClean, TimelineMetadataUtils.serializeCleanMetadata(cleanMetadata));
        // trigger archiver manually
        writeClient.archive();
      } catch (Exception ex) {
        throw new OneIOException("Unable to clean and/or archive Hudi timeline", ex);
      }
    }

    private Option<Map<String, String>> getExtraMetadata() {
      Map<String, String> extraMetadata = new HashMap<>(oneTableMetadata.asMap());
      return Option.of(extraMetadata);
    }

    private HoodieWriteConfig getWriteConfig(Schema schema) {
      Properties properties = new Properties();
      properties.setProperty(HoodieMetadataConfig.AUTO_INITIALIZE.key(), "false");
      return HoodieWriteConfig.newBuilder()
          .withPath(metaClient.getBasePathV2().toString())
          .withEmbeddedTimelineServerEnabled(false)
          .withSchema(schema == null ? "" : schema.toString())
          .withMetadataConfig(
              HoodieMetadataConfig.newBuilder()
                  .enable(true)
                  .withProperties(properties)
                  .withMetadataIndexColumnStats(true)
                  .build())
          .build();
    }
  }
}
