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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.Setter;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

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
  private static final Logger LOG = LogManager.getLogger(HudiTargetClient.class);
  private static final ZoneId UTC = ZoneId.of("UTC");

  private final BaseFileUpdatesExtractor baseFileUpdatesExtractor;
  private final AvroSchemaConverter avroSchemaConverter;
  private final HudiTableManager hudiTableManager;
  private final CommitStateCreator commitStateCreator;
  private Optional<HoodieTableMetaClient> metaClient;
  private CommitState commitState;

  public HudiTargetClient(PerTableConfig perTableConfig, Configuration configuration) {
    this(
        perTableConfig.getTableBasePath(),
        BaseFileUpdatesExtractor.of(
            new HoodieJavaEngineContext(configuration), perTableConfig.getTableBasePath()),
        AvroSchemaConverter.getInstance(),
        HudiTableManager.of(configuration),
        CommitState::new);
  }

  HudiTargetClient(
      String basePath,
      BaseFileUpdatesExtractor baseFileUpdatesExtractor,
      AvroSchemaConverter avroSchemaConverter,
      HudiTableManager hudiTableManager,
      CommitStateCreator commitStateCreator) {
    this.baseFileUpdatesExtractor = baseFileUpdatesExtractor;
    this.avroSchemaConverter = avroSchemaConverter;
    this.hudiTableManager = hudiTableManager;
    // create meta client if table already exists
    this.metaClient = hudiTableManager.loadTableMetaClientIfExists(basePath);
    this.commitStateCreator = commitStateCreator;
  }

  @FunctionalInterface
  interface CommitStateCreator {
    CommitState create(HoodieTableMetaClient metaClient, String instantTime);
  }

  @Override
  public void syncSchema(OneSchema schema) {
    if (metaClient.isPresent()) {
      Option<String[]> recordKeyFields = metaClient.get().getTableConfig().getRecordKeyFields();
      if (recordKeyFields.isPresent()) {
        Set<String> existingHudiRecordKeys =
            Arrays.stream(recordKeyFields.get()).collect(Collectors.toSet());
        Set<String> schemaFieldsSet =
            schema.getRecordKeyFields().stream().map(OneField::getPath).collect(Collectors.toSet());
        if (!schemaFieldsSet.equals(existingHudiRecordKeys)) {
          Set<String> newKeys =
              schemaFieldsSet.stream()
                  .filter(k -> !existingHudiRecordKeys.contains(k))
                  .collect(Collectors.toSet());
          Set<String> removedKeys =
              existingHudiRecordKeys.stream()
                  .filter(k -> !schemaFieldsSet.contains(k))
                  .collect(Collectors.toSet());
          LOG.error(
              String.format(
                  "Record key fields cannot be changed after creating Hudi table. "
                      + "New keys: %s, Removed keys: %s",
                  newKeys, removedKeys));
          throw new NotSupportedException(
              "Record key fields cannot be changed after creating Hudi table");
        }
      }
    }
    commitState.setSchema(avroSchemaConverter.fromOneSchema(schema));
  }

  @Override
  public void syncPartitionSpec(List<OnePartitionField> partitionSpec) {
    List<String> existingPartitionFields =
        getMetaClient()
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
      throw new NotSupportedException("Partition spec cannot be changed after creating Hudi table");
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
            snapshotFiles, getMetaClient(), commitState.getInstantTime());
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
    if (!metaClient.isPresent()) {
      metaClient = Optional.of(hudiTableManager.initializeHudiTable(table));
    } else {
      // make sure meta client has up-to-date view of the timeline
      getMetaClient().reloadActiveTimeline();
    }
    String instant = convertInstantToCommit(table.getLatestCommitTime());
    this.commitState = commitStateCreator.create(getMetaClient(), instant);
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
    return metaClient.flatMap(
        client ->
            client
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
                                  client.getActiveTimeline().getInstantDetails(instant).get(),
                                  HoodieReplaceCommitMetadata.class)
                              .getExtraMetadata();
                        } else {
                          return HoodieCommitMetadata.fromBytes(
                                  client.getActiveTimeline().getInstantDetails(instant).get(),
                                  HoodieCommitMetadata.class)
                              .getExtraMetadata();
                        }
                      } catch (IOException ex) {
                        throw new OneIOException("Unable to read Hudi commit metadata", ex);
                      }
                    })
                .flatMap(OneTableMetadata::fromMap));
  }

  private HoodieTableMetaClient getMetaClient() {
    return metaClient.orElseThrow(
        () -> new IllegalStateException("beginSync must be called before calling this method"));
  }

  static class CommitState {
    private final HoodieTableMetaClient metaClient;
    @Getter private final String instantTime;
    private List<WriteStatus> writeStatuses;
    @Setter private Schema schema;
    @Setter private OneTableMetadata oneTableMetadata;
    private Map<String, List<String>> partitionToReplacedFileIds;

    private CommitState(HoodieTableMetaClient metaClient, String instantTime) {
      this.metaClient = metaClient;
      this.instantTime = instantTime;
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
      try (HoodieJavaWriteClient<?> writeClient =
          new HoodieJavaWriteClient<>(
              new HoodieJavaEngineContext(new Configuration()), getWriteConfig(schema))) {
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
