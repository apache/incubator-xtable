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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.TableNotFoundException;

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

@Slf4j
public class HudiTargetClient implements TargetClient {
  private final BaseFileUpdatesExtractor baseFileUpdatesExtractor;
  private final AvroSchemaConverter avroSchemaConverter;
  private HoodieTableMetaClient metaClient;
  private CommitState commitState;

  public HudiTargetClient(PerTableConfig perTableConfig, Configuration configuration) {
    this(
        perTableConfig.getTableBasePath(),
        configuration,
        BaseFileUpdatesExtractor.of(
            new HoodieJavaEngineContext(configuration), perTableConfig.getTableBasePath()),
        AvroSchemaConverter.getInstance());
  }

  HudiTargetClient(
      String basePath,
      Configuration configuration,
      BaseFileUpdatesExtractor baseFileUpdatesExtractor,
      AvroSchemaConverter avroSchemaConverter) {
    this.baseFileUpdatesExtractor = baseFileUpdatesExtractor;
    this.avroSchemaConverter = avroSchemaConverter;
    // create meta client if table already exists
    try {
      this.metaClient =
          HoodieTableMetaClient.builder().setBasePath(basePath).setConf(configuration).build();
    } catch (TableNotFoundException ex) {
      log.info("Hudi table does not exist, will be created on first sync");
    }
  }

  private HoodieTableMetaClient initializeHudiTable(OneTable table) {
    try {
      return HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(HoodieTableType.COPY_ON_WRITE)
          .setTableName(table.getName())
          .setPayloadClass(HoodieAvroPayload.class)
          .setRecordKeyFields("") // TODO
          .setPartitionFields(
              table.getPartitioningFields().stream()
                  .map(OnePartitionField::getSourceField)
                  .map(OneField::getPath)
                  .collect(Collectors.joining(",")))
          .initTable(new Configuration(), table.getBasePath());
    } catch (IOException ex) {
      throw new OneIOException("Unable to initialize Hudi table", ex);
    }
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
      metaClient = initializeHudiTable(table);
    }
    HoodieJavaWriteClient<?> writeClient =
        new HoodieJavaWriteClient<>(
            new HoodieJavaEngineContext(new Configuration()), getWriteConfig());
    String instant =
        HoodieInstantTimeGenerator.getInstantFromTemporalAccessor(table.getLatestCommitTime());
    writeClient.startCommitWithTime(instant, HoodieTimeline.REPLACE_COMMIT_ACTION);
    this.commitState = new CommitState(writeClient, instant);
  }

  @Override
  public void completeSync() {
    metaClient
        .getActiveTimeline()
        .transitionReplaceRequestedToInflight(
            new HoodieInstant(
                HoodieInstant.State.REQUESTED,
                HoodieTimeline.REPLACE_COMMIT_ACTION,
                commitState.getInstantTime()),
            Option.empty());
    commitState.commit();
    commitState = null;
  }

  @Override
  public Optional<OneTableMetadata> getTableMetadata() {
    return Optional.empty();
  }

  private static class CommitState {
    private final HoodieJavaWriteClient<?> writeClient;
    @Getter private final String instantTime;
    private List<WriteStatus> writeStatuses;
    @Setter private Schema schema;
    @Setter private OneTableMetadata oneTableMetadata;
    private Map<String, List<String>> partitionToReplacedFileIds;

    CommitState(HoodieJavaWriteClient<?> writeClient, String instantTime) {
      this.writeClient = writeClient;
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
      try {
        writeClient.commit(
            instantTime,
            writeStatuses,
            getExtraMetadata(),
            HoodieTimeline.REPLACE_COMMIT_ACTION,
            partitionToReplacedFileIds);
      } finally {
        writeClient.close();
      }
    }

    private Option<Map<String, String>> getExtraMetadata() {
      Map<String, String> extraMetadata = new HashMap<>(oneTableMetadata.asMap());
      extraMetadata.put(HoodieCommitMetadata.SCHEMA_KEY, schema.toString());
      return Option.of(extraMetadata);
    }
  }

  private HoodieWriteConfig getWriteConfig() {
    return HoodieWriteConfig.newBuilder()
        .withPath(metaClient.getBasePathV2().toString())
        .withEmbeddedTimelineServerEnabled(false)
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(true).build())
        .build();
  }
}
