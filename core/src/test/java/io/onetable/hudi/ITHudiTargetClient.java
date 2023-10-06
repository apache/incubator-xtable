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

import static io.onetable.hudi.HudiTestUtil.SCHEMA_VERSION;
import static io.onetable.hudi.HudiTestUtil.createWriteStatus;
import static io.onetable.hudi.HudiTestUtil.getHoodieWriteConfig;
import static io.onetable.hudi.HudiTestUtil.initTableAndGetMetaClient;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import lombok.SneakyThrows;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.StringWrapper;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;

import io.onetable.client.PerTableConfig;
import io.onetable.model.OneTable;
import io.onetable.model.OneTableMetadata;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.Range;
import io.onetable.model.storage.DataLayoutStrategy;
import io.onetable.model.storage.FileFormat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.model.storage.TableFormat;

/**
 * A suite of functional tests that assert that the metadata for the hudi table is properly written
 * to disk.
 */
public class ITHudiTargetClient {
  @TempDir public static Path tempDir;
  private static final Configuration CONFIGURATION = new Configuration();
  private static final HoodieEngineContext CONTEXT = new HoodieJavaEngineContext(CONFIGURATION);

  private static final String TABLE_NAME = "test_table";
  private static final String FIELD_1 = "id";
  private static final String FIELD_2 = "partition_field";
  private static final String FIELD_3 = "content";
  private static final long FILE_SIZE = 100L;
  private static final long RECORD_COUNT = 200L;
  private static final long LAST_MODIFIED = System.currentTimeMillis();
  private static final OneSchema STRING_SCHEMA =
      OneSchema.builder().name("string").dataType(OneType.STRING).isNullable(false).build();

  private static final OneField PARTITION_FIELD =
      OneField.builder().name(FIELD_2).schema(STRING_SCHEMA).build();
  private static final String TEST_SCHEMA_NAME = "test_schema";
  private static final OneSchema SCHEMA =
      OneSchema.builder()
          .name(TEST_SCHEMA_NAME)
          .dataType(OneType.RECORD)
          .fields(
              Arrays.asList(
                  OneField.builder().name(FIELD_1).schema(STRING_SCHEMA).build(),
                  PARTITION_FIELD,
                  OneField.builder().name(FIELD_3).schema(STRING_SCHEMA).build()))
          .build();
  private final String tableBasePath = tempDir.resolve(UUID.randomUUID().toString()).toString();

  @Test
  void syncForExistingTable() {
    String partitionPath = "partition_path";
    String commitTime = "20231003013807542";
    String existingFileName1 = "existing_file_1.parquet";
    HoodieTableMetaClient setupMetaClient = initTableAndGetMetaClient(tableBasePath, FIELD_2);
    // initialize the table with only 2 of the 3 fields
    Schema initialSchema =
        SchemaBuilder.record(TEST_SCHEMA_NAME)
            .fields()
            .requiredString(FIELD_1)
            .requiredString(FIELD_2)
            .endRecord();
    HoodieWriteConfig writeConfig = getHoodieWriteConfig(setupMetaClient, initialSchema);
    List<WriteStatus> initialWriteStatuses =
        Collections.singletonList(
            createWriteStatus(
                existingFileName1, partitionPath, commitTime, 1, 10, Collections.emptyMap()));
    try (HoodieJavaWriteClient<?> writeClient = new HoodieJavaWriteClient<>(CONTEXT, writeConfig)) {
      String initialInstant =
          writeClient.startCommit(HoodieTimeline.REPLACE_COMMIT_ACTION, setupMetaClient);
      setupMetaClient
          .getActiveTimeline()
          .transitionReplaceRequestedToInflight(
              new HoodieInstant(
                  HoodieInstant.State.REQUESTED,
                  HoodieTimeline.REPLACE_COMMIT_ACTION,
                  initialInstant),
              Option.empty());
      writeClient.commit(
          initialInstant,
          initialWriteStatuses,
          Option.empty(),
          HoodieTimeline.REPLACE_COMMIT_ACTION,
          Collections.emptyMap());
    }

    OneDataFile fileToRemove =
        OneDataFile.builder()
            .fileFormat(FileFormat.APACHE_PARQUET)
            .lastModified(System.currentTimeMillis())
            .fileSizeBytes(100L)
            .columnStats(Collections.emptyMap())
            .partitionPath(partitionPath)
            .physicalPath(
                String.format("%s/%s/%s", tableBasePath, partitionPath, existingFileName1))
            .recordCount(2)
            .schemaVersion(SCHEMA_VERSION)
            .build();
    String fileName = "file_1.parquet";
    String filePath = partitionPath + "/" + fileName;

    OneDataFilesDiff dataFilesDiff =
        OneDataFilesDiff.builder()
            .fileAdded(getTestFile(partitionPath, fileName))
            .fileRemoved(fileToRemove)
            .build();
    // perform sync
    HudiTargetClient targetClient = getTargetClient();
    OneTable initialState = getState();
    targetClient.beginSync(initialState);
    targetClient.syncFilesForDiff(dataFilesDiff);
    targetClient.syncSchema(SCHEMA);
    OneTableMetadata latestState =
        OneTableMetadata.of(initialState.getLatestCommitTime(), Collections.emptyList());
    targetClient.syncMetadata(latestState);
    targetClient.completeSync();

    Instant syncedInstant = targetClient.getTableMetadata().get().getLastInstantSynced();
    String instantTime = HudiTargetClient.convertInstantToCommit(syncedInstant);
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(CONFIGURATION).setBasePath(tableBasePath).build();
    assertFileGroupCorrectness(metaClient, instantTime, partitionPath, filePath, fileName);
    HoodieBackedTableMetadata hoodieBackedTableMetadata =
        new HoodieBackedTableMetadata(
            CONTEXT, writeConfig.getMetadataConfig(), tableBasePath, true);
    assertColStats(hoodieBackedTableMetadata, partitionPath, fileName);
    // include meta fields since the table was created with meta fields enabled
    assertSchema(metaClient, true);
  }

  @Test
  void syncForNewTable() {
    String partitionPath = "partition_path";
    String fileName = "file_1.parquet";
    String filePath = partitionPath + "/" + fileName;
    OneDataFiles snapshot =
        OneDataFiles.collectionBuilder()
            .files(
                Collections.singletonList(
                    OneDataFiles.collectionBuilder()
                        .partitionPath(partitionPath)
                        .files(Collections.singletonList(getTestFile(partitionPath, fileName)))
                        .build()))
            .build();
    // sync snapshot and metadata
    OneTable initialState = getState();
    HudiTargetClient targetClient = getTargetClient();
    targetClient.beginSync(initialState);
    targetClient.syncFilesForSnapshot(snapshot);
    OneTableMetadata latestState =
        OneTableMetadata.of(initialState.getLatestCommitTime(), Collections.emptyList());
    targetClient.syncSchema(initialState.getReadSchema());
    targetClient.syncMetadata(latestState);
    targetClient.completeSync();

    Instant syncedInstant = targetClient.getTableMetadata().get().getLastInstantSynced();
    String instantTime = HudiTargetClient.convertInstantToCommit(syncedInstant);
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(CONFIGURATION).setBasePath(tableBasePath).build();
    assertFileGroupCorrectness(metaClient, instantTime, partitionPath, filePath, fileName);
    HoodieBackedTableMetadata hoodieBackedTableMetadata =
        new HoodieBackedTableMetadata(
            CONTEXT, getHoodieWriteConfig(metaClient).getMetadataConfig(), tableBasePath, true);
    assertColStats(hoodieBackedTableMetadata, partitionPath, fileName);
    assertSchema(metaClient, false);
  }

  @SneakyThrows
  private void assertSchema(HoodieTableMetaClient metaClient, boolean includeMetaFields) {
    TableSchemaResolver tableSchemaResolver = new TableSchemaResolver(metaClient);
    Schema actual = tableSchemaResolver.getTableAvroSchema();
    Schema expected;
    if (includeMetaFields) {
      expected =
          SchemaBuilder.record(TEST_SCHEMA_NAME)
              .fields()
              .optionalString(
                  HoodieRecord.HoodieMetadataField.COMMIT_TIME_METADATA_FIELD.getFieldName())
              .optionalString(
                  HoodieRecord.HoodieMetadataField.COMMIT_SEQNO_METADATA_FIELD.getFieldName())
              .optionalString(
                  HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD.getFieldName())
              .optionalString(
                  HoodieRecord.HoodieMetadataField.PARTITION_PATH_METADATA_FIELD.getFieldName())
              .optionalString(
                  HoodieRecord.HoodieMetadataField.FILENAME_METADATA_FIELD.getFieldName())
              .requiredString(FIELD_1)
              .requiredString(FIELD_2)
              .requiredString(FIELD_3)
              .endRecord();
    } else {
      expected =
          SchemaBuilder.record(TEST_SCHEMA_NAME)
              .fields()
              .requiredString(FIELD_1)
              .requiredString(FIELD_2)
              .requiredString(FIELD_3)
              .endRecord();
    }
    assertEquals(expected, actual);
  }

  private void assertFileGroupCorrectness(
      HoodieTableMetaClient metaClient,
      String instantTime,
      String partitionPath,
      String filePath,
      String fileId) {
    HoodieTableFileSystemView fsView =
        new HoodieMetadataFileSystemView(
            CONTEXT,
            metaClient,
            metaClient.reloadActiveTimeline(),
            getHoodieWriteConfig(metaClient).getMetadataConfig());
    List<HoodieFileGroup> fileGroups =
        fsView.getAllFileGroups(partitionPath).collect(Collectors.toList());
    assertEquals(1, fileGroups.size());
    Option<HoodieFileGroup> fileGroupOption =
        Option.fromJavaOptional(
            fileGroups.stream()
                .filter(fg -> fg.getFileGroupId().getFileId().equals(fileId))
                .findFirst());
    assertTrue(fileGroupOption.isPresent());
    HoodieFileGroup fileGroup = fileGroupOption.get();
    assertEquals(fileId, fileGroup.getFileGroupId().getFileId());
    assertEquals(partitionPath, fileGroup.getPartitionPath());
    HoodieBaseFile baseFile = fileGroup.getAllBaseFiles().findFirst().get();
    assertEquals(instantTime, baseFile.getCommitTime());
    assertEquals(metaClient.getBasePathV2().toString() + "/" + filePath, baseFile.getPath());
  }

  private void assertColStats(
      HoodieBackedTableMetadata hoodieBackedTableMetadata, String partitionPath, String fileName) {
    assertColStatsForField(
        hoodieBackedTableMetadata, partitionPath, fileName, FIELD_1, "id1", "id2", 2, 0, 5);
    assertColStatsForField(
        hoodieBackedTableMetadata, partitionPath, fileName, FIELD_2, "a", "b", 3, 1, 10);
    assertColStatsForField(
        hoodieBackedTableMetadata,
        partitionPath,
        fileName,
        FIELD_3,
        "content1",
        "content2",
        2,
        0,
        5);
  }

  private void assertColStatsForField(
      HoodieBackedTableMetadata hoodieBackedTableMetadata,
      String partitionPath,
      String fileName,
      String fieldName,
      String minValue,
      String maxValue,
      int valueCount,
      int nullCount,
      int totalSize) {
    Map<Pair<String, String>, HoodieMetadataColumnStats> fieldColStats =
        hoodieBackedTableMetadata.getColumnStats(
            Collections.singletonList(Pair.of(partitionPath, fileName)), fieldName);
    assertEquals(1, fieldColStats.size());
    HoodieMetadataColumnStats columnStats = fieldColStats.get(Pair.of(partitionPath, fileName));
    assertEquals(fieldName, columnStats.getColumnName());
    assertEquals(fileName, columnStats.getFileName());
    assertEquals(new StringWrapper(minValue), columnStats.getMinValue());
    assertEquals(new StringWrapper(maxValue), columnStats.getMaxValue());
    assertEquals(valueCount, columnStats.getValueCount());
    assertEquals(nullCount, columnStats.getNullCount());
    assertEquals(totalSize, columnStats.getTotalSize());
    assertEquals(-1, columnStats.getTotalUncompressedSize());
  }

  private OneDataFile getTestFile(String partitionPath, String fileName) {
    Map<OneField, ColumnStat> columnStats = new HashMap<>();
    columnStats.put(
        SCHEMA.getFields().get(0),
        ColumnStat.builder()
            .range(Range.vector("id1", "id2"))
            .numNulls(0)
            .numValues(2)
            .totalSize(5)
            .build());
    columnStats.put(
        SCHEMA.getFields().get(1),
        ColumnStat.builder()
            .range(Range.vector("a", "b"))
            .numNulls(1)
            .numValues(3)
            .totalSize(10)
            .build());
    columnStats.put(
        SCHEMA.getFields().get(2),
        ColumnStat.builder()
            .range(Range.vector("content1", "content2"))
            .numNulls(0)
            .numValues(2)
            .totalSize(5)
            .build());
    return OneDataFile.builder()
        .schemaVersion(SCHEMA_VERSION)
        .partitionPath(partitionPath)
        .physicalPath(String.format("%s/%s/%s", tableBasePath, partitionPath, fileName))
        .fileSizeBytes(FILE_SIZE)
        .fileFormat(FileFormat.APACHE_PARQUET)
        .lastModified(LAST_MODIFIED)
        .recordCount(RECORD_COUNT)
        .columnStats(columnStats)
        .build();
  }

  private OneTable getState() {
    return OneTable.builder()
        .basePath(tableBasePath)
        .name(TABLE_NAME)
        .latestCommitTime(Instant.now())
        .tableFormat(TableFormat.ICEBERG)
        .layoutStrategy(DataLayoutStrategy.HIVE_STYLE_PARTITION)
        .readSchema(SCHEMA)
        .partitioningFields(
            Collections.singletonList(
                OnePartitionField.builder()
                    .sourceField(PARTITION_FIELD)
                    .transformType(PartitionTransformType.VALUE)
                    .build()))
        .build();
  }

  private HudiTargetClient getTargetClient() {
    return new HudiTargetClient(
        PerTableConfig.builder()
            .tableBasePath(tableBasePath)
            .targetTableFormats(Collections.singletonList(TableFormat.HUDI))
            .tableName("test_table")
            .build(),
        CONFIGURATION);
  }
}
