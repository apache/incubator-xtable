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

import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Collectors;

import lombok.SneakyThrows;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.avro.model.StringWrapper;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;

import io.onetable.client.TargetTable;
import io.onetable.model.OneTable;
import io.onetable.model.OneTableMetadata;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.PartitionValue;
import io.onetable.model.stat.Range;
import io.onetable.model.storage.DataLayoutStrategy;
import io.onetable.model.storage.FileFormat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.model.storage.OneFileGroup;
import io.onetable.model.storage.TableFormat;
import io.onetable.spi.sync.TargetClient;

/**
 * A suite of functional tests that assert that the metadata for the hudi table is properly written
 * to disk.
 */
public class ITHudiTargetClient {
  @TempDir public static Path tempDir;
  private static final Configuration CONFIGURATION = new Configuration();
  private static final HoodieEngineContext CONTEXT = new HoodieJavaEngineContext(CONFIGURATION);

  private static final String TABLE_NAME = "test_table";
  private static final String KEY_FIELD_NAME = "id";
  private static final String PARTITION_FIELD_NAME = "partition_field";
  private static final String OTHER_FIELD_NAME = "content";
  private static final long FILE_SIZE = 100L;
  private static final long RECORD_COUNT = 200L;
  private static final long LAST_MODIFIED = System.currentTimeMillis();
  private static final OneSchema STRING_SCHEMA =
      OneSchema.builder().name("string").dataType(OneType.STRING).isNullable(false).build();

  private static final OneField PARTITION_FIELD_SOURCE =
      OneField.builder().name(PARTITION_FIELD_NAME).schema(STRING_SCHEMA).build();

  private static final OnePartitionField PARTITION_FIELD =
      OnePartitionField.builder()
          .sourceField(PARTITION_FIELD_SOURCE)
          .transformType(PartitionTransformType.VALUE)
          .build();
  private static final String TEST_SCHEMA_NAME = "test_schema";
  private static final OneSchema SCHEMA =
      OneSchema.builder()
          .name(TEST_SCHEMA_NAME)
          .dataType(OneType.RECORD)
          .fields(
              Arrays.asList(
                  OneField.builder().name(KEY_FIELD_NAME).schema(STRING_SCHEMA).build(),
                  PARTITION_FIELD_SOURCE,
                  OneField.builder().name(OTHER_FIELD_NAME).schema(STRING_SCHEMA).build()))
          .build();
  private final String tableBasePath = tempDir.resolve(UUID.randomUUID().toString()).toString();

  @BeforeAll
  public static void setupOnce() {
    // avoids issues with Hudi default usage of local timezone when setting up tests
    HoodieInstantTimeGenerator.setCommitTimeZone(HoodieTimelineTimeZone.UTC);
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
    System.setProperty("user.timezone", "GMT");
  }

  @Test
  void syncForExistingTable() {
    String partitionPath = "partition_path";
    String commitTime = "20231003013807542";
    String existingFileName1 = "existing_file_1.parquet";
    HoodieTableMetaClient setupMetaClient =
        initTableAndGetMetaClient(tableBasePath, PARTITION_FIELD_NAME);
    // initialize the table with only 2 of the 3 fields
    Schema initialSchema =
        SchemaBuilder.record(TEST_SCHEMA_NAME)
            .fields()
            .requiredString(KEY_FIELD_NAME)
            .requiredString(PARTITION_FIELD_NAME)
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
            .columnStats(Collections.emptyList())
            .physicalPath(
                String.format("file://%s/%s/%s", tableBasePath, partitionPath, existingFileName1))
            .recordCount(2)
            .schemaVersion(SCHEMA_VERSION)
            .build();
    String fileName = "file_1.parquet";
    String filePath = getFilePath(partitionPath, fileName);

    OneDataFilesDiff dataFilesDiff =
        OneDataFilesDiff.builder()
            .fileAdded(getTestFile(partitionPath, fileName))
            .fileRemoved(fileToRemove)
            .build();
    // perform sync
    HudiTargetClient targetClient = getTargetClient();
    OneTable initialState = getState(Instant.now());
    targetClient.beginSync(initialState);
    targetClient.syncFilesForDiff(dataFilesDiff);
    targetClient.syncSchema(SCHEMA);
    OneTableMetadata latestState =
        OneTableMetadata.of(initialState.getLatestCommitTime(), Collections.emptyList());
    targetClient.syncMetadata(latestState);
    targetClient.completeSync();

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(CONFIGURATION).setBasePath(tableBasePath).build();
    assertFileGroupCorrectness(
        metaClient, partitionPath, Collections.singletonList(Pair.of(fileName, filePath)));
    try (HoodieBackedTableMetadata hoodieBackedTableMetadata =
        new HoodieBackedTableMetadata(
            CONTEXT, writeConfig.getMetadataConfig(), tableBasePath, true)) {
      assertColStats(hoodieBackedTableMetadata, partitionPath, fileName);
    }
    // include meta fields since the table was created with meta fields enabled
    assertSchema(metaClient, true);
  }

  @Test
  void syncForNewTable() {
    String partitionPath = "partition_path";
    String fileName = "file_1.parquet";
    String filePath = getFilePath(partitionPath, fileName);
    List<OneFileGroup> snapshot =
        Collections.singletonList(
            OneFileGroup.builder()
                .files(Collections.singletonList(getTestFile(partitionPath, fileName)))
                .partitionValues(
                    Collections.singletonList(
                        PartitionValue.builder()
                            .partitionField(PARTITION_FIELD)
                            .range(Range.scalar("partitionPath"))
                            .build()))
                .build());
    // sync snapshot and metadata
    OneTable initialState = getState(Instant.now());
    HudiTargetClient targetClient = getTargetClient();
    targetClient.beginSync(initialState);
    targetClient.syncFilesForSnapshot(snapshot);
    OneTableMetadata latestState =
        OneTableMetadata.of(initialState.getLatestCommitTime(), Collections.emptyList());
    targetClient.syncSchema(initialState.getReadSchema());
    targetClient.syncMetadata(latestState);
    targetClient.completeSync();

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(CONFIGURATION).setBasePath(tableBasePath).build();
    assertFileGroupCorrectness(
        metaClient, partitionPath, Collections.singletonList(Pair.of(fileName, filePath)));
    try (HoodieBackedTableMetadata hoodieBackedTableMetadata =
        new HoodieBackedTableMetadata(
            CONTEXT, getHoodieWriteConfig(metaClient).getMetadataConfig(), tableBasePath, true)) {
      assertColStats(hoodieBackedTableMetadata, partitionPath, fileName);
    }
    assertSchema(metaClient, false);
  }

  @ParameterizedTest
  @ValueSource(strings = {"partition_path", ""})
  void archiveTimelineAndCleanMetadataTableAfterMultipleCommits(String partitionPath) {
    String fileName0 = "file_0.parquet";
    String filePath0 = getFilePath(partitionPath, fileName0);

    String fileName1 = "file_1.parquet";
    String filePath1 = getFilePath(partitionPath, fileName1);
    List<OneFileGroup> snapshot =
        Collections.singletonList(
            OneFileGroup.builder()
                .files(
                    Arrays.asList(
                        getTestFile(partitionPath, fileName0),
                        getTestFile(partitionPath, fileName1)))
                .partitionValues(
                    Collections.singletonList(
                        PartitionValue.builder()
                            .partitionField(PARTITION_FIELD)
                            .range(Range.scalar("partitionPath"))
                            .build()))
                .build());
    // sync snapshot and metadata
    OneTable initialState = getState(Instant.now().minus(24, ChronoUnit.HOURS));
    HudiTargetClient targetClient = getTargetClient();
    targetClient.beginSync(initialState);
    targetClient.syncFilesForSnapshot(snapshot);
    OneTableMetadata latestState =
        OneTableMetadata.of(initialState.getLatestCommitTime(), Collections.emptyList());
    targetClient.syncMetadata(latestState);
    targetClient.syncSchema(initialState.getReadSchema());
    targetClient.completeSync();

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(CONFIGURATION).setBasePath(tableBasePath).build();
    Pair<String, String> file0Pair = Pair.of(fileName0, filePath0);
    assertFileGroupCorrectness(
        metaClient, partitionPath, Arrays.asList(file0Pair, Pair.of(fileName1, filePath1)));
    try (HoodieBackedTableMetadata hoodieBackedTableMetadata =
        new HoodieBackedTableMetadata(
            CONTEXT, getHoodieWriteConfig(metaClient).getMetadataConfig(), tableBasePath, true)) {
      assertColStats(hoodieBackedTableMetadata, partitionPath, fileName1);
    }

    // create a new commit that removes fileName1 and adds fileName2
    String fileName2 = "file_2.parquet";
    String filePath2 = getFilePath(partitionPath, fileName2);
    incrementalSync(
        targetClient,
        Collections.singletonList(getTestFile(partitionPath, fileName2)),
        Collections.singletonList(getTestFile(partitionPath, fileName1)),
        Instant.now().minus(12, ChronoUnit.HOURS));

    assertFileGroupCorrectness(
        metaClient, partitionPath, Arrays.asList(file0Pair, Pair.of(fileName2, filePath2)));
    try (HoodieBackedTableMetadata hoodieBackedTableMetadata =
        new HoodieBackedTableMetadata(
            CONTEXT, getHoodieWriteConfig(metaClient).getMetadataConfig(), tableBasePath, true)) {
      // the metadata for fileName1 should still be present until the cleaner kicks in
      assertColStats(hoodieBackedTableMetadata, partitionPath, fileName1);
      // new file stats should be present
      assertColStats(hoodieBackedTableMetadata, partitionPath, fileName2);
    }

    // create a new commit that removes fileName2 and adds fileName3
    String fileName3 = "file_3.parquet";
    String filePath3 = getFilePath(partitionPath, fileName3);
    incrementalSync(
        targetClient,
        Collections.singletonList(getTestFile(partitionPath, fileName3)),
        Collections.singletonList(getTestFile(partitionPath, fileName2)),
        Instant.now().minus(8, ChronoUnit.HOURS));

    // create a commit that just adds fileName4
    String fileName4 = "file_4.parquet";
    String filePath4 = getFilePath(partitionPath, fileName4);
    incrementalSync(
        targetClient,
        Collections.singletonList(getTestFile(partitionPath, fileName4)),
        Collections.emptyList(),
        Instant.now());

    // create another commit that should trigger archival of the first two commits
    String fileName5 = "file_5.parquet";
    String filePath5 = getFilePath(partitionPath, fileName5);
    incrementalSync(
        targetClient,
        Collections.singletonList(getTestFile(partitionPath, fileName5)),
        Collections.emptyList(),
        Instant.now());

    assertFileGroupCorrectness(
        metaClient,
        partitionPath,
        Arrays.asList(
            file0Pair,
            Pair.of(fileName3, filePath3),
            Pair.of(fileName4, filePath4),
            Pair.of(fileName5, filePath5)));
    // col stats should be cleaned up for fileName1 but present for fileName2 and fileName3
    try (HoodieBackedTableMetadata hoodieBackedTableMetadata =
        new HoodieBackedTableMetadata(
            CONTEXT, getHoodieWriteConfig(metaClient).getMetadataConfig(), tableBasePath, true)) {
      // assertEmptyColStats(hoodieBackedTableMetadata, partitionPath, fileName1);
      assertColStats(hoodieBackedTableMetadata, partitionPath, fileName3);
      assertColStats(hoodieBackedTableMetadata, partitionPath, fileName4);
    }
    // the first commit to the timeline should be archived
    assertEquals(
        2, metaClient.getArchivedTimeline().reload().filterCompletedInstants().countInstants());
  }

  private OneTableMetadata incrementalSync(
      TargetClient targetClient,
      List<OneDataFile> filesToAdd,
      List<OneDataFile> filesToRemove,
      Instant commitStart) {
    OneDataFilesDiff dataFilesDiff2 =
        OneDataFilesDiff.builder().filesAdded(filesToAdd).filesRemoved(filesToRemove).build();
    OneTable state3 = getState(commitStart);
    targetClient.beginSync(state3);
    targetClient.syncFilesForDiff(dataFilesDiff2);
    OneTableMetadata latestState =
        OneTableMetadata.of(state3.getLatestCommitTime(), Collections.emptyList());
    targetClient.syncMetadata(latestState);
    targetClient.completeSync();
    return latestState;
  }

  private static String getFilePath(String partitionPath, String fileName) {
    if (partitionPath.isEmpty()) {
      return fileName;
    }
    return partitionPath + "/" + fileName;
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
              .requiredString(KEY_FIELD_NAME)
              .requiredString(PARTITION_FIELD_NAME)
              .requiredString(OTHER_FIELD_NAME)
              .endRecord();
    } else {
      expected =
          SchemaBuilder.record(TEST_SCHEMA_NAME)
              .fields()
              .requiredString(KEY_FIELD_NAME)
              .requiredString(PARTITION_FIELD_NAME)
              .requiredString(OTHER_FIELD_NAME)
              .endRecord();
    }
    assertEquals(expected, actual);
  }

  private void assertFileGroupCorrectness(
      HoodieTableMetaClient metaClient,
      String partitionPath,
      List<Pair<String, String>> fileIdAndPath) {
    HoodieTableFileSystemView fsView =
        new HoodieMetadataFileSystemView(
            CONTEXT,
            metaClient,
            metaClient.reloadActiveTimeline(),
            getHoodieWriteConfig(metaClient).getMetadataConfig());
    List<HoodieFileGroup> fileGroups =
        fsView
            .getAllFileGroups(partitionPath)
            .sorted(Comparator.comparing(HoodieFileGroup::getFileGroupId))
            .collect(Collectors.toList());
    assertEquals(fileIdAndPath.size(), fileGroups.size());
    for (int i = 0; i < fileIdAndPath.size(); i++) {
      HoodieFileGroup fileGroup = fileGroups.get(i);
      String expectedFileId = fileIdAndPath.get(i).getLeft();
      String expectedFilePath = fileIdAndPath.get(i).getRight();
      assertEquals(expectedFileId, fileGroup.getFileGroupId().getFileId());
      assertEquals(partitionPath, fileGroup.getPartitionPath());
      HoodieBaseFile baseFile = fileGroup.getAllBaseFiles().findFirst().get();
      assertEquals(
          metaClient.getBasePathV2().toString() + "/" + expectedFilePath, baseFile.getPath());
    }
    fsView.close();
  }

  private void assertEmptyColStats(
      HoodieBackedTableMetadata hoodieBackedTableMetadata, String partitionPath, String fileName) {
    Assertions.assertTrue(
        hoodieBackedTableMetadata
            .getColumnStats(
                Collections.singletonList(Pair.of(partitionPath, fileName)), KEY_FIELD_NAME)
            .isEmpty());
    Assertions.assertTrue(
        hoodieBackedTableMetadata
            .getColumnStats(
                Collections.singletonList(Pair.of(partitionPath, fileName)), PARTITION_FIELD_NAME)
            .isEmpty());
    Assertions.assertTrue(
        hoodieBackedTableMetadata
            .getColumnStats(
                Collections.singletonList(Pair.of(partitionPath, fileName)), OTHER_FIELD_NAME)
            .isEmpty());
  }

  private void assertColStats(
      HoodieBackedTableMetadata hoodieBackedTableMetadata, String partitionPath, String fileName) {
    assertColStatsForField(
        hoodieBackedTableMetadata, partitionPath, fileName, KEY_FIELD_NAME, "id1", "id2", 2, 0, 5);
    assertColStatsForField(
        hoodieBackedTableMetadata,
        partitionPath,
        fileName,
        PARTITION_FIELD_NAME,
        "a",
        "b",
        3,
        1,
        10);
    assertColStatsForField(
        hoodieBackedTableMetadata,
        partitionPath,
        fileName,
        OTHER_FIELD_NAME,
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
    List<ColumnStat> columnStats =
        Arrays.asList(
            ColumnStat.builder()
                .field(SCHEMA.getFields().get(0))
                .range(Range.vector("id1", "id2"))
                .numNulls(0)
                .numValues(2)
                .totalSize(5)
                .build(),
            ColumnStat.builder()
                .field(SCHEMA.getFields().get(1))
                .range(Range.vector("a", "b"))
                .numNulls(1)
                .numValues(3)
                .totalSize(10)
                .build(),
            ColumnStat.builder()
                .field(SCHEMA.getFields().get(2))
                .range(Range.vector("content1", "content2"))
                .numNulls(0)
                .numValues(2)
                .totalSize(5)
                .build());
    return OneDataFile.builder()
        .schemaVersion(SCHEMA_VERSION)
        .physicalPath(String.format("file://%s/%s/%s", tableBasePath, partitionPath, fileName))
        .fileSizeBytes(FILE_SIZE)
        .fileFormat(FileFormat.APACHE_PARQUET)
        .lastModified(LAST_MODIFIED)
        .recordCount(RECORD_COUNT)
        .columnStats(columnStats)
        .build();
  }

  private OneTable getState(Instant latestCommitTime) {
    return OneTable.builder()
        .basePath(tableBasePath)
        .name(TABLE_NAME)
        .latestCommitTime(latestCommitTime)
        .tableFormat(TableFormat.ICEBERG)
        .layoutStrategy(DataLayoutStrategy.HIVE_STYLE_PARTITION)
        .readSchema(SCHEMA)
        .partitioningFields(
            Collections.singletonList(
                OnePartitionField.builder()
                    .sourceField(PARTITION_FIELD_SOURCE)
                    .transformType(PartitionTransformType.VALUE)
                    .build()))
        .build();
  }

  private HudiTargetClient getTargetClient() {
    return new HudiTargetClient(
        TargetTable.builder()
            .basePath(tableBasePath)
            .formatName(TableFormat.HUDI)
            .name("test_table")
            .metadataRetentionInHours(4)
            .build(),
        CONFIGURATION,
        3);
  }
}
