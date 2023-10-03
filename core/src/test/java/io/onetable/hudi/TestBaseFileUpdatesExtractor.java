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

import static io.onetable.testutil.ColumnStatMapUtil.getColumnStatMap;
import static org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.SchemaVersion;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.storage.FileFormat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.OneDataFilesDiff;

public class TestBaseFileUpdatesExtractor {
  @TempDir public static java.nio.file.Path tempDir;
  private static final SchemaVersion SCHEMA_VERSION = new SchemaVersion(1, "");
  private static final String COMMIT_TIME = "20231003013807542";
  private static final long FILE_SIZE = 100L;
  private static final long RECORD_COUNT = 200L;
  private static final long LAST_MODIFIED = System.currentTimeMillis();
  private static final HoodieEngineContext CONTEXT =
      new HoodieJavaEngineContext(new Configuration());

  @Test
  void convertDiff() {
    String tableBasePath = "file://base";
    String partitionPath1 = "partition1";
    String fileName1 = "file1.parquet";
    // create file with empty stats to test edge case
    OneDataFile addedFile1 =
        createFile(
            partitionPath1,
            String.format("%s/%s/%s", tableBasePath, partitionPath1, fileName1),
            Collections.emptyMap());
    // create file with stats
    String partitionPath2 = "partition2";
    String fileName2 = "file2.parquet";
    OneDataFile addedFile2 =
        createFile(
            partitionPath2,
            String.format("%s/%s/%s", tableBasePath, partitionPath2, fileName2),
            getColumnStatMap());

    // remove files 3 files from two different partitions
    String fileName3 = "file3.parquet";
    OneDataFile removedFile1 =
        createFile(
            partitionPath1,
            String.format("%s/%s/%s", tableBasePath, partitionPath1, fileName3),
            getColumnStatMap());
    String fileName4 = "file4.parquet";
    OneDataFile removedFile2 =
        createFile(
            partitionPath1,
            String.format("%s/%s/%s", tableBasePath, partitionPath1, fileName4),
            Collections.emptyMap());
    String fileName5 = "file5.parquet";
    OneDataFile removedFile3 =
        createFile(
            partitionPath2,
            String.format("%s/%s/%s", tableBasePath, partitionPath2, fileName5),
            Collections.emptyMap());

    OneDataFilesDiff diff =
        OneDataFilesDiff.builder()
            .filesAdded(Arrays.asList(addedFile1, addedFile2))
            .filesRemoved(Arrays.asList(removedFile1, removedFile2, removedFile3))
            .build();

    BaseFileUpdatesExtractor extractor = BaseFileUpdatesExtractor.of(CONTEXT, tableBasePath);
    BaseFileUpdatesExtractor.ReplaceMetadata replaceMetadata =
        extractor.convertDiff(diff, COMMIT_TIME);

    // validate removed files
    Map<String, List<String>> expectedPartitionToReplacedFileIds = new HashMap<>();
    expectedPartitionToReplacedFileIds.put(partitionPath1, Arrays.asList(fileName3, fileName4));
    expectedPartitionToReplacedFileIds.put(partitionPath2, Collections.singletonList(fileName5));
    assertEquals(
        expectedPartitionToReplacedFileIds, replaceMetadata.getPartitionToReplacedFileIds());

    // validate added files
    List<WriteStatus> expectedWriteStatuses =
        Arrays.asList(
            getExpectedWriteStatus(fileName1, partitionPath1, COMMIT_TIME, Collections.emptyMap()),
            getExpectedWriteStatus(
                fileName2, partitionPath2, COMMIT_TIME, getExpectedColumnStats(fileName2)));
    assertWriteStatusesEquivalent(expectedWriteStatuses, replaceMetadata.getWriteStatuses());
  }

  @Test
  void extractSnapshotChanges_emptyTargetTable() throws IOException {
    String tableBasePath = tempDir.resolve(UUID.randomUUID().toString()).toString();
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.withPropertyBuilder()
            .setTableType(HoodieTableType.COPY_ON_WRITE)
            .setTableName("test_table")
            .setPayloadClass(HoodieAvroPayload.class)
            .setPartitionFields("partition_field")
            .initTable(new Configuration(), tableBasePath);

    String partitionPath1 = "partition1";
    String fileName1 = "file1.parquet";
    // create file with empty stats to test edge case
    OneDataFile addedFile1 =
        createFile(
            partitionPath1,
            String.format("%s/%s/%s", tableBasePath, partitionPath1, fileName1),
            Collections.emptyMap());
    // create file with stats
    String fileName2 = "file2.parquet";
    OneDataFile addedFile2 =
        createFile(
            partitionPath1,
            String.format("%s/%s/%s", tableBasePath, partitionPath1, fileName2),
            getColumnStatMap());
    // create file in a second partition
    String partitionPath2 = "partition2";
    String fileName3 = "file3.parquet";
    OneDataFile addedFile3 =
        createFile(
            partitionPath2,
            String.format("%s/%s/%s", tableBasePath, partitionPath2, fileName3),
            getColumnStatMap());

    BaseFileUpdatesExtractor extractor = BaseFileUpdatesExtractor.of(CONTEXT, tableBasePath);
    OneDataFiles snapshotFiles =
        OneDataFiles.collectionBuilder()
            .files(
                Arrays.asList(
                    OneDataFiles.collectionBuilder()
                        .files(Arrays.asList(addedFile1, addedFile2))
                        .partitionPath(partitionPath1)
                        .build(),
                    OneDataFiles.collectionBuilder()
                        .files(Collections.singletonList(addedFile3))
                        .partitionPath(partitionPath2)
                        .build()))
            .build();
    BaseFileUpdatesExtractor.ReplaceMetadata replaceMetadata =
        extractor.extractSnapshotChanges(snapshotFiles, metaClient, COMMIT_TIME);

    // table is empty so there be no removals
    assertEquals(Collections.emptyMap(), replaceMetadata.getPartitionToReplacedFileIds());

    List<WriteStatus> expectedWriteStatuses =
        Arrays.asList(
            getExpectedWriteStatus(fileName1, partitionPath1, COMMIT_TIME, Collections.emptyMap()),
            getExpectedWriteStatus(
                fileName2, partitionPath1, COMMIT_TIME, getExpectedColumnStats(fileName2)),
            getExpectedWriteStatus(
                fileName3, partitionPath2, COMMIT_TIME, getExpectedColumnStats(fileName3)));
    assertWriteStatusesEquivalent(expectedWriteStatuses, replaceMetadata.getWriteStatuses());
  }

  @Test
  void extractSnapshotChanges_existingPartitionedTargetTable() throws IOException {
    String tableBasePath = tempDir.resolve(UUID.randomUUID().toString()).toString();
    HoodieTableMetaClient setupMetaClient =
        initTableAndGetMetaClient(tableBasePath, "partition_field");
    HoodieWriteConfig writeConfig = getHoodieWriteConfig(setupMetaClient);

    String partitionPath1 = "partition1";
    String partitionPath2 = "partition2";
    String partitionPath3 = "partition3";
    // initialize the table with 1 file in partition1 and 2 files in partition2
    String existingFileName1 = "existing_file_1.parquet";
    String existingFileName2 = "existing_file_2.parquet";
    String existingFileName3 = "existing_file_3.parquet";

    List<WriteStatus> initialWriteStatuses =
        Arrays.asList(
            getExpectedWriteStatus(
                existingFileName1, partitionPath1, COMMIT_TIME, Collections.emptyMap()),
            getExpectedWriteStatus(
                existingFileName2, partitionPath2, COMMIT_TIME, Collections.emptyMap()),
            getExpectedWriteStatus(
                existingFileName3, partitionPath2, COMMIT_TIME, Collections.emptyMap()));
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

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(setupMetaClient);
    // create a snapshot without partition1 (dropped partition), a new file added to partition 2
    // along with one of the existing files, and a new file in partition 3
    String newFileName1 = "new_file_1.parquet";
    OneDataFile addedFile1 =
        createFile(
            partitionPath2,
            String.format("%s/%s/%s", tableBasePath, partitionPath2, newFileName1),
            Collections.emptyMap());
    String newFileName2 = "new_file_2.parquet";
    OneDataFile addedFile2 =
        createFile(
            partitionPath3,
            String.format("%s/%s/%s", tableBasePath, partitionPath3, newFileName2),
            getColumnStatMap());
    // OneDataFile for one of the existing files in partition2
    OneDataFile existingFile =
        createFile(
            partitionPath2,
            String.format("%s/%s/%s", tableBasePath, partitionPath2, existingFileName2),
            Collections.emptyMap());
    OneDataFiles snapshotFiles =
        OneDataFiles.collectionBuilder()
            .files(
                Arrays.asList(
                    OneDataFiles.collectionBuilder()
                        .files(Arrays.asList(addedFile1, existingFile))
                        .partitionPath(partitionPath2)
                        .build(),
                    OneDataFiles.collectionBuilder()
                        .files(Collections.singletonList(addedFile2))
                        .partitionPath(partitionPath3)
                        .build()))
            .build();
    BaseFileUpdatesExtractor extractor = BaseFileUpdatesExtractor.of(CONTEXT, tableBasePath);
    BaseFileUpdatesExtractor.ReplaceMetadata replaceMetadata =
        extractor.extractSnapshotChanges(snapshotFiles, metaClient, COMMIT_TIME);

    // validate removed files
    Map<String, List<String>> expectedPartitionToReplacedFileIds = new HashMap<>();
    expectedPartitionToReplacedFileIds.put(
        partitionPath1, Collections.singletonList(existingFileName1));
    expectedPartitionToReplacedFileIds.put(
        partitionPath2, Collections.singletonList(existingFileName3));
    assertEquals(
        expectedPartitionToReplacedFileIds, replaceMetadata.getPartitionToReplacedFileIds());

    // validate added files
    List<WriteStatus> expectedWriteStatuses =
        Arrays.asList(
            getExpectedWriteStatus(
                newFileName1, partitionPath2, COMMIT_TIME, getExpectedColumnStats(newFileName1)),
            getExpectedWriteStatus(
                newFileName2, partitionPath3, COMMIT_TIME, Collections.emptyMap()));
    assertWriteStatusesEquivalent(expectedWriteStatuses, replaceMetadata.getWriteStatuses());
  }

  @Test
  void extractSnapshotChanges_existingNonPartitionedTargetTable() throws IOException {
    String tableBasePath = tempDir.resolve(UUID.randomUUID().toString()).toString();
    HoodieTableMetaClient setupMetaClient = initTableAndGetMetaClient(tableBasePath, "");
    HoodieWriteConfig writeConfig = getHoodieWriteConfig(setupMetaClient);

    // initialize the table with 2 files
    String existingFileName1 = "existing_file_1.parquet";
    String existingFileName2 = "existing_file_2.parquet";

    List<WriteStatus> initialWriteStatuses =
        Arrays.asList(
            getExpectedWriteStatus(existingFileName1, "", COMMIT_TIME, Collections.emptyMap()),
            getExpectedWriteStatus(existingFileName2, "", COMMIT_TIME, Collections.emptyMap()));
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

    HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(setupMetaClient);
    // create a snapshot with a new file added along with one of the existing files
    String newFileName1 = "new_file_1.parquet";
    OneDataFile addedFile1 =
        createFile("", String.format("%s/%s", tableBasePath, newFileName1), getColumnStatMap());
    // OneDataFile for one of the existing files in partition2
    OneDataFile existingFile =
        createFile(
            "", String.format("%s/%s", tableBasePath, existingFileName2), Collections.emptyMap());
    OneDataFiles snapshotFiles =
        OneDataFiles.collectionBuilder()
            .files(
                Collections.singletonList(
                    OneDataFiles.collectionBuilder()
                        .files(Arrays.asList(addedFile1, existingFile))
                        .partitionPath("")
                        .build()))
            .build();
    BaseFileUpdatesExtractor extractor = BaseFileUpdatesExtractor.of(CONTEXT, tableBasePath);
    BaseFileUpdatesExtractor.ReplaceMetadata replaceMetadata =
        extractor.extractSnapshotChanges(snapshotFiles, metaClient, COMMIT_TIME);

    // validate removed files
    Map<String, List<String>> expectedPartitionToReplacedFileIds =
        Collections.singletonMap("", Collections.singletonList(existingFileName1));
    assertEquals(
        expectedPartitionToReplacedFileIds, replaceMetadata.getPartitionToReplacedFileIds());

    // validate added files
    List<WriteStatus> expectedWriteStatuses =
        Collections.singletonList(
            getExpectedWriteStatus(
                newFileName1, "", COMMIT_TIME, getExpectedColumnStats(newFileName1)));
    assertWriteStatusesEquivalent(expectedWriteStatuses, replaceMetadata.getWriteStatuses());
  }

  private static HoodieTableMetaClient initTableAndGetMetaClient(
      String tableBasePath, String partitionFields) throws IOException {
    return HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName("test_table")
        .setPayloadClass(HoodieAvroPayload.class)
        .setPartitionFields(partitionFields)
        .initTable(new Configuration(), tableBasePath);
  }

  private static void assertWriteStatusesEquivalent(
      List<WriteStatus> expected, List<WriteStatus> actual) {
    // write status does not implement equals, so we compare their toString results
    assertEquals(
        expected.stream().map(WriteStatus::toString).collect(Collectors.toSet()),
        actual.stream().map(WriteStatus::toString).collect(Collectors.toSet()));
  }

  private OneDataFile createFile(
      String partitionPath, String physicalPath, Map<OneField, ColumnStat> columnStats) {
    return OneDataFile.builder()
        .schemaVersion(SCHEMA_VERSION)
        .partitionPath(partitionPath)
        .physicalPath(physicalPath)
        .fileSizeBytes(FILE_SIZE)
        .fileFormat(FileFormat.APACHE_PARQUET)
        .lastModified(LAST_MODIFIED)
        .recordCount(RECORD_COUNT)
        .columnStats(columnStats)
        .build();
  }

  private WriteStatus getExpectedWriteStatus(
      String fileName,
      String partitionPath,
      String commitTime,
      Map<String, HoodieColumnRangeMetadata<Comparable>> recordStats) {
    WriteStatus writeStatus = new WriteStatus();
    writeStatus.setFileId(fileName);
    writeStatus.setPartitionPath(partitionPath);
    HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
    writeStat.setFileId(fileName);
    writeStat.setPartitionPath(partitionPath);
    writeStat.setPath(
        ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(
            partitionPath.isEmpty() ? fileName : String.format("%s/%s", partitionPath, fileName),
            commitTime));
    writeStat.setNumWrites(RECORD_COUNT);
    writeStat.setFileSizeInBytes(FILE_SIZE);
    writeStat.setTotalWriteBytes(FILE_SIZE);
    writeStat.putRecordsStats(recordStats);
    writeStatus.setStat(writeStat);
    return writeStatus;
  }

  /**
   * Get expected col stats for a file.
   *
   * @param fileName name of the file
   * @return stats matching the column stats in {@link
   *     io.onetable.testutil.ColumnStatMapUtil#getColumnStatMap()}
   */
  private Map<String, HoodieColumnRangeMetadata<Comparable>> getExpectedColumnStats(
      String fileName) {
    Map<String, HoodieColumnRangeMetadata<Comparable>> columnStats = new HashMap<>();
    columnStats.put(
        "long_field",
        HoodieColumnRangeMetadata.<Comparable>create(
            fileName, "long_field", 10L, 20L, 4, 5, 123L, -1L));
    columnStats.put(
        "string_field",
        HoodieColumnRangeMetadata.<Comparable>create(
            fileName, "string_field", "a", "c", 1, 6, 500L, -1L));
    columnStats.put(
        "null_string_field",
        HoodieColumnRangeMetadata.<Comparable>create(
            fileName, "null_string_field", (String) null, (String) null, 3, 3, 0L, -1L));
    columnStats.put(
        "timestamp_field",
        HoodieColumnRangeMetadata.<Comparable>create(
            fileName, "timestamp_field", 1665263297000L, 1665436097000L, 105, 145, 999L, -1L));
    columnStats.put(
        "timestamp_micros_field",
        HoodieColumnRangeMetadata.<Comparable>create(
            fileName,
            "timestamp_micros_field",
            1665263297000000L,
            1665436097000000L,
            1,
            20,
            400,
            -1L));
    columnStats.put(
        "local_timestamp_field",
        HoodieColumnRangeMetadata.<Comparable>create(
            fileName, "local_timestamp_field", 1665263297000L, 1665436097000L, 1, 20, 400, -1L));
    columnStats.put(
        "date_field",
        HoodieColumnRangeMetadata.<Comparable>create(
            fileName, "date_field", 18181, 18547, 250, 300, 12345, -1L));
    columnStats.put(
        "array_long_field.array",
        HoodieColumnRangeMetadata.<Comparable>create(
            fileName, "array_long_field.element", 50L, 100L, 2, 5, 1234, -1L));
    columnStats.put(
        "map_string_long_field.key_value.key",
        HoodieColumnRangeMetadata.<Comparable>create(
            fileName, "map_string_long_field.key_value.key", "key1", "key2", 3, 5, 1234, -1L));
    columnStats.put(
        "map_string_long_field.key_value.value",
        HoodieColumnRangeMetadata.<Comparable>create(
            fileName, "map_string_long_field.key_value.value", 200L, 300L, 3, 5, 1234, -1L));
    columnStats.put(
        "nested_struct_field.array_string_field.array",
        HoodieColumnRangeMetadata.<Comparable>create(
            fileName,
            "nested_array_string_field.element.element",
            "nested1",
            "nested2",
            7,
            15,
            1234,
            -1L));
    columnStats.put(
        "nested_struct_field.nested_long_field",
        HoodieColumnRangeMetadata.<Comparable>create(
            fileName, "nested_struct_field.nested_long_field", 500L, 600L, 4, 5, 1234, -1L));
    return columnStats;
  }

  private static HoodieWriteConfig getHoodieWriteConfig(HoodieTableMetaClient metaClient) {
    Properties properties = new Properties();
    properties.setProperty(HoodieMetadataConfig.AUTO_INITIALIZE.key(), "false");
    return HoodieWriteConfig.newBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(INMEMORY).build())
        .withPath(metaClient.getBasePathV2().toString())
        .withEmbeddedTimelineServerEnabled(false)
        .withMetadataConfig(
            HoodieMetadataConfig.newBuilder()
                .withMaxNumDeltaCommitsBeforeCompaction(2)
                .enable(true)
                .withMetadataIndexColumnStats(true)
                .withProperties(properties)
                .build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(1, 2).build())
        .withTableServicesEnabled(true)
        .build();
  }
}
