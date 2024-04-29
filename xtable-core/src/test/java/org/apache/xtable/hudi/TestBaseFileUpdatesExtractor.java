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
 
package org.apache.xtable.hudi;

import static org.apache.xtable.hudi.HudiTestUtil.createWriteStatus;
import static org.apache.xtable.hudi.HudiTestUtil.getHoodieWriteConfig;
import static org.apache.xtable.hudi.HudiTestUtil.initTableAndGetMetaClient;
import static org.apache.xtable.testutil.ColumnStatMapUtil.getColumnStats;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.CachingPath;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.DataFilesDiff;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.testutil.ColumnStatMapUtil;

public class TestBaseFileUpdatesExtractor {
  @TempDir public static java.nio.file.Path tempDir;
  private static final String COMMIT_TIME = "20231003013807542";
  private static final long FILE_SIZE = 100L;
  private static final long RECORD_COUNT = 200L;
  private static final long LAST_MODIFIED = System.currentTimeMillis();
  private static final HoodieEngineContext CONTEXT =
      new HoodieJavaEngineContext(new Configuration());
  private static final InternalPartitionField PARTITION_FIELD =
      InternalPartitionField.builder()
          .sourceField(
              InternalField.builder()
                  .name("string_field")
                  .schema(InternalSchema.builder().dataType(InternalType.STRING).build())
                  .build())
          .transformType(PartitionTransformType.VALUE)
          .build();

  @Test
  void convertDiff() {
    String tableBasePath = "file://base";
    String partitionPath1 = "partition1";
    String fileName1 = "file1.parquet";
    // create file with empty stats to test edge case
    InternalDataFile addedFile1 =
        createFile(
            String.format("%s/%s/%s", tableBasePath, partitionPath1, fileName1),
            Collections.emptyList());
    // create file with stats
    String partitionPath2 = "partition2";
    String fileName2 = "file2.parquet";
    InternalDataFile addedFile2 =
        createFile(
            String.format("%s/%s/%s", tableBasePath, partitionPath2, fileName2), getColumnStats());

    // remove files 3 files from two different partitions
    String fileName3 = "file3.parquet";
    InternalDataFile removedFile1 =
        createFile(
            String.format("%s/%s/%s", tableBasePath, partitionPath1, fileName3), getColumnStats());
    // create file that matches hudi format to mimic that a file create by hudi is now being removed
    // by another system
    String fileIdForFile4 = "d1cf0980-445c-4c74-bdeb-b7e5d18779f5-0";
    String fileName4 = fileIdForFile4 + "_0-1116-142216_20231003013807542.parquet";
    InternalDataFile removedFile2 =
        createFile(
            String.format("%s/%s/%s", tableBasePath, partitionPath1, fileName4),
            Collections.emptyList());
    String fileName5 = "file5.parquet";
    InternalDataFile removedFile3 =
        createFile(
            String.format("%s/%s/%s", tableBasePath, partitionPath2, fileName5),
            Collections.emptyList());

    DataFilesDiff diff =
        DataFilesDiff.builder()
            .filesAdded(Arrays.asList(addedFile1, addedFile2))
            .filesRemoved(Arrays.asList(removedFile1, removedFile2, removedFile3))
            .build();

    BaseFileUpdatesExtractor extractor =
        BaseFileUpdatesExtractor.of(CONTEXT, new CachingPath(tableBasePath));
    BaseFileUpdatesExtractor.ReplaceMetadata replaceMetadata =
        extractor.convertDiff(diff, COMMIT_TIME);

    // validate removed files
    Map<String, List<String>> expectedPartitionToReplacedFileIds = new HashMap<>();
    expectedPartitionToReplacedFileIds.put(
        partitionPath1, Arrays.asList(fileName3, fileIdForFile4));
    expectedPartitionToReplacedFileIds.put(partitionPath2, Collections.singletonList(fileName5));
    assertEquals(
        expectedPartitionToReplacedFileIds, replaceMetadata.getPartitionToReplacedFileIds());

    // validate added files
    List<WriteStatus> expectedWriteStatuses =
        Arrays.asList(
            getExpectedWriteStatus(fileName1, partitionPath1, Collections.emptyMap()),
            getExpectedWriteStatus(fileName2, partitionPath2, getExpectedColumnStats(fileName2)));
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
    InternalDataFile addedFile1 =
        createFile(
            String.format("%s/%s/%s", tableBasePath, partitionPath1, fileName1),
            Collections.emptyList());
    // create file with stats
    String fileName2 = "file2.parquet";
    InternalDataFile addedFile2 =
        createFile(
            String.format("%s/%s/%s", tableBasePath, partitionPath1, fileName2), getColumnStats());
    // create file in a second partition
    String partitionPath2 = "partition2";
    String fileName3 = "file3.parquet";
    InternalDataFile addedFile3 =
        createFile(
            String.format("%s/%s/%s", tableBasePath, partitionPath2, fileName3), getColumnStats());

    BaseFileUpdatesExtractor extractor =
        BaseFileUpdatesExtractor.of(CONTEXT, new CachingPath(tableBasePath));

    List<PartitionFileGroup> partitionedDataFiles =
        Arrays.asList(
            PartitionFileGroup.builder()
                .partitionValues(
                    Collections.singletonList(
                        PartitionValue.builder()
                            .partitionField(PARTITION_FIELD)
                            .range(Range.scalar(partitionPath1))
                            .build()))
                .files(Arrays.asList(addedFile1, addedFile2))
                .build(),
            PartitionFileGroup.builder()
                .partitionValues(
                    Collections.singletonList(
                        PartitionValue.builder()
                            .partitionField(PARTITION_FIELD)
                            .range(Range.scalar(partitionPath2))
                            .build()))
                .files(Arrays.asList(addedFile3))
                .build());
    BaseFileUpdatesExtractor.ReplaceMetadata replaceMetadata =
        extractor.extractSnapshotChanges(partitionedDataFiles, metaClient, COMMIT_TIME);

    // table is empty so there be no removals
    assertEquals(Collections.emptyMap(), replaceMetadata.getPartitionToReplacedFileIds());

    List<WriteStatus> expectedWriteStatuses =
        Arrays.asList(
            getExpectedWriteStatus(fileName1, partitionPath1, Collections.emptyMap()),
            getExpectedWriteStatus(fileName2, partitionPath1, getExpectedColumnStats(fileName2)),
            getExpectedWriteStatus(fileName3, partitionPath2, getExpectedColumnStats(fileName3)));
    assertWriteStatusesEquivalent(expectedWriteStatuses, replaceMetadata.getWriteStatuses());
  }

  @Test
  void extractSnapshotChanges_existingPartitionedTargetTable() {
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
            getExpectedWriteStatus(existingFileName1, partitionPath1, Collections.emptyMap()),
            getExpectedWriteStatus(existingFileName2, partitionPath2, Collections.emptyMap()),
            getExpectedWriteStatus(existingFileName3, partitionPath2, Collections.emptyMap()));
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
    InternalDataFile addedFile1 =
        createFile(
            String.format("%s/%s/%s", tableBasePath, partitionPath2, newFileName1),
            Collections.emptyList());
    String newFileName2 = "new_file_2.parquet";
    InternalDataFile addedFile2 =
        createFile(
            String.format("%s/%s/%s", tableBasePath, partitionPath3, newFileName2),
            getColumnStats());
    // InternalDataFile for one of the existing files in partition2
    InternalDataFile existingFile =
        createFile(
            String.format("%s/%s/%s", tableBasePath, partitionPath2, existingFileName2),
            Collections.emptyList());
    List<PartitionFileGroup> partitionedDataFiles =
        Arrays.asList(
            PartitionFileGroup.builder()
                .files(Arrays.asList(addedFile1, existingFile))
                .partitionValues(
                    Collections.singletonList(
                        PartitionValue.builder()
                            .partitionField(PARTITION_FIELD)
                            .range(Range.scalar(partitionPath2))
                            .build()))
                .build(),
            PartitionFileGroup.builder()
                .files(Collections.singletonList(addedFile2))
                .partitionValues(
                    Collections.singletonList(
                        PartitionValue.builder()
                            .partitionField(PARTITION_FIELD)
                            .range(Range.scalar(partitionPath3))
                            .build()))
                .build());
    BaseFileUpdatesExtractor extractor =
        BaseFileUpdatesExtractor.of(CONTEXT, new CachingPath(tableBasePath));
    BaseFileUpdatesExtractor.ReplaceMetadata replaceMetadata =
        extractor.extractSnapshotChanges(partitionedDataFiles, metaClient, COMMIT_TIME);

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
                newFileName1, partitionPath2, getExpectedColumnStats(newFileName1)),
            getExpectedWriteStatus(newFileName2, partitionPath3, Collections.emptyMap()));
    assertWriteStatusesEquivalent(expectedWriteStatuses, replaceMetadata.getWriteStatuses());
  }

  @Test
  void extractSnapshotChanges_existingNonPartitionedTargetTable() {
    String tableBasePath = tempDir.resolve(UUID.randomUUID().toString()).toString();
    HoodieTableMetaClient setupMetaClient = initTableAndGetMetaClient(tableBasePath, "");
    HoodieWriteConfig writeConfig = getHoodieWriteConfig(setupMetaClient);

    // initialize the table with 2 files
    String existingFileName1 = "existing_file_1.parquet";
    String existingFileName2 = "existing_file_2.parquet";

    List<WriteStatus> initialWriteStatuses =
        Arrays.asList(
            getExpectedWriteStatus(existingFileName1, "", Collections.emptyMap()),
            getExpectedWriteStatus(existingFileName2, "", Collections.emptyMap()));
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
    InternalDataFile addedFile1 =
        createFile(String.format("%s/%s", tableBasePath, newFileName1), getColumnStats());
    // InternalDataFile for one of the existing files in partition2
    InternalDataFile existingFile =
        createFile(
            String.format("%s/%s", tableBasePath, existingFileName2), Collections.emptyList());
    List<PartitionFileGroup> partitionedDataFiles =
        Collections.singletonList(
            PartitionFileGroup.builder()
                .files(Arrays.asList(addedFile1, existingFile))
                .partitionValues(Collections.emptyList())
                .build());
    BaseFileUpdatesExtractor extractor =
        BaseFileUpdatesExtractor.of(CONTEXT, new CachingPath(tableBasePath));
    BaseFileUpdatesExtractor.ReplaceMetadata replaceMetadata =
        extractor.extractSnapshotChanges(partitionedDataFiles, metaClient, COMMIT_TIME);

    // validate removed files
    Map<String, List<String>> expectedPartitionToReplacedFileIds =
        Collections.singletonMap("", Collections.singletonList(existingFileName1));
    assertEquals(
        expectedPartitionToReplacedFileIds, replaceMetadata.getPartitionToReplacedFileIds());

    // validate added files
    List<WriteStatus> expectedWriteStatuses =
        Collections.singletonList(
            getExpectedWriteStatus(newFileName1, "", getExpectedColumnStats(newFileName1)));
    assertWriteStatusesEquivalent(expectedWriteStatuses, replaceMetadata.getWriteStatuses());
  }

  private static void assertWriteStatusesEquivalent(
      List<WriteStatus> expected, List<WriteStatus> actual) {
    // write status does not implement equals, so we compare their toString results
    assertEquals(
        expected.stream().map(WriteStatus::toString).collect(Collectors.toSet()),
        actual.stream().map(WriteStatus::toString).collect(Collectors.toSet()));
  }

  private InternalDataFile createFile(String physicalPath, List<ColumnStat> columnStats) {
    return InternalDataFile.builder()
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
      Map<String, HoodieColumnRangeMetadata<Comparable>> recordStats) {
    return createWriteStatus(
        fileName, partitionPath, COMMIT_TIME, RECORD_COUNT, FILE_SIZE, recordStats);
  }

  /**
   * Get expected col stats for a file.
   *
   * @param fileName name of the file
   * @return stats matching the column stats in {@link ColumnStatMapUtil#getColumnStats()}
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
}
