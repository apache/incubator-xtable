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

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf;
import static org.apache.hudi.stats.XTableValueMetadata.getValueMetadata;
import static org.apache.xtable.hudi.HudiTestUtil.createWriteStatus;
import static org.apache.xtable.hudi.HudiTestUtil.getHoodieWriteConfig;
import static org.apache.xtable.hudi.HudiTestUtil.initTableAndGetMetaClient;
import static org.apache.xtable.testutil.ColumnStatMapUtil.getColumnStats;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.CachingPath;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.stats.ValueMetadata;
import org.apache.hudi.stats.ValueType;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.testutil.ColumnStatMapUtil;

public class TestBaseFileUpdatesExtractor {
  @TempDir public static java.nio.file.Path tempDir;
  private static final String COMMIT_TIME = "20231003013807542";
  private static final long FILE_SIZE = 100L;
  private static final long RECORD_COUNT = 200L;
  // starting value count assigned by getColumnStatsWithDistinctValueCounts()
  private static final long VALUE_COUNT_BASE = 100L;
  private static final long LAST_MODIFIED = System.currentTimeMillis();
  private static final HoodieEngineContext CONTEXT =
      new HoodieJavaEngineContext(getStorageConf(new Configuration()));
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
            String.format("%s/%s/%s", tableBasePath, partitionPath2, fileName2),
            getColumnStatsWithDistinctValueCounts());

    // remove files 3 files from two different partitions
    String fileName3 = "file3.parquet";
    InternalDataFile removedFile1 =
        createFile(
            String.format("%s/%s/%s", tableBasePath, partitionPath1, fileName3),
            getColumnStatsWithDistinctValueCounts());
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

    InternalFilesDiff diff =
        InternalFilesDiff.builder()
            .filesAdded(Arrays.asList(addedFile1, addedFile2))
            .filesRemoved(Arrays.asList(removedFile1, removedFile2, removedFile3))
            .build();

    BaseFileUpdatesExtractor extractor =
        BaseFileUpdatesExtractor.of(CONTEXT, new CachingPath(tableBasePath));
    BaseFileUpdatesExtractor.ReplaceMetadata replaceMetadata =
        extractor.convertDiff(diff, COMMIT_TIME, HoodieIndexVersion.V1);

    // validate removed files
    Map<String, List<String>> expectedPartitionToReplacedFileIds = new HashMap<>();
    expectedPartitionToReplacedFileIds.put(
        partitionPath1, Arrays.asList(fileName3, fileIdForFile4));
    expectedPartitionToReplacedFileIds.put(partitionPath2, Collections.singletonList(fileName5));
    assertEqualsIgnoreOrder(
        expectedPartitionToReplacedFileIds, replaceMetadata.getPartitionToReplacedFileIds());

    // validate added files
    List<WriteStatus> expectedWriteStatuses =
        Arrays.asList(
            getExpectedWriteStatus(fileName1, partitionPath1, Collections.emptyMap()),
            getExpectedWriteStatus(
                fileName2,
                partitionPath2,
                getExpectedColumnStats(fileName2, HoodieIndexVersion.V1)));
    assertWriteStatusesEquivalent(expectedWriteStatuses, replaceMetadata.getWriteStatuses());
  }

  private void assertEqualsIgnoreOrder(
      Map<String, List<String>> expected, Map<String, List<String>> actual) {
    for (Map.Entry<String, List<String>> entry : expected.entrySet()) {
      List<?> expectedList = entry.getValue();
      List<?> actualList = actual.get(entry.getKey());
      assertEquals(expectedList.size(), actualList.size());
      assertEquals(new HashSet<>(expectedList), new HashSet<>(actualList));
    }
  }

  @Test
  void extractSnapshotChanges_emptyTargetTable() throws IOException {
    String tableBasePath = tempDir.resolve(UUID.randomUUID().toString()).toString();
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.newTableBuilder()
            .setTableType(HoodieTableType.COPY_ON_WRITE)
            .setTableName("test_table")
            .setPayloadClass(HoodieAvroPayload.class)
            .setPartitionFields("partition_field")
            // XTable pins tables to version 6, whose column-stats index is V1
            .setTableVersion(HoodieTableVersion.SIX)
            .initTable(getStorageConf(new Configuration()), tableBasePath);

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
            String.format("%s/%s/%s", tableBasePath, partitionPath1, fileName2),
            getColumnStatsWithDistinctValueCounts());
    // create file in a second partition
    String partitionPath2 = "partition2";
    String fileName3 = "file3.parquet";
    InternalDataFile addedFile3 =
        createFile(
            String.format("%s/%s/%s", tableBasePath, partitionPath2, fileName3),
            getColumnStatsWithDistinctValueCounts());

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
            getExpectedWriteStatus(
                fileName2,
                partitionPath1,
                getExpectedColumnStats(fileName2, HoodieIndexVersion.V1)),
            getExpectedWriteStatus(
                fileName3,
                partitionPath2,
                getExpectedColumnStats(fileName3, HoodieIndexVersion.V1)));
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
                  initialInstant,
                  InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
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
            getColumnStatsWithDistinctValueCounts());
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
            getExpectedWriteStatus(newFileName1, partitionPath2, Collections.emptyMap()),
            getExpectedWriteStatus(
                newFileName2,
                partitionPath3,
                // existing target table -> column-stats index is read back at version V1
                getExpectedColumnStats(newFileName2, HoodieIndexVersion.V1)));
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
                  initialInstant,
                  InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR),
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
        createFile(
            String.format("%s/%s", tableBasePath, newFileName1),
            getColumnStatsWithDistinctValueCounts());
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
            getExpectedWriteStatus(
                newFileName1,
                "",
                // existing target table -> column-stats index is read back at version V1
                getExpectedColumnStats(newFileName1, HoodieIndexVersion.V1)));
    assertWriteStatusesEquivalent(expectedWriteStatuses, replaceMetadata.getWriteStatuses());
  }

  private static void assertWriteStatusesEquivalent(
      List<WriteStatus> expected, List<WriteStatus> actual) {
    // Hudi 1.2.0's WriteStatus#toString embeds per-instance identity state, so compare the fields
    // XTable populates rather than the toString output.
    assertEquals(expected.size(), actual.size(), "mismatched number of write statuses");
    Map<String, WriteStatus> actualByFileId =
        actual.stream().collect(Collectors.toMap(WriteStatus::getFileId, Function.identity()));
    for (WriteStatus expectedStatus : expected) {
      WriteStatus actualStatus = actualByFileId.get(expectedStatus.getFileId());
      assertNotNull(actualStatus, "no write status for fileId " + expectedStatus.getFileId());
      assertEquals(expectedStatus.getPartitionPath(), actualStatus.getPartitionPath());

      HoodieDeltaWriteStat expectedStat = (HoodieDeltaWriteStat) expectedStatus.getStat();
      HoodieDeltaWriteStat actualStat = (HoodieDeltaWriteStat) actualStatus.getStat();
      assertEquals(expectedStat.getFileId(), actualStat.getFileId());
      assertEquals(expectedStat.getPartitionPath(), actualStat.getPartitionPath());
      assertEquals(expectedStat.getPath(), actualStat.getPath());
      assertEquals(expectedStat.getNumWrites(), actualStat.getNumWrites());
      assertEquals(expectedStat.getNumInserts(), actualStat.getNumInserts());
      assertEquals(expectedStat.getTotalWriteBytes(), actualStat.getTotalWriteBytes());
      assertEquals(expectedStat.getFileSizeInBytes(), actualStat.getFileSizeInBytes());

      // Key by the column name embedded in each entry (not the map key), and compare the statistic
      // fields explicitly rather than HoodieColumnRangeMetadata#equals, whose ValueMetadata is an
      // index-version implementation detail.
      Map<String, HoodieColumnRangeMetadata<Comparable>> expectedStats =
          reKeyByColumnName(expectedStat.getRecordsStats());
      Map<String, HoodieColumnRangeMetadata<Comparable>> actualStats =
          reKeyByColumnName(actualStat.getRecordsStats());
      assertEquals(expectedStats.keySet(), actualStats.keySet(), "mismatched column-stats columns");
      for (Map.Entry<String, HoodieColumnRangeMetadata<Comparable>> entry :
          expectedStats.entrySet()) {
        String column = entry.getKey();
        HoodieColumnRangeMetadata<Comparable> expectedColumn = entry.getValue();
        HoodieColumnRangeMetadata<Comparable> actualColumn = actualStats.get(column);
        assertEquals(
            expectedColumn.getMinValue(), actualColumn.getMinValue(), "minValue " + column);
        assertEquals(
            expectedColumn.getMaxValue(), actualColumn.getMaxValue(), "maxValue " + column);
        assertEquals(
            expectedColumn.getNullCount(), actualColumn.getNullCount(), "nullCount " + column);
        assertEquals(
            expectedColumn.getValueCount(), actualColumn.getValueCount(), "valueCount " + column);
        assertEquals(
            expectedColumn.getTotalSize(), actualColumn.getTotalSize(), "totalSize " + column);
      }
    }
  }

  private static Map<String, HoodieColumnRangeMetadata<Comparable>> reKeyByColumnName(
      Option<Map<String, HoodieColumnRangeMetadata<Comparable>>> recordsStats) {
    return recordsStats.orElseGet(Collections::emptyMap).values().stream()
        .collect(Collectors.toMap(HoodieColumnRangeMetadata::getColumnName, Function.identity()));
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
   * @param indexVersion the Hudi index version
   * @return stats matching the column stats in {@link ColumnStatMapUtil#getColumnStats()}
   */
  private Map<String, HoodieColumnRangeMetadata<Comparable>> getExpectedColumnStats(
      String fileName, HoodieIndexVersion indexVersion) {
    // Expected scalar stats from getColumnStatsWithDistinctValueCounts(); convertColStats drops the
    // non-scalar (list/map/record) columns, so the value-count gaps are the dropped columns.
    Map<String, HoodieColumnRangeMetadata<Comparable>> columnStats = new HashMap<>();
    addStat(columnStats, fileName, "long_field", 10L, 20L, 4, 100, 123L);
    addStat(columnStats, fileName, "string_field", "a", "c", 1, 101, 500L);
    addStat(columnStats, fileName, "null_string_field", null, null, 3, 102, 0L);
    addStat(
        columnStats,
        fileName,
        "map_string_long_field.key_value.key",
        "key1",
        "key2",
        3,
        108,
        1234L);
    addStat(
        columnStats, fileName, "map_string_long_field.key_value.value", 200L, 300L, 3, 109, 1234L);
    addStat(
        columnStats,
        fileName,
        "nested_struct_field.array_string_field.array",
        "nested1",
        "nested2",
        7,
        110,
        1234L);
    addStat(
        columnStats, fileName, "nested_struct_field.nested_long_field", 500L, 600L, 4, 111, 1234L);
    addStat(
        columnStats,
        fileName,
        "nested_struct_field_primitive.nested_string_field",
        "alice",
        "zion",
        1,
        115,
        500L);
    addStat(
        columnStats,
        fileName,
        "decimal_field",
        new BigDecimal("1.00"),
        new BigDecimal("2.00"),
        1,
        112,
        123L);
    addStat(columnStats, fileName, "float_field", 1.23f, 6.54321f, 2, 113, 123L);
    addStat(columnStats, fileName, "double_field", 1.23, 6.54321, 3, 114, 123L);
    // Temporal columns: the recorded min/max depend on the index version (raw epoch values for V1,
    // standardized java-time types for V2).
    addTemporalStat(
        columnStats,
        fileName,
        "timestamp_field",
        ValueType.TIMESTAMP_MILLIS,
        indexVersion,
        1665263297000L,
        1665436097000L,
        105,
        103,
        999L);
    addTemporalStat(
        columnStats,
        fileName,
        "timestamp_micros_field",
        ValueType.TIMESTAMP_MICROS,
        indexVersion,
        1665263297000000L,
        1665436097000000L,
        1,
        104,
        400L);
    addTemporalStat(
        columnStats,
        fileName,
        "local_timestamp_field",
        ValueType.LOCAL_TIMESTAMP_MILLIS,
        indexVersion,
        1665263297000L,
        1665436097000L,
        1,
        105,
        400L);
    addTemporalStat(
        columnStats,
        fileName,
        "date_field",
        ValueType.DATE,
        indexVersion,
        18181,
        18547,
        250,
        106,
        12345L);
    return columnStats;
  }

  /**
   * {@link ColumnStatMapUtil#getColumnStats()} with a distinct value count per column (from {@link
   * #VALUE_COUNT_BASE}) so the comparison catches a stat mapped to the wrong column.
   */
  private static List<ColumnStat> getColumnStatsWithDistinctValueCounts() {
    AtomicLong nextValueCount = new AtomicLong(VALUE_COUNT_BASE);
    return getColumnStats().stream()
        .map(stat -> stat.toBuilder().numValues(nextValueCount.getAndIncrement()).build())
        .collect(Collectors.toList());
  }

  /**
   * Non-temporal column: production's min/max standardization is a no-op, so they are asserted
   * as-is.
   */
  private static void addStat(
      Map<String, HoodieColumnRangeMetadata<Comparable>> columnStats,
      String fileName,
      String columnName,
      Comparable min,
      Comparable max,
      long nullCount,
      long valueCount,
      long totalSize) {
    putColumnStat(
        columnStats,
        fileName,
        columnName,
        min,
        max,
        nullCount,
        valueCount,
        totalSize,
        ValueMetadata.V1EmptyMetadata.get());
  }

  /**
   * Temporal column whose min/max are standardized for the given index version, as production does.
   */
  private static void addTemporalStat(
      Map<String, HoodieColumnRangeMetadata<Comparable>> columnStats,
      String fileName,
      String columnName,
      ValueType valueType,
      HoodieIndexVersion indexVersion,
      Object rawMin,
      Object rawMax,
      long nullCount,
      long valueCount,
      long totalSize) {
    ValueMetadata valueMetadata = getValueMetadata(valueType, indexVersion);
    putColumnStat(
        columnStats,
        fileName,
        columnName,
        valueMetadata.standardizeJavaTypeAndPromote(rawMin),
        valueMetadata.standardizeJavaTypeAndPromote(rawMax),
        nullCount,
        valueCount,
        totalSize,
        valueMetadata);
  }

  private static void putColumnStat(
      Map<String, HoodieColumnRangeMetadata<Comparable>> columnStats,
      String fileName,
      String columnName,
      Comparable min,
      Comparable max,
      long nullCount,
      long valueCount,
      long totalSize,
      ValueMetadata valueMetadata) {
    columnStats.put(
        columnName,
        HoodieColumnRangeMetadata.<Comparable>create(
            fileName, columnName, min, max, nullCount, valueCount, totalSize, -1L, valueMetadata));
  }
}
