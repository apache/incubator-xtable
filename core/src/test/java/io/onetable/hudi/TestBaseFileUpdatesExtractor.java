package io.onetable.hudi;

import org.apache.hudi.avro.model.HoodieWriteStat;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.SchemaVersion;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.Range;
import io.onetable.model.storage.FileFormat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFilesDiff;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.onetable.testutil.ColumnStatMapUtil.getColumnStatMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestBaseFileUpdatesExtractor {
  private static final SchemaVersion SCHEMA_VERSION = new SchemaVersion(1, "");
  private static final long FILE_SIZE = 100L;
  private static final long RECORD_COUNT = 200L;
  private static final long LAST_MODIFIED = System.currentTimeMillis();
  private static final HoodieEngineContext CONTEXT = new HoodieLocalEngineContext(new Configuration());

  @Test
  void convertDiff() {
    String baseTablePath = "file://base";
    String partitionPath1 = "partition1";
    String fileName1 = "file1.parquet";
    // create file with empty stats to test edge case
    OneDataFile addedFile1 = createFile(partitionPath1, String.format("%s/%s/%s", baseTablePath, partitionPath1, fileName1), Collections.emptyMap());
    // create file with stats
    String partitionPath2 = "partition2";
    String fileName2 = "file2.parquet";
    OneDataFile addedFile2 = createFile(partitionPath2, String.format("%s/%s/%s", baseTablePath, partitionPath2, fileName2), getColumnStatMap());

    // remove files 3 files from two different partitions
    String fileName3 = "file3.parquet";
    OneDataFile removedFile1 = createFile(partitionPath1, String.format("%s/%s/%s", baseTablePath, partitionPath1, fileName3), getColumnStatMap());
    String fileName4 = "file4.parquet";
    OneDataFile removedFile2 = createFile(partitionPath1, String.format("%s/%s/%s", baseTablePath, partitionPath1, fileName4), Collections.emptyMap());
    String fileName5 = "file5.parquet";
    OneDataFile removedFile3 = createFile(partitionPath2, String.format("%s/%s/%s", baseTablePath, partitionPath2, fileName5), Collections.emptyMap());

    OneDataFilesDiff diff = OneDataFilesDiff.builder()
        .filesAdded(Arrays.asList(addedFile1, addedFile2))
        .filesRemoved(Arrays.asList(removedFile1, removedFile2, removedFile3))
        .build();

    BaseFileUpdatesExtractor extractor = BaseFileUpdatesExtractor.of(CONTEXT, baseTablePath);
    BaseFileUpdatesExtractor.ReplaceMetadata replaceMetadata = extractor.convertDiff(diff, "20231003013807542");

    // validate removed files
    Map<String, List<String>> expectedPartitionToReplacedFileIds = new HashMap<>();
    expectedPartitionToReplacedFileIds.put(partitionPath1, Arrays.asList(fileName3, fileName4));
    expectedPartitionToReplacedFileIds.put(partitionPath2, Collections.singletonList(fileName5));
    assertEquals(expectedPartitionToReplacedFileIds, replaceMetadata.getPartitionToReplacedFileIds());

    // validate added files
    List<WriteStatus> expectedWriteStatuses = Arrays.asList(getExpectedWriteStatus(fileName1, partitionPath1, Collections.emptyMap()),
        getExpectedWriteStatus(fileName2, partitionPath2, getExpectedColumnStats(fileName2)));
    assertEquals(expectedWriteStatuses, replaceMetadata.getWriteStatuses());
  }

  @Test
  void extractSnapshotChanges_emptyTargetTable() {

  }

  @Test
  void extractSnapshotChanges_existingTargetTable() {

  }

  private OneDataFile createFile(String partitionPath, String physicalPath, Map<OneField, ColumnStat> columnStats) {
    return OneDataFile
        .builder()
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

  private WriteStatus getExpectedWriteStatus(String fileName, String partitionPath, Map<String, HoodieColumnRangeMetadata<Comparable>> recordStats) {
    WriteStatus writeStatus = new WriteStatus();
    writeStatus.setFileId(fileName);
    writeStatus.setPartitionPath(partitionPath);
    HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
    writeStat.setFileId(fileName);
    writeStat.setNumWrites(RECORD_COUNT);
    writeStat.setFileSizeInBytes(FILE_SIZE);
    writeStat.setTotalWriteBytes(FILE_SIZE);
    writeStat.putRecordsStats(recordStats);
    writeStatus.setStat(writeStat);
    return writeStatus;
  }

  /**
   * Get expected col stats for a file.
   * @param fileName name of the file
   * @return stats matching the column stats in {@link io.onetable.testutil.ColumnStatMapUtil#getColumnStatMap()}
   */
  private Map<String, HoodieColumnRangeMetadata<Comparable>> getExpectedColumnStats(String fileName) {
    Map<String, HoodieColumnRangeMetadata<Comparable>> columnStats = new HashMap<>();
    columnStats.put("long_field", HoodieColumnRangeMetadata.<Comparable>create(fileName, "long_field", 10L, 20L, 4, 5, 123L, -1L));
    columnStats.put("string_field", HoodieColumnRangeMetadata.<Comparable>create(fileName, "string_field", "a", "c", 1, 6, 500L, -1L));
    columnStats.put("null_string_field", HoodieColumnRangeMetadata.<Comparable>create(fileName, "null_string_field", (String) null, (String) null, 3, 3, 0L, -1L));
    columnStats.put("timestamp_field", HoodieColumnRangeMetadata.<Comparable>create(fileName, "timestamp_field", 1665263297000L, 1665436097000L, 105, 145, 999L, -1L));
    columnStats.put("timestamp_micros_field", HoodieColumnRangeMetadata.<Comparable>create(fileName, "timestamp_micros_field", 1665263297000000L, 1665436097000000L, 1, 20, 400, -1L));
    columnStats.put("local_timestamp_field", HoodieColumnRangeMetadata.<Comparable>create(fileName, "local_timestamp_field", 1665263297000L, 1665436097000L, 1, 20, 400, -1L));
    columnStats.put("date_field", HoodieColumnRangeMetadata.<Comparable>create(fileName, "date_field", 18181, 18547, 250, 300, 12345, -1L));
    return columnStats;
  }
}
