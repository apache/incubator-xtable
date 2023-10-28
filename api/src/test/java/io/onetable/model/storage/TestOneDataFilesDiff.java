package io.onetable.model.storage;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestOneDataFilesDiff {
  @Test
  void testFrom() {
    OneDataFile sourceFile1 = OneDataFile.builder().physicalPath("file://new_source_file1.parquet").build();
    OneDataFile sourceFile2 = OneDataFile.builder().physicalPath("file://new_source_file2.parquet").build();
    OneDataFile targetFile1 = OneDataFile.builder().physicalPath("file://already_in_target1.parquet").build();
    OneDataFile targetFile2 = OneDataFile.builder().physicalPath("file://already_in_target2.parquet").build();
    OneDataFile sourceFileInTargetAlready = OneDataFile.builder().physicalPath("file://already_in_target3.parquet").build();
    OneDataFilesDiff actual = OneDataFilesDiff.from(Arrays.asList(sourceFile1, sourceFile2, sourceFileInTargetAlready), Arrays.asList(targetFile1, targetFile2, sourceFileInTargetAlready));

    OneDataFilesDiff expected = OneDataFilesDiff.builder()
        .filesAdded(Arrays.asList(sourceFile1, sourceFile2))
        .filesRemoved(Arrays.asList(targetFile1, targetFile2))
        .build();
    assertEquals(expected, actual);
  }
}
