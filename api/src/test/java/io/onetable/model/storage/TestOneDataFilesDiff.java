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
 
package io.onetable.model.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.*;

import org.junit.jupiter.api.Test;

public class TestOneDataFilesDiff {
  @Test
  void testFrom() {
    OneDataFile sourceFile1 =
        OneDataFile.builder().physicalPath("file://new_source_file1.parquet").build();
    OneDataFile sourceFile2 =
        OneDataFile.builder().physicalPath("file://new_source_file2.parquet").build();
    OneDataFile targetFile1 =
        OneDataFile.builder().physicalPath("file://already_in_target1.parquet").build();
    OneDataFile targetFile2 =
        OneDataFile.builder().physicalPath("file://already_in_target2.parquet").build();
    OneDataFile sourceFileInTargetAlready =
        OneDataFile.builder().physicalPath("file://already_in_target3.parquet").build();
    OneDataFilesDiff actual =
        OneDataFilesDiff.from(
            Arrays.asList(sourceFile1, sourceFile2, sourceFileInTargetAlready),
            Arrays.asList(targetFile1, targetFile2, sourceFileInTargetAlready));

    OneDataFilesDiff expected =
        OneDataFilesDiff.builder()
            .filesAdded(Arrays.asList(sourceFile1, sourceFile2))
            .filesRemoved(Arrays.asList(targetFile1, targetFile2))
            .build();
    assertEquals(expected, actual);
  }

  @Test
  void findDiffFromFileGroups() {
    OneDataFile file1Group1 = OneDataFile.builder().physicalPath("file1Group1").build();
    OneDataFile file2Group1 = OneDataFile.builder().physicalPath("file2Group1").build();
    OneDataFile file1Group2 = OneDataFile.builder().physicalPath("file1Group2").build();
    OneDataFile file2Group2 = OneDataFile.builder().physicalPath("file2Group2").build();

    List<OneFileGroup> latestFileGroups =
        OneFileGroup.fromFiles(Arrays.asList(file1Group1, file2Group1, file1Group2, file2Group2));

    Map<String, File> previousFiles = new HashMap<>();
    File file1 = mock(File.class);
    File file2 = mock(File.class);
    File file3 = mock(File.class);
    previousFiles.put("file1Group1", file1);
    previousFiles.put("file2NoGroup", file2);
    previousFiles.put("file2Group2", file3);

    Set<OneDataFile> newFiles =
        OneDataFilesDiff.findNewAndRemovedFiles(latestFileGroups, previousFiles);
    assertEquals(1, previousFiles.size());
    assertEquals(2, newFiles.size());
    assertTrue(previousFiles.containsKey("file2NoGroup"));
    assertTrue(newFiles.contains(file2Group1));
    assertTrue(newFiles.contains(file1Group2));
  }

  @Test
  void findDiffFromFilesNoPrevious() {
    File file1 = mock(File.class);
    File file2 = mock(File.class);

    Map<String, File> previousFiles = new HashMap<>();
    Map<String, File> latestFiles = new HashMap<>();
    latestFiles.put("file1", file1);
    latestFiles.put("file2", file2);

    Set<File> newFiles = OneDataFilesDiff.findNewAndRemovedFiles(latestFiles, previousFiles);
    assertEquals(0, previousFiles.size());
    assertEquals(2, newFiles.size());
    assertTrue(newFiles.contains(file1));
    assertTrue(newFiles.contains(file2));
  }

  @Test
  void findDiffFromFilesNoNew() {
    File file1 = mock(File.class);
    File file2 = mock(File.class);

    Map<String, File> previousFiles = new HashMap<>();
    previousFiles.put("file1", file1);
    previousFiles.put("file2", file2);

    Map<String, File> latestFiles = new HashMap<>();
    latestFiles.put("file1", file1);
    latestFiles.put("file2", file2);

    Set<File> newFiles = OneDataFilesDiff.findNewAndRemovedFiles(latestFiles, previousFiles);
    assertEquals(0, previousFiles.size());
    assertEquals(0, newFiles.size());
  }

  @Test
  void findDiffFromFiles() {
    File file1 = mock(File.class);
    File file2 = mock(File.class);
    File file3 = mock(File.class);

    Map<String, File> previousFiles = new HashMap<>();
    previousFiles.put("file1", file1);
    previousFiles.put("file2", file2);

    Map<String, File> latestFiles = new HashMap<>();
    latestFiles.put("file2", file2);
    latestFiles.put("file3", file3);

    Set<File> newFiles = OneDataFilesDiff.findNewAndRemovedFiles(latestFiles, previousFiles);
    assertEquals(1, previousFiles.size());
    assertEquals(1, newFiles.size());
    assertTrue(previousFiles.containsKey("file1"));
    assertTrue(newFiles.contains(file3));
  }
}
