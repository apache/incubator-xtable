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
 
package org.apache.xtable.model.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class TestFilesDiff {
  @Test
  void findDiffFromFileGroups() {
    InternalDataFile file1Group1 = InternalDataFile.builder().physicalPath("file1Group1").build();
    InternalDataFile file2Group1 = InternalDataFile.builder().physicalPath("file2Group1").build();
    InternalDataFile file1Group2 = InternalDataFile.builder().physicalPath("file1Group2").build();
    InternalDataFile file2Group2 = InternalDataFile.builder().physicalPath("file2Group2").build();

    List<PartitionFileGroup> latestFileGroups =
        PartitionFileGroup.fromFiles(
            Arrays.asList(file1Group1, file2Group1, file1Group2, file2Group2));

    Map<String, File> previousFiles = new HashMap<>();
    File file1 = mock(File.class);
    File file2 = mock(File.class);
    File file3 = mock(File.class);
    previousFiles.put("file1Group1", file1);
    previousFiles.put("file2NoGroup", file2);
    previousFiles.put("file2Group2", file3);

    FilesDiff<InternalDataFile, File> diff =
        FilesDiff.findNewAndRemovedFiles(latestFileGroups, previousFiles);
    assertEquals(2, diff.getFilesAdded().size());
    assertTrue(diff.getFilesAdded().contains(file1Group2));
    assertTrue(diff.getFilesAdded().contains(file2Group1));
    assertEquals(1, diff.getFilesRemoved().size());
    assertTrue(diff.getFilesRemoved().contains(file2));
  }

  @Test
  void findDiffFromFilesNoPrevious() {
    File file1 = mock(File.class);
    File file2 = mock(File.class);

    Map<String, File> previousFiles = new HashMap<>();
    Map<String, File> latestFiles = new HashMap<>();
    latestFiles.put("file1", file1);
    latestFiles.put("file2", file2);

    FilesDiff<File, File> diff = FilesDiff.findNewAndRemovedFiles(latestFiles, previousFiles);
    assertEquals(0, diff.getFilesRemoved().size());
    assertEquals(2, diff.getFilesAdded().size());
    assertTrue(diff.getFilesAdded().contains(file1));
    assertTrue(diff.getFilesAdded().contains(file2));
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

    FilesDiff<File, File> diff = FilesDiff.findNewAndRemovedFiles(latestFiles, previousFiles);
    assertEquals(0, diff.getFilesRemoved().size());
    assertEquals(0, diff.getFilesAdded().size());
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

    FilesDiff<File, File> diff = FilesDiff.findNewAndRemovedFiles(latestFiles, previousFiles);
    assertEquals(1, diff.getFilesAdded().size());
    assertTrue(diff.getFilesAdded().contains(file3));
    assertEquals(1, diff.getFilesRemoved().size());
    assertTrue(diff.getFilesRemoved().contains(file1));
  }
}
