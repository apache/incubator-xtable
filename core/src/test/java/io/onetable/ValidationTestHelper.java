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
 
package io.onetable;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import io.onetable.model.OneSnapshot;
import io.onetable.model.TableChange;
import io.onetable.model.storage.OneDataFile;

public class ValidationTestHelper {

  public static void validateOneSnapshot(OneSnapshot oneSnapshot, List<String> allActivePaths) {
    assertNotNull(oneSnapshot);
    assertNotNull(oneSnapshot.getTable());
    List<String> onetablePaths =
        oneSnapshot.getPartitionedDataFiles().stream()
            .flatMap(group -> group.getFiles().stream())
            .map(OneDataFile::getPhysicalPath)
            .collect(Collectors.toList());
    replaceFileScheme(allActivePaths);
    replaceFileScheme(onetablePaths);
    Collections.sort(allActivePaths);
    Collections.sort(onetablePaths);
    assertEquals(allActivePaths, onetablePaths);
  }

  public static void validateTableChanges(
      List<List<String>> allActiveFiles, List<TableChange> allTableChanges) {
    if (allTableChanges.isEmpty() && allActiveFiles.size() <= 1) {
      return;
    }
    assertEquals(
        allTableChanges.size(),
        allActiveFiles.size() - 1,
        "Number of table changes should be equal to number of commits - 1");
    IntStream.range(0, allActiveFiles.size() - 1)
        .forEach(
            i ->
                validateTableChange(
                    allActiveFiles.get(i), allActiveFiles.get(i + 1), allTableChanges.get(i)));
  }

  public static void validateTableChange(
      List<String> filePathsBefore, List<String> filePathsAfter, TableChange tableChange) {
    assertNotNull(tableChange);
    assertNotNull(tableChange.getTableAsOfChange());
    replaceFileScheme(filePathsBefore);
    replaceFileScheme(filePathsAfter);
    Set<String> filesForCommitBefore = new HashSet<>(filePathsBefore);
    Set<String> filesForCommitAfter = new HashSet<>(filePathsAfter);
    // Get files added by diffing filesForCommitAfter and filesForCommitBefore.
    Set<String> filesAdded =
        filesForCommitAfter.stream()
            .filter(file -> !filesForCommitBefore.contains(file))
            .collect(Collectors.toSet());
    // Get files removed by diffing filesForCommitBefore and filesForCommitAfter.
    Set<String> filesRemoved =
        filesForCommitBefore.stream()
            .filter(file -> !filesForCommitAfter.contains(file))
            .collect(Collectors.toSet());
    assertEquals(filesAdded, extractPathsFromDataFile(tableChange.getFilesDiff().getFilesAdded()));
    assertEquals(
        filesRemoved, extractPathsFromDataFile(tableChange.getFilesDiff().getFilesRemoved()));
  }

  private static Set<String> extractPathsFromDataFile(Set<OneDataFile> dataFiles) {
    return dataFiles.stream().map(OneDataFile::getPhysicalPath).collect(Collectors.toSet());
  }

  private static void replaceFileScheme(List<String> filePaths) {
    // if file paths start with file:///, replace it with file:/.
    filePaths.replaceAll(path -> path.replaceFirst("file:///", "file:/"));
  }
}
