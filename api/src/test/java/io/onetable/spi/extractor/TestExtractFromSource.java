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
 
package io.onetable.spi.extractor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import org.junit.jupiter.api.Test;

import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.TableChange;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.OneDataFilesDiff;

public class TestExtractFromSource {

  private final SourceClient<TestCommit> mockSourceClient = mock(SourceClient.class);

  @Test
  public void extractSnapshot() {
    TestCommit latestCommit = TestCommit.of("latest");
    when(mockSourceClient.getLatestCommit()).thenReturn(latestCommit);
    OneTable table = OneTable.builder().latestCommitTime(Instant.now()).build();
    when(mockSourceClient.getTable(latestCommit)).thenReturn(table);
    SchemaCatalog schemaCatalog = new SchemaCatalog(Collections.emptyMap());
    when(mockSourceClient.getSchemaCatalog(table, latestCommit)).thenReturn(schemaCatalog);
    OneDataFiles dataFiles = OneDataFiles.collectionBuilder().build();
    when(mockSourceClient.getFilesForAllPartitions(latestCommit, table)).thenReturn(dataFiles);

    OneSnapshot expected =
        OneSnapshot.builder()
            .schemaCatalog(schemaCatalog)
            .table(table)
            .dataFiles(dataFiles)
            .build();

    assertEquals(expected, ExtractFromSource.of(mockSourceClient).extractSnapshot());
  }

  @Test
  public void extractTableChanges() {
    String partition1 = "partition1";
    String partition2 = "partition2";
    String partition3 = "partition3";
    OneDataFile initialFile1 = getOneDataFile(partition1, "file1.parquet");
    OneDataFile initialFile2 = getOneDataFile(partition1, "file2.parquet");
    OneDataFile initialFile3 = getOneDataFile(partition2, "file3.parquet");
    OneDataFiles initialFiles =
        OneDataFiles.collectionBuilder()
            .files(
                Arrays.asList(
                    OneDataFiles.collectionBuilder()
                        .files(Arrays.asList(initialFile1, initialFile2))
                        .partitionPath(partition1)
                        .build(),
                    OneDataFiles.collectionBuilder()
                        .files(Collections.singletonList(initialFile3))
                        .partitionPath(partition2)
                        .build()))
            .build();

    Instant lastSyncTime = Instant.now().minus(2, ChronoUnit.DAYS);
    TestCommit lastCommitSynced = TestCommit.of("last_sync");
    when(mockSourceClient.getCommitAtInstant(lastSyncTime)).thenReturn(lastCommitSynced);
    TestCommit firstCommitToSync = TestCommit.of("first_commit");
    TestCommit secondCommitToSync = TestCommit.of("second_commit");
    when(mockSourceClient.getCommits(lastCommitSynced))
        .thenReturn(Arrays.asList(firstCommitToSync, secondCommitToSync));

    // drop a file and add a file in an existing partition
    OneDataFile newFile1 = getOneDataFile(partition1, "file4.parquet");
    OneTable tableAtFirstInstant =
        OneTable.builder().latestCommitTime(Instant.now().minus(1, ChronoUnit.DAYS)).build();
    when(mockSourceClient.getFilesDiffBetweenCommits(
            lastCommitSynced, firstCommitToSync, tableAtFirstInstant))
        .thenReturn(
            OneDataFilesDiff.builder().fileAdded(newFile1).fileRemoved(initialFile2).build());
    when(mockSourceClient.getTable(firstCommitToSync)).thenReturn(tableAtFirstInstant);
    OneDataFilesDiff expectedFirstFileDiff =
        OneDataFilesDiff.builder().fileAdded(newFile1).fileRemoved(initialFile2).build();
    TableChange expectedFirstTableChange =
        TableChange.builder()
            .currentTableState(tableAtFirstInstant)
            .filesDiff(expectedFirstFileDiff)
            .build();

    // add new file in a new partition, remove file from existing partition, remove partition
    OneDataFile newFile2 = getOneDataFile(partition1, "file5.parquet");
    OneDataFile newFile3 = getOneDataFile(partition3, "file6.parquet");
    OneDataFiles secondDiff =
        OneDataFiles.collectionBuilder()
            .files(
                Arrays.asList(
                    OneDataFiles.collectionBuilder()
                        .files(Arrays.asList(initialFile1, newFile2))
                        .partitionPath(partition1)
                        .build(),
                    OneDataFiles.collectionBuilder()
                        .files(Collections.singletonList(newFile3))
                        .partitionPath(partition3)
                        .build(),
                    OneDataFiles.collectionBuilder()
                        .files(Collections.emptyList())
                        .partitionPath(partition2)
                        .build()))
            .build();
    OneTable tableAtSecondInstant = OneTable.builder().latestCommitTime(Instant.now()).build();
    when(mockSourceClient.getFilesDiffBetweenCommits(
            firstCommitToSync, secondCommitToSync, tableAtSecondInstant))
        .thenReturn(
            OneDataFilesDiff.builder()
                .filesAdded(Arrays.asList(newFile2, newFile3))
                .filesRemoved(Arrays.asList(initialFile3, newFile1))
                .build());
    when(mockSourceClient.getTable(secondCommitToSync)).thenReturn(tableAtSecondInstant);
    OneDataFilesDiff expectedSecondFileDiff =
        OneDataFilesDiff.builder()
            .filesAdded(Arrays.asList(newFile2, newFile3))
            .filesRemoved(Arrays.asList(newFile1, initialFile3))
            .build();
    TableChange expectedSecondTableChange =
        TableChange.builder()
            .currentTableState(tableAtSecondInstant)
            .filesDiff(expectedSecondFileDiff)
            .build();

    List<TableChange> expected = Arrays.asList(expectedFirstTableChange, expectedSecondTableChange);
    assertEquals(
        expected, ExtractFromSource.of(mockSourceClient).extractTableChanges(lastSyncTime));
  }

  private OneDataFile getOneDataFile(String partitionPath, String physicalPath) {
    return OneDataFile.builder().partitionPath(partitionPath).physicalPath(physicalPath).build();
  }

  @AllArgsConstructor(staticName = "of")
  @EqualsAndHashCode
  @ToString
  private static class TestCommit {
    private final String name;
  }
}
