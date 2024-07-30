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
 
package org.apache.xtable.spi.extractor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import org.junit.jupiter.api.Test;

import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.IncrementalTableChanges;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.storage.DataFilesDiff;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.PartitionFileGroup;

public class TestExtractFromSource {
  private final ConversionSource<TestCommit> mockConversionSource = mock(ConversionSource.class);

  @Test
  public void extractSnapshot() {
    InternalTable table = InternalTable.builder().latestCommitTime(Instant.now()).build();
    List<PartitionFileGroup> dataFiles = Collections.emptyList();
    InternalSnapshot internalSnapshot =
        InternalSnapshot.builder().table(table).partitionedDataFiles(dataFiles).build();
    when(mockConversionSource.getCurrentSnapshot()).thenReturn(internalSnapshot);
    assertEquals(internalSnapshot, ExtractFromSource.of(mockConversionSource).extractSnapshot());
  }

  @Test
  public void extractTableChanges() {
    InternalDataFile initialFile2 = getDataFile("file2.parquet");
    InternalDataFile initialFile3 = getDataFile("file3.parquet");

    Instant lastSyncTime = Instant.now().minus(2, ChronoUnit.DAYS);
    TestCommit firstCommitToSync = TestCommit.of("first_commit");
    TestCommit secondCommitToSync = TestCommit.of("second_commit");
    Instant inflightInstant = Instant.now().minus(1, ChronoUnit.HOURS);
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder().lastSyncInstant(lastSyncTime).build();
    CommitsBacklog<TestCommit> commitsBacklogToReturn =
        CommitsBacklog.<TestCommit>builder()
            .commitsToProcess(Arrays.asList(firstCommitToSync, secondCommitToSync))
            .inFlightInstants(Collections.singletonList(inflightInstant))
            .build();
    when(mockConversionSource.getCommitsBacklog(instantsForIncrementalSync))
        .thenReturn(commitsBacklogToReturn);

    // drop a file and add a file
    InternalDataFile newFile1 = getDataFile("file4.parquet");
    InternalTable tableAtFirstInstant =
        InternalTable.builder().latestCommitTime(Instant.now().minus(1, ChronoUnit.DAYS)).build();
    TableChange tableChangeToReturnAtFirstInstant =
        TableChange.builder()
            .tableAsOfChange(tableAtFirstInstant)
            .filesDiff(
                DataFilesDiff.builder().fileAdded(newFile1).fileRemoved(initialFile2).build())
            .build();
    when(mockConversionSource.getTableChangeForCommit(firstCommitToSync))
        .thenReturn(tableChangeToReturnAtFirstInstant);
    TableChange expectedFirstTableChange =
        TableChange.builder()
            .tableAsOfChange(tableAtFirstInstant)
            .filesDiff(
                DataFilesDiff.builder().fileAdded(newFile1).fileRemoved(initialFile2).build())
            .build();

    // add 2 new files, remove 2 files
    InternalDataFile newFile2 = getDataFile("file5.parquet");
    InternalDataFile newFile3 = getDataFile("file6.parquet");

    InternalTable tableAtSecondInstant =
        InternalTable.builder().latestCommitTime(Instant.now()).build();
    TableChange tableChangeToReturnAtSecondInstant =
        TableChange.builder()
            .tableAsOfChange(tableAtSecondInstant)
            .filesDiff(
                DataFilesDiff.builder()
                    .filesAdded(Arrays.asList(newFile2, newFile3))
                    .filesRemoved(Arrays.asList(initialFile3, newFile1))
                    .build())
            .build();
    when(mockConversionSource.getTableChangeForCommit(secondCommitToSync))
        .thenReturn(tableChangeToReturnAtSecondInstant);
    TableChange expectedSecondTableChange =
        TableChange.builder()
            .tableAsOfChange(tableAtSecondInstant)
            .filesDiff(
                DataFilesDiff.builder()
                    .filesAdded(Arrays.asList(newFile2, newFile3))
                    .filesRemoved(Arrays.asList(initialFile3, newFile1))
                    .build())
            .build();

    IncrementalTableChanges actual =
        ExtractFromSource.of(mockConversionSource).extractTableChanges(instantsForIncrementalSync);
    assertEquals(Collections.singletonList(inflightInstant), actual.getPendingCommits());
    List<TableChange> actualTableChanges = new ArrayList<>();
    actual.getTableChanges().forEachRemaining(actualTableChanges::add);
    assertEquals(
        Arrays.asList(expectedFirstTableChange, expectedSecondTableChange), actualTableChanges);
  }

  private InternalDataFile getDataFile(String physicalPath) {
    return InternalDataFile.builder().physicalPath(physicalPath).build();
  }

  @AllArgsConstructor(staticName = "of")
  @EqualsAndHashCode
  @ToString
  private static class TestCommit {
    private final String name;
  }
}
