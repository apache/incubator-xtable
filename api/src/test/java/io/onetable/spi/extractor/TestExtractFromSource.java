/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import org.junit.jupiter.api.Test;

import io.onetable.model.*;
import io.onetable.model.CommitsBacklog;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.model.storage.OneFileGroup;

public class TestExtractFromSource {
  private final SourceClient<TestCommit> mockSourceClient = mock(SourceClient.class);

  @Test
  public void extractSnapshot() {
    OneTable table = OneTable.builder().latestCommitTime(Instant.now()).build();
    SchemaCatalog schemaCatalog = new SchemaCatalog(Collections.emptyMap());
    List<OneFileGroup> dataFiles = Collections.emptyList();
    OneSnapshot oneSnapshot =
        OneSnapshot.builder()
            .schemaCatalog(schemaCatalog)
            .table(table)
            .partitionedDataFiles(dataFiles)
            .build();
    when(mockSourceClient.getCurrentSnapshot()).thenReturn(oneSnapshot);
    assertEquals(oneSnapshot, ExtractFromSource.of(mockSourceClient).extractSnapshot());
  }

  @Test
  public void extractTableChanges() {
    OneDataFile initialFile2 = getOneDataFile("file2.parquet");
    OneDataFile initialFile3 = getOneDataFile("file3.parquet");

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
    when(mockSourceClient.getCommitsBacklog(instantsForIncrementalSync))
        .thenReturn(commitsBacklogToReturn);

    // drop a file and add a file
    OneDataFile newFile1 = getOneDataFile("file4.parquet");
    OneTable tableAtFirstInstant =
        OneTable.builder().latestCommitTime(Instant.now().minus(1, ChronoUnit.DAYS)).build();
    TableChange tableChangeToReturnAtFirstInstant =
        TableChange.builder()
            .tableAsOfChange(tableAtFirstInstant)
            .filesDiff(
                OneDataFilesDiff.builder().fileAdded(newFile1).fileRemoved(initialFile2).build())
            .build();
    when(mockSourceClient.getTableChangeForCommit(firstCommitToSync))
        .thenReturn(tableChangeToReturnAtFirstInstant);
    TableChange expectedFirstTableChange =
        TableChange.builder()
            .tableAsOfChange(tableAtFirstInstant)
            .filesDiff(
                OneDataFilesDiff.builder().fileAdded(newFile1).fileRemoved(initialFile2).build())
            .build();

    // add 2 new files, remove 2 files
    OneDataFile newFile2 = getOneDataFile("file5.parquet");
    OneDataFile newFile3 = getOneDataFile("file6.parquet");

    OneTable tableAtSecondInstant = OneTable.builder().latestCommitTime(Instant.now()).build();
    TableChange tableChangeToReturnAtSecondInstant =
        TableChange.builder()
            .tableAsOfChange(tableAtSecondInstant)
            .filesDiff(
                OneDataFilesDiff.builder()
                    .filesAdded(Arrays.asList(newFile2, newFile3))
                    .filesRemoved(Arrays.asList(initialFile3, newFile1))
                    .build())
            .build();
    when(mockSourceClient.getTableChangeForCommit(secondCommitToSync))
        .thenReturn(tableChangeToReturnAtSecondInstant);
    TableChange expectedSecondTableChange =
        TableChange.builder()
            .tableAsOfChange(tableAtSecondInstant)
            .filesDiff(
                OneDataFilesDiff.builder()
                    .filesAdded(Arrays.asList(newFile2, newFile3))
                    .filesRemoved(Arrays.asList(initialFile3, newFile1))
                    .build())
            .build();

    IncrementalTableChanges actual =
        ExtractFromSource.of(mockSourceClient).extractTableChanges(instantsForIncrementalSync);
    assertEquals(Collections.singletonList(inflightInstant), actual.getPendingCommits());
    List<TableChange> actualTableChanges = new ArrayList<>();
    actual.getTableChanges().forEachRemaining(actualTableChanges::add);
    assertEquals(
        Arrays.asList(expectedFirstTableChange, expectedSecondTableChange), actualTableChanges);
  }

  private OneDataFile getOneDataFile(String physicalPath) {
    return OneDataFile.builder().physicalPath(physicalPath).build();
  }

  @AllArgsConstructor(staticName = "of")
  @EqualsAndHashCode
  @ToString
  private static class TestCommit {
    private final String name;
  }
}
