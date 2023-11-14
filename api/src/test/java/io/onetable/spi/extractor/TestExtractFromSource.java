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
    String partition1 = "partition1";
    String partition2 = "partition2";
    String partition3 = "partition3";
    OneDataFile initialFile2 = getOneDataFile(partition1, "file2.parquet");
    OneDataFile initialFile3 = getOneDataFile(partition2, "file3.parquet");

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

    // drop a file and add a file in an existing partition
    OneDataFile newFile1 = getOneDataFile(partition1, "file4.parquet");
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

    // add new file in a new partition, remove file from existing partition, remove partition
    OneDataFile newFile2 = getOneDataFile(partition1, "file5.parquet");
    OneDataFile newFile3 = getOneDataFile(partition3, "file6.parquet");

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
