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
 
package io.onetable.spi.sync;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import io.onetable.model.IncrementalTableChanges;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.OneTableMetadata;
import io.onetable.model.TableChange;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.model.storage.OneFileGroup;
import io.onetable.model.storage.TableFormat;
import io.onetable.model.sync.SyncMode;
import io.onetable.model.sync.SyncResult;

public class TestTableFormatSync {
  private final TargetClient mockTargetClient1 = mock(TargetClient.class);
  private final TargetClient mockTargetClient2 = mock(TargetClient.class);

  @Test
  void syncSnapshotWithFailureForOneFormat() {
    Instant start = Instant.now();
    OneTable startingTableState = getTableState(1);
    List<OneFileGroup> fileGroups =
        Collections.singletonList(
            OneFileGroup.builder()
                .files(
                    Collections.singletonList(
                        OneDataFile.builder().physicalPath("/tmp/path/file.parquet").build()))
                .build());
    List<Instant> pendingCommitInstants = Collections.singletonList(Instant.now());
    OneSnapshot snapshot =
        OneSnapshot.builder()
            .table(startingTableState)
            .partitionedDataFiles(fileGroups)
            .pendingCommits(pendingCommitInstants)
            .build();
    when(mockTargetClient1.getTableFormat()).thenReturn(TableFormat.ICEBERG);
    when(mockTargetClient2.getTableFormat()).thenReturn(TableFormat.DELTA);
    doThrow(new RuntimeException("Failure")).when(mockTargetClient1).beginSync(startingTableState);
    Map<String, SyncResult> result =
        TableFormatSync.getInstance()
            .syncSnapshot(Arrays.asList(mockTargetClient1, mockTargetClient2), snapshot);

    assertEquals(2, result.size());
    SyncResult successResult = result.get(TableFormat.DELTA);
    assertEquals(SyncResult.SyncStatus.SUCCESS, successResult.getStatus());
    assertEquals(SyncMode.FULL, successResult.getMode());
    assertEquals(startingTableState.getLatestCommitTime(), successResult.getLastInstantSynced());
    assertSyncResultTimes(successResult, start);

    SyncResult failureResult = result.get(TableFormat.ICEBERG);
    assertEquals(SyncMode.FULL, failureResult.getMode());
    assertSyncResultTimes(failureResult, start);
    assertEquals(
        SyncResult.SyncStatus.builder()
            .statusCode(SyncResult.SyncStatusCode.ERROR)
            .errorMessage("Failure")
            .errorDescription("Failed to sync FULL")
            .canRetryOnFailure(true)
            .build(),
        failureResult.getStatus());

    verifyBaseClientCalls(mockTargetClient2, startingTableState, pendingCommitInstants);
    verify(mockTargetClient2).syncFilesForSnapshot(fileGroups);
    verify(mockTargetClient2).completeSync();
    verify(mockTargetClient1, never()).completeSync();
  }

  private static void assertSyncResultTimes(SyncResult syncResult, Instant start) {
    assertNotNull(syncResult.getSyncDuration());
    assertTrue(syncResult.getSyncStartTime().isAfter(start));
    assertTrue(syncResult.getSyncStartTime().isBefore(Instant.now()));
  }

  @Test
  void syncChangesWithFailureForOneFormat() {
    Instant start = Instant.now();
    OneTable tableState1 = getTableState(1);
    OneDataFilesDiff dataFilesDiff1 = getFilesDiff(1);
    TableChange tableChange1 =
        TableChange.builder().tableAsOfChange(tableState1).filesDiff(dataFilesDiff1).build();
    OneTable tableState2 = getTableState(2);
    OneDataFilesDiff dataFilesDiff2 = getFilesDiff(2);
    TableChange tableChange2 =
        TableChange.builder().tableAsOfChange(tableState2).filesDiff(dataFilesDiff2).build();
    OneTable tableState3 = getTableState(3);
    OneDataFilesDiff dataFilesDiff3 = getFilesDiff(3);
    TableChange tableChange3 =
        TableChange.builder().tableAsOfChange(tableState3).filesDiff(dataFilesDiff3).build();

    List<Instant> pendingCommitInstants = Collections.singletonList(Instant.now());
    when(mockTargetClient1.getTableFormat()).thenReturn(TableFormat.ICEBERG);
    when(mockTargetClient2.getTableFormat()).thenReturn(TableFormat.DELTA);
    // throw exception on second change and show that first change is still returned for this format
    // and other client is not affected
    doThrow(new RuntimeException("Failure")).when(mockTargetClient1).beginSync(tableState2);

    List<TableChange> tableChanges = Arrays.asList(tableChange1, tableChange2, tableChange3);
    IncrementalTableChanges incrementalTableChanges =
        IncrementalTableChanges.builder()
            .pendingCommits(pendingCommitInstants)
            .tableChanges(tableChanges.iterator())
            .build();

    Map<TargetClient, OneTableMetadata> clientWithMetadata = new HashMap<>();
    clientWithMetadata.put(
        mockTargetClient1,
        OneTableMetadata.of(Instant.now().minus(1, ChronoUnit.HOURS), Collections.emptyList()));
    clientWithMetadata.put(
        mockTargetClient2,
        OneTableMetadata.of(Instant.now().minus(1, ChronoUnit.HOURS), Collections.emptyList()));

    Map<String, List<SyncResult>> result =
        TableFormatSync.getInstance().syncChanges(clientWithMetadata, incrementalTableChanges);

    assertEquals(2, result.size());
    // Assert that there is one success followed by a failure
    List<SyncResult> partialSuccessResults = result.get(TableFormat.ICEBERG);
    assertEquals(2, partialSuccessResults.size());
    assertEquals(SyncMode.INCREMENTAL, partialSuccessResults.get(0).getMode());
    assertEquals(
        tableChanges.get(0).getTableAsOfChange().getLatestCommitTime(),
        partialSuccessResults.get(0).getLastInstantSynced());
    assertEquals(SyncResult.SyncStatus.SUCCESS, partialSuccessResults.get(0).getStatus());
    assertSyncResultTimes(partialSuccessResults.get(0), start);

    assertEquals(SyncMode.INCREMENTAL, partialSuccessResults.get(1).getMode());
    assertSyncResultTimes(partialSuccessResults.get(1), start);
    assertEquals(
        SyncResult.SyncStatus.builder()
            .statusCode(SyncResult.SyncStatusCode.ERROR)
            .errorMessage("Failure")
            .errorDescription("Failed to sync INCREMENTAL")
            .canRetryOnFailure(true)
            .build(),
        partialSuccessResults.get(1).getStatus());

    // Assert that all 3 changes are properly synced to the other format
    List<SyncResult> successResults = result.get(TableFormat.DELTA);
    assertEquals(3, successResults.size());
    for (int i = 0; i < 3; i++) {
      assertEquals(SyncMode.INCREMENTAL, successResults.get(i).getMode());
      assertEquals(
          tableChanges.get(i).getTableAsOfChange().getLatestCommitTime(),
          successResults.get(i).getLastInstantSynced());
      assertEquals(SyncResult.SyncStatus.SUCCESS, successResults.get(i).getStatus());
      assertSyncResultTimes(successResults.get(i), start);
    }

    verifyBaseClientCalls(mockTargetClient1, tableState1, pendingCommitInstants);
    verify(mockTargetClient1).syncFilesForDiff(dataFilesDiff1);
    verifyBaseClientCalls(mockTargetClient2, tableState1, pendingCommitInstants);
    verify(mockTargetClient2).syncFilesForDiff(dataFilesDiff1);
    verifyBaseClientCalls(mockTargetClient2, tableState2, pendingCommitInstants);
    verify(mockTargetClient2).syncFilesForDiff(dataFilesDiff2);
    verifyBaseClientCalls(mockTargetClient2, tableState3, pendingCommitInstants);
    verify(mockTargetClient2).syncFilesForDiff(dataFilesDiff3);
    verify(mockTargetClient1, times(1)).completeSync();
    verify(mockTargetClient2, times(3)).completeSync();
  }

  @Test
  void syncChangesWithDifferentFormatsAndMetadata() {
    Instant start = Instant.now();
    OneTable tableState1 = getTableState(1);
    OneDataFilesDiff dataFilesDiff1 = getFilesDiff(1);
    TableChange tableChange1 =
        TableChange.builder().tableAsOfChange(tableState1).filesDiff(dataFilesDiff1).build();
    OneTable tableState2 = getTableState(2);
    OneDataFilesDiff dataFilesDiff2 = getFilesDiff(2);
    TableChange tableChange2 =
        TableChange.builder().tableAsOfChange(tableState2).filesDiff(dataFilesDiff2).build();
    OneTable tableState3 = getTableState(3);
    OneDataFilesDiff dataFilesDiff3 = getFilesDiff(3);
    TableChange tableChange3 =
        TableChange.builder().tableAsOfChange(tableState3).filesDiff(dataFilesDiff3).build();

    List<Instant> pendingCommitInstants = Collections.singletonList(Instant.now());
    when(mockTargetClient1.getTableFormat()).thenReturn(TableFormat.ICEBERG);
    when(mockTargetClient2.getTableFormat()).thenReturn(TableFormat.DELTA);

    List<TableChange> tableChanges = Arrays.asList(tableChange1, tableChange2, tableChange3);
    IncrementalTableChanges incrementalTableChanges =
        IncrementalTableChanges.builder()
            .pendingCommits(pendingCommitInstants)
            .tableChanges(tableChanges.iterator())
            .build();

    Map<TargetClient, OneTableMetadata> clientWithMetadata = new HashMap<>();
    // mockTargetClient1 will have the first table change as a previously pending instant and
    // otherwise be synced up to the 2nd change
    clientWithMetadata.put(
        mockTargetClient1,
        OneTableMetadata.of(
            tableChange2.getTableAsOfChange().getLatestCommitTime(),
            Collections.singletonList(tableChange1.getTableAsOfChange().getLatestCommitTime())));
    // mockTargetClient2 will have synced the first table change previously
    clientWithMetadata.put(
        mockTargetClient2,
        OneTableMetadata.of(
            tableChange1.getTableAsOfChange().getLatestCommitTime(), Collections.emptyList()));

    Map<String, List<SyncResult>> result =
        TableFormatSync.getInstance().syncChanges(clientWithMetadata, incrementalTableChanges);

    assertEquals(2, result.size());
    // Assert that there is one success followed by a failure
    List<SyncResult> client1Results = result.get(TableFormat.ICEBERG);
    assertEquals(2, client1Results.size());
    for (SyncResult client1Result : client1Results) {
      assertEquals(SyncMode.INCREMENTAL, client1Result.getMode());
      assertEquals(SyncResult.SyncStatus.SUCCESS, client1Result.getStatus());
      assertSyncResultTimes(client1Result, start);
    }
    assertEquals(
        tableChanges.get(0).getTableAsOfChange().getLatestCommitTime(),
        client1Results.get(0).getLastInstantSynced());
    assertEquals(
        tableChanges.get(2).getTableAsOfChange().getLatestCommitTime(),
        client1Results.get(1).getLastInstantSynced());

    // Assert that all 2 changes are properly synced to the other format
    List<SyncResult> client2Results = result.get(TableFormat.DELTA);
    assertEquals(2, client2Results.size());
    for (int i = 0; i < 2; i++) {
      assertEquals(SyncMode.INCREMENTAL, client2Results.get(i).getMode());
      assertEquals(
          tableChanges.get(i + 1).getTableAsOfChange().getLatestCommitTime(),
          client2Results.get(i).getLastInstantSynced());
      assertEquals(SyncResult.SyncStatus.SUCCESS, client2Results.get(i).getStatus());
      assertSyncResultTimes(client2Results.get(i), start);
    }

    // client1 syncs table changes 1 and 3
    verifyBaseClientCalls(mockTargetClient1, tableState1, pendingCommitInstants);
    verify(mockTargetClient1).syncFilesForDiff(dataFilesDiff1);
    verifyBaseClientCalls(mockTargetClient1, tableState3, pendingCommitInstants);
    verify(mockTargetClient1).syncFilesForDiff(dataFilesDiff3);
    verify(mockTargetClient1, times(2)).completeSync();
    // client2 syncs table changes 2 and 3
    verifyBaseClientCalls(mockTargetClient2, tableState2, pendingCommitInstants);
    verify(mockTargetClient2).syncFilesForDiff(dataFilesDiff2);
    verifyBaseClientCalls(mockTargetClient2, tableState3, pendingCommitInstants);
    verify(mockTargetClient2).syncFilesForDiff(dataFilesDiff3);
    verify(mockTargetClient2, times(2)).completeSync();
  }

  @Test
  void syncChangesOneFormatWithNoRequiredChanges() {
    Instant start = Instant.now();
    OneTable tableState1 = getTableState(1);
    OneDataFilesDiff dataFilesDiff1 = getFilesDiff(1);
    TableChange tableChange1 =
        TableChange.builder().tableAsOfChange(tableState1).filesDiff(dataFilesDiff1).build();

    List<Instant> pendingCommitInstants = Collections.emptyList();
    when(mockTargetClient1.getTableFormat()).thenReturn(TableFormat.ICEBERG);
    when(mockTargetClient2.getTableFormat()).thenReturn(TableFormat.DELTA);

    List<TableChange> tableChanges = Collections.singletonList(tableChange1);
    IncrementalTableChanges incrementalTableChanges =
        IncrementalTableChanges.builder()
            .pendingCommits(pendingCommitInstants)
            .tableChanges(tableChanges.iterator())
            .build();

    Map<TargetClient, OneTableMetadata> clientWithMetadata = new HashMap<>();
    // mockTargetClient1 will have nothing to sync
    clientWithMetadata.put(
        mockTargetClient1, OneTableMetadata.of(Instant.now(), Collections.emptyList()));
    // mockTargetClient2 will have synced the first table change previously
    clientWithMetadata.put(
        mockTargetClient2,
        OneTableMetadata.of(Instant.now().minus(1, ChronoUnit.HOURS), Collections.emptyList()));

    Map<String, List<SyncResult>> result =
        TableFormatSync.getInstance().syncChanges(clientWithMetadata, incrementalTableChanges);
    assertEquals(1, result.size());
    List<SyncResult> client2Results = result.get(TableFormat.DELTA);
    assertEquals(1, client2Results.size());
    client2Results.forEach(
        syncResult -> {
          assertEquals(SyncMode.INCREMENTAL, syncResult.getMode());
          assertEquals(SyncResult.SyncStatus.SUCCESS, syncResult.getStatus());
          assertSyncResultTimes(syncResult, start);
        });

    verify(mockTargetClient1, never()).beginSync(any());
    verify(mockTargetClient1, never()).syncFilesForDiff(any());
    verify(mockTargetClient1, never()).completeSync();

    verifyBaseClientCalls(mockTargetClient2, tableState1, pendingCommitInstants);
    verify(mockTargetClient2).syncFilesForDiff(dataFilesDiff1);
  }

  /**
   * Creates a table state with the given id so the state is different between calls.
   *
   * @param id id to use in the names of certain fields
   * @return the table
   */
  private static OneTable getTableState(int id) {
    return OneTable.builder()
        .tableFormat(TableFormat.HUDI)
        .readSchema(OneSchema.builder().name(String.format("test_schema_%d", id)).build())
        .partitioningFields(
            Collections.singletonList(
                OnePartitionField.builder()
                    .sourceField(
                        OneField.builder().name(String.format("partition_field_%d", id)).build())
                    .transformType(PartitionTransformType.VALUE)
                    .build()))
        .latestCommitTime(Instant.now().minus(10 - id, ChronoUnit.MINUTES))
        .basePath("/tmp/test")
        .build();
  }

  private OneDataFilesDiff getFilesDiff(int id) {
    return OneDataFilesDiff.builder()
        .filesAdded(
            Collections.singletonList(
                OneDataFile.builder()
                    .physicalPath(String.format("/tmp/path/file_%d.parquet", id))
                    .build()))
        .build();
  }

  private void verifyBaseClientCalls(
      TargetClient mockClient, OneTable startingTableState, List<Instant> pendingCommitInstants) {
    verify(mockClient).beginSync(startingTableState);
    verify(mockClient).syncSchema(startingTableState.getReadSchema());
    verify(mockClient).syncPartitionSpec(startingTableState.getPartitioningFields());
    verify(mockClient)
        .syncMetadata(
            OneTableMetadata.of(startingTableState.getLatestCommitTime(), pendingCommitInstants));
  }
}
