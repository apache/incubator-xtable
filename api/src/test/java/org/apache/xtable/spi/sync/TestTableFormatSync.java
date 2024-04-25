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
 
package org.apache.xtable.spi.sync;

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

import org.apache.xtable.model.IncrementalTableChanges;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.storage.DataFilesDiff;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncMode;
import org.apache.xtable.model.sync.SyncResult;

public class TestTableFormatSync {
  private final ConversionTarget mockConversionTarget1 = mock(ConversionTarget.class);
  private final ConversionTarget mockConversionTarget2 = mock(ConversionTarget.class);

  @Test
  void syncSnapshotWithFailureForOneFormat() {
    Instant start = Instant.now();
    InternalTable startingTableState = getTableState(1);
    List<PartitionFileGroup> fileGroups =
        Collections.singletonList(
            PartitionFileGroup.builder()
                .files(
                    Collections.singletonList(
                        InternalDataFile.builder().physicalPath("/tmp/path/file.parquet").build()))
                .build());
    List<Instant> pendingCommitInstants = Collections.singletonList(Instant.now());
    InternalSnapshot snapshot =
        InternalSnapshot.builder()
            .table(startingTableState)
            .partitionedDataFiles(fileGroups)
            .pendingCommits(pendingCommitInstants)
            .build();
    when(mockConversionTarget1.getTableFormat()).thenReturn(TableFormat.ICEBERG);
    when(mockConversionTarget2.getTableFormat()).thenReturn(TableFormat.DELTA);
    doThrow(new RuntimeException("Failure"))
        .when(mockConversionTarget1)
        .beginSync(startingTableState);
    Map<String, SyncResult> result =
        TableFormatSync.getInstance()
            .syncSnapshot(Arrays.asList(mockConversionTarget1, mockConversionTarget2), snapshot);

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

    verifyBaseConversionTargetCalls(
        mockConversionTarget2, startingTableState, pendingCommitInstants);
    verify(mockConversionTarget2).syncFilesForSnapshot(fileGroups);
    verify(mockConversionTarget2).completeSync();
    verify(mockConversionTarget1, never()).completeSync();
  }

  private static void assertSyncResultTimes(SyncResult syncResult, Instant start) {
    assertNotNull(syncResult.getSyncDuration());
    assertTrue(syncResult.getSyncStartTime().isAfter(start));
    assertTrue(syncResult.getSyncStartTime().isBefore(Instant.now()));
  }

  @Test
  void syncChangesWithFailureForOneFormat() {
    Instant start = Instant.now();
    InternalTable tableState1 = getTableState(1);
    DataFilesDiff dataFilesDiff1 = getFilesDiff(1);
    TableChange tableChange1 =
        TableChange.builder().tableAsOfChange(tableState1).filesDiff(dataFilesDiff1).build();
    InternalTable tableState2 = getTableState(2);
    DataFilesDiff dataFilesDiff2 = getFilesDiff(2);
    TableChange tableChange2 =
        TableChange.builder().tableAsOfChange(tableState2).filesDiff(dataFilesDiff2).build();
    InternalTable tableState3 = getTableState(3);
    DataFilesDiff dataFilesDiff3 = getFilesDiff(3);
    TableChange tableChange3 =
        TableChange.builder().tableAsOfChange(tableState3).filesDiff(dataFilesDiff3).build();

    List<Instant> pendingCommitInstants = Collections.singletonList(Instant.now());
    when(mockConversionTarget1.getTableFormat()).thenReturn(TableFormat.ICEBERG);
    when(mockConversionTarget2.getTableFormat()).thenReturn(TableFormat.DELTA);
    // throw exception on second change and show that first change is still returned for this format
    // and other conversionTarget is not affected
    doThrow(new RuntimeException("Failure")).when(mockConversionTarget1).beginSync(tableState2);

    List<TableChange> tableChanges = Arrays.asList(tableChange1, tableChange2, tableChange3);
    IncrementalTableChanges incrementalTableChanges =
        IncrementalTableChanges.builder()
            .pendingCommits(pendingCommitInstants)
            .tableChanges(tableChanges.iterator())
            .build();

    Map<ConversionTarget, TableSyncMetadata> conversionTargetWithMetadata = new HashMap<>();
    conversionTargetWithMetadata.put(
        mockConversionTarget1,
        TableSyncMetadata.of(Instant.now().minus(1, ChronoUnit.HOURS), Collections.emptyList()));
    conversionTargetWithMetadata.put(
        mockConversionTarget2,
        TableSyncMetadata.of(Instant.now().minus(1, ChronoUnit.HOURS), Collections.emptyList()));

    Map<String, List<SyncResult>> result =
        TableFormatSync.getInstance()
            .syncChanges(conversionTargetWithMetadata, incrementalTableChanges);

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

    verifyBaseConversionTargetCalls(mockConversionTarget1, tableState1, pendingCommitInstants);
    verify(mockConversionTarget1).syncFilesForDiff(dataFilesDiff1);
    verifyBaseConversionTargetCalls(mockConversionTarget2, tableState1, pendingCommitInstants);
    verify(mockConversionTarget2).syncFilesForDiff(dataFilesDiff1);
    verifyBaseConversionTargetCalls(mockConversionTarget2, tableState2, pendingCommitInstants);
    verify(mockConversionTarget2).syncFilesForDiff(dataFilesDiff2);
    verifyBaseConversionTargetCalls(mockConversionTarget2, tableState3, pendingCommitInstants);
    verify(mockConversionTarget2).syncFilesForDiff(dataFilesDiff3);
    verify(mockConversionTarget1, times(1)).completeSync();
    verify(mockConversionTarget2, times(3)).completeSync();
  }

  @Test
  void syncChangesWithDifferentFormatsAndMetadata() {
    Instant start = Instant.now();
    InternalTable tableState1 = getTableState(1);
    DataFilesDiff dataFilesDiff1 = getFilesDiff(1);
    TableChange tableChange1 =
        TableChange.builder().tableAsOfChange(tableState1).filesDiff(dataFilesDiff1).build();
    InternalTable tableState2 = getTableState(2);
    DataFilesDiff dataFilesDiff2 = getFilesDiff(2);
    TableChange tableChange2 =
        TableChange.builder().tableAsOfChange(tableState2).filesDiff(dataFilesDiff2).build();
    InternalTable tableState3 = getTableState(3);
    DataFilesDiff dataFilesDiff3 = getFilesDiff(3);
    TableChange tableChange3 =
        TableChange.builder().tableAsOfChange(tableState3).filesDiff(dataFilesDiff3).build();

    List<Instant> pendingCommitInstants = Collections.singletonList(Instant.now());
    when(mockConversionTarget1.getTableFormat()).thenReturn(TableFormat.ICEBERG);
    when(mockConversionTarget2.getTableFormat()).thenReturn(TableFormat.DELTA);

    List<TableChange> tableChanges = Arrays.asList(tableChange1, tableChange2, tableChange3);
    IncrementalTableChanges incrementalTableChanges =
        IncrementalTableChanges.builder()
            .pendingCommits(pendingCommitInstants)
            .tableChanges(tableChanges.iterator())
            .build();

    Map<ConversionTarget, TableSyncMetadata> conversionTargetWithMetadata = new HashMap<>();
    // mockConversionTarget1 will have the first table change as a previously pending instant and
    // otherwise be synced up to the 2nd change
    conversionTargetWithMetadata.put(
        mockConversionTarget1,
        TableSyncMetadata.of(
            tableChange2.getTableAsOfChange().getLatestCommitTime(),
            Collections.singletonList(tableChange1.getTableAsOfChange().getLatestCommitTime())));
    // mockConversionTarget2 will have synced the first table change previously
    conversionTargetWithMetadata.put(
        mockConversionTarget2,
        TableSyncMetadata.of(
            tableChange1.getTableAsOfChange().getLatestCommitTime(), Collections.emptyList()));

    Map<String, List<SyncResult>> result =
        TableFormatSync.getInstance()
            .syncChanges(conversionTargetWithMetadata, incrementalTableChanges);

    assertEquals(2, result.size());
    // Assert that there is one success followed by a failure
    List<SyncResult> conversionTarget1Results = result.get(TableFormat.ICEBERG);
    assertEquals(2, conversionTarget1Results.size());
    for (SyncResult conversionTarget1Result : conversionTarget1Results) {
      assertEquals(SyncMode.INCREMENTAL, conversionTarget1Result.getMode());
      assertEquals(SyncResult.SyncStatus.SUCCESS, conversionTarget1Result.getStatus());
      assertSyncResultTimes(conversionTarget1Result, start);
    }
    assertEquals(
        tableChanges.get(0).getTableAsOfChange().getLatestCommitTime(),
        conversionTarget1Results.get(0).getLastInstantSynced());
    assertEquals(
        tableChanges.get(2).getTableAsOfChange().getLatestCommitTime(),
        conversionTarget1Results.get(1).getLastInstantSynced());

    // Assert that all 2 changes are properly synced to the other format
    List<SyncResult> conversionTarget2Results = result.get(TableFormat.DELTA);
    assertEquals(2, conversionTarget2Results.size());
    for (int i = 0; i < 2; i++) {
      assertEquals(SyncMode.INCREMENTAL, conversionTarget2Results.get(i).getMode());
      assertEquals(
          tableChanges.get(i + 1).getTableAsOfChange().getLatestCommitTime(),
          conversionTarget2Results.get(i).getLastInstantSynced());
      assertEquals(SyncResult.SyncStatus.SUCCESS, conversionTarget2Results.get(i).getStatus());
      assertSyncResultTimes(conversionTarget2Results.get(i), start);
    }

    // conversionTarget1 syncs table changes 1 and 3
    verifyBaseConversionTargetCalls(mockConversionTarget1, tableState1, pendingCommitInstants);
    verify(mockConversionTarget1).syncFilesForDiff(dataFilesDiff1);
    verifyBaseConversionTargetCalls(mockConversionTarget1, tableState3, pendingCommitInstants);
    verify(mockConversionTarget1).syncFilesForDiff(dataFilesDiff3);
    verify(mockConversionTarget1, times(2)).completeSync();
    // conversionTarget2 syncs table changes 2 and 3
    verifyBaseConversionTargetCalls(mockConversionTarget2, tableState2, pendingCommitInstants);
    verify(mockConversionTarget2).syncFilesForDiff(dataFilesDiff2);
    verifyBaseConversionTargetCalls(mockConversionTarget2, tableState3, pendingCommitInstants);
    verify(mockConversionTarget2).syncFilesForDiff(dataFilesDiff3);
    verify(mockConversionTarget2, times(2)).completeSync();
  }

  @Test
  void syncChangesOneFormatWithNoRequiredChanges() {
    Instant start = Instant.now();
    InternalTable tableState1 = getTableState(1);
    DataFilesDiff dataFilesDiff1 = getFilesDiff(1);
    TableChange tableChange1 =
        TableChange.builder().tableAsOfChange(tableState1).filesDiff(dataFilesDiff1).build();

    List<Instant> pendingCommitInstants = Collections.emptyList();
    when(mockConversionTarget1.getTableFormat()).thenReturn(TableFormat.ICEBERG);
    when(mockConversionTarget2.getTableFormat()).thenReturn(TableFormat.DELTA);

    List<TableChange> tableChanges = Collections.singletonList(tableChange1);
    IncrementalTableChanges incrementalTableChanges =
        IncrementalTableChanges.builder()
            .pendingCommits(pendingCommitInstants)
            .tableChanges(tableChanges.iterator())
            .build();

    Map<ConversionTarget, TableSyncMetadata> conversionTargetWithMetadata = new HashMap<>();
    // mockConversionTarget1 will have nothing to sync
    conversionTargetWithMetadata.put(
        mockConversionTarget1, TableSyncMetadata.of(Instant.now(), Collections.emptyList()));
    // mockConversionTarget2 will have synced the first table change previously
    conversionTargetWithMetadata.put(
        mockConversionTarget2,
        TableSyncMetadata.of(Instant.now().minus(1, ChronoUnit.HOURS), Collections.emptyList()));

    Map<String, List<SyncResult>> result =
        TableFormatSync.getInstance()
            .syncChanges(conversionTargetWithMetadata, incrementalTableChanges);
    assertEquals(1, result.size());
    List<SyncResult> conversionTarget2Results = result.get(TableFormat.DELTA);
    assertEquals(1, conversionTarget2Results.size());
    conversionTarget2Results.forEach(
        syncResult -> {
          assertEquals(SyncMode.INCREMENTAL, syncResult.getMode());
          assertEquals(SyncResult.SyncStatus.SUCCESS, syncResult.getStatus());
          assertSyncResultTimes(syncResult, start);
        });

    verify(mockConversionTarget1, never()).beginSync(any());
    verify(mockConversionTarget1, never()).syncFilesForDiff(any());
    verify(mockConversionTarget1, never()).completeSync();

    verifyBaseConversionTargetCalls(mockConversionTarget2, tableState1, pendingCommitInstants);
    verify(mockConversionTarget2).syncFilesForDiff(dataFilesDiff1);
  }

  /**
   * Creates a table state with the given id so the state is different between calls.
   *
   * @param id id to use in the names of certain fields
   * @return the table
   */
  private static InternalTable getTableState(int id) {
    return InternalTable.builder()
        .tableFormat(TableFormat.HUDI)
        .readSchema(InternalSchema.builder().name(String.format("test_schema_%d", id)).build())
        .partitioningFields(
            Collections.singletonList(
                InternalPartitionField.builder()
                    .sourceField(
                        InternalField.builder()
                            .name(String.format("partition_field_%d", id))
                            .build())
                    .transformType(PartitionTransformType.VALUE)
                    .build()))
        .latestCommitTime(Instant.now().minus(10 - id, ChronoUnit.MINUTES))
        .basePath("/tmp/test")
        .build();
  }

  private DataFilesDiff getFilesDiff(int id) {
    return DataFilesDiff.builder()
        .filesAdded(
            Collections.singletonList(
                InternalDataFile.builder()
                    .physicalPath(String.format("/tmp/path/file_%d.parquet", id))
                    .build()))
        .build();
  }

  private void verifyBaseConversionTargetCalls(
      ConversionTarget mockConversionTarget,
      InternalTable startingTableState,
      List<Instant> pendingCommitInstants) {
    verify(mockConversionTarget).beginSync(startingTableState);
    verify(mockConversionTarget).syncSchema(startingTableState.getReadSchema());
    verify(mockConversionTarget).syncPartitionSpec(startingTableState.getPartitioningFields());
    verify(mockConversionTarget)
        .syncMetadata(
            TableSyncMetadata.of(startingTableState.getLatestCommitTime(), pendingCommitInstants));
  }
}
