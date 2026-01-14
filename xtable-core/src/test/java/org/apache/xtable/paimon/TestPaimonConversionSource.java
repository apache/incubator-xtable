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
 
package org.apache.xtable.paimon;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Instant;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.xtable.GenericTable;
import org.apache.xtable.TestPaimonTable;
import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.model.storage.TableFormat;

public class TestPaimonConversionSource {

  @TempDir private Path tempDir;

  private Configuration hadoopConf;
  private TestPaimonTable testTable;
  private FileStoreTable paimonTable;
  private PaimonConversionSource conversionSource;

  @BeforeEach
  void setUp() {
    hadoopConf = new Configuration();
    testTable =
        ((TestPaimonTable)
            TestPaimonTable.createTable("test_table", "level", tempDir, hadoopConf, false));
    paimonTable = testTable.getPaimonTable();
    conversionSource = new PaimonConversionSource(paimonTable);
  }

  @Test
  void testGetTableWithPartitionedTable() {
    testTable.insertRows(5);

    Snapshot snapshot = paimonTable.snapshotManager().latestSnapshot();
    assertNotNull(snapshot);

    InternalTable result = conversionSource.getTable(snapshot);

    assertNotNull(result);
    assertEquals("test_table", result.getName());
    assertEquals(TableFormat.PAIMON, result.getTableFormat());
    assertNotNull(result.getReadSchema());
    assertEquals(DataLayoutStrategy.HIVE_STYLE_PARTITION, result.getLayoutStrategy());
    assertTrue(result.getBasePath().contains("test_table"));
    assertEquals(1, result.getPartitioningFields().size());
    assertEquals("level", result.getPartitioningFields().get(0).getSourceField().getName());
    assertEquals(Instant.ofEpochMilli(snapshot.timeMillis()), result.getLatestCommitTime());
    assertNotNull(result.getLatestMetadataPath());
  }

  @Test
  void testGetTableWithUnpartitionedTable() {
    GenericTable<?, String> unpartitionedTable =
        TestPaimonTable.createTable("unpartitioned_table", null, tempDir, hadoopConf, false);
    FileStoreTable unpartitionedPaimonTable =
        ((TestPaimonTable) unpartitionedTable).getPaimonTable();
    PaimonConversionSource unpartitionedSource =
        new PaimonConversionSource(unpartitionedPaimonTable);

    unpartitionedTable.insertRows(3);

    Snapshot snapshot = unpartitionedPaimonTable.snapshotManager().latestSnapshot();
    assertNotNull(snapshot);

    InternalTable result = unpartitionedSource.getTable(snapshot);

    assertNotNull(result);
    assertEquals("test_table", result.getName());
    assertEquals(TableFormat.PAIMON, result.getTableFormat());
    assertNotNull(result.getReadSchema());
    assertEquals(DataLayoutStrategy.HIVE_STYLE_PARTITION, result.getLayoutStrategy());
    assertTrue(result.getBasePath().contains("unpartitioned_table"));
    assertEquals(0, result.getPartitioningFields().size());
    assertEquals(Instant.ofEpochMilli(snapshot.timeMillis()), result.getLatestCommitTime());
    assertNotNull(result.getLatestMetadataPath());
  }

  @Test
  void testGetCurrentTableSuccess() {
    testTable.insertRows(3);

    InternalTable result = conversionSource.getCurrentTable();

    assertNotNull(result);
    assertEquals(TableFormat.PAIMON, result.getTableFormat());
    assertEquals("test_table", result.getName());
    assertNotNull(result.getReadSchema());
    assertEquals(DataLayoutStrategy.HIVE_STYLE_PARTITION, result.getLayoutStrategy());
    assertEquals(1, result.getPartitioningFields().size());
  }

  @Test
  void testGetCurrentTableThrowsExceptionWhenNoSnapshot() {
    GenericTable<?, String> emptyTable =
        TestPaimonTable.createTable("empty_table", "level", tempDir, hadoopConf, false);
    FileStoreTable emptyPaimonTable = ((TestPaimonTable) emptyTable).getPaimonTable();
    PaimonConversionSource emptySource = new PaimonConversionSource(emptyPaimonTable);

    ReadException exception = assertThrows(ReadException.class, emptySource::getCurrentTable);

    assertTrue(exception.getMessage().contains("No snapshots found for table"));
  }

  @Test
  void testGetCurrentSnapshotSuccess() {
    testTable.insertRows(5);

    InternalSnapshot result = conversionSource.getCurrentSnapshot();

    assertNotNull(result);
    assertNotNull(result.getTable());
    assertEquals(TableFormat.PAIMON, result.getTable().getTableFormat());
    assertNotNull(result.getVersion());
    assertNotNull(result.getSourceIdentifier());
    assertNotNull(result.getPartitionedDataFiles());

    List<PartitionFileGroup> partitionFileGroups = result.getPartitionedDataFiles();
    assertFalse(partitionFileGroups.isEmpty());
    assertTrue(partitionFileGroups.stream().allMatch(group -> !group.getDataFiles().isEmpty()));
  }

  @Test
  void testGetCurrentSnapshotThrowsExceptionWhenNoSnapshot() {
    GenericTable<?, String> emptyTable =
        TestPaimonTable.createTable("empty_table2", "level", tempDir, hadoopConf, false);
    FileStoreTable emptyPaimonTable = ((TestPaimonTable) emptyTable).getPaimonTable();
    PaimonConversionSource emptySource = new PaimonConversionSource(emptyPaimonTable);

    ReadException exception = assertThrows(ReadException.class, emptySource::getCurrentSnapshot);

    assertTrue(exception.getMessage().contains("No snapshots found for table"));
  }

  @Test
  void testGetCommitsBacklogReturnsCommitsAfterLastSync() {
    // Insert initial data to create first snapshot
    testTable.insertRows(5);
    Snapshot firstSnapshot = paimonTable.snapshotManager().latestSnapshot();
    assertNotNull(firstSnapshot);

    // Insert more data to create second snapshot
    testTable.insertRows(3);
    Snapshot secondSnapshot = paimonTable.snapshotManager().latestSnapshot();
    assertNotNull(secondSnapshot);
    assertNotEquals(firstSnapshot.id(), secondSnapshot.id());

    // Get commits backlog from first snapshot time
    InstantsForIncrementalSync instantsForSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(Instant.ofEpochMilli(firstSnapshot.timeMillis()))
            .build();

    CommitsBacklog<Snapshot> backlog = conversionSource.getCommitsBacklog(instantsForSync);

    // Verify we get at least the second snapshot (may get more if insertRows creates multiple)
    assertNotNull(backlog);
    assertTrue(backlog.getCommitsToProcess().size() >= 1);

    // Verify the last snapshot in the backlog is the second snapshot
    assertEquals(
        secondSnapshot.id(),
        backlog.getCommitsToProcess().get(backlog.getCommitsToProcess().size() - 1).id());

    // Verify the first snapshot is NOT in the list of commits to process
    assertFalse(
        backlog.getCommitsToProcess().stream()
            .anyMatch(snapshot -> snapshot.id() == firstSnapshot.id()),
        "First snapshot should not be in the backlog since we're syncing from that instant");
    assertTrue(backlog.getInFlightInstants().isEmpty());
  }

  @Test
  void testGetCommitsBacklogReturnsEmptyForFutureInstant() {
    testTable.insertRows(5);

    // Use a future instant
    InstantsForIncrementalSync instantsForSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(Instant.now().plusSeconds(3600))
            .build();

    CommitsBacklog<Snapshot> backlog = conversionSource.getCommitsBacklog(instantsForSync);

    // Verify no snapshots are returned
    assertNotNull(backlog);
    assertTrue(backlog.getCommitsToProcess().isEmpty());
  }

  @Test
  void testGetTableChangeForCommitReturnsCorrectFilesDiff() {
    // Insert initial data
    testTable.insertRows(5);
    Snapshot firstSnapshot = paimonTable.snapshotManager().latestSnapshot();
    assertNotNull(firstSnapshot);

    // Insert more data to create second snapshot
    testTable.insertRows(3);
    Snapshot secondSnapshot = paimonTable.snapshotManager().latestSnapshot();
    assertNotNull(secondSnapshot);

    // Get table change for second snapshot
    TableChange tableChange = conversionSource.getTableChangeForCommit(secondSnapshot);

    // Verify table change structure
    assertNotNull(tableChange);
    assertNotNull(tableChange.getFilesDiff());
    assertNotNull(tableChange.getTableAsOfChange());
    assertEquals(
        Long.toString(secondSnapshot.commitIdentifier()), tableChange.getSourceIdentifier());

    // For append-only table, we should have added files and no removed files
    assertTrue(tableChange.getFilesDiff().getFilesAdded().size() > 0);
  }

  @Test
  void testIsIncrementalSyncSafeFromReturnsTrueForValidInstant() {
    testTable.insertRows(5);
    Snapshot snapshot = paimonTable.snapshotManager().latestSnapshot();
    Instant snapshotTime = Instant.ofEpochMilli(snapshot.timeMillis());

    assertTrue(conversionSource.isIncrementalSyncSafeFrom(snapshotTime));
  }

  @Test
  void testIsIncrementalSyncSafeFromReturnsFalseForFutureInstant() {
    testTable.insertRows(5);
    Snapshot snapshot = paimonTable.snapshotManager().latestSnapshot();

    // Use an instant way in the future (well after the snapshot)
    Instant futureInstant = Instant.ofEpochMilli(snapshot.timeMillis()).plusSeconds(3600);

    assertFalse(conversionSource.isIncrementalSyncSafeFrom(futureInstant));
  }

  @Test
  void testIsIncrementalSyncSafeFromReturnsFalseForEmptyTable() {
    // Don't insert any data
    Instant someInstant = Instant.now();

    assertFalse(conversionSource.isIncrementalSyncSafeFrom(someInstant));
  }

  @Test
  void testIsIncrementalSyncSafeFromReturnsFalseForInstantBeforeFirstSnapshot() {
    testTable.insertRows(5);
    Snapshot snapshot = paimonTable.snapshotManager().latestSnapshot();

    Instant instantBeforeFirstSnapshot =
        Instant.ofEpochMilli(snapshot.timeMillis()).minusSeconds(3600);

    assertFalse(conversionSource.isIncrementalSyncSafeFrom(instantBeforeFirstSnapshot));
  }

  @Test
  void testGetCommitIdentifier() {
    testTable.insertRows(3);
    Snapshot snapshot = paimonTable.snapshotManager().latestSnapshot();

    String result = conversionSource.getCommitIdentifier(snapshot);

    assertNotNull(result);
    assertEquals(String.valueOf(snapshot.commitIdentifier()), result);
  }

  @Test
  void testCloseDoesNotThrowException() {
    assertDoesNotThrow(() -> conversionSource.close());
  }

  @Test
  void testConstructorInitializesFieldsCorrectly() {
    assertNotNull(conversionSource);

    testTable.insertRows(1);
    assertDoesNotThrow(() -> conversionSource.getCurrentTable());
  }

  @Test
  void testMultipleSnapshots() {
    testTable.insertRows(2);
    Snapshot firstSnapshot = paimonTable.snapshotManager().latestSnapshot();
    assertNotNull(firstSnapshot);

    testTable.insertRows(3);
    Snapshot secondSnapshot = paimonTable.snapshotManager().latestSnapshot();
    assertNotNull(secondSnapshot);

    assertNotEquals(firstSnapshot.id(), secondSnapshot.id());

    InternalTable firstTable = conversionSource.getTable(firstSnapshot);
    InternalTable secondTable = conversionSource.getTable(secondSnapshot);

    assertNotNull(firstTable);
    assertNotNull(secondTable);
    assertEquals(firstTable.getName(), secondTable.getName());
    assertEquals(firstTable.getTableFormat(), secondTable.getTableFormat());
  }

  @Test
  void testSchemaEvolution() {
    testTable.insertRows(2);

    GenericTable<?, String> tableWithExtraColumns =
        TestPaimonTable.createTable("table_with_extra", "level", tempDir, hadoopConf, true);
    FileStoreTable extraColumnsPaimonTable =
        ((TestPaimonTable) tableWithExtraColumns).getPaimonTable();
    PaimonConversionSource extraColumnsSource = new PaimonConversionSource(extraColumnsPaimonTable);

    tableWithExtraColumns.insertRows(2);

    InternalTable originalTable = conversionSource.getCurrentTable();
    InternalTable expandedTable = extraColumnsSource.getCurrentTable();

    assertNotNull(originalTable);
    assertNotNull(expandedTable);

    assertTrue(
        expandedTable.getReadSchema().getFields().size()
            >= originalTable.getReadSchema().getFields().size());
  }
}
