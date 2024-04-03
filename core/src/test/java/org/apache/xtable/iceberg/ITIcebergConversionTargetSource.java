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
 
package org.apache.xtable.iceberg;

import static org.apache.xtable.GenericTable.getTableName;
import static org.apache.xtable.ValidationTestHelper.validateSnapshot;
import static org.apache.xtable.ValidationTestHelper.validateTableChanges;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.Record;

import org.apache.xtable.TestIcebergTable;
import org.apache.xtable.conversion.PerTableConfig;
import org.apache.xtable.conversion.PerTableConfigImpl;
import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.storage.TableFormat;

public class ITIcebergConversionTargetSource {
  private static final Configuration hadoopConf = new Configuration();

  @TempDir public static Path tempDir;
  private IcebergConversionSourceProvider sourceProvider;

  @BeforeEach
  void setup() {
    sourceProvider = new IcebergConversionSourceProvider();
    sourceProvider.init(hadoopConf, null);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testInsertsUpsertsAndDeletes(boolean isPartitioned) {
    String tableName = getTableName();
    try (TestIcebergTable testIcebergTable =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, isPartitioned ? "level" : null, tempDir, hadoopConf)) {
      List<List<String>> allActiveFiles = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();

      testIcebergTable.insertRows(50);
      Long timestamp1 = testIcebergTable.getLastCommitTimestamp();
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      List<Record> records1 = testIcebergTable.insertRows(50);
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      testIcebergTable.upsertRows(records1.subList(0, 20));
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      testIcebergTable.insertRows(50);
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      testIcebergTable.deleteRows(records1.subList(0, 20));
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      testIcebergTable.insertRows(50);
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      PerTableConfig tableConfig =
          PerTableConfigImpl.builder()
              .tableName(testIcebergTable.getTableName())
              .tableBasePath(testIcebergTable.getBasePath())
              .targetTableFormats(Arrays.asList(TableFormat.HUDI, TableFormat.DELTA))
              .build();
      IcebergConversionSource conversionSource =
          sourceProvider.getConversionSourceInstance(tableConfig);
      assertEquals(180L, testIcebergTable.getNumRows());
      InternalSnapshot internalSnapshot = conversionSource.getCurrentSnapshot();

      if (isPartitioned) {
        validateIcebergPartitioning(internalSnapshot);
      }
      validateSnapshot(internalSnapshot, allActiveFiles.get(allActiveFiles.size() - 1));
      // Get changes in incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
              .build();
      CommitsBacklog<Snapshot> commitsBacklog =
          conversionSource.getCommitsBacklog(instantsForIncrementalSync);
      for (Snapshot snapshot : commitsBacklog.getCommitsToProcess()) {
        TableChange tableChange = conversionSource.getTableChangeForCommit(snapshot);
        allTableChanges.add(tableChange);
      }
      validateTableChanges(allActiveFiles, allTableChanges);
    }
  }

  @Test
  public void testDropPartition() {
    String tableName = getTableName();
    try (TestIcebergTable testIcebergTable =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, "level", tempDir, hadoopConf)) {
      List<List<String>> allActiveFiles = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();

      List<Record> records1 = testIcebergTable.insertRows(50);
      Long timestamp1 = testIcebergTable.getLastCommitTimestamp();
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      List<Record> records2 = testIcebergTable.insertRows(50);
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      List<Record> allRecords = new ArrayList<>();
      allRecords.addAll(records1);
      allRecords.addAll(records2);

      Map<String, List<Record>> recordsByPartition =
          testIcebergTable.groupRecordsByPartition(allRecords);
      String partitionValueToDelete = recordsByPartition.keySet().iterator().next();
      testIcebergTable.deletePartition(partitionValueToDelete);
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      // Insert few records again for the deleted partition.
      testIcebergTable.insertRecordsForPartition(20, partitionValueToDelete);
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      PerTableConfig tableConfig =
          PerTableConfigImpl.builder()
              .tableName(testIcebergTable.getTableName())
              .tableBasePath(testIcebergTable.getBasePath())
              .targetTableFormats(Arrays.asList(TableFormat.HUDI, TableFormat.DELTA))
              .build();
      IcebergConversionSource conversionSource =
          sourceProvider.getConversionSourceInstance(tableConfig);
      assertEquals(
          120 - recordsByPartition.get(partitionValueToDelete).size(),
          testIcebergTable.getNumRows());
      InternalSnapshot internalSnapshot = conversionSource.getCurrentSnapshot();

      validateIcebergPartitioning(internalSnapshot);
      validateSnapshot(internalSnapshot, allActiveFiles.get(allActiveFiles.size() - 1));
      // Get changes in incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
              .build();
      CommitsBacklog<Snapshot> commitsBacklog =
          conversionSource.getCommitsBacklog(instantsForIncrementalSync);
      for (Snapshot snapshot : commitsBacklog.getCommitsToProcess()) {
        TableChange tableChange = conversionSource.getTableChangeForCommit(snapshot);
        allTableChanges.add(tableChange);
      }
      validateTableChanges(allActiveFiles, allTableChanges);
    }
  }

  @Test
  public void testDeleteAllRecordsInPartition() {
    String tableName = getTableName();
    try (TestIcebergTable testIcebergTable =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, "level", tempDir, hadoopConf)) {
      List<List<String>> allActiveFiles = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();

      List<Record> records1 = testIcebergTable.insertRows(50);
      Long timestamp1 = testIcebergTable.getLastCommitTimestamp();
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      List<Record> records2 = testIcebergTable.insertRows(50);
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      List<Record> allRecords = new ArrayList<>();
      allRecords.addAll(records1);
      allRecords.addAll(records2);

      Map<String, List<Record>> recordsByPartition =
          testIcebergTable.groupRecordsByPartition(allRecords);
      String partitionValueToDelete = recordsByPartition.keySet().iterator().next();
      testIcebergTable.deleteRows(recordsByPartition.get(partitionValueToDelete));
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      // Insert few records again for the deleted partition.
      testIcebergTable.insertRecordsForPartition(20, partitionValueToDelete);
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      PerTableConfig tableConfig =
          PerTableConfigImpl.builder()
              .tableName(testIcebergTable.getTableName())
              .tableBasePath(testIcebergTable.getBasePath())
              .targetTableFormats(Arrays.asList(TableFormat.HUDI, TableFormat.DELTA))
              .build();
      IcebergConversionSource conversionSource =
          sourceProvider.getConversionSourceInstance(tableConfig);
      assertEquals(
          120 - recordsByPartition.get(partitionValueToDelete).size(),
          testIcebergTable.getNumRows());
      InternalSnapshot internalSnapshot = conversionSource.getCurrentSnapshot();

      validateIcebergPartitioning(internalSnapshot);
      validateSnapshot(internalSnapshot, allActiveFiles.get(allActiveFiles.size() - 1));
      // Get changes in incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
              .build();
      CommitsBacklog<Snapshot> commitsBacklog =
          conversionSource.getCommitsBacklog(instantsForIncrementalSync);
      for (Snapshot snapshot : commitsBacklog.getCommitsToProcess()) {
        TableChange tableChange = conversionSource.getTableChangeForCommit(snapshot);
        allTableChanges.add(tableChange);
      }
      validateTableChanges(allActiveFiles, allTableChanges);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testExpireSnapshots(boolean isPartitioned) throws InterruptedException {
    String tableName = getTableName();
    try (TestIcebergTable testIcebergTable =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, isPartitioned ? "level" : null, tempDir, hadoopConf)) {
      List<List<String>> allActiveFiles = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();

      List<Record> records1 = testIcebergTable.insertRows(50);
      Long timestamp1 = testIcebergTable.getLastCommitTimestamp();

      testIcebergTable.upsertRows(records1.subList(0, 20));
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());
      Thread.sleep(5 * 1000L);
      Instant instantAfterUpsert = Instant.now();

      testIcebergTable.insertRows(50);
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      // Since only snapshots are expired, nothing is added to AllActiveFiles.
      testIcebergTable.expireSnapshotsOlderThan(instantAfterUpsert);

      testIcebergTable.insertRows(50);
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      testIcebergTable.insertRows(50);
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      PerTableConfig tableConfig =
          PerTableConfigImpl.builder()
              .tableName(testIcebergTable.getTableName())
              .tableBasePath(testIcebergTable.getBasePath())
              .targetTableFormats(Arrays.asList(TableFormat.HUDI, TableFormat.DELTA))
              .build();
      IcebergConversionSource conversionSource =
          sourceProvider.getConversionSourceInstance(tableConfig);
      assertEquals(200L, testIcebergTable.getNumRows());
      InternalSnapshot internalSnapshot = conversionSource.getCurrentSnapshot();

      if (isPartitioned) {
        validateIcebergPartitioning(internalSnapshot);
      }
      validateSnapshot(internalSnapshot, allActiveFiles.get(allActiveFiles.size() - 1));
      // Get changes in incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
              .build();
      CommitsBacklog<Snapshot> commitsBacklog =
          conversionSource.getCommitsBacklog(instantsForIncrementalSync);
      for (Snapshot snapshot : commitsBacklog.getCommitsToProcess()) {
        TableChange tableChange = conversionSource.getTableChangeForCommit(snapshot);
        allTableChanges.add(tableChange);
      }
      validateTableChanges(allActiveFiles, allTableChanges);
    }
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void testForIncrementalSyncSafetyCheck(boolean shouldExpireSnapshots) {
    String tableName = getTableName();
    try (TestIcebergTable testIcebergTable =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, "level", tempDir, hadoopConf)) {
      // Insert 50 rows to INFO partition.
      List<Record> commit1Rows = testIcebergTable.insertRecordsForPartition(50, "INFO");
      Long timestamp1 = testIcebergTable.getLastCommitTimestamp();
      PerTableConfig tableConfig =
          PerTableConfigImpl.builder()
              .tableName(testIcebergTable.getTableName())
              .tableBasePath(testIcebergTable.getBasePath())
              .tableDataPath(testIcebergTable.getDataPath())
              .targetTableFormats(Arrays.asList(TableFormat.HUDI, TableFormat.DELTA))
              .build();

      // Upsert all rows inserted before, so all files are replaced.
      testIcebergTable.upsertRows(commit1Rows.subList(0, 50));
      long snapshotIdAfterCommit2 = testIcebergTable.getLatestSnapshot().snapshotId();

      // Insert 50 rows to different (ERROR) partition.
      testIcebergTable.insertRecordsForPartition(50, "ERROR");

      if (shouldExpireSnapshots) {
        // Expire snapshotAfterCommit2.
        testIcebergTable.expireSnapshot(snapshotIdAfterCommit2);
      }
      IcebergConversionSource conversionSource =
          sourceProvider.getConversionSourceInstance(tableConfig);
      if (shouldExpireSnapshots) {
        assertFalse(conversionSource.isIncrementalSyncSafeFrom(Instant.ofEpochMilli(timestamp1)));
      } else {
        assertTrue(conversionSource.isIncrementalSyncSafeFrom(Instant.ofEpochMilli(timestamp1)));
      }
      // Table doesn't have instant of this older commit, hence it is not safe.
      Instant instantAsOfHourAgo = Instant.now().minus(1, ChronoUnit.HOURS);
      assertFalse(conversionSource.isIncrementalSyncSafeFrom(instantAsOfHourAgo));
    }
  }

  private void validateIcebergPartitioning(InternalSnapshot internalSnapshot) {
    List<InternalPartitionField> partitionFields =
        internalSnapshot.getTable().getPartitioningFields();
    assertEquals(1, partitionFields.size());
    InternalPartitionField partitionField = partitionFields.get(0);
    assertEquals("level", partitionField.getSourceField().getName());
    assertEquals(PartitionTransformType.VALUE, partitionField.getTransformType());
  }
}
