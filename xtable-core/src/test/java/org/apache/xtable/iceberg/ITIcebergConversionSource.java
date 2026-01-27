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
import static org.apache.xtable.testutil.ITTestUtils.validateTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.data.Record;

import org.apache.xtable.TestIcebergTable;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.storage.TableFormat;

@Execution(ExecutionMode.SAME_THREAD)
public class ITIcebergConversionSource {
  private static final Configuration hadoopConf = new Configuration();

  @TempDir public static Path tempDir;
  private IcebergConversionSourceProvider sourceProvider;

  @BeforeEach
  void setup() {
    sourceProvider = new IcebergConversionSourceProvider();
    sourceProvider.init(hadoopConf);
  }

  @Test
  void getCurrentTableTest() {
    String tableName = getTableName();
    try (TestIcebergTable testIcebergTable =
        new TestIcebergTable(
            tableName,
            tempDir,
            hadoopConf,
            "field1",
            Collections.singletonList(null),
            TestIcebergDataHelper.SchemaType.BASIC)) {
      testIcebergTable.insertRows(50);
      SourceTable tableConfig =
          SourceTable.builder()
              .name(testIcebergTable.getTableName())
              .basePath(testIcebergTable.getBasePath())
              .formatName(TableFormat.ICEBERG)
              .build();
      IcebergConversionSource conversionSource =
          sourceProvider.getConversionSourceInstance(tableConfig);
      InternalTable internalTable = conversionSource.getCurrentTable();
      InternalSchema internalSchema =
          InternalSchema.builder()
              .name("record")
              .dataType(InternalType.RECORD)
              .fields(
                  Arrays.asList(
                      InternalField.builder()
                          .name("field1")
                          .fieldId(1)
                          .schema(
                              InternalSchema.builder()
                                  .name("string")
                                  .dataType(InternalType.STRING)
                                  .isNullable(true)
                                  .build())
                          .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                          .build(),
                      InternalField.builder()
                          .name("field2")
                          .fieldId(2)
                          .schema(
                              InternalSchema.builder()
                                  .name("string")
                                  .dataType(InternalType.STRING)
                                  .isNullable(true)
                                  .build())
                          .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                          .build()))
              .build();
      validateTable(
          internalTable,
          testIcebergTable.getBasePath(),
          TableFormat.ICEBERG,
          internalSchema,
          DataLayoutStrategy.FLAT,
          testIcebergTable.getBasePath(),
          testIcebergTable.getMetadataPath(),
          Collections.emptyList());
    }
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
      long timestamp1 = testIcebergTable.getLastCommitTimestamp();
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

      SourceTable tableConfig =
          SourceTable.builder()
              .name(testIcebergTable.getTableName())
              .basePath(testIcebergTable.getBasePath())
              .formatName(TableFormat.ICEBERG)
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
      long timestamp1 = testIcebergTable.getLastCommitTimestamp();
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

      SourceTable tableConfig =
          SourceTable.builder()
              .name(testIcebergTable.getTableName())
              .basePath(testIcebergTable.getBasePath())
              .formatName(TableFormat.ICEBERG)
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
      long timestamp1 = testIcebergTable.getLastCommitTimestamp();
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

      SourceTable tableConfig =
          SourceTable.builder()
              .name(testIcebergTable.getTableName())
              .basePath(testIcebergTable.getBasePath())
              .formatName(TableFormat.ICEBERG)
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
      long timestamp1 = testIcebergTable.getLastCommitTimestamp();

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

      SourceTable tableConfig =
          SourceTable.builder()
              .name(testIcebergTable.getTableName())
              .basePath(testIcebergTable.getBasePath())
              .formatName(TableFormat.ICEBERG)
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
      List<Record> firstCommitRows = testIcebergTable.insertRecordsForPartition(50, "INFO");
      long timestampAfterFirstCommit = testIcebergTable.getLastCommitTimestamp();
      SourceTable tableConfig =
          SourceTable.builder()
              .name(testIcebergTable.getTableName())
              .basePath(testIcebergTable.getBasePath())
              .formatName(TableFormat.ICEBERG)
              .build();

      // Upsert all rows inserted before, so all files are replaced.
      testIcebergTable.upsertRows(firstCommitRows.subList(0, 50));
      long timestampAfterSecondCommit = testIcebergTable.getLastCommitTimestamp();
      long snapshotIdAfterSecondCommit = testIcebergTable.getLatestSnapshot().snapshotId();

      // Insert 50 rows to different (ERROR) partition.
      testIcebergTable.insertRecordsForPartition(50, "ERROR");
      long timestampAfterThirdCommit = testIcebergTable.getLastCommitTimestamp();

      if (shouldExpireSnapshots) {
        // Expire snapshotAfterCommit2.
        testIcebergTable.expireSnapshot(snapshotIdAfterSecondCommit);
      }
      IcebergConversionSource conversionSource =
          sourceProvider.getConversionSourceInstance(tableConfig);
      if (shouldExpireSnapshots) {
        // Since the second snapshot is expired, we cannot safely perform incremental sync from the
        // first two commits
        assertFalse(
            conversionSource.isIncrementalSyncSafeFrom(
                Instant.ofEpochMilli(timestampAfterFirstCommit)));
        assertFalse(
            conversionSource.isIncrementalSyncSafeFrom(
                Instant.ofEpochMilli(timestampAfterSecondCommit)));
      } else {
        // The full history is still present so incremental sync is safe from any of these commits
        assertTrue(
            conversionSource.isIncrementalSyncSafeFrom(
                Instant.ofEpochMilli(timestampAfterFirstCommit)));
        assertTrue(
            conversionSource.isIncrementalSyncSafeFrom(
                Instant.ofEpochMilli(timestampAfterSecondCommit)));
      }
      // Table always has the last commit so incremental sync is safe
      assertTrue(
          conversionSource.isIncrementalSyncSafeFrom(
              Instant.ofEpochMilli(timestampAfterThirdCommit)));
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
