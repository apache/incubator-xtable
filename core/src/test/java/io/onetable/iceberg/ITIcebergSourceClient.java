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
 
package io.onetable.iceberg;

import static io.onetable.GenericTable.getTableName;
import static io.onetable.ValidationTestHelper.validateOneSnapshot;
import static io.onetable.ValidationTestHelper.validateTableChanges;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.iceberg.data.Record;

import io.onetable.TestIcebergTable;
import io.onetable.client.PerTableConfig;
import io.onetable.model.CommitsBacklog;
import io.onetable.model.InstantsForIncrementalSync;
import io.onetable.model.OneSnapshot;
import io.onetable.model.TableChange;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.storage.TableFormat;

public class ITIcebergSourceClient {
  private static final Configuration hadoopConf = new Configuration();

  @TempDir public static Path tempDir;
  private IcebergSourceClientProvider clientProvider;

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void testIcebergSourceClient(boolean isPartitioned) throws IOException {
    String tableName = getTableName();
    try (TestIcebergTable testIcebergTable =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, isPartitioned ? "level" : null, tempDir, hadoopConf)) {
      List<List<String>> allActiveFiles = new ArrayList<>();
      List<List<String>> allBaseFilePaths = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();
      List<Record> records = testIcebergTable.insertRows(50);
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
          PerTableConfig.builder()
              .tableName(testIcebergTable.getTableName())
              .tableBasePath(testIcebergTable.getBasePath())
              .targetTableFormats(Arrays.asList(TableFormat.HUDI, TableFormat.DELTA))
              .build();
      IcebergSourceClient icebergSourceClient = clientProvider.getSourceClientInstance(tableConfig);
      assertEquals(180L, testIcebergTable.getNumRows());
      OneSnapshot oneSnapshot = icebergSourceClient.getCurrentSnapshot();

      if (isPartitioned) {
        validateIcebergPartitioning(oneSnapshot);
      }
      validateOneSnapshot(oneSnapshot, allActiveFiles.get(allActiveFiles.size() - 1));
      // Get changes in incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
              .build();
      CommitsBacklog<Long> commitsBacklog =
          icebergSourceClient.getCommitsBacklog(instantsForIncrementalSync);
      for (Long snapshotId : commitsBacklog.getCommitsToProcess()) {
        TableChange tableChange = icebergSourceClient.getTableChangeForCommit(snapshotId);
        allTableChanges.add(tableChange);
      }
      validateTableChanges(allActiveFiles, allTableChanges);
    }
  }

  @Test
  public void canDelete() throws IOException {
    boolean isPartitioned = false;
    String tableName = getTableName();
    try (TestIcebergTable testIcebergTable =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, isPartitioned ? "level" : null, tempDir, hadoopConf)) {
      List<List<String>> allActiveFiles = new ArrayList<>();
      List<List<String>> allBaseFilePaths = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();
      List<Record> records = testIcebergTable.insertRows(50);
      Long timestamp1 = testIcebergTable.getLastCommitTimestamp();
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      // TODO(vamshigv): can't find a way to do partial updates/deletes for now, explore.
      testIcebergTable.upsertRows(records);
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      testIcebergTable.deleteRows(records);
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      testIcebergTable.insertRows(50);
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      testIcebergTable.insertRows(50);
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      testIcebergTable.insertRows(50);
      allActiveFiles.add(testIcebergTable.getAllActiveFiles());

      PerTableConfig tableConfig =
          PerTableConfig.builder()
              .tableName(testIcebergTable.getTableName())
              .tableBasePath(testIcebergTable.getBasePath())
              .targetTableFormats(Arrays.asList(TableFormat.HUDI, TableFormat.DELTA))
              .build();
      IcebergSourceClient icebergSourceClient = clientProvider.getSourceClientInstance(tableConfig);
      assertEquals(180L, testIcebergTable.getNumRows());
      OneSnapshot oneSnapshot = icebergSourceClient.getCurrentSnapshot();

      if (isPartitioned) {
        validateIcebergPartitioning(oneSnapshot);
      }
      validateOneSnapshot(oneSnapshot, allActiveFiles.get(allActiveFiles.size() - 1));
      // Get changes in incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
              .build();
      CommitsBacklog<Long> commitsBacklog =
          icebergSourceClient.getCommitsBacklog(instantsForIncrementalSync);
      for (Long snapshotId : commitsBacklog.getCommitsToProcess()) {
        TableChange tableChange = icebergSourceClient.getTableChangeForCommit(snapshotId);
        allTableChanges.add(tableChange);
      }
      validateTableChanges(allActiveFiles, allTableChanges);
    }
  }

  private void validateIcebergPartitioning(OneSnapshot oneSnapshot) {
    List<OnePartitionField> partitionFields = oneSnapshot.getTable().getPartitioningFields();
    assertEquals(1, partitionFields.size());
    OnePartitionField partitionField = partitionFields.get(0);
    assertEquals("level", partitionField.getSourceField().getName());
    assertEquals(PartitionTransformType.VALUE, partitionField.getTransformType());
  }
}
