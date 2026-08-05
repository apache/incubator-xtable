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
 
package org.apache.xtable;

import static org.apache.xtable.GenericTable.getTableName;
import static org.apache.xtable.hudi.HudiTestUtil.PartitionConfig;
import static org.apache.xtable.model.storage.TableFormat.HUDI;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;

import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.model.sync.SyncMode;

/** Conversion while the Hudi source is being written to concurrently or compacted. */
public class ITConversionControllerConcurrentWrites extends ConversionControllerTestBase {

  @ParameterizedTest
  @MethodSource("testCasesWithPartitioningAndSyncModes")
  public void testConcurrentInsertWritesInSource(
      SyncMode syncMode, PartitionConfig partitionConfig) {
    String tableName = getTableName();
    ConversionSourceProvider<?> conversionSourceProvider = getConversionSourceProvider(HUDI);
    List<String> targetTableFormats = getOtherFormats(HUDI);
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), HoodieTableType.COPY_ON_WRITE)) {
      // commit time 1 starts first but ends 2nd.
      // commit time 2 starts second but ends 1st.
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(50);
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit2 = table.generateRecords(50);
      String commitInstant1 = table.startCommit();

      String commitInstant2 = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit2, commitInstant2, true);

      ConversionConfig conversionConfig =
          getTableSyncConfig(
              HUDI,
              syncMode,
              tableName,
              table,
              targetTableFormats,
              partitionConfig.getXTableConfig(),
              null);
      conversionController.sync(conversionConfig, conversionSourceProvider);

      checkDatasetEquivalence(HUDI, table, targetTableFormats, 50);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, targetTableFormats, 100);
    }
  }

  @ParameterizedTest
  @MethodSource("testCasesWithPartitioningAndSyncModes")
  public void testConcurrentInsertsAndTableServiceWrites(
      SyncMode syncMode, PartitionConfig partitionConfig) {
    HoodieTableType tableType = HoodieTableType.MERGE_ON_READ;
    ConversionSourceProvider<?> conversionSourceProvider = getConversionSourceProvider(HUDI);
    List<String> targetTableFormats = getOtherFormats(HUDI);
    String tableName = getTableName();
    try (TestSparkHudiTable table =
        TestSparkHudiTable.forStandardSchema(
            tableName, tempDir, jsc, partitionConfig.getHudiConfig(), tableType)) {
      List<HoodieRecord<HoodieAvroPayload>> insertedRecords1 = table.insertRecords(50, true);

      ConversionConfig conversionConfig =
          getTableSyncConfig(
              HUDI,
              syncMode,
              tableName,
              table,
              targetTableFormats,
              partitionConfig.getXTableConfig(),
              null);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, targetTableFormats, 50);

      table.deleteRecords(insertedRecords1.subList(0, 20), true);
      // At this point table should have 30 records but only after compaction.
      String scheduledCompactionInstant = table.onlyScheduleCompaction();

      table.insertRecords(50, true);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      Map<String, String> sourceHudiOptions =
          Collections.singletonMap("hoodie.datasource.query.type", "read_optimized");
      // Because compaction is not completed yet and read optimized query, there are 100 records.
      checkDatasetEquivalence(
          HUDI, table, sourceHudiOptions, targetTableFormats, Collections.emptyMap(), 100);

      table.insertRecords(50, true);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      // Because compaction is not completed yet and read optimized query, there are 150 records.
      checkDatasetEquivalence(
          HUDI, table, sourceHudiOptions, targetTableFormats, Collections.emptyMap(), 150);

      table.completeScheduledCompaction(scheduledCompactionInstant);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, targetTableFormats, 130);
    }
  }
}
