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
 
package io.onetable.hudi;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import io.onetable.TestJavaHudiTable;
import io.onetable.model.CurrentCommitState;
import io.onetable.model.InstantsForIncrementalSync;
import io.onetable.model.OneSnapshot;
import io.onetable.model.TableChange;

/**
 * A suite of functional tests that the extraction from Hudi to Intermediate representation works.
 */
public class ITHudiSourceClient {
  @TempDir public static Path tempDir;
  private static final Configuration CONFIGURATION = new Configuration();

  /**
   * All tests to write. Combinations to test (MOR, COW), (Full Sync, Incremental Sync), (No
   * Partition & Partition). 1. Insert and upserts 2. Add Partition. 3. Delete Partition. 4. Add
   * Columns. 5. Table services (clean, cluster, savepoint & restore, rollback, compaction).
   */
  @Test
  @SneakyThrows
  public void insertAndUpsertData() {
    String tableName = "test_table_" + UUID.randomUUID();
    HudiTestUtil.PartitionConfig partitionConfig =
        HudiTestUtil.PartitionConfig.of("level:SIMPLE", "level:VALUE");
    HoodieTableType tableType = HoodieTableType.MERGE_ON_READ;
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {
      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(100);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      String commitInstant2 = table.startCommit();
      table.upsertRecordsWithCommitAlreadyStarted(
          insertsForCommit1.subList(0, 20), commitInstant2, true);
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        table.compact();
      }

      HudiClient hudiClient = getHudiSourceClient(CONFIGURATION, table.getBasePath());
      // Get the current snapshot
      OneSnapshot oneSnapshot = hudiClient.getCurrentSnapshot();
      // Get second change in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant1))
              .build();
      CurrentCommitState<HoodieInstant> instantCurrentCommitState =
          hudiClient.getCurrentCommitState(instantsForIncrementalSync);
      assertEquals(2, instantCurrentCommitState.getCommitsToProcess().size());
      TableChange tableChange =
          hudiClient.getTableChangeForCommit(
              instantCurrentCommitState.getCommitsToProcess().get(0));
      int x = 5;
    }
  }

  private HudiClient getHudiSourceClient(Configuration conf, String basePath) {
    HoodieTableMetaClient hoodieTableMetaClient =
        HoodieTableMetaClient.builder()
            .setConf(conf)
            .setBasePath(basePath)
            .setLoadActiveTimelineOnLoad(true)
            .build();
    HudiSourcePartitionSpecExtractor partitionSpecExtractor =
        new ConfigurationBasedPartitionSpecExtractor(
            HudiSourceConfig.builder().partitionFieldSpecConfig("level:VALUE").build());
    return new HudiClient(hoodieTableMetaClient, partitionSpecExtractor);
  }
}
