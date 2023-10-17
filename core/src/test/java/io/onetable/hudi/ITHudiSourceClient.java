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

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

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

  // TODO(vamshigv): Following tests to add:
  // 1. Test for Add Partition.
  // 2. Test for Delete Partition.
  // 3. Test for Add Columns.
  // 4. Test for Clean Table services.
  // 5. Test for Cluster.
  // 6. Test for Savepoint & Restore.
  // 7. Test for Rollback.
  @ParameterizedTest
  @MethodSource("testsForAllTableTypes")
  public void insertAndUpsertData(HoodieTableType tableType) {
    String tableName = "test_table_" + UUID.randomUUID();
    HudiTestUtil.PartitionConfig partitionConfig =
        HudiTestUtil.PartitionConfig.of("level:SIMPLE", "level:VALUE");
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
      assertNotNull(oneSnapshot);
      // Get second change in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant1))
              .build();
      CurrentCommitState<HoodieInstant> instantCurrentCommitState =
          hudiClient.getCurrentCommitState(instantsForIncrementalSync);
      if (HoodieTableType.MERGE_ON_READ == tableType) {
        assertEquals(2, instantCurrentCommitState.getCommitsToProcess().size());
        TableChange deleteTableChange =
            hudiClient.getTableChangeForCommit(
                instantCurrentCommitState.getCommitsToProcess().get(0));
        assertNotNull(deleteTableChange);
        TableChange compactionTableChange =
            hudiClient.getTableChangeForCommit(
                instantCurrentCommitState.getCommitsToProcess().get(1));
        assertNotNull(compactionTableChange);
      } else {
        assertEquals(1, instantCurrentCommitState.getCommitsToProcess().size());
        TableChange deleteTableChange =
            hudiClient.getTableChangeForCommit(
                instantCurrentCommitState.getCommitsToProcess().get(0));
        assertNotNull(deleteTableChange);
      }
    }
  }

  // Tests for concurrent writes (inserts & compaction).
  @Test
  public void testConcurrentWrites() {
    String tableName = "test_table_" + UUID.randomUUID();
    HudiTestUtil.PartitionConfig partitionConfig =
        HudiTestUtil.PartitionConfig.of("level:SIMPLE", "level:VALUE");
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), HoodieTableType.MERGE_ON_READ)) {
      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(100);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);

      String commitInstant2 = table.startCommit();
      table.upsertRecordsWithCommitAlreadyStarted(
          insertsForCommit1.subList(0, 20), commitInstant2, true);

      String compactInstant = table.onlyScheduleCompaction();

      String commitInstant3 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit3 = table.generateRecords(100);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit3, commitInstant3, true);

      HudiClient hudiClient = getHudiSourceClient(CONFIGURATION, table.getBasePath());
      // Get the current snapshot
      OneSnapshot oneSnapshot = hudiClient.getCurrentSnapshot();
      // Extraction should return compaction as pending instant.
      assertNotNull(oneSnapshot);
      assertEquals(1, oneSnapshot.getPendingCommits().size());
      assertEquals(
          HudiInstantUtils.parseFromInstantTime(compactInstant),
          oneSnapshot.getPendingCommits().get(0));

      table.completeScheduledCompaction(compactInstant);
      hudiClient.reloadTimeline();

      // Get completed compaction change in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant3))
              .pendingCommits(Arrays.asList(HudiInstantUtils.parseFromInstantTime(compactInstant)))
              .build();
      CurrentCommitState<HoodieInstant> instantCurrentCommitState =
          hudiClient.getCurrentCommitState(instantsForIncrementalSync);
      assertEquals(1, instantCurrentCommitState.getCommitsToProcess().size());
      TableChange compactionTableChange =
          hudiClient.getTableChangeForCommit(
              instantCurrentCommitState.getCommitsToProcess().get(0));
      assertNotNull(compactionTableChange);
    }
  }

  private static Stream<Arguments> testsForAllTableTypes() {
    return Stream.of(
        Arguments.of(HoodieTableType.COPY_ON_WRITE), Arguments.of(HoodieTableType.MERGE_ON_READ));
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
