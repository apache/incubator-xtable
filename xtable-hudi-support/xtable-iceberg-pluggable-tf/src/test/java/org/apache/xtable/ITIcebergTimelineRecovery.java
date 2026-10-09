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

import static org.apache.xtable.IcebergTableAssertions.assertIcebergReferencesExactly;
import static org.apache.xtable.IcebergTableAssertions.assertNothingPending;
import static org.apache.xtable.IcebergTableAssertions.completedCommitTimes;
import static org.apache.xtable.IcebergTableAssertions.icebergRowCount;
import static org.apache.xtable.IcebergTableAssertions.icebergTable;
import static org.apache.xtable.IcebergTableAssertions.reconstructedTimeline;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.StreamSupport;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import org.apache.iceberg.Table;

import org.apache.xtable.hudi.HudiTestUtil;

/**
 * Hudi completes an instant before the Iceberg hook runs, so the two can diverge when a writer dies
 * in between, and Iceberg history can be shortened by an expire-snapshots run. The table has to
 * recover from each without losing committed data or wedging the next write.
 */
class ITIcebergTimelineRecovery {
  @TempDir public static Path tempDir;
  private static SparkSession sparkSession;

  @BeforeAll
  static void setupOnce() {
    SparkConf sparkConf = HudiTestUtil.getSparkConf(tempDir);
    sparkSession =
        SparkSession.builder().config(HoodieReadClient.addHoodieSupport(sparkConf)).getOrCreate();
  }

  @AfterAll
  static void teardown() {
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

  private static TestJavaHudiTable newTable() {
    return TestJavaHudiTable.forStandardSchema(
        "recovery_" + UUID.randomUUID(), tempDir, null, HoodieTableType.COPY_ON_WRITE);
  }

  /** Simulates the writer dying after Hudi completed the instant and before its snapshot landed. */
  private static void dropLatestSnapshot(String basePath) {
    Table table = icebergTable(basePath);
    table.manageSnapshots().rollbackTo(table.currentSnapshot().parentId()).commit();
  }

  @Test
  void completedCommitWithoutASnapshotIsRolledBackByTheNextWrite() {
    try (TestJavaHudiTable table = newTable()) {
      String commit1 = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(100), commit1, true);
      String commit2 = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(50), commit2, true);
      String commit3 = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(30), commit3, true);
      dropLatestSnapshot(table.getBasePath());

      HoodieTimeline before = reconstructedTimeline(table.getBasePath());
      assertEquals(Arrays.asList(commit1, commit2), completedCommitTimes(before));
      assertTrue(
          before
              .filterInflightsAndRequested()
              .getInstantsAsStream()
              .anyMatch(instant -> commit3.equals(instant.requestedTime())),
          "the unrecorded commit must be reported as pending");

      String commit4 = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(20), commit4, true);
      // The test tables use optimistic concurrency, which makes failed-write cleaning lazy: the
      // pending commit is rolled back by the next clean once its heartbeat is gone, rather than by
      // the next write as under the eager policy. That rollback has nothing to remove from Iceberg.
      table.getWriteClient().clean();

      HoodieTimeline after = reconstructedTimeline(table.getBasePath());
      assertEquals(Arrays.asList(commit1, commit2, commit4), completedCommitTimes(after));
      assertNothingPending(after);
      assertIcebergReferencesExactly(table.getBasePath(), table.getAllLatestBaseFilePaths());
      assertEquals(170, icebergRowCount(sparkSession, table.getBasePath()));
    }
  }

  @Test
  void completedCleanWithoutASnapshotIsRecoveredByTheNextWrite() {
    try (TestJavaHudiTable table = newTable()) {
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(100, true);
      table.upsertRecords(inserts.subList(0, 20), true);
      table.insertRecords(50, true);
      table.clean();
      dropLatestSnapshot(table.getBasePath());

      table.insertRecords(10, true);
      // Hudi re-executes the pending clean at the next clean, and records it in Iceberg again.
      table.getWriteClient().clean();

      HoodieTimeline timeline = reconstructedTimeline(table.getBasePath());
      assertNothingPending(timeline);
      assertEquals(1, timeline.getCleanerTimeline().filterCompletedInstants().countInstants());
      assertEquals(4, completedCommitTimes(timeline).size());
      assertIcebergReferencesExactly(table.getBasePath(), table.getAllLatestBaseFilePaths());
      assertEquals(160, icebergRowCount(sparkSession, table.getBasePath()));
    }
  }

  @Test
  void expiredSnapshotsDoNotTurnCommittedHistoryIntoPendingWork() {
    try (TestJavaHudiTable table = newTable()) {
      // Four commits stay under the archival threshold, so expiry alone shortens the history.
      List<String> commits = new ArrayList<>();
      for (int i = 0; i < 4; i++) {
        String commit = table.startCommit();
        table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(10), commit, true);
        commits.add(commit);
      }
      // An operator runs Iceberg's expire-snapshots on the table, keeping only the current one.
      Table icebergTable = icebergTable(table.getBasePath());
      icebergTable
          .expireSnapshots()
          .expireOlderThan(System.currentTimeMillis())
          .retainLast(1)
          .commit();
      assertEquals(1, StreamSupport.stream(icebergTable.snapshots().spliterator(), false).count());

      HoodieTimeline timeline = reconstructedTimeline(table.getBasePath());
      assertEquals(commits, completedCommitTimes(timeline));
      assertNothingPending(timeline);

      String nextCommit = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(10), nextCommit, true);
      assertIcebergReferencesExactly(table.getBasePath(), table.getAllLatestBaseFilePaths());
      assertEquals(50, icebergRowCount(sparkSession, table.getBasePath()));
      assertNothingPending(reconstructedTimeline(table.getBasePath()));
    }
  }
}
