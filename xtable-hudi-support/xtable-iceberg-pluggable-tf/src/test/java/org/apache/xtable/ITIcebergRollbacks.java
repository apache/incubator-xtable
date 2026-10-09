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
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

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

import org.apache.iceberg.types.Types;

import org.apache.xtable.hudi.HudiTestUtil;

/**
 * A Hudi rollback is recorded in Iceberg as a forward snapshot, so the Iceberg table has to match
 * the Hudi table after every rollback Hudi permits, including ones that follow a clean or another
 * rollback, and rollbacks of upserts, where the previous file versions become live again.
 */
class ITIcebergRollbacks {
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
        "rollback_" + UUID.randomUUID(), tempDir, null, HoodieTableType.COPY_ON_WRITE);
  }

  @Test
  void rollbackOfAnUpsertRestoresThePreviousFileVersions() {
    try (TestJavaHudiTable table = newTable()) {
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(100, true);
      List<String> filesAfterInsert = table.getAllLatestBaseFilePaths();
      String upsert =
          table.getMetaClient().reloadActiveTimeline().lastInstant().get().requestedTime();
      table.upsertRecords(inserts.subList(0, 20), true);
      String upsertCommit =
          table.getMetaClient().reloadActiveTimeline().lastInstant().get().requestedTime();
      assertFalse(upsert.equals(upsertCommit));
      assertFalse(
          filesAfterInsert.equals(table.getAllLatestBaseFilePaths()),
          "the upsert should rewrite the file group");

      table.rollback(upsertCommit);

      // The rewritten base file is gone and the version the upsert superseded is live again.
      assertIcebergReferencesExactly(table.getBasePath(), filesAfterInsert);
      assertEquals(100, icebergRowCount(sparkSession, table.getBasePath()));
      table.insertRecords(50, true);
      assertIcebergReferencesExactly(table.getBasePath(), table.getAllLatestBaseFilePaths());
      assertEquals(150, icebergRowCount(sparkSession, table.getBasePath()));
    }
  }

  @Test
  void rollbackOfTheLatestCommitAfterACleanIsAccepted() {
    try (TestJavaHudiTable table = newTable()) {
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(100, true);
      table.upsertRecords(inserts.subList(0, 20), true);
      String lastInsert = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(50), lastInsert, true);
      // The clean is recorded as the current Iceberg snapshot; Hudi still permits rolling back the
      // latest commit, since only completed commits after it would block the rollback.
      table.clean();
      List<String> filesBeforeLastInsert =
          table.getAllLatestBaseFilePaths().stream()
              .filter(path -> !path.contains("_" + lastInsert + "."))
              .collect(java.util.stream.Collectors.toList());

      table.rollback(lastInsert);

      assertIcebergReferencesExactly(table.getBasePath(), table.getAllLatestBaseFilePaths());
      assertIcebergReferencesExactly(table.getBasePath(), filesBeforeLastInsert);
      assertEquals(100, icebergRowCount(sparkSession, table.getBasePath()));
      HoodieTimeline timeline = reconstructedTimeline(table.getBasePath());
      assertNothingPending(timeline);
      assertEquals(1, timeline.getCleanerTimeline().filterCompletedInstants().countInstants());
      assertEquals(1, timeline.getRollbackTimeline().filterCompletedInstants().countInstants());
    }
  }

  @Test
  void rollbackAfterARolledBackPendingWriteIsAccepted() {
    try (TestJavaHudiTable table = newTable()) {
      String commit1 = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(100), commit1, true);
      List<String> filesAfterCommit1 = table.getAllLatestBaseFilePaths();
      String commit2 = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(50), commit2, true);
      // A write that never completed: its files exist but it was never recorded in Iceberg, so
      // its rollback has nothing to remove there.
      String crashedCommit = table.startCommit();
      table.bulkInsertWithoutCommit(table.generateRecords(30), crashedCommit);
      table.rollback(crashedCommit);

      // The current Iceberg snapshot now records a rollback rather than commit 2.
      table.rollback(commit2);

      assertIcebergReferencesExactly(table.getBasePath(), filesAfterCommit1);
      assertEquals(100, icebergRowCount(sparkSession, table.getBasePath()));
      HoodieTimeline timeline = reconstructedTimeline(table.getBasePath());
      assertEquals(Collections.singletonList(commit1), completedCommitTimes(timeline));
      assertEquals(2, timeline.getRollbackTimeline().filterCompletedInstants().countInstants());
      assertNothingPending(timeline);
    }
  }

  @Test
  void rollbackOfTheFirstCommitLeavesAnEmptyTable() {
    try (TestJavaHudiTable table = newTable()) {
      String commit1 = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(40), commit1, true);
      Types.StructType schemaBeforeRollback = icebergTable(table.getBasePath()).schema().asStruct();

      // Hudi has no commit left to describe the table with, so the rollback snapshot is built from
      // the Iceberg table itself, and must not alter its schema.
      table.rollback(commit1);

      assertEquals(schemaBeforeRollback, icebergTable(table.getBasePath()).schema().asStruct());
      assertIcebergReferencesExactly(table.getBasePath(), Collections.emptyList());
      assertEquals(0, icebergRowCount(sparkSession, table.getBasePath()));
      String commit2 = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(30), commit2, true);
      assertIcebergReferencesExactly(table.getBasePath(), table.getAllLatestBaseFilePaths());
      assertEquals(30, icebergRowCount(sparkSession, table.getBasePath()));
      assertEquals(
          Arrays.asList(commit2), completedCommitTimes(reconstructedTimeline(table.getBasePath())));
    }
  }
}
