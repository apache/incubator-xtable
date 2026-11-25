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

import static org.apache.xtable.IcebergTableAssertions.assertNothingPending;
import static org.apache.xtable.IcebergTableAssertions.assertPublishedAsARewrite;
import static org.apache.xtable.IcebergTableAssertions.icebergTable;
import static org.apache.xtable.IcebergTableAssertions.reconstructedTimeline;
import static org.apache.xtable.MergeOnReadAssertions.assertIcebergMatchesHudi;
import static org.apache.xtable.MergeOnReadAssertions.deletedKeysByDataFileInIceberg;
import static org.apache.xtable.MergeOnReadAssertions.deletedRowCount;
import static org.apache.xtable.MergeOnReadAssertions.deletionVectorTable;
import static org.apache.xtable.MergeOnReadAssertions.icebergRows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.CompactionUtils;

import org.apache.iceberg.SnapshotSummary;

/**
 * A delete written while a compaction of its file group is scheduled carries positions of the base
 * file being compacted, yet belongs to the file slice the compaction produces, so the compaction
 * does not apply it and Hudi readers apply it to the compacted base file by record key. Iceberg has
 * to end up with the same deletes on the compacted base file, in the commit that brings it in, and
 * a compaction Iceberg has not recorded has to stay a pending compaction.
 */
class ITIcebergMorDvPendingCompaction {
  @TempDir public static Path tempDir;

  private static SparkSession spark;

  @BeforeAll
  static void setupOnce() {
    spark = MergeOnReadAssertions.sparkSession(tempDir);
  }

  @AfterAll
  static void teardown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static String newTableName() {
    return "mor_" + UUID.randomUUID().toString().replace('-', '_');
  }

  private static TestJavaHudiTable openTable(String tableName, int tableVersion) {
    return TestJavaHudiTable.forStandardSchema(
        tableName, tempDir, null, HoodieTableType.MERGE_ON_READ, deletionVectorTable(tableVersion));
  }

  private static List<HoodieCompactionOperation> compactionPlan(
      TestJavaHudiTable table, String compaction) {
    return CompactionUtils.getCompactionPlan(table.getMetaClient(), compaction).getOperations();
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 9, 10})
  void deletesWrittenWhileACompactionIsScheduledSurviveTheCompaction(int tableVersion) {
    try (TestJavaHudiTable table = openTable(newTableName(), tableVersion)) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(100, true);
      table.upsertRecords(inserts.subList(0, 20), true);
      String compaction = table.onlyScheduleCompaction();
      assertIcebergMatchesHudi(spark, basePath);

      table.upsertRecords(inserts.subList(20, 80), true);
      table.deleteRecords(inserts.subList(80, 85), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(85, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(95, icebergRows(spark, basePath).size());

      table.completeScheduledCompaction(compaction);
      assertPublishedAsARewrite(basePath);
      assertEquals(
          "1",
          icebergTable(basePath).currentSnapshot().summary().get(SnapshotSummary.ADDED_DVS_PROP));
      assertEquals(95, MergeOnReadAssertions.hudiRows(spark, basePath).size());
      assertIcebergMatchesHudi(spark, basePath);
      // The compaction applied the twenty deletes written before it was scheduled; the sixty-five
      // written after it are carried onto the compacted base file.
      assertEquals(65, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));

      table.deleteRecords(inserts.subList(85, 90), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(90, icebergRows(spark, basePath).size());

      table.compact();
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(0, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(90, icebergRows(spark, basePath).size());
      assertNothingPending(reconstructedTimeline(basePath));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 9, 10})
  void deleteTaggedBeforeACompactionCompletesAndCommittedAfterIt(int tableVersion) {
    try (TestJavaHudiTable table = openTable(newTableName(), tableVersion)) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(100, true);
      table.upsertRecords(inserts.subList(0, 20), true);
      String compaction = table.onlyScheduleCompaction();

      // The writer looks the keys up and writes its deletes against the base file the compaction
      // is about to replace, and commits only once the compaction is done.
      String update = table.startCommit();
      List<WriteStatus> written =
          table
              .getWriteClient()
              .upsert(table.generateUpdatesForRecords(inserts.subList(20, 50)), update);
      table.completeScheduledCompaction(compaction);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(0, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));

      table.commitInstant(update, written);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(30, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(100, icebergRows(spark, basePath).size());

      table.deleteRecords(inserts.subList(50, 55), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(35, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(95, icebergRows(spark, basePath).size());
      assertNothingPending(reconstructedTimeline(basePath));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 9, 10})
  void rollbackOfADeleteWhoseRowsACompactionCarriedOver(int tableVersion) {
    try (TestJavaHudiTable table = openTable(newTableName(), tableVersion)) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(100, true);
      table.deleteRecords(inserts.subList(0, 10), true);
      String compaction = table.onlyScheduleCompaction();
      table.deleteRecords(inserts.subList(10, 15), true);
      table.deleteRecords(inserts.subList(15, 30), true);
      String lateDelete =
          table.getMetaClient().reloadActiveTimeline().lastInstant().get().requestedTime();
      table.completeScheduledCompaction(compaction);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(20, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));

      // The rolled-back delete was published against the base file the compaction replaced; its
      // rows have to come back on the compacted base file.
      table.rollback(lateDelete);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(5, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(85, icebergRows(spark, basePath).size());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 9, 10})
  void compactionWhosePublishFailedStaysAPendingCompactionAndIsRunAgain(int tableVersion)
      throws IOException {
    try (TestJavaHudiTable table = openTable(newTableName(), tableVersion)) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(100, true);
      table.upsertRecords(inserts.subList(0, 20), true);
      String compaction = table.onlyScheduleCompaction();
      List<HoodieCompactionOperation> plan = compactionPlan(table, compaction);
      Map<String, Set<String>> beforeCompaction = deletedKeysByDataFileInIceberg(spark, basePath);

      // A read-only Iceberg metadata directory fails the publish after Hudi has completed the
      // compaction.
      Path metadataDirectory = Paths.get(URI.create(basePath)).resolve("metadata");
      Set<PosixFilePermission> writable = Files.getPosixFilePermissions(metadataDirectory);
      Files.setPosixFilePermissions(
          metadataDirectory, PosixFilePermissions.fromString("r-xr-xr-x"));
      assumeTrue(!Files.isWritable(metadataDirectory), "needs a user the permissions bind");
      try {
        assertThrows(Exception.class, () -> table.completeScheduledCompaction(compaction));
      } finally {
        Files.setPosixFilePermissions(metadataDirectory, writable);
      }

      HoodieTimeline timeline = reconstructedTimeline(basePath);
      assertEquals(plan, compactionPlan(table, compaction));
      assertEquals(
          Collections.singletonList(compaction),
          timeline
              .filterPendingCompactionTimeline()
              .getInstantsAsStream()
              .map(HoodieInstant::requestedTime)
              .collect(Collectors.toList()),
          timeline.getInstants().toString());
      assertEquals(1, timeline.filterInflightsAndRequested().countInstants());
      assertEquals(beforeCompaction, deletedKeysByDataFileInIceberg(spark, basePath));
      assertIcebergMatchesHudi(spark, basePath);

      // Ingestion goes on while the compaction is pending, without rolling it back.
      table.upsertRecords(inserts.subList(20, 40), true);
      table.deleteRecords(inserts.subList(40, 45), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(
          1, reconstructedTimeline(basePath).filterPendingCompactionTimeline().countInstants());
      assertEquals(plan, compactionPlan(table, compaction));

      table.completeScheduledCompaction(compaction);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(25, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(95, icebergRows(spark, basePath).size());
      assertNothingPending(reconstructedTimeline(basePath));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 9, 10})
  void compactionAndClusteringByASecondClientInterleavedWithIngestion(int tableVersion) {
    String tableName = newTableName();
    try (TestJavaHudiTable ingestion = openTable(tableName, tableVersion);
        TestJavaHudiTable services = openTable(tableName, tableVersion)) {
      String basePath = ingestion.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> first = ingestion.insertRecords(100, true);
      ingestion.upsertRecords(first.subList(0, 20), true);
      assertIcebergMatchesHudi(spark, basePath);

      String compaction = services.onlyScheduleCompaction();
      assertIcebergMatchesHudi(spark, basePath);
      ingestion.upsertRecords(first.subList(20, 50), true);
      assertIcebergMatchesHudi(spark, basePath);
      ingestion.deleteRecords(first.subList(50, 55), true);
      assertIcebergMatchesHudi(spark, basePath);
      services.completeScheduledCompaction(compaction);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(95, icebergRows(spark, basePath).size());

      ingestion.upsertRecords(first.subList(55, 65), true);
      assertIcebergMatchesHudi(spark, basePath);
      String secondCompaction = services.onlyScheduleCompaction();
      ingestion.deleteRecords(first.subList(65, 70), true);
      assertIcebergMatchesHudi(spark, basePath);
      services.completeScheduledCompaction(secondCompaction);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(90, icebergRows(spark, basePath).size());

      String clustering = services.onlyScheduleClustering();
      assertIcebergMatchesHudi(spark, basePath);
      // Hudi refuses updates to file groups under a pending clustering, so only new rows arrive.
      ingestion.insertRecords(20, true);
      assertIcebergMatchesHudi(spark, basePath);
      services.completeScheduledClustering(clustering);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(110, icebergRows(spark, basePath).size());

      ingestion.deleteRecords(first.subList(70, 75), true);
      assertIcebergMatchesHudi(spark, basePath);
      services.clean();
      assertIcebergMatchesHudi(spark, basePath);
      ingestion.insertRecords(5, true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(110, icebergRows(spark, basePath).size());
      assertNothingPending(reconstructedTimeline(basePath));
    }
  }
}
