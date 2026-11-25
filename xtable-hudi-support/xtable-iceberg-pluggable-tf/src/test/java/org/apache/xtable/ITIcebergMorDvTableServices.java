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
import static org.apache.xtable.MergeOnReadAssertions.deletionVectorFiles;
import static org.apache.xtable.MergeOnReadAssertions.deletionVectorTable;
import static org.apache.xtable.MergeOnReadAssertions.filesWithDeletes;
import static org.apache.xtable.MergeOnReadAssertions.icebergRows;
import static org.apache.xtable.MergeOnReadAssertions.liveDeletionVectorFiles;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.WriteConcurrencyMode;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieWriteConflictException;

/**
 * Clustering, cleaning and archival on a merge-on-read table published with deletion vectors: a
 * replaced base file takes its deletion vector with it, cleaning leaves the current Iceberg table
 * untouched, and a deletion vector file is deleted once no retained snapshot applies any deletion
 * vector in it.
 */
class ITIcebergMorDvTableServices {
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

  private static TestJavaHudiTable newTable(Properties properties) {
    return TestJavaHudiTable.forStandardSchema(
        "mor_" + UUID.randomUUID().toString().replace('-', '_'),
        tempDir,
        null,
        HoodieTableType.MERGE_ON_READ,
        properties);
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 9, 10})
  void clusteringReplacesBaseFilesAndTheirDeletionVectors(int tableVersion) {
    try (TestJavaHudiTable table = newTable(deletionVectorTable(tableVersion))) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(50, true);
      table.insertRecords(30, true);
      table.upsertRecords(inserts.subList(0, 10), true);
      table.deleteRecords(inserts.subList(10, 15), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(15, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));

      table.cluster();
      assertPublishedAsARewrite(basePath);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(0, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(75, icebergRows(spark, basePath).size());

      table.upsertRecords(inserts.subList(15, 25), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(10, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(75, icebergRows(spark, basePath).size());
      assertNothingPending(reconstructedTimeline(basePath));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 9, 10})
  void rollbackOfAClusteringRestoresTheReplacedBaseFilesWithTheirDeletionVectors(int tableVersion) {
    try (TestJavaHudiTable table = newTable(deletionVectorTable(tableVersion))) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(50, true);
      table.insertRecords(30, true);
      table.upsertRecords(inserts.subList(0, 10), true);
      Map<String, Set<String>> beforeClustering = deletedKeysByDataFileInIceberg(spark, basePath);
      List<String> rowsBeforeClustering = icebergRows(spark, basePath);
      assertEquals(10, deletedRowCount(beforeClustering));

      table.cluster();
      String clustering =
          table.getMetaClient().reloadActiveTimeline().lastInstant().get().requestedTime();
      assertIcebergMatchesHudi(spark, basePath);
      assertFalse(beforeClustering.equals(deletedKeysByDataFileInIceberg(spark, basePath)));

      table.rollback(clustering);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(beforeClustering, deletedKeysByDataFileInIceberg(spark, basePath));
      assertEquals(rowsBeforeClustering, icebergRows(spark, basePath));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 9, 10})
  void cleaningLeavesTheIcebergTableAsItIs(int tableVersion) {
    try (TestJavaHudiTable table = newTable(deletionVectorTable(tableVersion))) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(50, true);
      // Two rounds of update and compaction leave replaced file slices for the cleaner.
      table.upsertRecords(inserts.subList(0, 10), true);
      table.compact();
      table.upsertRecords(inserts.subList(10, 20), true);
      table.compact();
      table.deleteRecords(inserts.subList(20, 25), true);
      assertIcebergMatchesHudi(spark, basePath);
      Map<String, Set<String>> beforeClean = deletedKeysByDataFileInIceberg(spark, basePath);
      List<String> rowsBeforeClean = icebergRows(spark, basePath);

      table.clean();
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(beforeClean, deletedKeysByDataFileInIceberg(spark, basePath));
      assertEquals(rowsBeforeClean, icebergRows(spark, basePath));
      assertEquals(45, rowsBeforeClean.size());

      table.deleteRecords(inserts.subList(25, 30), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(40, icebergRows(spark, basePath).size());
      assertNothingPending(reconstructedTimeline(basePath));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 9, 10})
  void archivalDeletesDeletionVectorFilesNoRetainedSnapshotApplies(int tableVersion) {
    Properties properties = deletionVectorTable(tableVersion);
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            "mor_" + UUID.randomUUID().toString().replace('-', '_'),
            tempDir,
            null,
            HoodieTableType.MERGE_ON_READ,
            HoodieArchivalConfig.newBuilder().archiveCommitsWith(2, 3).build(),
            properties)) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> first = table.insertRecords(40, true);
      List<HoodieRecord<HoodieAvroPayload>> second = table.insertRecords(40, true);
      // One deltacommit deletes from both base files, so one file holds both deletion vectors.
      table.deleteRecords(concat(first.subList(0, 5), second.subList(0, 5)), true);
      Set<String> sharedFile = deletionVectorFiles(basePath);
      assertEquals(1, sharedFile.size(), "one deletion vector file per deltacommit");
      assertEquals(2, filesWithDeletes(deletedKeysByDataFileInIceberg(spark, basePath)).size());

      // Supersedes the deletion vector of the first base file only: the shared file still holds
      // the live deletion vector of the second.
      table.deleteRecords(first.subList(5, 10), true);
      assertEquals(2, deletionVectorFiles(basePath).size());
      assertIcebergMatchesHudi(spark, basePath);

      for (int commit = 0; commit < 4; commit++) {
        table.insertRecords(5, true);
        assertIcebergMatchesHudi(spark, basePath);
      }
      long snapshots =
          StreamSupport.stream(icebergTable(basePath).snapshots().spliterator(), false).count();
      assertTrue(snapshots < 7, "archival must have expired snapshots, kept " + snapshots);
      assertEquals(2, liveDeletionVectorFiles(basePath).size());
      assertEquals(liveDeletionVectorFiles(basePath), deletionVectorFiles(basePath));

      // Supersedes the deletion vector of the second base file as well, after which nothing live
      // is left in the shared file and archival may delete it.
      table.deleteRecords(second.subList(5, 10), true);
      for (int commit = 0; commit < 4; commit++) {
        table.insertRecords(5, true);
        assertIcebergMatchesHudi(spark, basePath);
      }
      assertEquals(liveDeletionVectorFiles(basePath), deletionVectorFiles(basePath));
      assertFalse(
          deletionVectorFiles(basePath).containsAll(sharedFile),
          "the file whose deletion vectors were all superseded must be deleted");
      assertEquals(120 - 20, icebergRows(spark, basePath).size());
    }
  }

  /**
   * An update whose rows were looked up before a clustering replaced their file group, and that
   * commits after the clustering, deletes from a base file that is no longer live. Hudi rejects it
   * under optimistic concurrency control; a single writer does not check, and the publish refuses
   * it instead. Either way Iceberg keeps its last snapshot, and the write succeeds when retried.
   */
  @ParameterizedTest
  @EnumSource(
      value = WriteConcurrencyMode.class,
      names = {"OPTIMISTIC_CONCURRENCY_CONTROL", "SINGLE_WRITER"})
  void deleteTaggedBeforeAClusteringCompletesAndCommittedAfterItIsRefused(
      WriteConcurrencyMode concurrencyMode) {
    Properties properties = deletionVectorTable(9);
    properties.setProperty(HoodieWriteConfig.WRITE_CONCURRENCY_MODE.key(), concurrencyMode.name());
    String tableName = "mor_" + UUID.randomUUID().toString().replace('-', '_');
    try (TestJavaHudiTable ingestion =
            TestJavaHudiTable.forStandardSchema(
                tableName, tempDir, null, HoodieTableType.MERGE_ON_READ, properties);
        TestJavaHudiTable services =
            TestJavaHudiTable.forStandardSchema(
                tableName, tempDir, null, HoodieTableType.MERGE_ON_READ, properties)) {
      String basePath = ingestion.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = ingestion.insertRecords(50, true);
      ingestion.insertRecords(30, true);
      ingestion.upsertRecords(inserts.subList(0, 10), true);

      String update = ingestion.startCommit();
      List<WriteStatus> written =
          ingestion
              .getWriteClient()
              .upsert(ingestion.generateUpdatesForRecords(inserts.subList(20, 30)), update);
      services.cluster();
      assertIcebergMatchesHudi(spark, basePath);
      Map<String, Set<String>> afterClustering = deletedKeysByDataFileInIceberg(spark, basePath);
      List<String> rowsAfterClustering = icebergRows(spark, basePath);

      Throwable refused =
          assertThrows(Throwable.class, () -> ingestion.commitInstant(update, written));
      Throwable conflict = refused;
      while (conflict != null && !(conflict instanceof HoodieWriteConflictException)) {
        conflict = conflict.getCause();
      }
      assertNotNull(conflict, () -> "not refused as a write conflict: " + refused);
      assertEquals(afterClustering, deletedKeysByDataFileInIceberg(spark, basePath));
      assertEquals(rowsAfterClustering, icebergRows(spark, basePath));
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(
          1, reconstructedTimeline(basePath).filterInflightsAndRequested().countInstants());

      // A single writer refuses to roll back an instant that a later commit completed after, here
      // the clustering, so the refused deltacommit stays pending under that mode.
      boolean singleWriter = concurrencyMode == WriteConcurrencyMode.SINGLE_WRITER;
      if (!singleWriter) {
        ingestion.rollback(update);
      }
      ingestion.upsertRecords(inserts.subList(20, 30), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(10, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(80, icebergRows(spark, basePath).size());
      assertEquals(
          singleWriter ? Collections.singletonList(update) : Collections.emptyList(),
          reconstructedTimeline(basePath)
              .filterInflightsAndRequested()
              .getInstantsAsStream()
              .map(HoodieInstant::requestedTime)
              .collect(Collectors.toList()));
    }
  }

  private static <T> List<T> concat(List<T> first, List<T> second) {
    List<T> all = new ArrayList<>(first);
    all.addAll(second);
    return all;
  }
}
