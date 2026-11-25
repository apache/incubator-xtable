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
import static org.apache.xtable.MergeOnReadAssertions.UPDATES_AS_DELETES_AND_INSERTS;
import static org.apache.xtable.MergeOnReadAssertions.assertIcebergMatchesHudi;
import static org.apache.xtable.MergeOnReadAssertions.deletedKeysByDataFileInIceberg;
import static org.apache.xtable.MergeOnReadAssertions.deletedRowCount;
import static org.apache.xtable.MergeOnReadAssertions.deletionVectorFiles;
import static org.apache.xtable.MergeOnReadAssertions.deletionVectorTable;
import static org.apache.xtable.MergeOnReadAssertions.filesWithDeletes;
import static org.apache.xtable.MergeOnReadAssertions.icebergRows;
import static org.apache.xtable.MergeOnReadAssertions.rowCount;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.SneakyThrows;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.storage.StoragePath;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;

/**
 * A merge-on-read table whose writer turns every update into a positional delete plus an insert is
 * published as base files with deletion vectors: each deltacommit is one Iceberg snapshot holding
 * the new base files and one deletion vector per base file it deleted from. After every step the
 * Iceberg table has to read the same rows as Hudi and reference exactly the files and deleted keys
 * of Hudi's latest file slices.
 */
class ITIcebergMergeOnReadDeletionVectors {
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

  private static TestJavaHudiTable newTable(String partitionConfig, Properties properties) {
    return TestJavaHudiTable.forStandardSchema(
        "mor_" + UUID.randomUUID().toString().replace('-', '_'),
        tempDir,
        partitionConfig,
        HoodieTableType.MERGE_ON_READ,
        properties);
  }

  private static String latestInstant(TestJavaHudiTable table) {
    return table.getMetaClient().reloadActiveTimeline().lastInstant().get().requestedTime();
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 9, 10})
  void insertsUpdatesDeletesAndCompaction(int tableVersion) {
    try (TestJavaHudiTable table = newTable(null, deletionVectorTable(tableVersion))) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(50, true);
      assertEquals(3, ((BaseTable) icebergTable(basePath)).operations().current().formatVersion());
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(0, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));

      table.upsertRecords(inserts.subList(0, 10), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(10, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(50, icebergRows(spark, basePath).size());
      // Version 10 writes deletes to a native delete file, earlier versions to an inline log file.
      assertEquals(
          tableVersion, table.getMetaClient().getTableConfig().getTableVersion().versionCode());
      assertEquals(tableVersion >= 10, hasNativeDeleteFile(basePath));

      // A second round deletes more rows of the first base file, whose deletion vector has to be
      // superseded by one that holds both rounds.
      table.upsertRecords(inserts.subList(10, 20), true);
      assertIcebergMatchesHudi(spark, basePath);
      Map<String, Set<String>> deleted = deletedKeysByDataFileInIceberg(spark, basePath);
      assertEquals(20, deletedRowCount(deleted));
      assertEquals(1, filesWithDeletes(deleted).size(), "both rounds deleted from one base file");
      assertEquals(50, icebergRows(spark, basePath).size());

      table.deleteRecords(inserts.subList(20, 25), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(25, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(45, icebergRows(spark, basePath).size());

      table.compact();
      assertPublishedAsARewrite(basePath);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(0, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(45, icebergRows(spark, basePath).size());

      // Deletes keep landing on the compacted base file.
      table.deleteRecords(inserts.subList(25, 30), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(40, icebergRows(spark, basePath).size());
      assertNothingPending(reconstructedTimeline(basePath));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 9, 10})
  void partitionedTable(int tableVersion) {
    try (TestJavaHudiTable table = newTable("level:SIMPLE", deletionVectorTable(tableVersion))) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(60, true);
      assertTrue(icebergTable(basePath).spec().isPartitioned());
      assertIcebergMatchesHudi(spark, basePath);

      table.upsertRecords(inserts.subList(0, 15), true);
      assertIcebergMatchesHudi(spark, basePath);
      Map<String, Set<String>> deleted = deletedKeysByDataFileInIceberg(spark, basePath);
      assertEquals(15, deletedRowCount(deleted));
      assertTrue(filesWithDeletes(deleted).size() > 1, "deletes in more than one partition");
      assertEquals(60, icebergRows(spark, basePath).size());

      table.deleteRecords(inserts.subList(15, 20), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(55, icebergRows(spark, basePath).size());

      table.compact();
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(0, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(55, icebergRows(spark, basePath).size());
      assertNothingPending(reconstructedTimeline(basePath));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 9, 10})
  void repeatedUpdatesOfTheSameKeys(int tableVersion) {
    try (TestJavaHudiTable table = newTable(null, deletionVectorTable(tableVersion))) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> latest = table.insertRecords(50, true).subList(0, 10);
      // Each round moves the same ten keys to a new file group and deletes them from the one the
      // previous round wrote.
      for (int round = 1; round <= 3; round++) {
        latest = table.upsertRecords(latest, true);
        assertIcebergMatchesHudi(spark, basePath);
        Map<String, Set<String>> deleted = deletedKeysByDataFileInIceberg(spark, basePath);
        assertEquals(10L * round, deletedRowCount(deleted));
        assertEquals(round, filesWithDeletes(deleted).size());
        assertEquals(50, icebergRows(spark, basePath).size());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 9, 10})
  void rollbackOfADeltacommitRestoresTheDeletionVectorsItSuperseded(int tableVersion) {
    try (TestJavaHudiTable table = newTable(null, deletionVectorTable(tableVersion))) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(40, true);
      Map<String, Set<String>> afterInsert = deletedKeysByDataFileInIceberg(spark, basePath);
      List<String> rowsAfterInsert = icebergRows(spark, basePath);

      table.upsertRecords(inserts.subList(0, 5), true);
      String firstUpdate = latestInstant(table);
      Map<String, Set<String>> afterFirstUpdate = deletedKeysByDataFileInIceberg(spark, basePath);
      List<String> rowsAfterFirstUpdate = icebergRows(spark, basePath);
      assertEquals(5, deletedRowCount(afterFirstUpdate));

      // Deletes eight more rows of the first base file and two rows the first update wrote.
      table.upsertRecords(inserts.subList(3, 13), true);
      String secondUpdate = latestInstant(table);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(15, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));

      table.rollback(secondUpdate);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(afterFirstUpdate, deletedKeysByDataFileInIceberg(spark, basePath));
      assertEquals(rowsAfterFirstUpdate, icebergRows(spark, basePath));

      table.rollback(firstUpdate);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(afterInsert, deletedKeysByDataFileInIceberg(spark, basePath));
      assertEquals(rowsAfterInsert, icebergRows(spark, basePath));

      table.upsertRecords(inserts.subList(0, 5), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(5, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertNothingPending(reconstructedTimeline(basePath));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 9, 10})
  void rollbackOfACompactionRestoresTheBaseFilesWithTheirDeletionVectors(int tableVersion) {
    try (TestJavaHudiTable table = newTable(null, deletionVectorTable(tableVersion))) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(40, true);
      table.upsertRecords(inserts.subList(0, 5), true);
      table.deleteRecords(inserts.subList(5, 8), true);
      Map<String, Set<String>> beforeCompaction = deletedKeysByDataFileInIceberg(spark, basePath);
      List<String> rowsBeforeCompaction = icebergRows(spark, basePath);
      assertEquals(8, deletedRowCount(beforeCompaction));

      table.compact();
      String compaction = latestInstant(table);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(0, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));

      table.rollback(compaction);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(beforeCompaction, deletedKeysByDataFileInIceberg(spark, basePath));
      assertEquals(rowsBeforeCompaction, icebergRows(spark, basePath));

      table.deleteRecords(inserts.subList(8, 10), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(10, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
    }
  }

  /**
   * A delete of every row of a base file removes the data file in a delete snapshot, as Iceberg's
   * own writer does, instead of giving it a deletion vector of all its rows. Hudi keeps the base
   * file until a compaction or a clustering replaces it, and the instants that name it later leave
   * Iceberg holding the base files with live rows: a rollback of the delete, an upsert of the
   * deleted keys and its rollback, a clustering, a compaction and a clean.
   */
  @ParameterizedTest
  @ValueSource(ints = {9, 10})
  void deleteOfEveryRowOfABaseFileRemovesTheDataFile(int tableVersion) {
    try (TestJavaHudiTable table = newTable(null, deletionVectorTable(tableVersion))) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(40, true);
      String baseFile = localPaths(table.getAllLatestBaseFilePaths()).get(0);
      table.insertRecords(20, true);
      Set<String> keysOfBaseFile =
          spark
              .read()
              .parquet(baseFile)
              .select(HoodieRecord.RECORD_KEY_METADATA_FIELD)
              .collectAsList()
              .stream()
              .map(row -> row.getString(0))
              .collect(Collectors.toSet());
      List<HoodieRecord<HoodieAvroPayload>> deleted =
          inserts.stream()
              .filter(record -> keysOfBaseFile.contains(record.getRecordKey()))
              .collect(Collectors.toList());
      assertEquals(40, deleted.size(), "rows of the first base file");
      int rowsLeft = 20;

      table.deleteRecords(deleted.subList(0, 2), true);
      assertIcebergMatchesHudi(spark, basePath);
      table.deleteRecords(deleted.subList(2, deleted.size()), true);
      String delete = latestInstant(table);
      Snapshot snapshot = icebergTable(basePath).currentSnapshot();
      assertEquals(DataOperations.DELETE, snapshot.operation());
      assertEquals("1", snapshot.summary().get(SnapshotSummary.DELETED_FILES_PROP));
      assertEquals("1", snapshot.summary().get(SnapshotSummary.REMOVED_DVS_PROP));
      assertNull(snapshot.summary().get(SnapshotSummary.ADDED_DVS_PROP));
      assertEquals(
          String.valueOf(rowsLeft), snapshot.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP));
      assertIcebergHoldsTheBaseFilesWithLiveRows(table, rowsLeft);
      assertFalse(deletedKeysByDataFileInIceberg(spark, basePath).containsKey(baseFile));

      // The rollback restores the data file with the deletion vector of the first delete.
      table.rollback(delete);
      assertIcebergHoldsTheBaseFilesWithLiveRows(table, rowsLeft + deleted.size() - 2);
      assertEquals(2, deletedKeysByDataFileInIceberg(spark, basePath).get(baseFile).size());
      table.deleteRecords(deleted.subList(2, deleted.size()), true);
      assertIcebergHoldsTheBaseFilesWithLiveRows(table, rowsLeft);

      // The index still finds the deleted keys in the base file, so the upsert deletes them from
      // it again and inserts them into another file group.
      table.upsertRecords(deleted, true);
      String upsert = latestInstant(table);
      assertIcebergHoldsTheBaseFilesWithLiveRows(table, 60);
      table.rollback(upsert);
      assertIcebergHoldsTheBaseFilesWithLiveRows(table, rowsLeft);

      table.cluster();
      assertPublishedAsARewrite(basePath);
      assertIcebergHoldsTheBaseFilesWithLiveRows(table, rowsLeft);

      // The compaction of a base file whose rows a delete removed in full.
      List<String> beforeInsert = localPaths(table.getAllLatestBaseFilePaths());
      List<HoodieRecord<HoodieAvroPayload>> inserted = table.insertRecords(10, true);
      List<String> insertedBaseFiles = localPaths(table.getAllLatestBaseFilePaths());
      insertedBaseFiles.removeAll(beforeInsert);
      assertEquals(1, insertedBaseFiles.size());
      table.deleteRecords(inserted, true);
      assertIcebergHoldsTheBaseFilesWithLiveRows(table, rowsLeft);
      assertFalse(
          deletedKeysByDataFileInIceberg(spark, basePath).containsKey(insertedBaseFiles.get(0)));
      table.compact();
      assertIcebergHoldsTheBaseFilesWithLiveRows(table, rowsLeft);

      table.upsertRecords(deleted, true);
      table.clean();
      assertIcebergHoldsTheBaseFilesWithLiveRows(table, 60);
      assertNothingPending(reconstructedTimeline(basePath));
    }
  }

  /**
   * Iceberg reads the rows Hudi reads, holds the base files of Hudi's latest file slices with a
   * live row, and has no data file with rows whose deletion vector deletes them all.
   */
  private static void assertIcebergHoldsTheBaseFilesWithLiveRows(
      TestJavaHudiTable table, int rows) {
    String basePath = table.getBasePath();
    assertIcebergMatchesHudi(spark, basePath);
    Map<String, Set<String>> deletedKeys = deletedKeysByDataFileInIceberg(spark, basePath);
    deletedKeys.forEach(
        (file, keys) ->
            assertTrue(
                rowCount(file) == 0 || keys.size() < rowCount(file),
                "every row of data file " + file + " is deleted"));
    assertTrue(
        localPaths(table.getAllLatestBaseFilePaths()).containsAll(deletedKeys.keySet()),
        "Iceberg holds a file that is not a latest Hudi base file: " + deletedKeys.keySet());
    assertEquals(rows, icebergRows(spark, basePath).size());
  }

  private static List<String> localPaths(List<String> paths) {
    return paths.stream()
        .map(path -> new StoragePath(path).toUri().getPath())
        .collect(Collectors.toList());
  }

  @Test
  void tableAtAPlainBasePath() {
    String tableName = "mor_" + UUID.randomUUID().toString().replace('-', '_');
    // Spark and Hudi Streamer pass a plain path through, which leaves the base path without a
    // scheme, while Iceberg records files under their qualified paths.
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchemaAtBasePath(
            tableName,
            tempDir.resolve(tableName).toString(),
            "level:SIMPLE",
            HoodieTableType.MERGE_ON_READ,
            deletionVectorTable(9))) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(40, true);
      table.upsertRecords(inserts.subList(0, 10), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(10, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      table.deleteRecords(inserts.subList(10, 15), true);
      String delete = latestInstant(table);
      assertIcebergMatchesHudi(spark, basePath);

      table.rollback(delete);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(10, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(40, icebergRows(spark, basePath).size());

      table.compact();
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(40, icebergRows(spark, basePath).size());
    }
  }

  @Test
  void deltacommitWhosePublishFailedIsRolledBackAndWrittenAgain() throws IOException {
    try (TestJavaHudiTable table = newTable(null, deletionVectorTable(9))) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(40, true);
      table.upsertRecords(inserts.subList(0, 5), true);
      Map<String, Set<String>> beforeFailure = deletedKeysByDataFileInIceberg(spark, basePath);
      List<String> rowsBeforeFailure = icebergRows(spark, basePath);
      Set<String> deletionVectorFilesBeforeFailure = deletionVectorFiles(basePath);

      // A read-only Iceberg metadata directory fails the publish after Hudi has completed the
      // deltacommit and after its deletion vectors were written.
      Path metadataDirectory = Paths.get(URI.create(basePath)).resolve("metadata");
      Set<PosixFilePermission> writable = Files.getPosixFilePermissions(metadataDirectory);
      Files.setPosixFilePermissions(
          metadataDirectory, PosixFilePermissions.fromString("r-xr-xr-x"));
      assumeTrue(!Files.isWritable(metadataDirectory), "needs a user the permissions bind");
      String update = table.startCommit();
      try {
        assertThrows(
            Exception.class,
            () ->
                table.upsertRecordsWithCommitAlreadyStarted(inserts.subList(3, 13), update, true));
      } finally {
        Files.setPosixFilePermissions(metadataDirectory, writable);
      }
      assertEquals(beforeFailure, deletedKeysByDataFileInIceberg(spark, basePath));
      assertEquals(rowsBeforeFailure, icebergRows(spark, basePath));
      assertEquals(deletionVectorFilesBeforeFailure, deletionVectorFiles(basePath));
      assertIcebergMatchesHudi(spark, basePath);

      table.rollback(update);
      assertNothingPending(reconstructedTimeline(basePath));
      assertEquals(beforeFailure, deletedKeysByDataFileInIceberg(spark, basePath));
      assertIcebergMatchesHudi(spark, basePath);

      table.upsertRecords(inserts.subList(3, 13), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(15, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(40, icebergRows(spark, basePath).size());
    }
  }

  @Test
  void aWriterThatLogsUpdatedRecordsFailsBeforeIcebergMissesThem() {
    Properties withoutTheWriteMode = deletionVectorTable(9);
    withoutTheWriteMode.remove(UPDATES_AS_DELETES_AND_INSERTS);
    String tableName = "mor_" + UUID.randomUUID().toString().replace('-', '_');
    String basePath;
    List<HoodieRecord<HoodieAvroPayload>> inserts;
    List<String> rowsAfterInsert;
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.MERGE_ON_READ, withoutTheWriteMode)) {
      basePath = table.getBasePath();
      inserts = table.insertRecords(30, true);
      assertIcebergMatchesHudi(spark, basePath);
      rowsAfterInsert = icebergRows(spark, basePath);

      // Without the write mode an update is logged as a record, which Iceberg cannot read.
      String update = table.startCommit();
      Exception failure =
          assertThrows(
              Exception.class,
              () ->
                  table.upsertRecordsWithCommitAlreadyStarted(
                      inserts.subList(0, 10), update, true));
      String message = rootCause(failure).getMessage();
      assertTrue(message.contains(UPDATES_AS_DELETES_AND_INSERTS + "=true"), message);
      assertTrue(message.contains("index that supplies record positions"), message);

      // Hudi does not serve the update either, since the instant never completed for readers.
      assertEquals(rowsAfterInsert, icebergRows(spark, basePath));
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(
          Collections.singleton(update),
          reconstructedTimeline(basePath)
              .filterInflightsAndRequested()
              .getInstantsAsStream()
              .map(HoodieInstant::requestedTime)
              .collect(Collectors.toSet()),
          "the deltacommit Iceberg did not record must stay pending");

      table.rollback(update);
      assertNothingPending(reconstructedTimeline(basePath));
      assertEquals(rowsAfterInsert, icebergRows(spark, basePath));
      assertIcebergMatchesHudi(spark, basePath);
    }

    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.MERGE_ON_READ, deletionVectorTable(9))) {
      table.upsertRecords(inserts.subList(0, 10), true);
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(10, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(30, icebergRows(spark, basePath).size());
      assertNothingPending(reconstructedTimeline(basePath));
    }
  }

  @SneakyThrows
  private static boolean hasNativeDeleteFile(String basePath) {
    try (Stream<Path> files = Files.list(Paths.get(URI.create(basePath)))) {
      return files.anyMatch(file -> FSUtils.isNativeDeleteLogFile(file.getFileName().toString()));
    }
  }

  private static Throwable rootCause(Throwable throwable) {
    Throwable cause = throwable;
    while (cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause;
  }
}
