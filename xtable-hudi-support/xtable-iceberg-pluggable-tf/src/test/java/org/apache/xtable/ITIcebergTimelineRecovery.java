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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.storage.StoragePath;

import org.apache.iceberg.Table;

/**
 * Hudi completes an instant before the Iceberg hook runs, so the two can diverge when a writer dies
 * in between, and Iceberg history can be shortened by an expire-snapshots run. The table has to
 * recover from each without losing committed data or wedging the next write.
 */
class ITIcebergTimelineRecovery {
  @TempDir public static Path tempDir;

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
  void firstCommitWhosePublishFailedIsRolledBackBeforeTheIcebergTableExists() throws IOException {
    try (TestJavaHudiTable table = newTable()) {
      Path tableDirectory = Paths.get(URI.create(table.getBasePath()));
      // A file where Iceberg wants its metadata directory makes the first publish fail after Hudi
      // has completed the commit, which leaves the commit's markers in place.
      Files.createFile(tableDirectory.resolve("metadata"));
      String commit1 = table.startCommit();
      assertThrows(
          Exception.class,
          () ->
              table.insertRecordsWithCommitAlreadyStarted(
                  table.generateRecords(100), commit1, true));
      Files.delete(tableDirectory.resolve("metadata"));
      // Hudi completed the commit natively and, with postCommit never reached, kept its markers.
      try (Stream<Path> instants =
          Files.list(tableDirectory.resolve(".hoodie").resolve("timeline"))) {
        assertTrue(
            instants
                .map(path -> path.getFileName().toString())
                .anyMatch(name -> name.startsWith(commit1 + "_") && name.endsWith(".commit")),
            "the failed publish must leave a natively completed commit behind");
      }
      assertTrue(
          Files.isDirectory(tableDirectory.resolve(".hoodie").resolve(".temp").resolve(commit1)),
          "the failed publish must leave the commit's markers behind");

      HoodieTimeline before = reconstructedTimeline(table.getBasePath());
      assertEquals(Collections.emptyList(), completedCommitTimes(before));
      assertTrue(
          before
              .filterInflightsAndRequested()
              .getInstantsAsStream()
              .anyMatch(instant -> commit1.equals(instant.requestedTime())),
          "the first commit without a snapshot must be reported as pending");

      // The rollback runs while there is still no Iceberg table to record it in.
      table.getWriteClient().rollback(commit1);
      HoodieTimeline rolledBack = reconstructedTimeline(table.getBasePath());
      assertNothingPending(rolledBack);
      assertEquals(Collections.emptyList(), completedCommitTimes(rolledBack));
      assertEquals(
          1,
          rolledBack.getRollbackTimeline().filterCompletedInstants().countInstants(),
          "the rollback stays completed with no Iceberg table to record it");
      try (Stream<Path> files = Files.list(tableDirectory)) {
        assertEquals(
            Collections.emptyList(),
            files
                .map(Path::toString)
                .filter(name -> name.endsWith(".parquet"))
                .collect(Collectors.toList()),
            "the rollback removes the data files of the failed first publish");
      }

      String commit2 = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(50), commit2, true);
      HoodieTimeline after = reconstructedTimeline(table.getBasePath());
      assertEquals(Collections.singletonList(commit2), completedCommitTimes(after));
      assertNothingPending(after);
      assertIcebergReferencesExactly(table.getBasePath(), table.getAllLatestBaseFilePaths());
      assertEquals(50, icebergRowCount(table.getBasePath()));
    }
  }

  @Test
  void crashedFirstCommitStaysPendingOnceALaterWriteCreatesTheIcebergTable() throws IOException {
    try (TestJavaHudiTable table = newTable()) {
      Path tableDirectory = Paths.get(URI.create(table.getBasePath()));
      Files.createFile(tableDirectory.resolve("metadata"));
      String commit1 = table.startCommit();
      assertThrows(
          Exception.class,
          () ->
              table.insertRecordsWithCommitAlreadyStarted(
                  table.generateRecords(100), commit1, true));
      Files.delete(tableDirectory.resolve("metadata"));

      // Under lazy failed-write cleaning the next write does not roll the crashed commit back
      // first; its snapshot is the first one, and the crashed commit predates it on both clocks.
      // Its markers tell it apart from a commit whose snapshot was expired: a publish that failed
      // never reached postCommit, which is what deletes them.
      String commit2 = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(50), commit2, true);
      HoodieTimeline after = reconstructedTimeline(table.getBasePath());
      assertEquals(Collections.singletonList(commit2), completedCommitTimes(after));
      assertTrue(
          after
              .filterInflightsAndRequested()
              .getInstantsAsStream()
              .anyMatch(instant -> commit1.equals(instant.requestedTime())),
          "the crashed commit must stay pending, not become expired history");
      assertEquals(50, icebergRowCount(table.getBasePath()));

      table.getWriteClient().rollback(commit1);
      assertNothingPending(reconstructedTimeline(table.getBasePath()));
      assertIcebergReferencesExactly(table.getBasePath(), table.getAllLatestBaseFilePaths());
      assertEquals(50, icebergRowCount(table.getBasePath()));
    }
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
      assertEquals(170, icebergRowCount(table.getBasePath()));
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
      assertEquals(160, icebergRowCount(table.getBasePath()));
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
      assertEquals(50, icebergRowCount(table.getBasePath()));
      assertNothingPending(reconstructedTimeline(table.getBasePath()));
    }
  }

  @Test
  void expiredCommitThatKeptItsMarkersStaysHistory() throws IOException {
    try (TestJavaHudiTable table = newTable()) {
      List<String> commits = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        String commit = table.startCommit();
        table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(10), commit, true);
        commits.add(commit);
      }
      // The second commit published its snapshot but kept its markers, as when deleting them
      // failed or the writer stopped before postCommit.
      table
          .getMetaClient()
          .getStorage()
          .createDirectory(
              new StoragePath(table.getMetaClient().getMarkerFolderPath(commits.get(1))));
      icebergTable(table.getBasePath())
          .expireSnapshots()
          .expireOlderThan(System.currentTimeMillis())
          .retainLast(1)
          .commit();

      HoodieTimeline timeline = reconstructedTimeline(table.getBasePath());
      assertEquals(commits, completedCommitTimes(timeline));
      assertNothingPending(timeline);

      // Reported pending, the commit would be rolled back by the next clean, which deletes its
      // files while Iceberg keeps referencing them.
      table.insertRecords(10, true);
      table.getWriteClient().clean();
      assertIcebergReferencesExactly(table.getBasePath(), table.getAllLatestBaseFilePaths());
      assertEquals(40, icebergRowCount(table.getBasePath()));
    }
  }

  @Test
  void rollbackOfACommitWhoseSnapshotWasExpiredIsRefused() {
    try (TestJavaHudiTable table = newTable()) {
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(40, true);
      table.upsertRecords(inserts.subList(0, 10), true);
      table.upsertRecords(inserts.subList(0, 10), true);
      String upsertCommit =
          table.getMetaClient().reloadActiveTimeline().lastInstant().get().requestedTime();
      // The clean's snapshot is the one the expiry keeps, so the latest commit loses its own.
      table.clean();
      List<String> filesBeforeRollback = table.getAllLatestBaseFilePaths();
      icebergTable(table.getBasePath())
          .expireSnapshots()
          .expireOlderThan(System.currentTimeMillis())
          .retainLast(1)
          .commit();
      HoodieTimeline expired = reconstructedTimeline(table.getBasePath());
      assertNothingPending(expired);
      assertTrue(completedCommitTimes(expired).contains(upsertCommit));

      // Hudi still permits rolling back its latest commit, but with the snapshot gone Iceberg no
      // longer knows which files the commit added or superseded, so the rollback is refused before
      // Hudi deletes anything and before a rollback instant is left pending.
      assertThrows(Exception.class, () -> table.rollback(upsertCommit));

      assertEquals(filesBeforeRollback, table.getAllLatestBaseFilePaths());
      assertIcebergReferencesExactly(table.getBasePath(), filesBeforeRollback);
      assertEquals(40, icebergRowCount(table.getBasePath()));
      HoodieTimeline timeline = reconstructedTimeline(table.getBasePath());
      assertNothingPending(timeline);
      assertTrue(timeline.getRollbackTimeline().empty(), "no rollback instant may be left behind");
      table.insertRecords(10, true);
      assertIcebergReferencesExactly(table.getBasePath(), table.getAllLatestBaseFilePaths());
      assertEquals(50, icebergRowCount(table.getBasePath()));
    }
  }

  @Test
  void clusteringWhosePublishFailedIsRolledBackByTheNextWriter() throws IOException {
    try (TestJavaHudiTable table = newTable()) {
      table.insertRecords(100, true);
      table.insertRecords(100, true);
      List<String> filesBeforeClustering = table.getAllLatestBaseFilePaths();
      // A read-only Iceberg metadata directory fails the publish after Hudi has completed the
      // clustering, so the completed replacecommit has no snapshot.
      Path metadataDirectory = Paths.get(URI.create(table.getBasePath())).resolve("metadata");
      Set<PosixFilePermission> writable = Files.getPosixFilePermissions(metadataDirectory);
      Files.setPosixFilePermissions(
          metadataDirectory, PosixFilePermissions.fromString("r-xr-xr-x"));
      assumeTrue(!Files.isWritable(metadataDirectory), "needs a user the permissions bind");
      try {
        assertThrows(Exception.class, table::cluster);
      } finally {
        Files.setPosixFilePermissions(metadataDirectory, writable);
      }

      // The pending instant has to be reported with the clustering action, since the only
      // requested file on disk is the clustering plan, which Hudi reads on every write and on
      // every file system view build.
      HoodieTimeline before = reconstructedTimeline(table.getBasePath());
      assertEquals(
          1,
          before.filterPendingClusteringTimeline().countInstants(),
          before.getInstants().toString());
      String clustering =
          before.filterPendingClusteringTimeline().firstInstant().get().requestedTime();
      assertEquals(200, icebergRowCount(table.getBasePath()));

      table.insertRecords(10, true);
      table.getWriteClient().rollback(clustering);

      // As for a native table, the rollback deletes the clustered files and leaves the clustering
      // plan pending for the next clustering run; the replaced file groups are live again in both.
      HoodieTimeline after = reconstructedTimeline(table.getBasePath());
      assertEquals(1, after.filterPendingClusteringTimeline().countInstants());
      assertEquals(1, after.filterInflightsAndRequested().countInstants());
      assertIcebergReferencesExactly(table.getBasePath(), table.getAllLatestBaseFilePaths());
      assertTrue(table.getAllLatestBaseFilePaths().containsAll(filesBeforeClustering));
      assertEquals(210, icebergRowCount(table.getBasePath()));
    }
  }
}
