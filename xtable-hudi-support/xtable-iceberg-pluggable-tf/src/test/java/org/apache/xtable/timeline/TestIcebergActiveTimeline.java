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
 
package org.apache.xtable.timeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

class TestIcebergActiveTimeline {

  @Test
  void instantKeySeparatesASavepointFromTheCommitItSavepoints() {
    // Savepointing a commit produces a savepoint instant at that commit's own requested time.
    String sharedRequestedTime = "20260819224951993";
    assertNotEquals(
        IcebergActiveTimeline.instantKey(
            instant(HoodieTimeline.COMMIT_ACTION, sharedRequestedTime)),
        IcebergActiveTimeline.instantKey(
            instant(HoodieTimeline.SAVEPOINT_ACTION, sharedRequestedTime)),
        "keying by requested time alone collides the two and drops one from the timeline");
  }

  @Test
  void instantKeyIgnoresCompletionTimeAndState() {
    HoodieInstant completed =
        new HoodieInstant(
            HoodieInstant.State.COMPLETED,
            HoodieTimeline.COMMIT_ACTION,
            "20260819224951993",
            "20260819224956869",
            InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    HoodieInstant inflight =
        new HoodieInstant(
            HoodieInstant.State.INFLIGHT,
            HoodieTimeline.COMMIT_ACTION,
            "20260819224951993",
            "20260819999999999",
            InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
    assertEquals(
        IcebergActiveTimeline.instantKey(completed),
        IcebergActiveTimeline.instantKey(inflight),
        "the same action at the same requested time is one instant regardless of its state");
  }

  @Test
  void historyOlderThanTheRetainedSnapshotsOnBothClocksIsTrusted() {
    HoodieInstant oldestRetained = completed("20260101000000400", "20260101000000500");
    assertTrue(
        IcebergActiveTimeline.completedBeforeRetainedHistory(
            completed("20260101000000100", "20260101000000200"), Option.of(oldestRetained)));
    // A savepoint snapshot carries the requested time of the commit it keeps.
    assertTrue(
        IcebergActiveTimeline.completedBeforeRetainedHistory(
            completed("20260101000000400", "20260101000000450"), Option.of(oldestRetained)));
  }

  @Test
  void aCommitRequestedAfterTheOldestRetainedInstantIsNotHistory() {
    // Requested after the oldest retained instant but completed before it: a hook that never ran
    // under a concurrent writer, so it stays pending and is rolled back.
    HoodieInstant oldestRetained = completed("20260101000000400", "20260101000000500");
    assertFalse(
        IcebergActiveTimeline.completedBeforeRetainedHistory(
            completed("20260101000000401", "20260101000000450"), Option.of(oldestRetained)));
    assertFalse(
        IcebergActiveTimeline.completedBeforeRetainedHistory(
            completed("20260101000000100", "20260101000000600"), Option.of(oldestRetained)));
    assertFalse(
        IcebergActiveTimeline.completedBeforeRetainedHistory(
            completed("20260101000000100", "20260101000000200"), Option.empty()));
  }

  private static HoodieInstant completed(String requestedTime, String completionTime) {
    return new HoodieInstant(
        HoodieInstant.State.COMPLETED,
        HoodieTimeline.COMMIT_ACTION,
        requestedTime,
        completionTime,
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }

  @Test
  void theOnlyCompletedCommitIsPendingUntilItsSnapshotLands() {
    // Hudi completes the commit before the hook publishes the first snapshot, and a timeline
    // loaded in between, by the embedded timeline server for one, must not treat it as committed.
    List<HoodieInstant> reported =
        IcebergActiveTimeline.instantsBeforeTheFirstSnapshot(
            "t",
            new StoragePath("/tmp/t"),
            Arrays.asList(instant(HoodieTimeline.COMMIT_ACTION, "20260819224951993")),
            instant -> true);
    assertEquals(1, reported.size());
    assertTrue(reported.get(0).isInflight(), "the unrecorded first commit is pending");
    assertEquals("20260819224951993", reported.get(0).requestedTime());
  }

  @Test
  void aRollbackBeforeTheFirstSnapshotStaysCompleted() {
    // The rollback of a first commit whose publish failed has nothing to record in Iceberg, and
    // reporting it pending would make every later write re-run it.
    List<HoodieInstant> reported =
        IcebergActiveTimeline.instantsBeforeTheFirstSnapshot(
            "t",
            new StoragePath("/tmp/t"),
            Arrays.asList(
                instant(HoodieTimeline.ROLLBACK_ACTION, "20260819224951993"),
                new HoodieInstant(
                    HoodieInstant.State.REQUESTED,
                    HoodieTimeline.COMMIT_ACTION,
                    "20260819224952000",
                    null,
                    InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR)),
            instant -> false);
    assertTrue(reported.get(0).isCompleted(), "the rollback keeps its state");
    assertTrue(reported.get(1).isRequested(), "a pending commit keeps its state");
  }

  @Test
  void twoCompletedCommitsWithoutAnIcebergTableMeanTheMetadataIsGone() {
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                IcebergActiveTimeline.instantsBeforeTheFirstSnapshot(
                    "t",
                    new StoragePath("/tmp/t"),
                    Arrays.asList(
                        instant(HoodieTimeline.COMMIT_ACTION, "20260819224951993"),
                        instant(HoodieTimeline.COMMIT_ACTION, "20260819224952000")),
                    instant -> true));
    assertTrue(exception.getMessage().contains("no Iceberg table"), exception.getMessage());
  }

  @Test
  void aCompletedCommitWhoseMarkersAreGoneWithoutAnIcebergTableMeansTheMetadataIsGone() {
    // The writer deletes the markers only after the snapshot is published, so a commit without
    // markers finished its publish and its snapshot has since been removed.
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () ->
                IcebergActiveTimeline.instantsBeforeTheFirstSnapshot(
                    "t",
                    new StoragePath("/tmp/t"),
                    Arrays.asList(instant(HoodieTimeline.COMMIT_ACTION, "20260819224951993")),
                    instant -> false));
    assertTrue(exception.getMessage().contains("markers"), exception.getMessage());
  }

  private static HoodieInstant instant(String action, String requestedTime) {
    return new HoodieInstant(
        HoodieInstant.State.COMPLETED,
        action,
        requestedTime,
        requestedTime,
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }
}
