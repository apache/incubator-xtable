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
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.util.Option;

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

  private static HoodieInstant instant(String action, String requestedTime) {
    return new HoodieInstant(
        HoodieInstant.State.COMPLETED,
        action,
        requestedTime,
        requestedTime,
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }
}
