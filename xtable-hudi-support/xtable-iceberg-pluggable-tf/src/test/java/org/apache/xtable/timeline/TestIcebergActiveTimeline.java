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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;

class TestIcebergActiveTimeline {

  @ParameterizedTest
  @ValueSource(
      strings = {
        HoodieTimeline.COMMIT_ACTION,
        HoodieTimeline.DELTA_COMMIT_ACTION,
        HoodieTimeline.REPLACE_COMMIT_ACTION,
        HoodieTimeline.CLUSTERING_ACTION,
        HoodieTimeline.COMPACTION_ACTION,
        HoodieTimeline.CLEAN_ACTION,
        HoodieTimeline.ROLLBACK_ACTION
      })
  void actionsThatChangeDataNeedAnIcebergSnapshot(String action) {
    assertTrue(
        IcebergActiveTimeline.changesDataFiles(action),
        action + " adds or removes data files, so an Iceberg snapshot has to record it");
  }

  @ParameterizedTest
  @ValueSource(strings = {HoodieTimeline.SAVEPOINT_ACTION, HoodieTimeline.RESTORE_ACTION})
  void actionsThatChangeNoDataAreTakenFromTheHudiTimeline(String action) {
    assertFalse(
        IcebergActiveTimeline.changesDataFiles(action),
        action
            + " changes no data files, so no Iceberg snapshot records it and requiring one would"
            + " report a completed instant as inflight");
  }

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

  private static HoodieInstant instant(String action, String requestedTime) {
    return new HoodieInstant(
        HoodieInstant.State.COMPLETED,
        action,
        requestedTime,
        requestedTime,
        InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
  }
}
