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

import static org.apache.xtable.IcebergTableAssertions.icebergTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import org.apache.xtable.timeline.IcebergSnapshotInstants;

/**
 * Hudi has no table-format hook for deleting a savepoint, so the snapshot a savepoint recorded
 * outlives the savepoint. Archival must then expire it like any other snapshot whose instant is
 * gone from the active timeline, instead of keeping it and everything newer forever.
 */
class ITIcebergSavepointArchival {
  @TempDir public static Path tempDir;

  @Test
  void deletedSavepointDoesNotPinSnapshots() {
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            "savepoint_archival_" + UUID.randomUUID(),
            tempDir,
            null,
            HoodieTableType.COPY_ON_WRITE)) {
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(10, true);
      String savepointed =
          table.getMetaClient().reloadActiveTimeline().lastInstant().get().requestedTime();
      table.getWriteClient().savepoint(savepointed, "user", "pin the first commit");
      // Upserts rewrite the file group, so the clean that archival depends on has work to do.
      for (int i = 0; i < 5; i++) {
        table.upsertRecords(inserts, true);
      }
      table.clean();

      // While the savepoint stands, Hudi archives nothing after it, so every snapshot stays.
      int snapshotsBefore = snapshotCount(table.getBasePath());
      table.getWriteClient().archive();
      assertEquals(snapshotsBefore, snapshotCount(table.getBasePath()));

      table.getWriteClient().deleteSavepoint(savepointed);
      table.getWriteClient().archive();

      Table icebergTable = icebergTable(table.getBasePath());
      Set<String> activeInstantKeys = activeInstantKeys(table.getMetaClient());
      List<HoodieInstant> retained =
          StreamSupport.stream(icebergTable.snapshots().spliterator(), false)
              .map(
                  snapshot ->
                      IcebergSnapshotInstants.recordedInstant(
                          snapshot, table.getMetaClient().getInstantGenerator()))
              .collect(Collectors.toList());
      assertTrue(snapshotCount(table.getBasePath()) < snapshotsBefore, "archival expired nothing");
      assertFalse(
          retained.stream().anyMatch(i -> HoodieTimeline.SAVEPOINT_ACTION.equals(i.getAction())),
          "the deleted savepoint's snapshot must be expired: " + retained);
      for (HoodieInstant instant : retained) {
        assertTrue(
            activeInstantKeys.contains(instant.requestedTime() + "." + instant.getAction()),
            "a retained snapshot records an instant Hudi has archived: " + instant);
      }
    }
  }

  private static int snapshotCount(String basePath) {
    int count = 0;
    for (Snapshot ignored : icebergTable(basePath).snapshots()) {
      count++;
    }
    return count;
  }

  private static Set<String> activeInstantKeys(HoodieTableMetaClient metaClient) {
    return metaClient
        .reloadActiveTimeline()
        .getInstantsAsStream()
        .map(i -> i.requestedTime() + "." + i.getAction())
        .collect(Collectors.toSet());
  }
}
