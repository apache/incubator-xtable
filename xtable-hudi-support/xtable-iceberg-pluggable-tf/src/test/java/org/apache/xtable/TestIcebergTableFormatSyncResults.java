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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.xtable.exception.UpdateException;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.ErrorDetails;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.model.sync.SyncStatusCode;

/**
 * {@code TableFormatSync} reports a failed sync in its result rather than throwing, and the Hudi
 * instant is already complete when the hook runs, so the hook has to turn anything but a success
 * into a failure of the Hudi operation.
 */
class TestIcebergTableFormatSyncResults {

  @Test
  void successfulSyncPasses() {
    assertDoesNotThrow(
        () ->
            IcebergTableFormat.failUnlessSynced(
                results(
                    SyncResult.builder()
                        .tableFormatSyncStatus(SyncResult.SyncStatus.SUCCESS)
                        .build())));
  }

  @Test
  void failedSyncFailsTheHookWithTheReportedError() {
    SyncResult failed =
        SyncResult.builder()
            .tableFormatSyncStatus(
                SyncResult.SyncStatus.builder()
                    .statusCode(SyncStatusCode.ERROR)
                    .errorDetails(
                        ErrorDetails.builder()
                            .errorMessage("CommitFailedException: metadata location changed")
                            .build())
                    .build())
            .build();
    UpdateException exception =
        assertThrows(
            UpdateException.class, () -> IcebergTableFormat.failUnlessSynced(results(failed)));
    assertTrue(exception.getMessage().contains("CommitFailedException"), exception.getMessage());
  }

  @Test
  void missingResultFailsTheHook() {
    // syncChanges returns nothing for a change it deems not applicable, which leaves the instant
    // unrecorded all the same.
    assertThrows(
        UpdateException.class, () -> IcebergTableFormat.failUnlessSynced(Collections.emptyMap()));
    Map<String, java.util.List<SyncResult>> noIceberg = new HashMap<>();
    noIceberg.put(TableFormat.DELTA, Collections.emptyList());
    assertThrows(UpdateException.class, () -> IcebergTableFormat.failUnlessSynced(noIceberg));
  }

  private static Map<String, java.util.List<SyncResult>> results(SyncResult result) {
    return Collections.singletonMap(TableFormat.ICEBERG, Collections.singletonList(result));
  }
}
