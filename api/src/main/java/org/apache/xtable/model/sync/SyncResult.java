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
 
package org.apache.xtable.model.sync;

import java.time.Duration;
import java.time.Instant;

import lombok.Builder;
import lombok.Value;

/**
 * Result of a sync operation
 *
 * @since 0.1
 */
@Value
@Builder
public class SyncResult {
  // Mode used for the sync
  SyncMode mode;
  Instant lastInstantSynced;
  Instant syncStartTime;
  // Duration
  Duration syncDuration;
  // Status of the sync
  SyncStatus status;
  // The Sync Mode recommended for the next sync (Usually filled on an error)
  SyncMode recommendedSyncMode;

  public enum SyncStatusCode {
    SUCCESS,
    ABORTED,
    ERROR
  }

  /** Represents the status of a Sync operation. */
  @Value
  @Builder
  public static class SyncStatus {
    public static SyncStatus SUCCESS =
        SyncStatus.builder().statusCode(SyncStatusCode.SUCCESS).build();
    // Status code
    SyncStatusCode statusCode;
    // error Message if any
    String errorMessage;
    // Readable description of the error
    String errorDescription;
    // Can the client retry for this type of error (Transient error=true, persistent error=false)
    boolean canRetryOnFailure;
  }
}
