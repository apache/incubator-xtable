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
 
package io.onetable.model;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import lombok.Builder;
import lombok.Value;

/**
 * Represents the current state of commits that are ready for immediate processing and syncing,
 * while also tracking pending commits intended for future incremental syncs.
 *
 * <p>'commitsToProcess' captures commits that should be processed and synced in the current round.
 * 'inFlightInstants' tracks instants that are pending at the start of the sync process and should
 * be considered for future incremental syncs.
 */
@Value
@Builder
public class CurrentCommitState<COMMIT> {
  @Builder.Default List<COMMIT> commitsToProcess = Collections.emptyList();

  /**
   * The instants of commits that were incomplete or pending at a given time. For e.g. the commits
   * that were started but not completed when performing the sync. Tracking these commits is
   * necessary to avoid missing commits in case of concurrent writers in Hudi.
   */
  @Builder.Default List<Instant> inFlightInstants = Collections.emptyList();
}
