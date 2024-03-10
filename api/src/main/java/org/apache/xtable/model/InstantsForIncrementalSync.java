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
 
package org.apache.xtable.model;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import lombok.Builder;
import lombok.Value;

/**
 * Instants to consider for incremental sync. Along with lastSyncInstant, pendingCommits are also
 * captured to account for commits that are started earlier than the last sync instant but not yet
 * completed during the last sync.
 *
 * <p>Example Scenario:
 *
 * <p>Suppose we have a table with a history of commits: - t1 (completed) - t2 (in-progress) - t3
 * (completed) - t4 (completed)
 *
 * <p>When performing the first sync, we sync t1, t3, and t4, while tracking t2 as pending.
 *
 * <p>Now, if the table's state has evolved to: - t1 (completed) - t2 (completed) - t3 (completed) -
 * t4 (completed) - t5 (completed) - t6 (completed) - t7 (in-progress)
 *
 * <p>For the next incremental sync, we provide the lastSyncInstant as t4 and pendingCommits as
 * [t2]. This means we'll sync t2, t5, and t6. We do not track t7 because it doesn't come before the
 * lastSyncInstant.
 */
@Value
@Builder
public class InstantsForIncrementalSync {
  Instant lastSyncInstant;
  // pending commits that are not yet synced and to be considered for next incremental sync.
  @Builder.Default List<Instant> pendingCommits = Collections.emptyList();
}
