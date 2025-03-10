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
 * Represents a collection of commits that are awaiting processing and syncing w.r.t. a sync
 * instant, say T. These commits include three types of commits,
 *
 * <ol>
 *   <li>the commits that started and completed after T are ready for syncing
 *   <li>the commits that started before and completed before T, but were in-flight in the previous
 *       sync cycle. Since these commits were in flight in the previous sync cycle, they were not
 *       processed in the previous sync cycle. So they need to be synced in the current sync cycle.
 *   <li>the commits that were in-flight at T, i.e. commits that started before T and have not
 *       completed. They need to be tracked and added to the next sync cycle.
 * </ol>
 *
 * Note that the collection is valid for a given instant T only and current timestamp. The goal of
 * the class is to provide commits that are ready for immediate processing and syncing, while also
 * tracking pending commits intended for future incremental syncs. Tracking the pending commits is
 * necessary to avoid missing commits in case of concurrent writers (for e.g. in Hudi) and "slow"
 * commits.
 *
 * <p>{@see InstantsForIncrementalSync} for more details on pending commits.
 *
 * <p>For e.g., assume T1[t1, t4] is a commit that started at t1 and completed at t4.
 *
 * <p>Also, T2[t2,t6], T3[t6, t7] are other commits
 *
 * <p>Now say the first sync starts at current time = t3. Both T1 and T2 are visible but incomplete
 * at t3. So the backlog at t3: commitsToProcess = [], inFlightInstants = [T1, T2].
 *
 * <p>Now assume the next sync starts at t5. So the backlog at t5: commitsToProcess = [T1],
 * inFlightInstants = [T2]. commitsToProcess = [T1] as T1 completed before current time t5, and
 * inFlightInstants = [T2] as T2 started before t5 but did not complete before t5. T3 did not start
 * till t5, so it is not part of the backlog.
 *
 * <p>Now in a next sync cycle at time=t7, and last sync instant is t5, the backlog at t7 =
 * commitsToProcess = [T2, T3]. Although T2 completed before t7, but it was in-flight at previous
 * sync instant, t5. So T2 is part of backlog. If T2 was not tracked explicitly as an in-flight
 * commit, it could get missed resulting in incomplete replication.
 *
 * <p>'commitsToProcess' captures commits that should be processed and synced in the current round.
 * 'inFlightInstants' tracks instants that are pending at the start of the sync process and should
 * be considered for future incremental syncs.
 */
@Value
@Builder
public class CommitsBacklog<COMMIT> {
  /**
   * The commits that are ready for processing and syncing as they have completed before the current
   * sync time. These commits were either in-flight in the previous sync cycle, or started and
   * completed between previous sync and the current sync.
   */
  @Builder.Default List<COMMIT> commitsToProcess = Collections.emptyList();

  /**
   * The instants of commits that were incomplete or pending at a given time, say T. For e.g. the
   * commits that were started but not completed when performing the sync. Tracking these commits is
   * necessary to avoid missing commits in case of concurrent writers in Hudi.
   */
  @Builder.Default List<Instant> inFlightInstants = Collections.emptyList();
}
