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
import java.util.Optional;

import lombok.Builder;
import lombok.Value;

/** Instants to consider for incremental sync. */
@Value
@Builder
public class InstantsForIncrementalSync {
  Optional<Instant> lastSyncInstant;
  // pending commits that are not yet synced and to be considered for next incremental sync.
  @Builder.Default List<Instant> pendingCommits = Collections.emptyList();
}
