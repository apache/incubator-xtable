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
 
package org.apache.xtable.model.catalog.policy;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import lombok.Builder;
import lombok.Value;

/** A snapshot of all access control data at a given point in time. */
@Value
@Builder
public class InternalAccessControlPolicySnapshot {
  /**
   * A unique identifier representing this snapshot's version.
   *
   * <p>This could be a UUID, timestamp string, or any value that guarantees uniqueness across
   * snapshots.
   */
  String versionId;

  /**
   * The moment in time when this snapshot was created.
   *
   * <p>Useful for maintaining an audit trail or comparing how policies have changed over time.
   */
  Instant timestamp;

  /**
   * A map of user names to {@link InternalUser} objects, capturing individual users' details such
   * as assigned roles, auditing metadata, etc.
   */
  @Builder.Default Map<String, InternalUser> usersByName = Collections.emptyMap();

  /**
   * A map of group names to {@link InternalUserGroup} objects, representing logical groupings of
   * users for easier role management.
   */
  @Builder.Default Map<String, InternalUserGroup> groupsByName = Collections.emptyMap();

  /**
   * A map of role names to {@link InternalRole} objects, defining the privileges and security rules
   * each role entails.
   */
  @Builder.Default Map<String, InternalRole> rolesByName = Collections.emptyMap();

  /**
   * A map of additional properties or metadata related to this snapshot. This map provides
   * flexibility for storing information without modifying the main schema of the snapshot.
   */
  @Builder.Default Map<String, String> properties = Collections.emptyMap();
}
