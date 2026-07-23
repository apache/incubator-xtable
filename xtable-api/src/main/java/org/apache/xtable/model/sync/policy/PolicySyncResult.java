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
 
package org.apache.xtable.model.sync.policy;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.Value;

import org.apache.xtable.annotations.Evolving;
import org.apache.xtable.model.sync.ErrorDetails;
import org.apache.xtable.model.sync.SyncStatusCode;

/**
 * Represents the result of a policy sync between catalogs, capturing details of roles, users, and
 * user groups.
 */
@Value
@Builder
@Evolving
public class PolicySyncResult {

  /** List of role sync results. */
  @Builder.Default List<RoleSyncResult> rolesSyncResult = new ArrayList<>();

  /** List of user sync results. */
  @Builder.Default List<UserSyncResult> usersSyncResult = new ArrayList<>();

  /** List of user group sync results. */
  @Builder.Default List<UserGroupSyncResult> userGroupsSyncResult = new ArrayList<>();

  /** The start time of the policy sync. */
  Instant startTime;

  /** The duration of the policy sync. */
  Duration duration;

  /** Represents the sync result for a specific role, including privileges and status. */
  @Value
  public static class RoleSyncResult {
    /** The name of the role. */
    String roleName;

    /** Mapping of securable objects to their privilege sync info details. */
    Map<SecurableObjectInfo, Collection<PrivilegeSyncInfo>> privilegeSyncInfos;

    /** The status of the role sync (e.g., SUCCESS or ERROR). */
    SyncStatusCode statusCode;

    /** Details of any errors that occurred during synchronization. */
    ErrorDetails errorDetails;

    /**
     * Builds a {@link PolicySyncResult.RoleSyncResult} for a given role.
     *
     * @param roleName The name of the role.
     * @param privilegeSyncInfos A map of privileges associated with the role.
     * @param errorDetails exception details if an error occurred, otherwise null.
     * @return A RoleSyncResult object containing the synchronization details.
     */
    public static PolicySyncResult.RoleSyncResult create(
        String roleName,
        Map<SecurableObjectInfo, Collection<PrivilegeSyncInfo>> privilegeSyncInfos,
        ErrorDetails errorDetails) {

      SyncStatusCode statusCode =
          errorDetails == null ? SyncStatusCode.SUCCESS : SyncStatusCode.ERROR;
      return new RoleSyncResult(roleName, privilegeSyncInfos, statusCode, errorDetails);
    }
  }

  /** Represents the sync result for a specific user. */
  @Value
  public static class UserSyncResult {
    /** The name of the user. */
    String userName;

    /** List of role grant synchronization details. */
    List<RoleGrantSyncInfo> roleGrantSyncInfos;

    /** The status of the user synchronization. */
    SyncStatusCode statusCode;

    /** Details of any errors that occurred during synchronization. */
    ErrorDetails errorDetails;

    /**
     * Builds a {@link PolicySyncResult.UserSyncResult} for a given user.
     *
     * @param userName The name of the user.
     * @param roleGrantSyncInfos List of role grant synchronization information.
     * @param errorDetails exception details if an error occurred, otherwise null.
     * @return A UserSyncResult object containing the synchronization details.
     */
    public static PolicySyncResult.UserSyncResult create(
        String userName, List<RoleGrantSyncInfo> roleGrantSyncInfos, ErrorDetails errorDetails) {

      SyncStatusCode statusCode =
          errorDetails == null ? SyncStatusCode.SUCCESS : SyncStatusCode.ERROR;
      return new UserSyncResult(userName, roleGrantSyncInfos, statusCode, errorDetails);
    }
  }

  /** Represents the synchronization result for a user group. */
  @Value
  public static class UserGroupSyncResult {
    /** The name of the user group. */
    String userGroupName;

    /** List of role grant sync details. */
    List<RoleGrantSyncInfo> roleGrantSyncInfos;

    /** The status of the user group synchronization. */
    SyncStatusCode statusCode;

    /** Details of any errors that occurred during synchronization. */
    ErrorDetails errorDetails;

    /**
     * Builds a {@link PolicySyncResult.UserGroupSyncResult} for a given user group.
     *
     * @param groupName The name of the user group.
     * @param roleGrantSyncInfos List of role grant synchronization information.
     * @param errorDetails exception details if an error occurred, otherwise null.
     * @return A UserGroupSyncResult object containing the synchronization details.
     */
    public static PolicySyncResult.UserGroupSyncResult create(
        String groupName, List<RoleGrantSyncInfo> roleGrantSyncInfos, ErrorDetails errorDetails) {

      SyncStatusCode statusCode =
          errorDetails == null ? SyncStatusCode.SUCCESS : SyncStatusCode.ERROR;
      return new UserGroupSyncResult(groupName, roleGrantSyncInfos, statusCode, errorDetails);
    }
  }
}
