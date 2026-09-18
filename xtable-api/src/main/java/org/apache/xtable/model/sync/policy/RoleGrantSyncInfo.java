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

import lombok.Value;

import org.apache.xtable.annotations.Evolving;
import org.apache.xtable.model.sync.ErrorDetails;
import org.apache.xtable.model.sync.SyncStatusCode;

/** Represents the synchronization result of a role grant operation, */
@Value
@Evolving
public class RoleGrantSyncInfo {

  /** The name of the role being granted. */
  String roleName;

  /** The status of the role grant synchronization (e.g., SUCCESS, ERROR). */
  SyncStatusCode status;

  /** Details of any errors that occurred during the synchronization. */
  ErrorDetails errorDetails;

  /**
   * Builds a {@link RoleGrantSyncInfo} object for a given role.
   *
   * @param roleName The name of the role.
   * @param errorDetails exception details if an error occurred, otherwise null.
   * @return A RoleGrantSyncInfo object.
   */
  public static RoleGrantSyncInfo create(String roleName, ErrorDetails errorDetails) {
    SyncStatusCode statusCode =
        errorDetails == null ? SyncStatusCode.SUCCESS : SyncStatusCode.ERROR;
    return new RoleGrantSyncInfo(roleName, statusCode, errorDetails);
  }
}
