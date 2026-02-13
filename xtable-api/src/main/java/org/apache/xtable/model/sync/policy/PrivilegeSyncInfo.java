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
import org.apache.xtable.model.catalog.policy.InternalPrivilegeType;
import org.apache.xtable.model.sync.ErrorDetails;
import org.apache.xtable.model.sync.SyncStatusCode;

/**
 * Represents the synchronization details of a specific privilege, including its type, status, and
 * any associated errors.
 */
@Value
@Evolving
public class PrivilegeSyncInfo {

  /** The type of privilege (e.g., SELECT, INSERT, DELETE). */
  String privilegeType;

  /** The status of the privilege sync (e.g., SUCCESS, ERROR). */
  SyncStatusCode statusCode;

  /** Details of any errors that occurred during synchronization. */
  ErrorDetails errorDetails;

  public static PrivilegeSyncInfo create(
      InternalPrivilegeType internalPrivilegeType, ErrorDetails errorDetails) {

    SyncStatusCode statusCode =
        errorDetails == null ? SyncStatusCode.SUCCESS : SyncStatusCode.ERROR;
    return new PrivilegeSyncInfo(internalPrivilegeType.name(), statusCode, errorDetails);
  }
}
