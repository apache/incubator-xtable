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

import lombok.Builder;
import lombok.Value;

/**
 * Contains change-log information for roles, users, or user groups, enabling traceability of who
 * created or last modified them.
 *
 * <p>This class is useful for governance and compliance scenarios, where an audit trail is
 * necessary. It can be extended to include additional fields such as reasonForChange or
 * changeDescription.
 */
@Value
@Builder
public class InternalChangeLogInfo {
  /** The username or identifier of the entity that created this record. */
  String createdBy;

  /** The username or identifier of the entity that last modified this record. */
  String lastModifiedBy;

  /** The timestamp when this record was created. */
  Instant createdAt;

  /** The timestamp when this record was last modified. */
  Instant lastModifiedAt;
}
