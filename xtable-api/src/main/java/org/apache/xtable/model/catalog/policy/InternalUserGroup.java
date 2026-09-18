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

import java.util.List;

import lombok.Builder;
import lombok.Value;

/**
 * Represents a user group within the catalog.
 *
 * <p>Groups can have multiple roles assigned, and also include audit information to track creation
 * and modifications.
 */
@Value
@Builder
public class InternalUserGroup {
  /** The unique name or identifier for the user group. */
  String name;

  /** The list of roles assigned to this group. */
  List<InternalRole> roles;

  /** Contains information about how and when this group was created and last modified. */
  InternalChangeLogInfo changeLogInfo;
}
