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

import lombok.Builder;
import lombok.Value;

/**
 * Represents a single privilege assignment for a securable object.
 *
 * <p>This defines the kind of operation (e.g., SELECT, CREATE, MODIFY) and whether it is allowed or
 * denied. Some catalogs may only accept ALLOW rules and treat all other operations as denied by
 * default.
 */
@Value
@Builder
public class InternalPrivilege {
  /**
   * The type of privilege, such as SELECT, CREATE, or MODIFY. Each implementation can define its
   * own set of enums.
   */
  InternalPrivilegeType privilegeType;

  /**
   * The decision, typically ALLOW or DENY. Some catalogs may not support DENY explicitly,
   * defaulting to ALLOW.
   */
  String privilegeDecision;
}
