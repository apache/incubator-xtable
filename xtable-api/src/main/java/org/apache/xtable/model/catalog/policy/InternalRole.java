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
import java.util.Map;

import lombok.Builder;
import lombok.Value;

/**
 * Represents a role within the catalog.
 *
 * <p>A role can be granted access to multiple securable objects, each with its own set of
 * privileges. Audit info is stored to track the role's creation and modifications, and a properties
 * map can hold additional metadata.
 */
@Value
@Builder
public class InternalRole {
  /** The unique name or identifier for the role. */
  String name;

  /** The list of securable objects this role can access. */
  List<InternalSecurableObject> securableObjects;

  /** Contains information about how and when this role was created and last modified. */
  InternalChangeLogInfo changeLogInfo;

  /**
   * A map to store additional metadata or properties related to this role. For example, this might
   * include a description, usage instructions, or any catalog-specific fields.
   */
  Map<String, String> properties;
}
