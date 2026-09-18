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
 * Represents a securable object in the catalog, which can be managed by access control.
 *
 * <p>Examples of securable objects include catalogs, schemas, tables, views, or any other data
 * objects that require fine-grained privilege management. Each securable object can have one or
 * more privileges assigned to it.
 */
@Value
@Builder
public class InternalSecurableObject {
  /** The identifier of the securable object. */
  InternalSecurableObjectIdentifier securableObjectIdentifier;
  /**
   * The type of securable object, such as TABLE, VIEW, FUNCTION, etc. Each implementation can
   * define its own set of enums.
   */
  InternalSecurableObjectType securableObjectType;
  /** The set of privileges assigned to this object. */
  List<InternalPrivilege> privileges;
}
