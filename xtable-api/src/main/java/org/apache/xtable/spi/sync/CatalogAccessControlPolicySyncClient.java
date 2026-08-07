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
 
package org.apache.xtable.spi.sync;

import org.apache.xtable.annotations.Evolving;
import org.apache.xtable.model.catalog.policy.InternalAccessControlPolicySnapshot;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.model.sync.policy.PolicySyncResult;

/**
 * Defines the contract for synchronizing access control policies between a specific catalog and the
 * internal canonical model.
 *
 * <p>Implementations of this interface are responsible for:
 *
 * <ul>
 *   <li>Fetching the catalog’s native policy definitions and converting them into the canonical
 *       model.
 *   <li>Converting the canonical model back into the catalog’s format and updating the catalog
 *       accordingly.
 * </ul>
 */
@Evolving
public interface CatalogAccessControlPolicySyncClient {
  /**
   * Fetches the current policies from the catalog, converting them into the internal canonical
   * model.
   *
   * <p>This method allows you to pull in the catalog’s native policy definitions (e.g., roles,
   * privileges, user/groups) and map them into a {@link InternalAccessControlPolicySnapshot} so
   * that they can be managed or merged with your centralized policy framework.
   *
   * @return A {@code CatalogAccessControlPolicySnapshot} containing the catalog’s current policies.
   */
  InternalAccessControlPolicySnapshot fetchPolicies();

  /**
   * Pushes the canonical policy snapshot into the target catalog, converting it into the catalog’s
   * native policy definitions and applying any necessary updates.
   *
   * <p>This method typically performs the following steps:
   *
   * <ol>
   *   <li>Transforms the given {@code InternalAccessControlPolicySnapshot} into the catalog’s
   *       native format (roles, privileges, etc.).
   *   <li>Applies the resulting policy definitions to the catalog, potentially overwriting or
   *       merging existing policies.
   *   <li>Returns a {@link SyncResult} detailing the success or failure of the operation.
   * </ol>
   *
   * @param snapshot The access control policy snapshot to be synchronized with the catalog.
   */
  PolicySyncResult pushPolicies(InternalAccessControlPolicySnapshot snapshot);
}
