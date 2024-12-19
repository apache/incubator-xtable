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
 
package org.apache.xtable.conversion;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import org.apache.xtable.model.catalog.CatalogTableIdentifier;

/**
 * TargetCatalogConfig contains the parameters that are required when syncing {@link TargetTable} to
 * a catalog.
 */
@Value
@Builder
public class TargetCatalogConfig {
  /**
   * The tableIdentifiers(catalogName.databaseName.tableName) that will be used when syncing {@link
   * TargetTable} to the catalog.
   */
  @NonNull CatalogTableIdentifier catalogTableIdentifier;

  /** Configuration for the catalog. */
  @NonNull ExternalCatalogConfig catalogConfig;
}
