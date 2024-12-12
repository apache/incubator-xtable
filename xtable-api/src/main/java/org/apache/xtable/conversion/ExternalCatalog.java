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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

/**
 * Defines a reference to an external catalog, used for conversion between source and target
 * catalogs.
 */
@Getter
@EqualsAndHashCode
class ExternalCatalog {
  /**
   * An identifier to be used for the catalog if there are multiple catalogs of the same type but in
   * different accounts or regions.
   */
  @NonNull String catalogId;

  /** Configuration of the catalog - catalogImpl, catalogName and properties. */
  @NonNull CatalogConfig catalogConfig;

  public ExternalCatalog(@NonNull String catalogId, @NonNull CatalogConfig catalogConfig) {
    this.catalogId = catalogId;
    this.catalogConfig = catalogConfig;
  }
}
