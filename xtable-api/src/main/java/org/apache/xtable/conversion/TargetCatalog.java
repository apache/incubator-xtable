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

import java.util.Map;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;

import org.apache.xtable.model.catalog.CatalogTableIdentifier;

@EqualsAndHashCode(callSuper = true)
@Getter
public class TargetCatalog extends ExternalCatalog {

  /** The target table that will be synced to {@link TargetCatalog} */
  TargetTable targetTable;

  /**
   * The table formats that will be synced to this catalog along with their {@link
   * CatalogTableIdentifier}. Eg: ICEBERG -> {marketing, price}, HUDI -> {marketing, price_hudi},
   * DELTA -> {delta_tables, price}
   */
  @NonNull Map<String, CatalogTableIdentifier> tableFormatsToSync;

  @Builder(toBuilder = true)
  public TargetCatalog(
      @NonNull String catalogIdentifier,
      @NonNull CatalogConfig catalogConfig,
      @NonNull Map<String, CatalogTableIdentifier> tableFormatsToSync) {
    super(catalogIdentifier, catalogConfig);
    this.tableFormatsToSync = tableFormatsToSync;
  }
}
