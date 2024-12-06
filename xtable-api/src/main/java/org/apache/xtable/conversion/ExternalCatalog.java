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
import lombok.NonNull;
import lombok.Value;

import org.apache.xtable.model.catalog.CatalogType;

/**
 * This class represents the configuration for an external catalog. It holds information required to
 * interact with the catalog, such as its identifier, type, table formats to sync and other catalog
 * specific properties related to permissions, catalog uris, access-tokens etc.
 */
@Value
@Builder
public class ExternalCatalog {
  /**
   * An identifier to be used for the catalog if there are multiple catalogs of the same type but in
   * different accounts or regions.
   */
  @NonNull String catalogIdentifier;

  /** The type of catalog. */
  @NonNull CatalogType catalogType;

  /**
   * The table formats that will be synced to this catalog along with their {@link TableIdentifier}.
   * Eg: ICEBERG -> {marketing, price}, HUDI -> {marketing, price_hudi}, DELTA -> {delta_tables,
   * price}
   */
  @NonNull Map<String, TableIdentifier> tableFormatsToSync;

  /**
   * These are properties specific for each catalog, it can be parsed into POJO for the specific
   * catalog eg: GlueCatalogConfig
   */
  @NonNull Map<String, String> catalogProperties;

  /** This class represents the unique identifier for a table in a catalog. */
  @Value
  @Builder
  public static class TableIdentifier {
    /**
     * Catalogs have the ability to group tables logically, databaseName is the identifier for such
     * logical classification. The alternate names for this field include namespace, schemaName etc.
     */
    @NonNull String databaseName;

    /**
     * The table name used when syncing the table to the catalog. NOTE: This name can be different
     * from the table name in storage.
     */
    @NonNull String tableName;

    public String getId() {
      return String.format("%s-%s", databaseName, tableName);
    }
  }
}
