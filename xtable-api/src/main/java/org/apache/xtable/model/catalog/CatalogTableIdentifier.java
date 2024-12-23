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
 
package org.apache.xtable.model.catalog;

import lombok.Getter;
import lombok.NonNull;

/**
 * An internal representation of a fully qualified table identifier within a catalog. The naming
 * convention varies across different catalogs but many of them follow a three level hierarchy, few
 * examples can be found below.
 *
 * <ul>
 *   <li>1. catalog.database.table
 *   <li>2. catalog.schema.table
 *   <li>3. database.schema.table
 * </ul>
 *
 * We have selected the first naming convention and will interoperate among other catalogs following
 * a different convention.
 */
@Getter
public class CatalogTableIdentifier {
  /**
   * The top level hierarchy/namespace for organizing tables. Each catalog can have multiple
   * databases/schemas. This is an optional field as many catalogs have a "default" catalog whose
   * name varies depending on the catalogType.
   */
  String catalogName;
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

  public CatalogTableIdentifier(
      String catalogName, @NonNull String databaseName, @NonNull String tableName) {
    this.catalogName = catalogName;
    this.databaseName = databaseName;
    this.tableName = tableName;
  }

  // 2. Use constructor chaining for the two-argument constructor
  public CatalogTableIdentifier(String databaseName, String tableName) {
    this(null, databaseName, tableName); // calls the above constructor with catalogName = null
  }

  /**
   * Constructs a new {@code CatalogTableIdentifier} from a hierarchical string identifier.
   *
   * <p>The identifier is expected to be in one of the following formats:
   *
   * <ul>
   *   <li>{@code database.table} (two parts, no catalog)
   *   <li>{@code catalog.database.table} (three parts)
   * </ul>
   *
   * If the identifier does not match one of these formats, an {@link IllegalArgumentException} is
   * thrown.
   *
   * @param hierarchicalTableIdentifier The hierarchical string identifier (e.g.,
   *     "myCatalog.myDatabase.myTable" or "myDatabase.myTable").
   * @throws IllegalArgumentException If the provided string does not match a valid two-part or
   *     three-part identifier.
   */
  public CatalogTableIdentifier(String hierarchicalTableIdentifier) {
    String[] parts = hierarchicalTableIdentifier.split("\\.");
    if (parts.length == 2) {
      this.catalogName = null;
      this.databaseName = parts[0];
      this.tableName = parts[1];
    } else if (parts.length == 3) {
      this.catalogName = parts[0];
      this.databaseName = parts[1];
      this.tableName = parts[2];
    } else {
      throw new IllegalArgumentException("Invalid table identifier " + hierarchicalTableIdentifier);
    }
  }

  @Override
  public String toString() {
    if (catalogName != null && !catalogName.isEmpty()) {
      return catalogName + "." + databaseName + "." + tableName;
    }
    return databaseName + "." + tableName;
  }
}
