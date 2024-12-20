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

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

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
@Value
@Builder
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

  @Override
  public String toString() {
    return String.format("%s.%s", databaseName, tableName);
  }
}
