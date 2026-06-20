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

/**
 * Represents a hierarchical table identifier, often including catalog, database (or schema), and
 * table names. Some catalogs may omit the catalog name.
 */
public interface HierarchicalTableIdentifier extends CatalogTableIdentifier {
  /**
   * @return the catalog name if present, otherwise null
   */
  String getCatalogName();

  /**
   * @return the database (or schema) name; required
   */
  String getDatabaseName();

  /**
   * @return the table name; required
   */
  String getTableName();
}
