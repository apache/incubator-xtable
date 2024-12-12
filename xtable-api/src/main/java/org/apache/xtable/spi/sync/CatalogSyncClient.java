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

import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;

public interface CatalogSyncClient<TABLE> extends AutoCloseable {
  /**
   * Returns a unique identifier for the catalog, allows user to sync table to multiple catalogs of
   * the same type eg: HMS catalogs with url1, HMS catalog with url2.
   */
  String getCatalogId();

  /**
   * Returns the full class name which implements the interface for CatalogSyncClient.
   *
   * @return catalogImplClassName
   */
  String getCatalogImpl();

  /**
   * Returns the table format for the table being synced to catalog.
   *
   * @return tableFormat
   */
  String getTableFormat();

  /** Returns the unique identifier for the table being synced to catalog. */
  CatalogTableIdentifier getTableIdentifier();

  /** Returns the storage location of the table synced to the catalog. */
  String getStorageDescriptorLocation(TABLE table);

  /** Checks whether a database exists in the catalog. */
  boolean hasDatabase(String databaseName);

  /** Creates a database in the catalog. */
  void createDatabase(String databaseName);

  /**
   * Return the TABLE object used by the catalog implementation. Eg: HMS uses
   * org.apache.hadoop.hive.metastore.api.Table, Glue uses
   * software.amazon.awssdk.services.glue.model.Table etc.
   */
  TABLE getTable(CatalogTableIdentifier tableIdentifier);

  /**
   * Create a table in the catalog using the canonical InternalTable representation and
   * tableIdentifier.
   */
  void createTable(InternalTable table, CatalogTableIdentifier tableIdentifier);

  /** Refreshes the table metadata in the catalog with tableIdentifier. */
  void refreshTable(
      InternalTable table, TABLE catalogTable, CatalogTableIdentifier tableIdentifier);

  /**
   * Tries to re-create the table in the catalog replacing state with the new canonical
   * InternalTable representation and tableIdentifier.
   */
  void createOrReplaceTable(InternalTable table, CatalogTableIdentifier tableIdentifier);

  /** Drops a table from the catalog. */
  void dropTable(InternalTable table, CatalogTableIdentifier tableIdentifier);
}
