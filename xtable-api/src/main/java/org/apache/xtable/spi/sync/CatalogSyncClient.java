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

import org.apache.hadoop.conf.Configuration;

import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;

/**
 * An interface for syncing {@link InternalTable} object to {@link TABLE} object defined by the
 * catalog.
 *
 * @param <TABLE>
 */
public interface CatalogSyncClient<TABLE> extends AutoCloseable {
  /**
   * Returns the user-defined unique identifier for the catalog, allows user to sync table to
   * multiple catalogs of the same name/type eg: HMS catalog with url1, HMS catalog with url2.
   */
  String getCatalogId();

  /** Returns the {@link org.apache.xtable.model.storage.CatalogType} the client syncs to */
  String getCatalogType();

  /** Returns the storage location of the table synced to the catalog. */
  String getStorageLocation(TABLE table);

  /** Checks whether a database used by tableIdentifier exists in the catalog. */
  boolean hasDatabase(CatalogTableIdentifier tableIdentifier);

  /** Creates a database used by tableIdentifier in the catalog. */
  void createDatabase(CatalogTableIdentifier tableIdentifier);

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

  /** Initializes the client with provided configuration */
  void init(ExternalCatalogConfig catalogConfig, String tableFormat, Configuration configuration);
}
