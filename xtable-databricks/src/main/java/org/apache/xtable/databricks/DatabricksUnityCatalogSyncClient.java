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
 
package org.apache.xtable.databricks;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.spi.sync.CatalogSyncClient;

/**
 * Databricks Unity Catalog implementation skeleton for CatalogSyncClient.
 *
 * <p>This is a placeholder to wire Databricks UC as a catalog target via the SQL Statement
 * Execution API. Actual DDL execution and schema diffing should be implemented in a follow-up
 * change.
 */
@Log4j2
public class DatabricksUnityCatalogSyncClient implements CatalogSyncClient<Object> {

  private ExternalCatalogConfig catalogConfig;
  private DatabricksUnityCatalogConfig databricksConfig;
  private Configuration hadoopConf;
  private String tableFormat;

  // For loading the instance using ServiceLoader
  public DatabricksUnityCatalogSyncClient() {}

  public DatabricksUnityCatalogSyncClient(
      ExternalCatalogConfig catalogConfig, String tableFormat, Configuration configuration) {
    init(catalogConfig, tableFormat, configuration);
  }

  @Override
  public String getCatalogId() {
    return catalogConfig.getCatalogId();
  }

  @Override
  public String getCatalogType() {
    return CatalogType.DATABRICKS_UC;
  }

  @Override
  public String getStorageLocation(Object table) {
    throw new UnsupportedOperationException("Databricks UC sync not implemented");
  }

  @Override
  public boolean hasDatabase(CatalogTableIdentifier tableIdentifier) {
    throw new UnsupportedOperationException("Databricks UC sync not implemented");
  }

  @Override
  public void createDatabase(CatalogTableIdentifier tableIdentifier) {
    throw new UnsupportedOperationException("Databricks UC sync not implemented");
  }

  @Override
  public Object getTable(CatalogTableIdentifier tableIdentifier) {
    throw new UnsupportedOperationException("Databricks UC sync not implemented");
  }

  @Override
  public void createTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    throw new UnsupportedOperationException("Databricks UC sync not implemented");
  }

  @Override
  public void refreshTable(
      InternalTable table, Object catalogTable, CatalogTableIdentifier tableIdentifier) {
    throw new UnsupportedOperationException("Databricks UC sync not implemented");
  }

  @Override
  public void createOrReplaceTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    throw new UnsupportedOperationException("Databricks UC sync not implemented");
  }

  @Override
  public void dropTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    throw new UnsupportedOperationException("Databricks UC sync not implemented");
  }

  @Override
  public void init(
      ExternalCatalogConfig catalogConfig, String tableFormat, Configuration configuration) {
    this.catalogConfig = catalogConfig;
    this.tableFormat = tableFormat;
    this.hadoopConf = configuration;
    this.databricksConfig = DatabricksUnityCatalogConfig.from(catalogConfig);

    if (databricksConfig.getHost() == null || databricksConfig.getWarehouseId() == null) {
      throw new CatalogSyncException(
          "Databricks UC catalog requires host and warehouseId in catalogProperties");
    }
    log.info(
        "Initialized Databricks UC sync client for catalogId={} tableFormat={}",
        catalogConfig.getCatalogId(),
        tableFormat);
  }
}
