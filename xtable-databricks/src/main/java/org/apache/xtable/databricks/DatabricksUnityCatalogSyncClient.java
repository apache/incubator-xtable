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

import java.util.Objects;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.error.platform.NotFound;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.SchemasAPI;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TablesAPI;
import com.databricks.sdk.service.sql.Disposition;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ExecuteStatementRequestOnWaitTimeout;
import com.databricks.sdk.service.sql.Format;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;

import org.apache.xtable.catalog.CatalogUtils;
import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.sync.CatalogSyncClient;

/**
 * Databricks Unity Catalog implementation skeleton for CatalogSyncClient.
 *
 * <p>This is a placeholder to wire Databricks UC as a catalog target via the SQL Statement
 * Execution API. Actual DDL execution and schema diffing should be implemented in a follow-up
 * change.
 */
@Log4j2
public class DatabricksUnityCatalogSyncClient implements CatalogSyncClient<TableInfo> {

  private ExternalCatalogConfig catalogConfig;
  private DatabricksUnityCatalogConfig databricksConfig;
  private Configuration hadoopConf;
  private String tableFormat;
  private WorkspaceClient workspaceClient;
  private StatementExecutionAPI statementExecution;
  private TablesAPI tablesApi;
  private SchemasAPI schemasApi;

  // For loading the instance using ServiceLoader
  public DatabricksUnityCatalogSyncClient() {}

  public DatabricksUnityCatalogSyncClient(
      ExternalCatalogConfig catalogConfig, String tableFormat, Configuration configuration) {
    init(catalogConfig, tableFormat, configuration);
  }

  DatabricksUnityCatalogSyncClient(
      ExternalCatalogConfig catalogConfig,
      String tableFormat,
      Configuration configuration,
      StatementExecutionAPI statementExecution,
      TablesAPI tablesApi,
      SchemasAPI schemasApi) {
    this.statementExecution = statementExecution;
    this.tablesApi = tablesApi;
    this.schemasApi = schemasApi;
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
  public String getStorageLocation(TableInfo table) {
    if (table == null) {
      return null;
    }
    return table.getStorageLocation();
  }

  @Override
  public boolean hasDatabase(CatalogTableIdentifier tableIdentifier) {
    HierarchicalTableIdentifier hierarchical =
        CatalogUtils.toHierarchicalTableIdentifier(tableIdentifier);
    String catalog = hierarchical.getCatalogName();
    if (StringUtils.isBlank(catalog)) {
      throw new CatalogSyncException(
          "Databricks UC requires a catalog name (expected catalog.schema.table)");
    }
    String schema = hierarchical.getDatabaseName();
    if (StringUtils.isBlank(schema)) {
      throw new CatalogSyncException("Databricks UC requires a schema name");
    }

    String fullName = catalog + "." + schema;
    try {
      SchemaInfo schemaInfo = schemasApi.get(fullName);
      return schemaInfo != null;
    } catch (NotFound e) {
      return false;
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to get schema: " + fullName, e);
    }
  }

  @Override
  public void createDatabase(CatalogTableIdentifier tableIdentifier) {
    throw new UnsupportedOperationException("Databricks UC sync not implemented");
  }

  @Override
  public TableInfo getTable(CatalogTableIdentifier tableIdentifier) {
    String fullName = getFullName(tableIdentifier);
    try {
      return tablesApi.get(fullName);
    } catch (NotFound e) {
      return null;
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to get table: " + fullName, e);
    }
  }

  @Override
  public void createTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    ensureDeltaOnly();
    String fullName = getFullName(tableIdentifier);
    String location = table.getBasePath();
    if (StringUtils.isBlank(location)) {
      throw new CatalogSyncException("Storage location is required for external Delta tables");
    }

    String statement =
        String.format(
            "CREATE TABLE IF NOT EXISTS %s USING DELTA LOCATION '%s'",
            fullName, escapeSqlString(location));
    executeStatement(statement);
  }

  @Override
  public void refreshTable(
      InternalTable table, TableInfo catalogTable, CatalogTableIdentifier tableIdentifier) {
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
    if (this.statementExecution == null) {
      this.workspaceClient = new WorkspaceClient(buildConfig(databricksConfig));
      this.statementExecution = workspaceClient.statementExecution();
    }
    if (this.tablesApi == null) {
      if (this.workspaceClient == null) {
        this.workspaceClient = new WorkspaceClient(buildConfig(databricksConfig));
      }
      this.tablesApi = this.workspaceClient.tables();
    }
    if (this.schemasApi == null) {
      if (this.workspaceClient == null) {
        this.workspaceClient = new WorkspaceClient(buildConfig(databricksConfig));
      }
      this.schemasApi = this.workspaceClient.schemas();
    }
    log.info(
        "Initialized Databricks UC sync client for catalogId={} tableFormat={}",
        catalogConfig.getCatalogId(),
        tableFormat);
  }

  private void ensureDeltaOnly() {
    if (!Objects.equals(tableFormat, TableFormat.DELTA)) {
      throw new CatalogSyncException(
          "Databricks UC sync client currently supports external DELTA only");
    }
  }

  private String getFullName(CatalogTableIdentifier tableIdentifier) {
    HierarchicalTableIdentifier hierarchical =
        CatalogUtils.toHierarchicalTableIdentifier(tableIdentifier);
    String catalog = hierarchical.getCatalogName();
    if (StringUtils.isBlank(catalog)) {
      throw new CatalogSyncException(
          "Databricks UC requires a catalog name (expected catalog.schema.table)");
    }
    return hierarchical.getId();
  }

  private StatementResponse executeStatement(String statement) {
    ExecuteStatementRequest request =
        new ExecuteStatementRequest()
            .setStatement(statement)
            .setWarehouseId(databricksConfig.getWarehouseId())
            .setFormat(Format.JSON_ARRAY)
            .setDisposition(Disposition.INLINE)
            .setWaitTimeout("30s")
            .setOnWaitTimeout(ExecuteStatementRequestOnWaitTimeout.CANCEL);

    StatementResponse response = statementExecution.executeStatement(request);
    if (response.getStatus() != null && response.getStatus().getState() == StatementState.FAILED) {
      throw new CatalogSyncException("Databricks UC statement failed: " + statement);
    }
    return response;
  }

  private DatabricksConfig buildConfig(DatabricksUnityCatalogConfig config) {
    DatabricksConfig dbConfig = new DatabricksConfig().setHost(config.getHost());
    if (!StringUtils.isBlank(config.getAuthType())) {
      dbConfig.setAuthType(config.getAuthType());
    }
    if (!StringUtils.isBlank(config.getToken())) {
      dbConfig.setToken(config.getToken());
    } else if (!StringUtils.isBlank(config.getClientId())
        && !StringUtils.isBlank(config.getClientSecret())) {
      dbConfig.setClientId(config.getClientId());
      dbConfig.setClientSecret(config.getClientSecret());
      if (StringUtils.isBlank(config.getAuthType())) {
        dbConfig.setAuthType("oauth-m2m");
      }
    }
    return dbConfig;
  }

  private static String escapeSqlString(String value) {
    return value.replace("'", "''");
  }

  @Override
  public void close() {
    // WorkspaceClient has no explicit close hook; no-op for now.
  }
}
