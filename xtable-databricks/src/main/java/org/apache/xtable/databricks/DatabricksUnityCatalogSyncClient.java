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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.error.platform.NotFound;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.CreateSchema;
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
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.sync.CatalogSyncClient;

/**
 * Databricks Unity Catalog implementation of {@link CatalogSyncClient}.
 *
 * <p>Supports external Delta table sync operations (create, drop, create-or-replace, get table and
 * schema/database operations) using Databricks SDK APIs and SQL Statement Execution API. Schema
 * refresh compares desired and existing schemas and triggers metadata sync when differences are
 * detected.
 */
@Log4j2
public class DatabricksUnityCatalogSyncClient implements CatalogSyncClient<TableInfo> {
  private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("[a-zA-Z0-9_]+");

  private ExternalCatalogConfig catalogConfig;
  private DatabricksUnityCatalogConfig databricksConfig;
  private Configuration hadoopConf;
  private String tableFormat;
  private WorkspaceClient workspaceClient;
  private StatementExecutionAPI statementExecution;
  private TablesAPI tablesApi;
  private SchemasAPI schemasApi;

  /**
   * For loading the instance using ServiceLoader.
   *
   * <p>This constructor requires {@link #init(ExternalCatalogConfig, String, Configuration)} to be
   * called before invoking catalog operations.
   */
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
    ensureInitialized();
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
    ensureInitialized();
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
    validateIdentifierComponent("catalog", catalog);
    validateIdentifierComponent("schema", schema);

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
    ensureInitialized();
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
    validateIdentifierComponent("catalog", catalog);
    validateIdentifierComponent("schema", schema);

    try {
      schemasApi.create(new CreateSchema().setCatalogName(catalog).setName(schema));
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to create database: " + schema, e);
    }
  }

  @Override
  public TableInfo getTable(CatalogTableIdentifier tableIdentifier) {
    ensureInitialized();
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
    ensureInitialized();
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
    log.info("Databricks UC create table: {}", fullName);
    executeStatement(statement);
  }

  @Override
  public void refreshTable(
      InternalTable table, TableInfo catalogTable, CatalogTableIdentifier tableIdentifier) {
    ensureInitialized();
    ensureDeltaOnly();
    if (catalogTable == null) {
      log.warn(
          "Databricks UC refreshTable called with null catalog table for {}",
          tableIdentifier.getId());
      return;
    }
    InternalSchema schema = table.getReadSchema();
    if (schema == null || schema.getFields() == null || schema.getFields().isEmpty()) {
      log.warn(
          "Databricks UC refreshTable skipped due to missing schema for {}",
          tableIdentifier.getId());
      return;
    }
    if (!schemasMatch(schema, catalogTable)) {
      String fullName = getFullName(tableIdentifier);
      log.info("Databricks UC refresh table metadata (MSCK REPAIR TABLE): {}", fullName);
      executeStatement(String.format("MSCK REPAIR TABLE %s SYNC METADATA", fullName));
    } else {
      log.info(
          "Databricks UC refreshTable: schema already up to date for {}", tableIdentifier.getId());
    }
  }

  @Override
  public void createOrReplaceTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    ensureInitialized();
    ensureDeltaOnly();
    String fullName = getFullName(tableIdentifier);
    dropTable(table, tableIdentifier);
    try {
      createTable(table, tableIdentifier);
    } catch (Exception e) {
      log.error(
          "Databricks UC createOrReplaceTable failed to recreate {} after drop. "
              + "Table was dropped but not recreated.",
          fullName,
          e);
      throw new CatalogSyncException(
          "Databricks UC createOrReplaceTable failed after drop for "
              + fullName
              + ": table was dropped but not recreated",
          e);
    }
  }

  @Override
  public void dropTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    ensureInitialized();
    String fullName = getFullName(tableIdentifier);
    try {
      log.info("Databricks UC drop table: {}", fullName);
      tablesApi.delete(fullName);
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to drop table: " + fullName, e);
    }
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
    if (shouldInitializeWorkspaceClient()) {
      this.workspaceClient = new WorkspaceClient(buildConfig(databricksConfig));
    }
    if (this.statementExecution == null) {
      this.statementExecution = workspaceClient.statementExecution();
    }
    if (this.tablesApi == null) {
      this.tablesApi = this.workspaceClient.tables();
    }
    if (this.schemasApi == null) {
      this.schemasApi = this.workspaceClient.schemas();
    }
    log.info(
        "Initialized Databricks UC sync client for catalogId={} tableFormat={}",
        catalogConfig.getCatalogId(),
        tableFormat);
  }

  private boolean shouldInitializeWorkspaceClient() {
    boolean missingApis =
        this.statementExecution == null || this.tablesApi == null || this.schemasApi == null;
    return this.workspaceClient == null && missingApis;
  }

  private void ensureInitialized() {
    if (this.catalogConfig == null
        || this.databricksConfig == null
        || this.tableFormat == null
        || this.statementExecution == null
        || this.tablesApi == null
        || this.schemasApi == null) {
      throw new CatalogSyncException(
          "DatabricksUnityCatalogSyncClient is not initialized. "
              + "Call init(catalogConfig, tableFormat, configuration) before use.");
    }
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
    String schema = hierarchical.getDatabaseName();
    if (StringUtils.isBlank(schema)) {
      throw new CatalogSyncException("Databricks UC requires a schema name");
    }
    String table = hierarchical.getTableName();
    if (StringUtils.isBlank(table)) {
      throw new CatalogSyncException("Databricks UC requires a table name");
    }

    validateIdentifierComponent("catalog", catalog);
    validateIdentifierComponent("schema", schema);
    validateIdentifierComponent("table", table);
    return hierarchical.getId();
  }

  private static void validateIdentifierComponent(String componentName, String value) {
    if (!IDENTIFIER_PATTERN.matcher(value).matches()) {
      throw new CatalogSyncException(
          String.format(
              "Invalid Databricks UC %s name '%s'. Only [a-zA-Z0-9_]+ is allowed.",
              componentName, value));
    }
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
      String errorMessage = null;
      if (response.getStatus().getError() != null) {
        errorMessage = response.getStatus().getError().getMessage();
      }
      if (StringUtils.isBlank(errorMessage)) {
        throw new CatalogSyncException("Databricks UC statement failed: " + statement);
      }
      throw new CatalogSyncException(
          "Databricks UC statement failed: " + statement + " (" + errorMessage + ")");
    }
    return response;
  }

  private DatabricksConfig buildConfig(DatabricksUnityCatalogConfig config) {
    DatabricksConfig dbConfig = new DatabricksConfig().setHost(config.getHost());
    if (!StringUtils.isBlank(config.getAuthType())) {
      dbConfig.setAuthType(config.getAuthType());
    }
    if (!StringUtils.isBlank(config.getClientId())
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

  private static boolean schemasMatch(InternalSchema desired, TableInfo existing) {
    Map<String, ColumnSignature> desiredColumns = toColumnSignatureMap(desired);
    Map<String, ColumnSignature> existingColumns = toColumnSignatureMap(existing);
    if (desiredColumns.size() != existingColumns.size()) {
      return false;
    }
    for (Map.Entry<String, ColumnSignature> entry : desiredColumns.entrySet()) {
      ColumnSignature existingSignature = existingColumns.get(entry.getKey());
      if (existingSignature == null) {
        return false;
      }
      if (!entry.getValue().equals(existingSignature)) {
        return false;
      }
    }
    return true;
  }

  private static Map<String, ColumnSignature> toColumnSignatureMap(InternalSchema schema) {
    Map<String, ColumnSignature> result = new HashMap<>();
    for (InternalField field : schema.getFields()) {
      String name = normalizeName(field.getName());
      String type = normalizeType(toSparkSqlType(field));
      boolean nullable = field.getSchema().isNullable();
      String comment = StringUtils.defaultString(field.getSchema().getComment());
      result.put(name, new ColumnSignature(type, nullable, comment));
    }
    return result;
  }

  private static Map<String, ColumnSignature> toColumnSignatureMap(TableInfo tableInfo) {
    Map<String, ColumnSignature> result = new HashMap<>();
    if (tableInfo == null || tableInfo.getColumns() == null) {
      return result;
    }
    for (ColumnInfo column : tableInfo.getColumns()) {
      String name = normalizeName(column.getName());
      String type = normalizeType(column.getTypeText());
      boolean nullable = column.getNullable() == null || column.getNullable();
      String comment = StringUtils.defaultString(column.getComment());
      result.put(name, new ColumnSignature(type, nullable, comment));
    }
    return result;
  }

  private static final class ColumnSignature {
    private final String type;
    private final boolean nullable;
    private final String comment;

    private ColumnSignature(String type, boolean nullable, String comment) {
      this.type = type;
      this.nullable = nullable;
      this.comment = comment;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ColumnSignature that = (ColumnSignature) o;
      return nullable == that.nullable
          && Objects.equals(type, that.type)
          && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, nullable, comment);
    }
  }

  private static String normalizeName(String value) {
    return value == null ? "" : value.toLowerCase(Locale.ROOT);
  }

  private static String normalizeType(String value) {
    if (value == null) {
      return "";
    }
    return value.replaceAll("\\s+", "").toLowerCase(Locale.ROOT);
  }

  private static String toSparkSqlType(InternalField field) {
    switch (field.getSchema().getDataType()) {
      case ENUM:
      case STRING:
        return "string";
      case INT:
        return "int";
      case LONG:
        return "bigint";
      case BYTES:
      case FIXED:
      case UUID:
        return "binary";
      case BOOLEAN:
        return "boolean";
      case FLOAT:
        return "float";
      case DATE:
        return "date";
      case TIMESTAMP:
        return "timestamp";
      case TIMESTAMP_NTZ:
        return "timestamp_ntz";
      case DOUBLE:
        return "double";
      case DECIMAL:
        int precision =
            (int) field.getSchema().getMetadata().get(InternalSchema.MetadataKey.DECIMAL_PRECISION);
        int scale =
            (int) field.getSchema().getMetadata().get(InternalSchema.MetadataKey.DECIMAL_SCALE);
        return String.format("decimal(%d,%d)", precision, scale);
      case RECORD:
        return toStructType(field.getSchema());
      case MAP:
        InternalField key =
            field.getSchema().getFields().stream()
                .filter(
                    mapField ->
                        InternalField.Constants.MAP_KEY_FIELD_NAME.equals(mapField.getName()))
                .findFirst()
                .orElseThrow(() -> new CatalogSyncException("Invalid map schema"));
        InternalField value =
            field.getSchema().getFields().stream()
                .filter(
                    mapField ->
                        InternalField.Constants.MAP_VALUE_FIELD_NAME.equals(mapField.getName()))
                .findFirst()
                .orElseThrow(() -> new CatalogSyncException("Invalid map schema"));
        return String.format("map<%s,%s>", toSparkSqlType(key), toSparkSqlType(value));
      case LIST:
        InternalField element =
            field.getSchema().getFields().stream()
                .filter(
                    arrayField ->
                        InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME.equals(
                            arrayField.getName()))
                .findFirst()
                .orElseThrow(() -> new CatalogSyncException("Invalid array schema"));
        return String.format("array<%s>", toSparkSqlType(element));
      default:
        throw new CatalogSyncException("Unsupported type: " + field.getSchema().getDataType());
    }
  }

  private static String toStructType(InternalSchema schema) {
    StringBuilder builder = new StringBuilder("struct<");
    boolean first = true;
    for (InternalField field : schema.getFields()) {
      if (!first) {
        builder.append(",");
      }
      builder.append(field.getName()).append(":").append(toSparkSqlType(field));
      first = false;
    }
    builder.append(">");
    return builder.toString();
  }

  @Override
  public void close() {
    // WorkspaceClient has no explicit close hook; no-op for now.
  }
}
