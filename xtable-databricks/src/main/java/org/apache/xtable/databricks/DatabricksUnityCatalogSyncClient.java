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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

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

    try {
      schemasApi.create(new CreateSchema().setCatalogName(catalog).setName(schema));
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to create database: " + schema, e);
    }
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

    String fullName = getFullName(tableIdentifier);
    Map<String, DesiredColumn> desired = buildDesiredColumns(schema);
    Map<String, ColumnInfo> existing = buildExistingColumns(catalogTable);

    List<String> statements = new ArrayList<>();

    // Add columns
    List<String> addColumnDefs = new ArrayList<>();
    for (DesiredColumn column : desired.values()) {
      if (!existing.containsKey(column.normalizedName)) {
        addColumnDefs.add(formatColumnDefinition(column));
      }
    }
    if (!addColumnDefs.isEmpty()) {
      statements.add(
          String.format(
              "ALTER TABLE %s ADD COLUMNS (%s)", fullName, String.join(", ", addColumnDefs)));
    }

    // Alter columns (type/comment/nullability)
    for (DesiredColumn column : desired.values()) {
      ColumnInfo current = existing.get(column.normalizedName);
      if (current == null) {
        continue;
      }
      String currentType = normalizeType(current.getTypeText());
      String desiredType = normalizeType(column.typeText);
      if (!Objects.equals(currentType, desiredType)) {
        statements.add(
            String.format(
                "ALTER TABLE %s ALTER COLUMN %s TYPE %s",
                fullName, quoteIdentifier(column.name), column.typeText));
      }

      Boolean currentNullable = current.getNullable();
      boolean desiredNullable = column.nullable;
      if (currentNullable != null && currentNullable.booleanValue() != desiredNullable) {
        statements.add(
            String.format(
                "ALTER TABLE %s ALTER COLUMN %s %s",
                fullName,
                quoteIdentifier(column.name),
                desiredNullable ? "DROP NOT NULL" : "SET NOT NULL"));
      }

      String currentComment = current.getComment();
      String desiredComment = column.comment;
      if (!Objects.equals(
          StringUtils.defaultString(currentComment), StringUtils.defaultString(desiredComment))) {
        String commentValue = StringUtils.defaultString(desiredComment);
        statements.add(
            String.format(
                "ALTER TABLE %s ALTER COLUMN %s COMMENT '%s'",
                fullName, quoteIdentifier(column.name), escapeSqlString(commentValue)));
      }
    }

    // Drop columns
    for (ColumnInfo current : existing.values()) {
      String normalized = normalizeName(current.getName());
      if (!desired.containsKey(normalized)) {
        statements.add(
            String.format(
                "ALTER TABLE %s DROP COLUMN %s", fullName, quoteIdentifier(current.getName())));
      }
    }

    if (statements.isEmpty()) {
      log.info("Databricks UC refreshTable: no schema changes for {}", tableIdentifier.getId());
      return;
    }

    for (String statement : statements) {
      executeStatement(statement);
    }
  }

  @Override
  public void createOrReplaceTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    ensureDeltaOnly();
    String fullName = getFullName(tableIdentifier);
    String location = table.getBasePath();
    if (StringUtils.isBlank(location)) {
      throw new CatalogSyncException("Storage location is required for external Delta tables");
    }

    String statement =
        String.format(
            "CREATE OR REPLACE TABLE %s USING DELTA LOCATION '%s'",
            fullName, escapeSqlString(location));
    executeStatement(statement);
  }

  @Override
  public void dropTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    String fullName = getFullName(tableIdentifier);
    try {
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

  private static String quoteIdentifier(String value) {
    return "`" + value.replace("`", "``") + "`";
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
    List<String> fields = new ArrayList<>();
    for (InternalField field : schema.getFields()) {
      fields.add(field.getName() + ":" + toSparkSqlType(field));
    }
    return "struct<" + String.join(",", fields) + ">";
  }

  private static Map<String, DesiredColumn> buildDesiredColumns(InternalSchema schema) {
    Map<String, DesiredColumn> result = new LinkedHashMap<>();
    for (InternalField field : schema.getFields()) {
      String name = field.getName();
      String normalized = normalizeName(name);
      String typeText = toSparkSqlType(field);
      boolean nullable = field.getSchema().isNullable();
      String comment = field.getSchema().getComment();
      result.put(normalized, new DesiredColumn(name, normalized, typeText, nullable, comment));
    }
    return result;
  }

  private static Map<String, ColumnInfo> buildExistingColumns(TableInfo tableInfo) {
    Map<String, ColumnInfo> result = new LinkedHashMap<>();
    if (tableInfo == null || tableInfo.getColumns() == null) {
      return result;
    }
    for (ColumnInfo column : tableInfo.getColumns()) {
      result.put(normalizeName(column.getName()), column);
    }
    return result;
  }

  private static String formatColumnDefinition(DesiredColumn column) {
    StringBuilder builder = new StringBuilder();
    builder.append(quoteIdentifier(column.name)).append(" ").append(column.typeText);
    if (!column.nullable) {
      builder.append(" NOT NULL");
    }
    if (!StringUtils.isBlank(column.comment)) {
      builder.append(" COMMENT '").append(escapeSqlString(column.comment)).append("'");
    }
    return builder.toString();
  }

  private static final class DesiredColumn {
    private final String name;
    private final String normalizedName;
    private final String typeText;
    private final boolean nullable;
    private final String comment;

    private DesiredColumn(
        String name, String normalizedName, String typeText, boolean nullable, String comment) {
      this.name = name;
      this.normalizedName = normalizedName;
      this.typeText = typeText;
      this.nullable = nullable;
      this.comment = comment;
    }
  }

  @Override
  public void close() {
    // WorkspaceClient has no explicit close hook; no-op for now.
  }
}
