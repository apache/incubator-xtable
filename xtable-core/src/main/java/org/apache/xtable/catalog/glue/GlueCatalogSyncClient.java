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
 
package org.apache.xtable.catalog.glue;

import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

import java.time.ZonedDateTime;
import java.util.Locale;
import java.util.Properties;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;

import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.extractor.CatalogConversionSource;
import org.apache.xtable.spi.sync.CatalogSyncClient;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

/** AWS Glue implementation for CatalogSyncClient for registering InternalTable in Glue */
@Log4j2
public class GlueCatalogSyncClient implements CatalogSyncClient<Table>, CatalogConversionSource {

  protected static final String GLUE_EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";
  private static final String TEMP_SUFFIX = "_temp";

  private final ExternalCatalogConfig catalogConfig;
  private final GlueClient glueClient;
  private final GlueCatalogConfig glueCatalogConfig;
  @Getter private final Configuration configuration;
  @Getter private final GlueSchemaExtractor schemaExtractor;
  private final IcebergGlueCatalogSyncHelper icebergGlueCatalogSyncHelper;

  public GlueCatalogSyncClient(ExternalCatalogConfig catalogConfig, Configuration configuration) {
    this.catalogConfig = catalogConfig;
    this.glueCatalogConfig = GlueCatalogConfig.of(catalogConfig.getCatalogOptions());
    this.glueClient = new DefaultGlueClientFactory(glueCatalogConfig).getGlueClient();
    this.configuration = new Configuration(configuration);
    this.schemaExtractor = GlueSchemaExtractor.getInstance();
    this.icebergGlueCatalogSyncHelper = new IcebergGlueCatalogSyncHelper(this);
  }

  @VisibleForTesting
  GlueCatalogSyncClient(
      ExternalCatalogConfig catalogConfig,
      Configuration configuration,
      GlueCatalogConfig glueCatalogConfig,
      GlueClient glueClient,
      GlueSchemaExtractor schemaExtractor,
      IcebergGlueCatalogSyncHelper icebergGlueCatalogSyncHelper) {
    this.catalogConfig = catalogConfig;
    this.configuration = new Configuration(configuration);
    this.glueCatalogConfig = glueCatalogConfig;
    this.glueClient = glueClient;
    this.schemaExtractor = schemaExtractor;
    this.icebergGlueCatalogSyncHelper = icebergGlueCatalogSyncHelper;
  }

  @Override
  public String getCatalogName() {
    return catalogConfig.getCatalogName();
  }

  @Override
  public String getCatalogImpl() {
    return this.getClass().getCanonicalName();
  }

  @Override
  public String getStorageDescriptorLocation(Table table) {
    if (table == null || table.storageDescriptor() == null) {
      return null;
    }
    return table.storageDescriptor().location();
  }

  @Override
  public boolean hasDatabase(String databaseName) {
    try {
      return glueClient
              .getDatabase(
                  GetDatabaseRequest.builder()
                      .catalogId(glueCatalogConfig.getCatalogId())
                      .name(databaseName)
                      .build())
              .database()
          != null;
    } catch (EntityNotFoundException e) {
      return false;
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to get database: " + databaseName, e);
    }
  }

  @Override
  public void createDatabase(String databaseName) {
    try {
      glueClient.createDatabase(
          CreateDatabaseRequest.builder()
              .catalogId(glueCatalogConfig.getCatalogId())
              .databaseInput(
                  DatabaseInput.builder()
                      .name(databaseName)
                      .description("Created by " + this.getClass().getName())
                      .build())
              .build());
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to create database: " + databaseName, e);
    }
  }

  @Override
  public Table getTable(CatalogTableIdentifier tableIdentifier) {
    try {
      GetTableResponse response =
          glueClient.getTable(
              GetTableRequest.builder()
                  .catalogId(glueCatalogConfig.getCatalogId())
                  .databaseName(tableIdentifier.getDatabaseName())
                  .name(tableIdentifier.getTableName())
                  .build());
      return response.table();
    } catch (EntityNotFoundException e) {
      return null;
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to get table: " + tableIdentifier.getId(), e);
    }
  }

  @Override
  public void createTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    TableInput tableInput;
    switch (table.getTableFormat()) {
      case TableFormat.ICEBERG:
        tableInput = icebergGlueCatalogSyncHelper.getCreateTableInput(table, tableIdentifier);
        break;
      default:
        throw new NotSupportedException(
            "GlueCatalogSync not supported for " + table.getTableFormat());
    }
    try {
      glueClient.createTable(
          CreateTableRequest.builder()
              .catalogId(glueCatalogConfig.getCatalogId())
              .databaseName(tableIdentifier.getDatabaseName())
              .tableInput(tableInput)
              .build());
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to create table: " + tableIdentifier.getId(), e);
    }
  }

  @Override
  public void refreshTable(
      InternalTable table, Table catalogTable, CatalogTableIdentifier tableIdentifier) {
    TableInput tableInput;
    switch (table.getTableFormat()) {
      case TableFormat.ICEBERG:
        tableInput =
            icebergGlueCatalogSyncHelper.getUpdateTableInput(table, catalogTable, tableIdentifier);
        break;
      default:
        throw new NotSupportedException(
            "GlueCatalogSync not supported for " + table.getTableFormat());
    }
    try {
      glueClient.updateTable(
          UpdateTableRequest.builder()
              .catalogId(glueCatalogConfig.getCatalogId())
              .databaseName(tableIdentifier.getDatabaseName())
              .skipArchive(true)
              .tableInput(tableInput)
              .build());
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to refresh table: " + tableIdentifier.getId(), e);
    }
  }

  @Override
  public void createOrReplaceTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    // validate before dropping the table
    validateTempTableCreation(table, tableIdentifier);
    dropTable(table, tableIdentifier);
    createTable(table, tableIdentifier);
  }

  @Override
  public void dropTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    try {
      glueClient.deleteTable(
          DeleteTableRequest.builder()
              .catalogId(glueCatalogConfig.getCatalogId())
              .databaseName(tableIdentifier.getDatabaseName())
              .name(tableIdentifier.getTableName())
              .build());
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to drop table: " + tableIdentifier.getId(), e);
    }
  }

  @Override
  public void close() throws Exception {
    if (glueClient != null) {
      glueClient.close();
    }
  }

  /**
   * creates a temp table with new metadata and properties to ensure table creation succeeds before
   * dropping the table and recreating it. This ensures that actual table is not dropped in case
   * there are any issues
   */
  private void validateTempTableCreation(
      InternalTable table, CatalogTableIdentifier tableIdentifier) {
    String tempTableName =
        tableIdentifier.getTableName() + TEMP_SUFFIX + ZonedDateTime.now().toEpochSecond();
    CatalogTableIdentifier tempTableIdentifier =
        CatalogTableIdentifier.builder()
            .tableName(tempTableName)
            .databaseName(tableIdentifier.getDatabaseName())
            .build();
    createTable(table, tempTableIdentifier);
    dropTable(table, tempTableIdentifier);
  }

  @Override
  public SourceTable getSourceTable(CatalogTableIdentifier tableIdentifier) {
    Table table = this.getTable(tableIdentifier);
    if (table == null) {
      throw new IllegalStateException(
          String.format("table: %s not found", tableIdentifier.getId()));
    }

    String tableFormat = table.parameters().get(TABLE_TYPE_PROP).toUpperCase(Locale.ENGLISH);
    if (!Strings.isNullOrEmpty(tableFormat)) {
      throw new IllegalStateException("TableFormat must not be null or empty");
    }

    String tableLocation = table.storageDescriptor().location();
    String dataPath;
    switch (tableFormat) {
      case TableFormat.ICEBERG:
        dataPath = icebergGlueCatalogSyncHelper.dataLocation(tableLocation, table.parameters());
        break;
      case TableFormat.HUDI:
        dataPath = tableLocation;
        break;
      default:
        throw new NotSupportedException("Unsupported table format: " + tableFormat);
    }

    Properties tableProperties = new Properties();
    tableProperties.putAll(table.parameters());
    return SourceTable.builder()
        .name(table.name())
        .basePath(tableLocation)
        .dataPath(dataPath)
        .formatName(tableFormat)
        .catalogConfig(catalogConfig)
        .additionalProperties(tableProperties)
        .build();
  }
}
