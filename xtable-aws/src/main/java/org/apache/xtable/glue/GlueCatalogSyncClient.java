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
 
package org.apache.xtable.glue;

import static org.apache.xtable.catalog.CatalogUtils.toHierarchicalTableIdentifier;

import java.time.ZonedDateTime;
import java.util.Optional;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.catalog.CatalogPartitionSyncTool;
import org.apache.xtable.catalog.CatalogTableBuilder;
import org.apache.xtable.catalog.CatalogUtils;
import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;
import org.apache.xtable.model.catalog.ThreePartHierarchicalTableIdentifier;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.spi.sync.CatalogSyncClient;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

/** AWS Glue implementation for CatalogSyncClient for registering InternalTable in Glue */
@Log4j2
public class GlueCatalogSyncClient implements CatalogSyncClient<Table> {

  public static final String GLUE_EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";
  private static final String TEMP_SUFFIX = "_temp";

  private ExternalCatalogConfig catalogConfig;
  private GlueClient glueClient;
  private GlueCatalogConfig glueCatalogConfig;
  private Configuration configuration;
  private CatalogTableBuilder<TableInput, Table> tableBuilder;
  private Optional<CatalogPartitionSyncTool> partitionSyncTool;

  // For loading the instance using ServiceLoader
  public GlueCatalogSyncClient() {}

  public GlueCatalogSyncClient(
      ExternalCatalogConfig catalogConfig, String tableFormat, Configuration configuration) {
    _init(catalogConfig, tableFormat, configuration);
  }

  @VisibleForTesting
  GlueCatalogSyncClient(
      ExternalCatalogConfig catalogConfig,
      Configuration configuration,
      GlueCatalogConfig glueCatalogConfig,
      GlueClient glueClient,
      CatalogTableBuilder tableBuilder,
      Optional<CatalogPartitionSyncTool> partitionSyncTool) {
    this.catalogConfig = catalogConfig;
    this.configuration = new Configuration(configuration);
    this.glueCatalogConfig = glueCatalogConfig;
    this.glueClient = glueClient;
    this.tableBuilder = tableBuilder;
    this.partitionSyncTool = partitionSyncTool;
  }

  @Override
  public String getCatalogId() {
    return catalogConfig.getCatalogId();
  }

  @Override
  public String getCatalogType() {
    return CatalogType.GLUE;
  }

  @Override
  public String getStorageLocation(Table table) {
    if (table == null || table.storageDescriptor() == null) {
      return null;
    }
    return table.storageDescriptor().location();
  }

  @Override
  public boolean hasDatabase(CatalogTableIdentifier tableIdentifier) {
    String databaseName = toHierarchicalTableIdentifier(tableIdentifier).getDatabaseName();
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
  public void createDatabase(CatalogTableIdentifier tableIdentifier) {
    String databaseName = toHierarchicalTableIdentifier(tableIdentifier).getDatabaseName();
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
      return GlueCatalogTableUtils.getTable(
          glueClient, glueCatalogConfig.getCatalogId(), tableIdentifier);
    } catch (EntityNotFoundException e) {
      return null;
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to get table: " + tableIdentifier.getId(), e);
    }
  }

  @Override
  public void createTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    HierarchicalTableIdentifier tblIdentifier = toHierarchicalTableIdentifier(tableIdentifier);
    TableInput tableInput = tableBuilder.getCreateTableRequest(table, tableIdentifier);
    try {
      glueClient.createTable(
          CreateTableRequest.builder()
              .catalogId(glueCatalogConfig.getCatalogId())
              .databaseName(tblIdentifier.getDatabaseName())
              .tableInput(tableInput)
              .build());
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to create table: " + tblIdentifier.getId(), e);
    }

    partitionSyncTool.ifPresent(tool -> tool.syncPartitions(table, tableIdentifier));
  }

  @Override
  public void refreshTable(
      InternalTable table, Table catalogTable, CatalogTableIdentifier tableIdentifier) {
    HierarchicalTableIdentifier tblIdentifier = toHierarchicalTableIdentifier(tableIdentifier);
    TableInput tableInput =
        tableBuilder.getUpdateTableRequest(table, catalogTable, tableIdentifier);
    try {
      glueClient.updateTable(
          UpdateTableRequest.builder()
              .catalogId(glueCatalogConfig.getCatalogId())
              .databaseName(tblIdentifier.getDatabaseName())
              .skipArchive(true)
              .tableInput(tableInput)
              .build());
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to refresh table: " + tblIdentifier.getId(), e);
    }

    partitionSyncTool.ifPresent(tool -> tool.syncPartitions(table, tableIdentifier));
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
    HierarchicalTableIdentifier tblIdentifier = toHierarchicalTableIdentifier(tableIdentifier);
    try {
      glueClient.deleteTable(
          DeleteTableRequest.builder()
              .catalogId(glueCatalogConfig.getCatalogId())
              .databaseName(tblIdentifier.getDatabaseName())
              .name(tblIdentifier.getTableName())
              .build());
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to drop table: " + tableIdentifier.getId(), e);
    }
  }

  @Override
  public void init(
      ExternalCatalogConfig catalogConfig, String tableFormat, Configuration configuration) {
    _init(catalogConfig, tableFormat, configuration);
  }

  private void _init(
      ExternalCatalogConfig catalogConfig, String tableFormat, Configuration configuration) {
    this.catalogConfig = catalogConfig;
    this.glueCatalogConfig = GlueCatalogConfig.of(catalogConfig.getCatalogProperties());
    this.glueClient = new DefaultGlueClientFactory(glueCatalogConfig).getGlueClient();
    this.configuration = new Configuration(configuration);
    this.tableBuilder =
        GlueCatalogTableBuilderFactory.getInstance(
            tableFormat, glueCatalogConfig, this.configuration);
    this.partitionSyncTool =
        CatalogUtils.getPartitionSyncTool(
            tableFormat,
            glueCatalogConfig.getPartitionExtractorClass(),
            new GlueCatalogPartitionSyncOperations(glueClient, glueCatalogConfig),
            configuration);
    ;
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
    HierarchicalTableIdentifier tblIdentifier = toHierarchicalTableIdentifier(tableIdentifier);
    String tempTableName =
        tblIdentifier.getTableName() + TEMP_SUFFIX + ZonedDateTime.now().toEpochSecond();
    ThreePartHierarchicalTableIdentifier tempTableIdentifier =
        new ThreePartHierarchicalTableIdentifier(tblIdentifier.getDatabaseName(), tempTableName);
    createTable(table, tempTableIdentifier);
    dropTable(table, tempTableIdentifier);
  }
}
