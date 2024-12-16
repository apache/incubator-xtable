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
 
package org.apache.xtable.catalog.hms;

import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Properties;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;

import com.google.common.base.Preconditions;
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

@Log4j2
public class HMSCatalogSyncClient implements CatalogSyncClient<Table>, CatalogConversionSource {

  private static final String TEMP_SUFFIX = "_temp";
  private final ExternalCatalogConfig catalogConfig;
  @Getter private final HMSCatalogConfig hmsCatalogConfig;
  @Getter private Configuration configuration;
  @Getter private IMetaStoreClient metaStoreClient;
  @Getter private HMSSchemaExtractor schemaExtractor;
  private final IcebergHMSCatalogSyncHelper icebergHMSCatalogSyncHelper;

  protected HMSCatalogSyncClient(ExternalCatalogConfig catalogConfig, Configuration configuration) {
    this.catalogConfig = catalogConfig;
    this.hmsCatalogConfig = HMSCatalogConfig.of(catalogConfig.getCatalogOptions());
    this.configuration = configuration;
    this.schemaExtractor = HMSSchemaExtractor.getInstance();
    try {
      this.metaStoreClient = new HMSClient(hmsCatalogConfig, configuration).getMSC();
    } catch (MetaException | HiveException e) {
      throw new CatalogSyncException("HiveMetastoreClient could not be created", e);
    }
    this.icebergHMSCatalogSyncHelper = new IcebergHMSCatalogSyncHelper(this);
  }

  protected HMSCatalogSyncClient(
      ExternalCatalogConfig catalogConfig,
      HMSCatalogConfig hmsCatalogConfig,
      Configuration configuration,
      IMetaStoreClient metaStoreClient,
      HMSSchemaExtractor schemaExtractor,
      IcebergHMSCatalogSyncHelper icebergHMSCatalogSyncHelper) {
    this.catalogConfig = catalogConfig;
    this.hmsCatalogConfig = hmsCatalogConfig;
    this.configuration = configuration;
    this.metaStoreClient = metaStoreClient;
    this.schemaExtractor = schemaExtractor;
    this.icebergHMSCatalogSyncHelper = icebergHMSCatalogSyncHelper;
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
    if (table == null || table.getSd() == null) {
      return null;
    }
    return table.getSd().getLocation();
  }

  @Override
  public boolean hasDatabase(String databaseName) {
    try {
      return metaStoreClient.getDatabase(databaseName) != null;
    } catch (NoSuchObjectException e) {
      return false;
    } catch (TException e) {
      throw new CatalogSyncException("Failed to get database: " + databaseName, e);
    }
  }

  @Override
  public void createDatabase(String databaseName) {
    try {
      Database database =
          new Database(
              databaseName,
              "Created by " + this.getClass().getName(),
              null,
              Collections.emptyMap());
      metaStoreClient.createDatabase(database);
    } catch (TException e) {
      throw new CatalogSyncException("Failed to create database: " + databaseName, e);
    }
  }

  @Override
  public Table getTable(CatalogTableIdentifier tableIdentifier) {
    try {
      return metaStoreClient.getTable(
          tableIdentifier.getDatabaseName(), tableIdentifier.getTableName());
    } catch (NoSuchObjectException e) {
      return null;
    } catch (TException e) {
      throw new CatalogSyncException("Failed to get table: " + tableIdentifier.getId(), e);
    }
  }

  @Override
  public void createTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    Table hmsTable;
    switch (table.getTableFormat()) {
      case TableFormat.ICEBERG:
        hmsTable = icebergHMSCatalogSyncHelper.getNewTable(table, tableIdentifier);
        break;
      default:
        throw new NotSupportedException(
            "HMSCatalogSyncClient not supported for " + table.getTableFormat());
    }
    try {
      metaStoreClient.createTable(hmsTable);
    } catch (TException e) {
      throw new CatalogSyncException("Failed to create table: " + tableIdentifier.getId(), e);
    }
  }

  @Override
  public void refreshTable(
      InternalTable table, Table catalogTable, CatalogTableIdentifier tableIdentifier) {
    switch (table.getTableFormat()) {
      case TableFormat.ICEBERG:
        catalogTable = icebergHMSCatalogSyncHelper.getUpdatedTable(table, catalogTable);
        break;
      default:
        throw new NotSupportedException(
            "HMSCatalogSyncClient not supported for " + table.getTableFormat());
    }
    try {
      metaStoreClient.alter_table(
          tableIdentifier.getDatabaseName(), tableIdentifier.getTableName(), catalogTable);
    } catch (TException e) {
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
      metaStoreClient.dropTable(tableIdentifier.getDatabaseName(), tableIdentifier.getTableName());
    } catch (TException e) {
      throw new CatalogSyncException("Failed to drop table: " + tableIdentifier.getId(), e);
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
  public void close() throws Exception {
    if (metaStoreClient != null) {
      metaStoreClient.close();
    }
  }

  @Override
  public SourceTable getSourceTable(CatalogTableIdentifier tableIdentifier) {
    Table table = this.getTable(tableIdentifier);
    Preconditions.checkNotNull(
        table, String.format("table: %s not found", tableIdentifier.getId()));

    String tableFormat = table.getParameters().get(TABLE_TYPE_PROP);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(tableFormat), "TableFormat must not be null or empty");
    Properties tableProperties = new Properties();
    tableProperties.putAll(table.getParameters());
    return SourceTable.builder()
        .name(table.getTableName())
        .basePath(table.getSd().getLocation())
        // TODO: check if this holds true for all the formats
        .dataPath(table.getSd().getLocation())
        .formatName(tableFormat)
        .catalogConfig(catalogConfig)
        .additionalProperties(tableProperties)
        .build();
  }
}
