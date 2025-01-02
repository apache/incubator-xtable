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
 
package org.apache.xtable.hms;

import static org.apache.xtable.catalog.CatalogUtils.castToHierarchicalTableIdentifier;

import java.time.ZonedDateTime;
import java.util.Collections;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.catalog.CatalogTableBuilder;
import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;
import org.apache.xtable.model.catalog.ThreePartHierarchicalTableIdentifier;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.spi.sync.CatalogSyncClient;

@Log4j2
public class HMSCatalogSyncClient implements CatalogSyncClient<Table> {

  private static final String TEMP_SUFFIX = "_temp";
  private final ExternalCatalogConfig catalogConfig;
  private final HMSCatalogConfig hmsCatalogConfig;
  private final Configuration configuration;
  private final IMetaStoreClient metaStoreClient;
  private final CatalogTableBuilder<Table, Table> tableBuilder;

  public HMSCatalogSyncClient(
      ExternalCatalogConfig catalogConfig, String tableFormat, Configuration configuration) {
    this.catalogConfig = catalogConfig;
    this.hmsCatalogConfig = HMSCatalogConfig.of(catalogConfig.getCatalogProperties());
    this.configuration = configuration;
    try {
      this.metaStoreClient = new HMSClientProvider(hmsCatalogConfig, configuration).getMSC();
    } catch (MetaException | HiveException e) {
      throw new CatalogSyncException("HiveMetastoreClient could not be created", e);
    }
    this.tableBuilder =
        HMSCatalogTableBuilderFactory.getTableBuilder(tableFormat, this.configuration);
  }

  @VisibleForTesting
  HMSCatalogSyncClient(
      ExternalCatalogConfig catalogConfig,
      HMSCatalogConfig hmsCatalogConfig,
      Configuration configuration,
      IMetaStoreClient metaStoreClient,
      CatalogTableBuilder tableBuilder) {
    this.catalogConfig = catalogConfig;
    this.hmsCatalogConfig = hmsCatalogConfig;
    this.configuration = configuration;
    this.metaStoreClient = metaStoreClient;
    this.tableBuilder = tableBuilder;
  }

  @Override
  public String getCatalogId() {
    return catalogConfig.getCatalogId();
  }

  @Override
  public String getCatalogType() {
    return CatalogType.HMS;
  }

  @Override
  public String getStorageLocation(Table table) {
    if (table == null || table.getSd() == null) {
      return null;
    }
    return table.getSd().getLocation();
  }

  @Override
  public boolean hasDatabase(CatalogTableIdentifier tableIdentifier) {
    String databaseName = castToHierarchicalTableIdentifier(tableIdentifier).getDatabaseName();
    try {
      return metaStoreClient.getDatabase(databaseName) != null;
    } catch (NoSuchObjectException e) {
      return false;
    } catch (TException e) {
      throw new CatalogSyncException("Failed to get database: " + databaseName, e);
    }
  }

  @Override
  public void createDatabase(CatalogTableIdentifier tableIdentifier) {
    String databaseName = castToHierarchicalTableIdentifier(tableIdentifier).getDatabaseName();
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
  public Table getTable(CatalogTableIdentifier tblIdentifier) {
    HierarchicalTableIdentifier tableIdentifier = castToHierarchicalTableIdentifier(tblIdentifier);
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
    Table hmsTable = tableBuilder.getCreateTableRequest(table, tableIdentifier);
    try {
      metaStoreClient.createTable(hmsTable);
    } catch (TException e) {
      throw new CatalogSyncException("Failed to create table: " + tableIdentifier.getId(), e);
    }
  }

  @Override
  public void refreshTable(
      InternalTable table, Table catalogTable, CatalogTableIdentifier tblIdentifier) {
    HierarchicalTableIdentifier tableIdentifier = castToHierarchicalTableIdentifier(tblIdentifier);
    catalogTable = tableBuilder.getUpdateTableRequest(table, catalogTable, tableIdentifier);
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
  public void dropTable(InternalTable table, CatalogTableIdentifier tblIdentifier) {
    HierarchicalTableIdentifier tableIdentifier = castToHierarchicalTableIdentifier(tblIdentifier);
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
      InternalTable table, CatalogTableIdentifier tblIdentifier) {
    HierarchicalTableIdentifier tableIdentifier = castToHierarchicalTableIdentifier(tblIdentifier);
    String tempTableName =
        tableIdentifier.getTableName() + TEMP_SUFFIX + ZonedDateTime.now().toEpochSecond();
    ThreePartHierarchicalTableIdentifier tempTableIdentifier =
        new ThreePartHierarchicalTableIdentifier(tableIdentifier.getDatabaseName(), tempTableName);
    createTable(table, tempTableIdentifier);
    dropTable(table, tempTableIdentifier);
  }

  @Override
  public void close() throws Exception {
    if (metaStoreClient != null) {
      metaStoreClient.close();
    }
  }
}
