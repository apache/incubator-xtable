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

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.METASTOREURIS;

import java.lang.reflect.InvocationTargetException;
import java.time.ZonedDateTime;
import java.util.Collections;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;

import org.apache.hudi.common.util.VisibleForTesting;

import org.apache.xtable.conversion.TargetCatalog;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.sync.CatalogSyncClient;

@Log4j2
public class HMSCatalogSyncClient implements CatalogSyncClient<Table> {

  private static final String TEMP_SUFFIX = "_temp";
  private final TargetCatalog targetCatalog;
  @Getter private final HMSCatalogConfig hmsCatalogConfig;
  @Getter private Configuration configuration;
  @Getter private IMetaStoreClient metaStoreClient;
  @Getter private HMSSchemaExtractor schemaExtractor;
  private final IcebergHMSCatalogSyncOperations icebergHMSCatalogSyncOperations;

  protected HMSCatalogSyncClient(TargetCatalog targetCatalog, Configuration configuration) {
    this.targetCatalog = targetCatalog;
    this.hmsCatalogConfig =
        HMSCatalogConfig.of(targetCatalog.getCatalogConfig().getCatalogOptions());
    this.configuration = configuration;
    this.schemaExtractor = HMSSchemaExtractor.getInstance();
    try {
      this.metaStoreClient = getMSC();
    } catch (MetaException | HiveException e) {
      throw new CatalogSyncException("HiveMetastoreClient could not be created", e);
    }
    this.icebergHMSCatalogSyncOperations = new IcebergHMSCatalogSyncOperations(this);
  }

  protected HMSCatalogSyncClient(
      TargetCatalog targetCatalog,
      HMSCatalogConfig hmsCatalogConfig,
      Configuration configuration,
      IMetaStoreClient metaStoreClient,
      HMSSchemaExtractor schemaExtractor,
      IcebergHMSCatalogSyncOperations icebergHMSCatalogSyncOperations) {
    this.targetCatalog = targetCatalog;
    this.hmsCatalogConfig = hmsCatalogConfig;
    this.configuration = configuration;
    this.metaStoreClient = metaStoreClient;
    this.schemaExtractor = schemaExtractor;
    this.icebergHMSCatalogSyncOperations = icebergHMSCatalogSyncOperations;
  }

  @Override
  public String getCatalogId() {
    return targetCatalog.getCatalogId();
  }

  @Override
  public String getCatalogImpl() {
    return this.getClass().getCanonicalName();
  }

  @Override
  public CatalogTableIdentifier getTableIdentifier() {
    return targetCatalog.getCatalogTableIdentifier();
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
    switch (table.getTableFormat()) {
      case TableFormat.ICEBERG:
        icebergHMSCatalogSyncOperations.createTable(table, tableIdentifier);
        return;
      default:
        throw new NotSupportedException(
            "HMSCatalogSyncClient not supported for " + table.getTableFormat());
    }
  }

  @Override
  public void refreshTable(
      InternalTable table, Table catalogTable, CatalogTableIdentifier tableIdentifier) {
    switch (table.getTableFormat()) {
      case TableFormat.ICEBERG:
        icebergHMSCatalogSyncOperations.refreshTable(table, catalogTable, tableIdentifier);
        break;
      default:
        throw new NotSupportedException(
            "HMSCatalogSyncClient not supported for " + table.getTableFormat());
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

  @VisibleForTesting
  IMetaStoreClient getMSC() throws MetaException, HiveException {
    HiveConf hiveConf = new HiveConf(configuration, HiveConf.class);
    hiveConf.set(METASTOREURIS.varname, hmsCatalogConfig.getServerUrl());
    IMetaStoreClient metaStoreClient;
    try {
      metaStoreClient =
          ((Hive)
                  Hive.class
                      .getMethod("getWithoutRegisterFns", HiveConf.class)
                      .invoke(null, hiveConf))
              .getMSC();
    } catch (NoSuchMethodException
        | IllegalAccessException
        | IllegalArgumentException
        | InvocationTargetException ex) {
      metaStoreClient = Hive.get(hiveConf).getMSC();
    }
    log.debug("Connected to metastore with uri: {}", hmsCatalogConfig.getServerUrl());
    return metaStoreClient;
  }

  @Override
  public void close() throws Exception {
    if (metaStoreClient != null) {
      metaStoreClient.close();
    }
  }
}
