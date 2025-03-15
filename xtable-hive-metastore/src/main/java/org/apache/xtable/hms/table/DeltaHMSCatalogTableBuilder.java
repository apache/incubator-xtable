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
 
package org.apache.xtable.hms.table;

import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.xtable.catalog.Constants.PROP_EXTERNAL;
import static org.apache.xtable.catalog.Constants.PROP_PATH;
import static org.apache.xtable.catalog.Constants.PROP_SERIALIZATION_FORMAT;
import static org.apache.xtable.hms.HMSCatalogTableBuilderFactory.newHmsTable;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;

import com.google.common.annotations.VisibleForTesting;

import io.delta.hive.DeltaStorageHandler;

import org.apache.xtable.catalog.CatalogTableBuilder;
import org.apache.xtable.hms.HMSSchemaExtractor;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.storage.TableFormat;

public class DeltaHMSCatalogTableBuilder implements CatalogTableBuilder<Table, Table> {

  private final HMSSchemaExtractor schemaExtractor;
  private static final String tableFormat = TableFormat.DELTA;

  public DeltaHMSCatalogTableBuilder() {
    this.schemaExtractor = HMSSchemaExtractor.getInstance();
  }

  @Override
  public Table getCreateTableRequest(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    return newHmsTable(tableIdentifier, getStorageDescriptor(table), getTableParameters());
  }

  @Override
  public Table getUpdateTableRequest(
      InternalTable table, Table catalogTable, CatalogTableIdentifier tableIdentifier) {
    Table copyTb = new Table(catalogTable);
    copyTb.getSd().setCols(schemaExtractor.toColumns(tableFormat, table.getReadSchema()));
    return copyTb;
  }

  @VisibleForTesting
  StorageDescriptor getStorageDescriptor(InternalTable table) {
    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(schemaExtractor.toColumns(tableFormat, table.getReadSchema()));
    storageDescriptor.setLocation(table.getBasePath());
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setParameters(getSerDeParameters(table));
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  @VisibleForTesting
  Map<String, String> getTableParameters() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put(PROP_EXTERNAL, "TRUE");
    parameters.put(TABLE_TYPE_PROP, tableFormat.toUpperCase(Locale.ENGLISH));
    parameters.put(
        hive_metastoreConstants.META_TABLE_STORAGE, DeltaStorageHandler.class.getCanonicalName());
    return parameters;
  }

  private Map<String, String> getSerDeParameters(InternalTable table) {
    Map<String, String> parameters = new HashMap<>();
    parameters.put(PROP_SERIALIZATION_FORMAT, "1");
    parameters.put(PROP_PATH, table.getBasePath());
    return parameters;
  }
}
