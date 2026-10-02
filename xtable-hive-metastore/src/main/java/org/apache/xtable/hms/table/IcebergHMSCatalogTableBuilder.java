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

import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.xtable.catalog.Constants.PROP_EXTERNAL;
import static org.apache.xtable.hms.HMSCatalogTableBuilderFactory.newHmsTable;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mr.hive.HiveIcebergInputFormat;
import org.apache.iceberg.mr.hive.HiveIcebergOutputFormat;
import org.apache.iceberg.mr.hive.HiveIcebergSerDe;
import org.apache.iceberg.mr.hive.HiveIcebergStorageHandler;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.catalog.CatalogTableBuilder;
import org.apache.xtable.hms.HMSSchemaExtractor;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.storage.TableFormat;

public class IcebergHMSCatalogTableBuilder implements CatalogTableBuilder<Table, Table> {

  private static final String ICEBERG_CATALOG_NAME_PROP = "iceberg.catalog";
  private static final String ICEBERG_HADOOP_TABLE_NAME = "location_based_table";
  private static final String tableFormat = TableFormat.ICEBERG;
  private final HMSSchemaExtractor schemaExtractor;
  private final HadoopTables hadoopTables;

  public IcebergHMSCatalogTableBuilder(Configuration configuration) {
    this.schemaExtractor = HMSSchemaExtractor.getInstance();
    this.hadoopTables = new HadoopTables(configuration);
  }

  @VisibleForTesting
  IcebergHMSCatalogTableBuilder(HMSSchemaExtractor schemaExtractor, HadoopTables hadoopTables) {
    this.schemaExtractor = schemaExtractor;
    this.hadoopTables = hadoopTables;
  }

  @Override
  public Table getCreateTableRequest(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    return newHmsTable(
        tableIdentifier,
        getStorageDescriptor(table),
        getTableParameters(loadTableFromFs(table.getBasePath())));
  }

  @Override
  public Table getUpdateTableRequest(
      InternalTable table, Table catalogTable, CatalogTableIdentifier tableIdentifier) {
    BaseTable icebergTable = loadTableFromFs(table.getBasePath());
    Table copyTb = new Table(catalogTable);
    Map<String, String> parameters = copyTb.getParameters();
    parameters.putAll(icebergTable.properties());
    String currentMetadataLocation = parameters.get(METADATA_LOCATION_PROP);
    parameters.put(PREVIOUS_METADATA_LOCATION_PROP, currentMetadataLocation);
    parameters.put(METADATA_LOCATION_PROP, getMetadataFileLocation(icebergTable));
    copyTb.setParameters(parameters);
    copyTb.getSd().setCols(schemaExtractor.toColumns(tableFormat, table.getReadSchema()));
    return copyTb;
  }

  @VisibleForTesting
  StorageDescriptor getStorageDescriptor(InternalTable table) {
    final StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(schemaExtractor.toColumns(tableFormat, table.getReadSchema()));
    storageDescriptor.setLocation(table.getBasePath());
    storageDescriptor.setInputFormat(HiveIcebergInputFormat.class.getCanonicalName());
    storageDescriptor.setOutputFormat(HiveIcebergOutputFormat.class.getCanonicalName());
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setSerializationLib(HiveIcebergSerDe.class.getCanonicalName());
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  @VisibleForTesting
  Map<String, String> getTableParameters(BaseTable icebergTable) {
    Map<String, String> parameters = new HashMap<>(icebergTable.properties());
    parameters.put(PROP_EXTERNAL, "TRUE");
    parameters.put(TABLE_TYPE_PROP, tableFormat);
    parameters.put(METADATA_LOCATION_PROP, getMetadataFileLocation(icebergTable));
    parameters.put(
        hive_metastoreConstants.META_TABLE_STORAGE,
        HiveIcebergStorageHandler.class.getCanonicalName());
    parameters.put(ICEBERG_CATALOG_NAME_PROP, ICEBERG_HADOOP_TABLE_NAME);
    return parameters;
  }

  private BaseTable loadTableFromFs(String tableBasePath) {
    return (BaseTable) hadoopTables.load(tableBasePath);
  }

  private String getMetadataFileLocation(BaseTable table) {
    return table.operations().current().metadataFileLocation();
  }
}
