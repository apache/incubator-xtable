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

import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.xtable.catalog.glue.GlueCatalogSyncClient.GLUE_EXTERNAL_TABLE_TYPE;

import java.util.HashMap;
import java.util.Map;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.hadoop.HadoopTables;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.storage.TableFormat;

import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

public class IcebergGlueCatalogSyncHelper {

  private final GlueCatalogSyncClient syncClient;
  private final HadoopTables hadoopTables;

  IcebergGlueCatalogSyncHelper(GlueCatalogSyncClient syncClient) {
    this.syncClient = syncClient;
    this.hadoopTables = new HadoopTables(syncClient.getConfiguration());
  }

  IcebergGlueCatalogSyncHelper(GlueCatalogSyncClient syncClient, HadoopTables hadoopTables) {
    this.syncClient = syncClient;
    this.hadoopTables = hadoopTables;
  }

  public void createTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
    BaseTable fsTable = loadTableFromFs(table.getBasePath());
    try {
      syncClient
          .getGlueClient()
          .createTable(
              CreateTableRequest.builder()
                  .catalogId(syncClient.getGlueCatalogConfig().getCatalogId())
                  .databaseName(tableIdentifier.getDatabaseName())
                  .tableInput(
                      TableInput.builder()
                          .name(tableIdentifier.getTableName())
                          .tableType(GLUE_EXTERNAL_TABLE_TYPE)
                          .parameters(getTableParameters(fsTable))
                          .storageDescriptor(
                              StorageDescriptor.builder()
                                  .location(table.getBasePath())
                                  .columns(
                                      syncClient
                                          .getSchemaExtractor()
                                          .toColumns(TableFormat.ICEBERG, table.getReadSchema()))
                                  .build())
                          .build())
                  .build());
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to create table: " + tableIdentifier.getId(), e);
    }
  }

  public void refreshTable(
      InternalTable table, Table catalogTable, CatalogTableIdentifier tableIdentifier) {
    BaseTable fsTable = loadTableFromFs(table.getBasePath());
    Map<String, String> parameters = new HashMap<>(catalogTable.parameters());
    parameters.put(PREVIOUS_METADATA_LOCATION_PROP, parameters.get(METADATA_LOCATION_PROP));
    parameters.putAll(getTableParameters(fsTable));
    try {
      syncClient
          .getGlueClient()
          .updateTable(
              UpdateTableRequest.builder()
                  .catalogId(syncClient.getGlueCatalogConfig().getCatalogId())
                  .databaseName(tableIdentifier.getDatabaseName())
                  .skipArchive(true)
                  .tableInput(
                      TableInput.builder()
                          .name(tableIdentifier.getTableName())
                          .tableType(GLUE_EXTERNAL_TABLE_TYPE)
                          .parameters(parameters)
                          .storageDescriptor(
                              StorageDescriptor.builder()
                                  .location(table.getBasePath())
                                  .columns(
                                      syncClient
                                          .getSchemaExtractor()
                                          .toColumns(
                                              TableFormat.ICEBERG,
                                              table.getReadSchema(),
                                              catalogTable))
                                  .build())
                          .build())
                  .build());
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to refresh table: " + tableIdentifier.getId(), e);
    }
  }

  @VisibleForTesting
  protected Map<String, String> getTableParameters(BaseTable table) {
    Map<String, String> parameters = new HashMap<>();
    parameters.put(TABLE_TYPE_PROP, TableFormat.ICEBERG);
    parameters.put(METADATA_LOCATION_PROP, getMetadataFileLocation(table));
    return parameters;
  }

  private BaseTable loadTableFromFs(String tableBasePath) {
    return (BaseTable) hadoopTables.load(tableBasePath);
  }

  private String getMetadataFileLocation(BaseTable table) {
    return table.operations().current().metadataFileLocation();
  }
}
