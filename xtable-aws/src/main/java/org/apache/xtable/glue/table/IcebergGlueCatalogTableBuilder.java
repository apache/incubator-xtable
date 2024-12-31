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
 
package org.apache.xtable.glue.table;

import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.xtable.catalog.CatalogUtils.castToHierarchicalTableIdentifier;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.hadoop.HadoopTables;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.catalog.CatalogTableBuilder;
import org.apache.xtable.glue.GlueCatalogSyncClient;
import org.apache.xtable.glue.GlueSchemaExtractor;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;
import org.apache.xtable.model.storage.TableFormat;

import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

/** Iceberg specific table operations for Glue catalog sync */
public class IcebergGlueCatalogTableBuilder implements CatalogTableBuilder<TableInput, Table> {

  private final GlueSchemaExtractor schemaExtractor;
  private final HadoopTables hadoopTables;
  private static final String tableFormat = TableFormat.ICEBERG;

  public IcebergGlueCatalogTableBuilder(Configuration configuration) {
    this.schemaExtractor = GlueSchemaExtractor.getInstance();
    this.hadoopTables = new HadoopTables(configuration);
  }

  @VisibleForTesting
  IcebergGlueCatalogTableBuilder(GlueSchemaExtractor schemaExtractor, HadoopTables hadoopTables) {
    this.schemaExtractor = schemaExtractor;
    this.hadoopTables = hadoopTables;
  }

  @Override
  public TableInput getCreateTableRequest(
      InternalTable table, CatalogTableIdentifier tableIdentifier) {
    HierarchicalTableIdentifier tblIdentifier = castToHierarchicalTableIdentifier(tableIdentifier);
    BaseTable fsTable = loadTableFromFs(table.getBasePath());
    return TableInput.builder()
        .name(tblIdentifier.getTableName())
        .tableType(GlueCatalogSyncClient.GLUE_EXTERNAL_TABLE_TYPE)
        .parameters(getTableParameters(fsTable))
        .storageDescriptor(
            StorageDescriptor.builder()
                .location(table.getBasePath())
                .columns(schemaExtractor.toColumns(tableFormat, table.getReadSchema()))
                .build())
        .build();
  }

  @Override
  public TableInput getUpdateTableRequest(
      InternalTable table, Table catalogTable, CatalogTableIdentifier tableIdentifier) {
    HierarchicalTableIdentifier tblIdentifier = castToHierarchicalTableIdentifier(tableIdentifier);
    BaseTable icebergTable = loadTableFromFs(table.getBasePath());
    Map<String, String> parameters = new HashMap<>(catalogTable.parameters());
    parameters.put(PREVIOUS_METADATA_LOCATION_PROP, parameters.get(METADATA_LOCATION_PROP));
    parameters.put(METADATA_LOCATION_PROP, getMetadataFileLocation(icebergTable));
    parameters.putAll(icebergTable.properties());

    return TableInput.builder()
        .name(tblIdentifier.getTableName())
        .tableType(GlueCatalogSyncClient.GLUE_EXTERNAL_TABLE_TYPE)
        .parameters(parameters)
        .storageDescriptor(
            StorageDescriptor.builder()
                .location(table.getBasePath())
                .columns(
                    schemaExtractor.toColumns(tableFormat, table.getReadSchema(), catalogTable))
                .build())
        .build();
  }

  @VisibleForTesting
  Map<String, String> getTableParameters(BaseTable icebergTable) {
    Map<String, String> parameters = new HashMap<>(icebergTable.properties());
    parameters.put(TABLE_TYPE_PROP, tableFormat);
    parameters.put(METADATA_LOCATION_PROP, getMetadataFileLocation(icebergTable));
    return parameters;
  }

  private BaseTable loadTableFromFs(String tableBasePath) {
    return (BaseTable) hadoopTables.load(tableBasePath);
  }

  private String getMetadataFileLocation(BaseTable table) {
    return table.operations().current().metadataFileLocation();
  }
}
