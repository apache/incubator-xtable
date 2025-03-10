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

import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.xtable.catalog.CatalogUtils.toHierarchicalTableIdentifier;
import static org.apache.xtable.catalog.Constants.PROP_EXTERNAL;
import static org.apache.xtable.catalog.Constants.PROP_PATH;
import static org.apache.xtable.catalog.Constants.PROP_SERIALIZATION_FORMAT;
import static org.apache.xtable.catalog.Constants.PROP_SPARK_SQL_SOURCES_PROVIDER;
import static org.apache.xtable.glue.GlueCatalogSyncClient.GLUE_EXTERNAL_TABLE_TYPE;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.catalog.CatalogTableBuilder;
import org.apache.xtable.glue.GlueSchemaExtractor;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;
import org.apache.xtable.model.storage.TableFormat;

import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

/** Delta specific table operations for Glue catalog sync */
public class DeltaGlueCatalogTableBuilder implements CatalogTableBuilder<TableInput, Table> {

  private final GlueSchemaExtractor schemaExtractor;
  private static final String tableFormat = TableFormat.DELTA;

  public DeltaGlueCatalogTableBuilder() {
    this.schemaExtractor = GlueSchemaExtractor.getInstance();
  }

  @Override
  public TableInput getCreateTableRequest(
      InternalTable table, CatalogTableIdentifier tblIdentifier) {
    HierarchicalTableIdentifier tableIdentifier = toHierarchicalTableIdentifier(tblIdentifier);
    Map<String, Column> columnsMap =
        schemaExtractor.toColumns(tableFormat, table.getReadSchema()).stream()
            .collect(Collectors.toMap(Column::name, c -> c));

    return TableInput.builder()
        .name(tableIdentifier.getTableName())
        .tableType(GLUE_EXTERNAL_TABLE_TYPE)
        .parameters(getTableParameters())
        .storageDescriptor(
            StorageDescriptor.builder()
                .columns(schemaExtractor.getNonPartitionColumns(table, columnsMap))
                .location(table.getBasePath())
                .serdeInfo(SerDeInfo.builder().parameters(getSerDeParameters(table)).build())
                .build())
        .partitionKeys(schemaExtractor.getPartitionColumns(table, columnsMap))
        .build();
  }

  @Override
  public TableInput getUpdateTableRequest(
      InternalTable table, Table catalogTable, CatalogTableIdentifier tblIdentifier) {
    HierarchicalTableIdentifier tableIdentifier = toHierarchicalTableIdentifier(tblIdentifier);
    Map<String, String> parameters = new HashMap<>(catalogTable.parameters());
    Map<String, Column> columnsMap =
        schemaExtractor.toColumns(tableFormat, table.getReadSchema(), catalogTable).stream()
            .collect(Collectors.toMap(Column::name, c -> c));
    return TableInput.builder()
        .name(tableIdentifier.getTableName())
        .tableType(GLUE_EXTERNAL_TABLE_TYPE)
        .parameters(parameters)
        .storageDescriptor(
            catalogTable.storageDescriptor().toBuilder()
                .columns(schemaExtractor.getNonPartitionColumns(table, columnsMap))
                .build())
        .partitionKeys(schemaExtractor.getPartitionColumns(table, columnsMap))
        .build();
  }

  @VisibleForTesting
  Map<String, String> getTableParameters() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put(TABLE_TYPE_PROP, tableFormat);
    parameters.put(PROP_SPARK_SQL_SOURCES_PROVIDER, tableFormat);
    parameters.put(PROP_EXTERNAL, "TRUE");
    return parameters;
  }

  @VisibleForTesting
  Map<String, String> getSerDeParameters(InternalTable table) {
    Map<String, String> parameters = new HashMap<>();
    parameters.put(PROP_SERIALIZATION_FORMAT, "1");
    parameters.put(PROP_PATH, table.getBasePath());
    return parameters;
  }
}
