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

import static org.apache.xtable.catalog.CatalogUtils.toHierarchicalTableIdentifier;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.catalog.CatalogTableBuilder;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.glue.GlueCatalogConfig;
import org.apache.xtable.glue.GlueSchemaExtractor;
import org.apache.xtable.hudi.HudiTableManager;
import org.apache.xtable.hudi.catalog.HudiCatalogTablePropertiesExtractor;
import org.apache.xtable.hudi.catalog.HudiInputFormatUtils;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.storage.TableFormat;

import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

public class HudiGlueCatalogTableBuilder implements CatalogTableBuilder<TableInput, Table> {
  protected static final String GLUE_EXTERNAL_TABLE_TYPE = "EXTERNAL_TABLE";

  private final GlueSchemaExtractor schemaExtractor;
  private final HudiTableManager hudiTableManager;
  private final GlueCatalogConfig glueCatalogConfig;
  private HoodieTableMetaClient metaClient;
  private final HudiCatalogTablePropertiesExtractor tablePropertiesExtractor;

  public HudiGlueCatalogTableBuilder(
      GlueCatalogConfig glueCatalogConfig, Configuration configuration) {
    this.glueCatalogConfig = glueCatalogConfig;
    this.schemaExtractor = GlueSchemaExtractor.getInstance();
    this.hudiTableManager = HudiTableManager.of(configuration);
    this.tablePropertiesExtractor = HudiCatalogTablePropertiesExtractor.getInstance();
  }

  @VisibleForTesting
  HudiGlueCatalogTableBuilder(
      GlueCatalogConfig glueCatalogConfig,
      GlueSchemaExtractor schemaExtractor,
      HudiTableManager hudiTableManager,
      HoodieTableMetaClient metaClient,
      HudiCatalogTablePropertiesExtractor propertiesExtractor) {
    this.glueCatalogConfig = glueCatalogConfig;
    this.hudiTableManager = hudiTableManager;
    this.schemaExtractor = schemaExtractor;
    this.metaClient = metaClient;
    this.tablePropertiesExtractor = propertiesExtractor;
  }

  HoodieTableMetaClient getMetaClient(String basePath) {
    if (metaClient == null) {
      Optional<HoodieTableMetaClient> metaClientOpt =
          hudiTableManager.loadTableMetaClientIfExists(basePath);

      if (!metaClientOpt.isPresent()) {
        throw new CatalogSyncException(
            "Failed to get meta client since table is not present in the base path " + basePath);
      }

      metaClient = metaClientOpt.get();
    }
    return metaClient;
  }

  @Override
  public TableInput getCreateTableRequest(
      InternalTable table, CatalogTableIdentifier catalogTableIdentifier) {
    HierarchicalTableIdentifier tableIdentifier =
        toHierarchicalTableIdentifier(catalogTableIdentifier);
    final Instant now = Instant.now();
    return TableInput.builder()
        .name(tableIdentifier.getTableName())
        .tableType(GLUE_EXTERNAL_TABLE_TYPE)
        .parameters(
            tablePropertiesExtractor.getTableProperties(
                table, glueCatalogConfig.getSchemaLengthThreshold()))
        .partitionKeys(getSchemaPartitionKeys(table.getPartitioningFields()))
        .storageDescriptor(getStorageDescriptor(table))
        .lastAccessTime(now)
        .lastAnalyzedTime(now)
        .build();
  }

  @Override
  public TableInput getUpdateTableRequest(
      InternalTable table, Table glueTable, CatalogTableIdentifier catalogTableIdentifier) {
    HierarchicalTableIdentifier tableIdentifier =
        toHierarchicalTableIdentifier(catalogTableIdentifier);
    Map<String, String> tableParameters = new HashMap<>(glueTable.parameters());
    tableParameters.putAll(
        tablePropertiesExtractor.getTableProperties(
            table, glueCatalogConfig.getSchemaLengthThreshold()));
    List<Column> newColumns = getSchemaWithoutPartitionKeys(table);
    StorageDescriptor sd = glueTable.storageDescriptor();
    StorageDescriptor partitionSD = sd.copy(copySd -> copySd.columns(newColumns));

    final Instant now = Instant.now();
    return TableInput.builder()
        .name(tableIdentifier.getTableName())
        .tableType(glueTable.tableType())
        .parameters(tableParameters)
        .partitionKeys(getSchemaPartitionKeys(table.getPartitioningFields()))
        .storageDescriptor(partitionSD)
        .lastAccessTime(now)
        .lastAnalyzedTime(now)
        .build();
  }

  private StorageDescriptor getStorageDescriptor(InternalTable table) {
    HoodieFileFormat baseFileFormat =
        getMetaClient(table.getBasePath()).getTableConfig().getBaseFileFormat();
    SerDeInfo serDeInfo =
        SerDeInfo.builder()
            .serializationLibrary(HudiInputFormatUtils.getSerDeClassName(baseFileFormat))
            .parameters(tablePropertiesExtractor.getSerdeProperties(table.getBasePath()))
            .build();
    return StorageDescriptor.builder()
        .serdeInfo(serDeInfo)
        .location(table.getBasePath())
        .inputFormat(HudiInputFormatUtils.getInputFormatClassName(baseFileFormat, false))
        .outputFormat(HudiInputFormatUtils.getOutputFormatClassName(baseFileFormat))
        .columns(getSchemaWithoutPartitionKeys(table))
        .build();
  }

  private List<Column> getSchemaWithoutPartitionKeys(InternalTable table) {
    List<String> partitionKeys =
        table.getPartitioningFields().stream()
            .map(field -> field.getSourceField().getName())
            .collect(Collectors.toList());
    return schemaExtractor.toColumns(TableFormat.HUDI, table.getReadSchema()).stream()
        .filter(c -> !partitionKeys.contains(c.name()))
        .collect(Collectors.toList());
  }

  private List<Column> getSchemaPartitionKeys(List<InternalPartitionField> partitioningFields) {
    return partitioningFields.stream()
        .map(
            field -> {
              String fieldName = field.getSourceField().getName();
              String fieldType =
                  schemaExtractor.toTypeString(
                      field.getSourceField().getSchema(), TableFormat.HUDI);
              return Column.builder().name(fieldName).type(fieldType).build();
            })
        .collect(Collectors.toList());
  }
}
