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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import org.apache.xtable.glue.GlueCatalogSyncTestBase;
import org.apache.xtable.glue.GlueSchemaExtractor;
import org.apache.xtable.hudi.HudiTableManager;
import org.apache.xtable.hudi.catalog.HudiCatalogTablePropertiesExtractor;

import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

@ExtendWith(MockitoExtension.class)
public class TestHudiGlueCatalogTableBuilder extends GlueCatalogSyncTestBase {

  @Mock private HoodieTableMetaClient mockMetaClient;
  @Mock private HudiTableManager mockHudiTableManager;
  @Mock private HoodieTableConfig mockTableConfig;
  @Mock private HudiCatalogTablePropertiesExtractor mockTablePropertiesExtractor;

  private HudiGlueCatalogTableBuilder hudiGlueCatalogTableBuilder;

  private HudiGlueCatalogTableBuilder createMockHudiGlueCatalogSyncRequestProvider() {
    return new HudiGlueCatalogTableBuilder(
        mockGlueCatalogConfig,
        GlueSchemaExtractor.getInstance(),
        mockHudiTableManager,
        mockMetaClient,
        mockTablePropertiesExtractor);
  }

  void setupCommonMocks() {
    hudiGlueCatalogTableBuilder = createMockHudiGlueCatalogSyncRequestProvider();
    when(mockGlueCatalogConfig.getSchemaLengthThreshold()).thenReturn(1000);
  }

  void setupMetaClientMocks() {
    when(mockTableConfig.getBaseFileFormat()).thenReturn(HoodieFileFormat.PARQUET);
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
  }

  @Test
  void testGetCreateTableInput() {
    setupCommonMocks();
    setupMetaClientMocks();

    TableInput table =
        hudiGlueCatalogTableBuilder.getCreateTableRequest(
            TEST_HUDI_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);

    verify(mockTablePropertiesExtractor)
        .getTableProperties(eq(TEST_HUDI_INTERNAL_TABLE), any(Integer.class));
    assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getTableName(), table.name());
    assertEquals(2, table.storageDescriptor().columns().size());
    assertEquals(1, table.partitionKeys().size());
    assertNotNull(table.parameters());
  }

  @Test
  void testGetUpdateTableInput() {
    setupCommonMocks();
    setupMetaClientMocks();

    List<String> partitionFields =
        TEST_HUDI_INTERNAL_TABLE.getPartitioningFields().stream()
            .map(partitionField -> partitionField.getSourceField().getName())
            .collect(Collectors.toList());

    TableInput tableInput =
        hudiGlueCatalogTableBuilder.getCreateTableRequest(
            TEST_HUDI_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    Table table =
        Table.builder()
            .name(tableInput.name())
            .parameters(tableInput.parameters())
            .storageDescriptor(tableInput.storageDescriptor())
            .partitionKeys(tableInput.partitionKeys())
            .build();
    TableInput updatedTable =
        hudiGlueCatalogTableBuilder.getUpdateTableRequest(
            TEST_EVOLVED_HUDI_INTERNAL_TABLE, table, TEST_CATALOG_TABLE_IDENTIFIER);

    verify(mockTablePropertiesExtractor)
        .getTableProperties(eq(TEST_HUDI_INTERNAL_TABLE), any(Integer.class));
    assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getTableName(), updatedTable.name());
    assertEquals(3, updatedTable.storageDescriptor().columns().size());
    assertEquals(1, updatedTable.partitionKeys().size());
    assertNotNull(updatedTable.parameters());
  }
}
