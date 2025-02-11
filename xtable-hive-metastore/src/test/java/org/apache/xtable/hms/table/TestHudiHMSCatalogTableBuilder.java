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

import static org.apache.xtable.hms.table.HudiHMSCatalogTableBuilder.HUDI_METADATA_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import org.apache.xtable.hms.HMSCatalogSyncTestBase;
import org.apache.xtable.hms.HMSSchemaExtractor;
import org.apache.xtable.hudi.HudiTableManager;
import org.apache.xtable.hudi.catalog.HudiCatalogTablePropertiesExtractor;

@ExtendWith(MockitoExtension.class)
public class TestHudiHMSCatalogTableBuilder extends HMSCatalogSyncTestBase {

  @Mock private HoodieTableMetaClient mockMetaClient;
  @Mock private HudiTableManager mockHudiTableManager;
  @Mock private HoodieTableConfig mockTableConfig;
  @Mock private HudiCatalogTablePropertiesExtractor mockTablePropertiesExtractor;

  private HudiHMSCatalogTableBuilder hudiHMSCatalogTableBuilder;

  private HudiHMSCatalogTableBuilder createMockHudiHMSCatalogSyncRequestProvider() {
    return new HudiHMSCatalogTableBuilder(
        mockHMSCatalogConfig,
        HMSSchemaExtractor.getInstance(),
        mockHudiTableManager,
        mockMetaClient,
        mockTablePropertiesExtractor);
  }

  void setupCommonMocks() {
    hudiHMSCatalogTableBuilder = createMockHudiHMSCatalogSyncRequestProvider();
    when(mockHMSCatalogConfig.getSchemaLengthThreshold()).thenReturn(1000);
  }

  void setupMetaClientMocks() {
    when(mockTableConfig.getBaseFileFormat()).thenReturn(HoodieFileFormat.PARQUET);
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
  }

  @Test
  void testGetCreateTableInput() {
    setupCommonMocks();
    setupMetaClientMocks();

    List<String> partitionFields =
        TEST_HUDI_INTERNAL_TABLE.getPartitioningFields().stream()
            .map(partitionField -> partitionField.getSourceField().getName())
            .collect(Collectors.toList());

    Table table =
        hudiHMSCatalogTableBuilder.getCreateTableRequest(
            TEST_HUDI_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);

    ArgumentCaptor<List<String>> partitionsCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockTablePropertiesExtractor)
        .getSparkTableProperties(
            partitionsCaptor.capture(),
            eq(""),
            any(Integer.class),
            eq(TEST_HUDI_INTERNAL_TABLE.getReadSchema()));
    assertEquals(partitionFields.size(), partitionsCaptor.getValue().size());
    assertEquals(partitionFields, partitionsCaptor.getValue());
    assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getTableName(), table.getTableName());
    assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(), table.getDbName());
    assertEquals(3, table.getSd().getCols().size());
    assertEquals(1, table.getPartitionKeys().size());
    assertNotNull(table.getParameters());
    assertFalse(table.getParameters().isEmpty());
    assertEquals(table.getParameters().get(HUDI_METADATA_CONFIG), "true");
  }

  @Test
  void testGetUpdateTableInput() {
    setupCommonMocks();
    setupMetaClientMocks();

    List<String> partitionFields =
        TEST_HUDI_INTERNAL_TABLE.getPartitioningFields().stream()
            .map(partitionField -> partitionField.getSourceField().getName())
            .collect(Collectors.toList());
    Table table =
        hudiHMSCatalogTableBuilder.getCreateTableRequest(
            TEST_HUDI_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    Table updatedTable =
        hudiHMSCatalogTableBuilder.getUpdateTableRequest(
            TEST_EVOLVED_HUDI_INTERNAL_TABLE, table, TEST_CATALOG_TABLE_IDENTIFIER);

    ArgumentCaptor<List<String>> partitionsCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockTablePropertiesExtractor)
        .getSparkTableProperties(
            partitionsCaptor.capture(),
            eq(""),
            any(Integer.class),
            eq(TEST_HUDI_INTERNAL_TABLE.getReadSchema()));
    assertEquals(partitionFields.size(), partitionsCaptor.getValue().size());
    assertEquals(partitionFields, partitionsCaptor.getValue());
    assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getTableName(), updatedTable.getTableName());
    assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(), updatedTable.getDbName());
    assertEquals(4, updatedTable.getSd().getCols().size());
    assertEquals(1, updatedTable.getPartitionKeys().size());
    assertNotNull(updatedTable.getParameters());
    assertFalse(table.getParameters().isEmpty());
    assertEquals(table.getParameters().get(HUDI_METADATA_CONFIG), "true");
  }
}
