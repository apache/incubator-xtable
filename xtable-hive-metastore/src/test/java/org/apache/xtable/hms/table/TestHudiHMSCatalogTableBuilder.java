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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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

    Table table =
        hudiHMSCatalogTableBuilder.getCreateTableRequest(
            TEST_HUDI_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);

    verify(mockTablePropertiesExtractor)
        .getTableProperties(eq(TEST_HUDI_INTERNAL_TABLE), any(Integer.class));
    assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getTableName(), table.getTableName());
    assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(), table.getDbName());
    assertEquals(3, table.getSd().getCols().size());
    assertEquals(1, table.getPartitionKeys().size());
  }

  @Test
  void testGetUpdateTableInput() {
    setupCommonMocks();
    setupMetaClientMocks();

    Table table =
        hudiHMSCatalogTableBuilder.getCreateTableRequest(
            TEST_HUDI_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    Table updatedTable =
        hudiHMSCatalogTableBuilder.getUpdateTableRequest(
            TEST_EVOLVED_HUDI_INTERNAL_TABLE, table, TEST_CATALOG_TABLE_IDENTIFIER);

    verify(mockTablePropertiesExtractor)
        .getTableProperties(eq(TEST_HUDI_INTERNAL_TABLE), any(Integer.class));
    assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getTableName(), updatedTable.getTableName());
    assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(), updatedTable.getDbName());
    assertEquals(4, updatedTable.getSd().getCols().size());
    assertEquals(1, updatedTable.getPartitionKeys().size());
  }
}
