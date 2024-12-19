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

import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import lombok.SneakyThrows;

import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.hadoop.HadoopTables;

import org.apache.xtable.model.storage.TableFormat;

@ExtendWith(MockitoExtension.class)
public class TestIcebergHMSCatalogSyncRequestProvider extends HMSCatalogSyncClientTestBase {

  @Mock private HadoopTables mockIcebergHadoopTables;
  @Mock private BaseTable mockIcebergBaseTable;
  @Mock private TableOperations mockIcebergTableOperations;
  @Mock private TableMetadata mockIcebergTableMetadata;

  private IcebergHMSCatalogSyncRequestProvider mockIcebergHmsCatalogSyncRequestProvider;

  private IcebergHMSCatalogSyncRequestProvider createIcebergHMSHelper() {
    return new IcebergHMSCatalogSyncRequestProvider(
        mockHmsSchemaExtractor, mockIcebergHadoopTables);
  }

  void setupCommonMocks() {
    mockIcebergHmsCatalogSyncRequestProvider = createIcebergHMSHelper();
  }

  void mockHadoopTables() {
    when(mockIcebergHadoopTables.load(TEST_BASE_PATH)).thenReturn(mockIcebergBaseTable);
    mockMetadataFileLocation();
  }

  void mockMetadataFileLocation() {
    when(mockIcebergBaseTable.operations()).thenReturn(mockIcebergTableOperations);
    when(mockIcebergTableOperations.current()).thenReturn(mockIcebergTableMetadata);
    when(mockIcebergTableMetadata.metadataFileLocation())
        .thenReturn(ICEBERG_METADATA_FILE_LOCATION);
  }

  @SneakyThrows
  @Test
  void testGetNewTable() {
    mockIcebergHmsCatalogSyncRequestProvider = createIcebergHMSHelper();
    mockHadoopTables();
    when(mockHmsSchemaExtractor.toColumns(
            TableFormat.ICEBERG, TEST_ICEBERG_INTERNAL_TABLE.getReadSchema()))
        .thenReturn(Collections.emptyList());
    ZonedDateTime zonedDateTime =
        Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault());
    try (MockedStatic<ZonedDateTime> mockZonedDateTime = mockStatic(ZonedDateTime.class)) {
      mockZonedDateTime.when(ZonedDateTime::now).thenReturn(zonedDateTime);
      Table expected = new Table();
      expected.setDbName(TEST_HMS_DATABASE);
      expected.setTableName(TEST_HMS_TABLE);
      expected.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
      expected.setCreateTime((int) zonedDateTime.toEpochSecond());
      expected.setSd(getTestStorageDescriptor());
      expected.setTableType("EXTERNAL_TABLE");
      expected.setParameters(getTestParameters());

      assertEquals(
          expected,
          mockIcebergHmsCatalogSyncRequestProvider.getCreateTableInput(
              TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER));
      verify(mockHmsSchemaExtractor, times(1))
          .toColumns(TableFormat.ICEBERG, TEST_ICEBERG_INTERNAL_TABLE.getReadSchema());
      verify(mockIcebergBaseTable, times(1)).properties();
      verify(mockIcebergHadoopTables, times(1)).load(TEST_BASE_PATH);
    }
  }

  @SneakyThrows
  @Test
  void testGetUpdatedTable() {
    setupCommonMocks();
    mockHadoopTables();
    when(mockHmsSchemaExtractor.toColumns(
            TableFormat.ICEBERG, TEST_ICEBERG_INTERNAL_TABLE.getReadSchema()))
        .thenReturn(Collections.emptyList());

    Map<String, String> tableParams = new HashMap<>();
    tableParams.put(METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION);
    Table hmsTable =
        newTable(TEST_HMS_DATABASE, TEST_HMS_TABLE, tableParams, getTestStorageDescriptor());

    when(mockIcebergTableMetadata.metadataFileLocation())
        .thenReturn(ICEBERG_METADATA_FILE_LOCATION_V2);
    when(mockIcebergBaseTable.properties()).thenReturn(Collections.emptyMap());
    Table output =
        mockIcebergHmsCatalogSyncRequestProvider.getUpdateTableInput(
            TEST_ICEBERG_INTERNAL_TABLE, hmsTable);
    tableParams.put(PREVIOUS_METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION);
    tableParams.put(METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION_V2);
    Table expected =
        newTable(TEST_HMS_DATABASE, TEST_HMS_TABLE, tableParams, getTestStorageDescriptor());
    assertEquals(expected, output);
    assertEquals(tableParams, hmsTable.getParameters());
    verify(mockHmsSchemaExtractor, times(1))
        .toColumns(TableFormat.ICEBERG, TEST_ICEBERG_INTERNAL_TABLE.getReadSchema());
  }

  @Test
  void testGetStorageDescriptor() {
    mockIcebergHmsCatalogSyncRequestProvider = createIcebergHMSHelper();
    when(mockHmsSchemaExtractor.toColumns(
            TableFormat.ICEBERG, TEST_ICEBERG_INTERNAL_TABLE.getReadSchema()))
        .thenReturn(Collections.emptyList());
    StorageDescriptor expected = getTestStorageDescriptor();
    assertEquals(
        expected,
        mockIcebergHmsCatalogSyncRequestProvider.getStorageDescriptor(TEST_ICEBERG_INTERNAL_TABLE));
    verify(mockHmsSchemaExtractor, times(1))
        .toColumns(TableFormat.ICEBERG, TEST_ICEBERG_INTERNAL_TABLE.getReadSchema());
  }

  @Test
  void testGetHmsTableParameters() {
    mockIcebergHmsCatalogSyncRequestProvider = createIcebergHMSHelper();
    mockMetadataFileLocation();
    when(mockIcebergBaseTable.properties()).thenReturn(Collections.emptyMap());
    Map<String, String> expected = getTestParameters();
    assertEquals(
        expected,
        mockIcebergHmsCatalogSyncRequestProvider.getTableParameters(mockIcebergBaseTable));
    verify(mockIcebergBaseTable, times(1)).properties();
    verify(mockIcebergHadoopTables, never()).load(any());
  }

  private StorageDescriptor getTestStorageDescriptor() {
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    SerDeInfo serDeInfo = new SerDeInfo();
    storageDescriptor.setCols(Collections.emptyList());
    storageDescriptor.setLocation(TEST_BASE_PATH);
    storageDescriptor.setInputFormat("org.apache.iceberg.mr.hive.HiveIcebergInputFormat");
    storageDescriptor.setOutputFormat("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");
    serDeInfo.setSerializationLib("org.apache.iceberg.mr.hive.HiveIcebergSerDe");
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  private Map<String, String> getTestParameters() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("EXTERNAL", "TRUE");
    parameters.put("table_type", "ICEBERG");
    parameters.put("metadata_location", ICEBERG_METADATA_FILE_LOCATION);
    parameters.put("storage_handler", "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler");
    parameters.put("iceberg.catalog", "location_based_table");
    return parameters;
  }
}
