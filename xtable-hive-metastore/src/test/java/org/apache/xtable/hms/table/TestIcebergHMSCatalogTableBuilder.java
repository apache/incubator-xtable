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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.SneakyThrows;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
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

import org.apache.xtable.hms.HMSCatalogSyncTestBase;
import org.apache.xtable.hms.HMSSchemaExtractor;

@ExtendWith(MockitoExtension.class)
public class TestIcebergHMSCatalogTableBuilder extends HMSCatalogSyncTestBase {

  @Mock private HadoopTables mockIcebergHadoopTables;
  @Mock private BaseTable mockIcebergBaseTable;
  @Mock private TableOperations mockIcebergTableOperations;
  @Mock private TableMetadata mockIcebergTableMetadata;

  private IcebergHMSCatalogTableBuilder mockIcebergHmsCatalogSyncRequestProvider;

  private IcebergHMSCatalogTableBuilder createIcebergHMSCatalogTableBuilder() {
    return new IcebergHMSCatalogTableBuilder(
        HMSSchemaExtractor.getInstance(), mockIcebergHadoopTables);
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
  void testGetCreateTableRequest() {
    mockIcebergHmsCatalogSyncRequestProvider = createIcebergHMSCatalogTableBuilder();
    mockHadoopTables();

    Instant createdTime = Instant.now();
    try (MockedStatic<Instant> mockZonedDateTime = mockStatic(Instant.class)) {
      mockZonedDateTime.when(Instant::now).thenReturn(createdTime);
      Table expected = new Table();
      expected.setDbName(TEST_HMS_DATABASE);
      expected.setTableName(TEST_HMS_TABLE);
      expected.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
      expected.setCreateTime((int) createdTime.getEpochSecond());
      expected.setSd(getTestHmsTableStorageDescriptor(FIELD_SCHEMA));
      expected.setTableType("EXTERNAL_TABLE");
      expected.setParameters(getTestHmsTableParameters());

      assertEquals(
          expected,
          mockIcebergHmsCatalogSyncRequestProvider.getCreateTableRequest(
              TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER));
      verify(mockIcebergBaseTable, times(1)).properties();
      verify(mockIcebergHadoopTables, times(1)).load(TEST_BASE_PATH);
    }
  }

  @SneakyThrows
  @Test
  void testGetUpdateTableRequest() {
    mockIcebergHmsCatalogSyncRequestProvider = createIcebergHMSCatalogTableBuilder();
    mockHadoopTables();

    Map<String, String> tableParams = new HashMap<>();
    tableParams.put(METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION);
    Table hmsTable =
        newTable(
            TEST_HMS_DATABASE,
            TEST_HMS_TABLE,
            tableParams,
            getTestHmsTableStorageDescriptor(FIELD_SCHEMA));

    when(mockIcebergTableMetadata.metadataFileLocation())
        .thenReturn(ICEBERG_METADATA_FILE_LOCATION_V2);
    when(mockIcebergBaseTable.properties()).thenReturn(Collections.emptyMap());
    Table output =
        mockIcebergHmsCatalogSyncRequestProvider.getUpdateTableRequest(
            TEST_UPDATED_ICEBERG_INTERNAL_TABLE, hmsTable, TEST_CATALOG_TABLE_IDENTIFIER);
    tableParams.put(PREVIOUS_METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION);
    tableParams.put(METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION_V2);
    Table expected = new Table(hmsTable);
    expected.getSd().setCols(UPDATED_FIELD_SCHEMA);

    assertEquals(expected, output);
    assertEquals(tableParams, hmsTable.getParameters());
  }

  @Test
  void testGetStorageDescriptor() {
    mockIcebergHmsCatalogSyncRequestProvider = createIcebergHMSCatalogTableBuilder();
    StorageDescriptor expected = getTestHmsTableStorageDescriptor(FIELD_SCHEMA);
    assertEquals(
        expected,
        mockIcebergHmsCatalogSyncRequestProvider.getStorageDescriptor(TEST_ICEBERG_INTERNAL_TABLE));
  }

  @Test
  void testGetTableParameters() {
    mockIcebergHmsCatalogSyncRequestProvider = createIcebergHMSCatalogTableBuilder();
    mockMetadataFileLocation();
    when(mockIcebergBaseTable.properties()).thenReturn(Collections.emptyMap());
    Map<String, String> expected = getTestHmsTableParameters();
    assertEquals(
        expected,
        mockIcebergHmsCatalogSyncRequestProvider.getTableParameters(mockIcebergBaseTable));
    verify(mockIcebergBaseTable, times(1)).properties();
    verify(mockIcebergHadoopTables, never()).load(any());
  }

  public static StorageDescriptor getTestHmsTableStorageDescriptor(List<FieldSchema> columns) {
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    SerDeInfo serDeInfo = new SerDeInfo();
    storageDescriptor.setCols(columns);
    storageDescriptor.setLocation(TEST_BASE_PATH);
    storageDescriptor.setInputFormat("org.apache.iceberg.mr.hive.HiveIcebergInputFormat");
    storageDescriptor.setOutputFormat("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat");
    serDeInfo.setSerializationLib("org.apache.iceberg.mr.hive.HiveIcebergSerDe");
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  public static Map<String, String> getTestHmsTableParameters() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("EXTERNAL", "TRUE");
    parameters.put("table_type", "ICEBERG");
    parameters.put("metadata_location", ICEBERG_METADATA_FILE_LOCATION);
    parameters.put("storage_handler", "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler");
    parameters.put("iceberg.catalog", "location_based_table");
    return parameters;
  }
}
