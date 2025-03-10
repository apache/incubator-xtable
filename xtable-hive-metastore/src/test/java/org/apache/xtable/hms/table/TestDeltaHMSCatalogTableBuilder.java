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
import static org.mockito.Mockito.mockStatic;

import java.time.Instant;
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
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.hms.HMSCatalogSyncClientTestBase;
import org.apache.xtable.model.storage.TableFormat;

@ExtendWith(MockitoExtension.class)
public class TestDeltaHMSCatalogTableBuilder extends HMSCatalogSyncClientTestBase {

  private DeltaHMSCatalogTableBuilder mockDeltaHmsCatalogSyncRequestProvider;

  private DeltaHMSCatalogTableBuilder createDeltaHMSCatalogTableBuilder() {
    return new DeltaHMSCatalogTableBuilder();
  }

  @SneakyThrows
  @Test
  void testGetCreateTableRequest() {
    mockDeltaHmsCatalogSyncRequestProvider = createDeltaHMSCatalogTableBuilder();

    Instant createdTime = Instant.now();
    try (MockedStatic<Instant> mockZonedDateTime = mockStatic(Instant.class)) {
      mockZonedDateTime.when(Instant::now).thenReturn(createdTime);
      Table expected = new Table();
      expected.setDbName(TEST_HMS_DATABASE);
      expected.setTableName(TEST_HMS_TABLE);
      expected.setOwner(UserGroupInformation.getCurrentUser().getShortUserName());
      expected.setCreateTime((int) createdTime.getEpochSecond());
      expected.setSd(getTestStorageDescriptor(FIELD_SCHEMA));
      expected.setTableType("EXTERNAL_TABLE");
      expected.setParameters(getTestParameters());

      assertEquals(
          expected,
          mockDeltaHmsCatalogSyncRequestProvider.getCreateTableRequest(
              TEST_DELTA_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER));
    }
  }

  @SneakyThrows
  @Test
  void testGetUpdateTableRequest() {
    mockDeltaHmsCatalogSyncRequestProvider = createDeltaHMSCatalogTableBuilder();

    Table hmsTable =
        newTable(
            TEST_HMS_DATABASE,
            TEST_HMS_TABLE,
            getTestParameters(),
            getTestStorageDescriptor(FIELD_SCHEMA));
    Table expected = new Table(hmsTable);
    expected.getSd().setCols(UPDATED_FIELD_SCHEMA);

    Table output =
        mockDeltaHmsCatalogSyncRequestProvider.getUpdateTableRequest(
            TEST_UPDATED_DELTA_INTERNAL_TABLE, hmsTable, TEST_CATALOG_TABLE_IDENTIFIER);
    assertEquals(expected, output);
  }

  @Test
  void testGetStorageDescriptor() {
    mockDeltaHmsCatalogSyncRequestProvider = createDeltaHMSCatalogTableBuilder();
    StorageDescriptor expected = getTestStorageDescriptor(FIELD_SCHEMA);
    assertEquals(
        expected,
        mockDeltaHmsCatalogSyncRequestProvider.getStorageDescriptor(TEST_DELTA_INTERNAL_TABLE));
  }

  @Test
  void testGetTableParameters() {
    mockDeltaHmsCatalogSyncRequestProvider = createDeltaHMSCatalogTableBuilder();
    Map<String, String> expected = getTestParameters();
    assertEquals(expected, mockDeltaHmsCatalogSyncRequestProvider.getTableParameters());
  }

  private StorageDescriptor getTestStorageDescriptor(List<FieldSchema> columns) {
    Map<String, String> serDeParams = new HashMap<>();
    serDeParams.put("serialization.format", "1");
    serDeParams.put("path", TEST_BASE_PATH);

    StorageDescriptor storageDescriptor = new StorageDescriptor();
    storageDescriptor.setCols(columns);
    storageDescriptor.setLocation(TEST_BASE_PATH);
    SerDeInfo serDeInfo = new SerDeInfo();
    serDeInfo.setParameters(serDeParams);
    storageDescriptor.setSerdeInfo(serDeInfo);
    return storageDescriptor;
  }

  private Map<String, String> getTestParameters() {
    Map<String, String> parameters = new HashMap<>();
    parameters.put("EXTERNAL", "TRUE");
    parameters.put("table_type", TableFormat.DELTA);
    parameters.put("storage_handler", "io.delta.hive.DeltaStorageHandler");
    return parameters;
  }
}
