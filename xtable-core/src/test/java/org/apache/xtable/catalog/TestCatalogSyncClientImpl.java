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
 
package org.apache.xtable.catalog;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.conversion.ExternalCatalog;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogType;
import org.apache.xtable.spi.sync.CatalogSyncClient;

@ExtendWith(MockitoExtension.class)
public class TestCatalogSyncClientImpl<Database, Table> {

  @Mock private CatalogSyncOperations<Database, Table> mockOperations;
  @Mock private CatalogType mockCatalogType;
  @Mock Table mockCatalogTable;
  @Mock Database mockCatalogDb;
  private CatalogSyncClient mockSyncClient;

  private static final String TEST_DATABASE = "hms_db";
  private static final String TEST_TABLE = "hms_table";
  private static final String BASE_PATH = "base-path";
  private static final InternalTable TEST_ONETABLE =
      InternalTable.builder().basePath(BASE_PATH).build();
  private static final ExternalCatalog.TableIdentifier TEST_TABLE_IDENTIFIER =
      ExternalCatalog.TableIdentifier.builder()
          .databaseName(TEST_DATABASE)
          .tableName(TEST_TABLE)
          .build();

  @BeforeEach
  void setup() {
    when(mockOperations.getTableIdentifier()).thenReturn(TEST_TABLE_IDENTIFIER);
    mockSyncClient = new CatalogSyncClientImpl<>(mockOperations, mockCatalogType);
  }

  @Test
  void testSyncTable_DatabaseAndTableDoesNotExists() {
    // mock database does not exists
    when(mockOperations.getDatabase(TEST_DATABASE)).thenReturn(null);
    // mock table does not exist
    when(mockOperations.getTable(TEST_TABLE_IDENTIFIER)).thenReturn(null);
    mockSyncClient.syncTable(TEST_ONETABLE);

    verifyCatalogOperation_DbApiCalls(1, 1);
    verifyCatalogOperation_TableApiCalls(1, 1, 0, 0);
  }

  @Test
  void testSyncTable_DatabaseExistsButTableDoesNotExists() {
    // mock databases exists
    when(mockOperations.getDatabase(TEST_DATABASE)).thenReturn(mockCatalogDb);
    // mock table does not exist
    when(mockOperations.getTable(TEST_TABLE_IDENTIFIER)).thenReturn(null);

    mockSyncClient.syncTable(TEST_ONETABLE);

    verifyCatalogOperation_DbApiCalls(1, 0);
    verifyCatalogOperation_TableApiCalls(1, 1, 0, 0);
  }

  @Test
  void testSyncTable_CreateOrReplaceTableDueToLocationMismatch() {
    // mock databases exists
    when(mockOperations.getDatabase(TEST_DATABASE)).thenReturn(mockCatalogDb);
    // mock table exists
    when(mockOperations.getTable(TEST_TABLE_IDENTIFIER)).thenReturn(mockCatalogTable);
    when(mockOperations.getStorageDescriptorLocation(mockCatalogTable))
        .thenReturn("modified-location");

    mockSyncClient.syncTable(TEST_ONETABLE);

    verifyCatalogOperation_DbApiCalls(1, 0);
    verifyCatalogOperation_TableApiCalls(1, 0, 0, 1);
  }

  @Test
  void testSyncTable_RefreshTable() {
    // mock databases exists
    when(mockOperations.getDatabase(TEST_DATABASE)).thenReturn(mockCatalogDb);
    // mock table exists
    when(mockOperations.getTable(TEST_TABLE_IDENTIFIER)).thenReturn(mockCatalogTable);
    when(mockOperations.getStorageDescriptorLocation(mockCatalogTable))
        .thenReturn(BASE_PATH);

    mockSyncClient.syncTable(TEST_ONETABLE);

    verifyCatalogOperation_DbApiCalls(1, 0);
    verifyCatalogOperation_TableApiCalls(1, 0, 1, 0);
  }

  @Test
  void testSyncTable_FailureWhenUpdatingTable() {
    // mock databases exists
    when(mockOperations.getDatabase(TEST_DATABASE)).thenReturn(mockCatalogDb);
    // mock table exists
    when(mockOperations.getTable(TEST_TABLE_IDENTIFIER)).thenReturn(mockCatalogTable);
    when(mockOperations.getStorageDescriptorLocation(mockCatalogTable))
        .thenReturn(BASE_PATH);
    doThrow(new RuntimeException("something went wrong"))
        .when(mockOperations)
        .refreshTable(TEST_ONETABLE, mockCatalogTable, TEST_TABLE_IDENTIFIER);

    assertThrows(RuntimeException.class, () -> mockSyncClient.syncTable(TEST_ONETABLE));

    verifyCatalogOperation_DbApiCalls(1, 0);
    verifyCatalogOperation_TableApiCalls(1, 0, 1, 0);
  }

  private void verifyCatalogOperation_DbApiCalls(int getDbCount, int createDbCount) {
    verify(mockOperations, times(getDbCount)).getDatabase(TEST_DATABASE);
    verify(mockOperations, times(createDbCount)).createDatabase(TEST_DATABASE);
  }

  private void verifyCatalogOperation_TableApiCalls(
      int getTableCount, int createTableCount, int refreshTable, int createOrReplaceTable) {
    verify(mockOperations, times(getTableCount)).getTable(TEST_TABLE_IDENTIFIER);
    verify(mockOperations, times(createTableCount))
        .createTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER);
    verify(mockOperations, times(refreshTable))
        .refreshTable(TEST_ONETABLE, mockCatalogTable, TEST_TABLE_IDENTIFIER);
    verify(mockOperations, times(createOrReplaceTable))
        .createOrReplaceTable(TEST_ONETABLE, TEST_TABLE_IDENTIFIER);
  }
}
