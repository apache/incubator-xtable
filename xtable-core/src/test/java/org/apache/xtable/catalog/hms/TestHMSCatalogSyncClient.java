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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Collections;

import lombok.SneakyThrows;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;

@ExtendWith(MockitoExtension.class)
public class TestHMSCatalogSyncClient extends HMSCatalogSyncClientTestBase {

  @Mock private IcebergHMSCatalogSyncOperations mockIcebergHmsCatalogSyncClient;
  private HMSCatalogSyncClient hmsCatalogSyncClient;

  private HMSCatalogSyncClient createHMSCatalogSyncClient() {
    return new HMSCatalogSyncClient(
        TEST_TARGET_CATALOG,
        mockHMSCatalogConfig,
        testConfiguration,
        mockMetaStoreClient,
        mockHmsSchemaExtractor,
        mockIcebergHmsCatalogSyncClient);
  }

  void setupCommonMocks() {
    hmsCatalogSyncClient = createHMSCatalogSyncClient();
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHasDatabase(boolean isDbPresent) {
    setupCommonMocks();
    Database db = new Database(TEST_HMS_DATABASE, null, null, Collections.emptyMap());
    if (isDbPresent) {
      when(mockMetaStoreClient.getDatabase(TEST_HMS_DATABASE)).thenReturn(db);
    } else {
      when(mockMetaStoreClient.getDatabase(TEST_HMS_DATABASE))
          .thenThrow(new NoSuchObjectException("db not found"));
    }
    boolean output = hmsCatalogSyncClient.hasDatabase(TEST_HMS_DATABASE);
    if (isDbPresent) {
      assertTrue(output);
    } else {
      assertFalse(output);
    }
  }

  @SneakyThrows
  @Test
  void testHasDatabaseFailure() {
    setupCommonMocks();
    when(mockMetaStoreClient.getDatabase(TEST_HMS_DATABASE))
        .thenThrow(new TException("something went wrong"));
    assertThrows(
        CatalogSyncException.class, () -> hmsCatalogSyncClient.hasDatabase(TEST_HMS_DATABASE));
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetTable(boolean isTablePresent) {
    setupCommonMocks();
    Table table = newTable(TEST_HMS_DATABASE, TEST_HMS_TABLE);
    if (isTablePresent) {
      when(mockMetaStoreClient.getTable(TEST_HMS_DATABASE, TEST_HMS_TABLE)).thenReturn(table);
    } else {
      when(mockMetaStoreClient.getTable(TEST_HMS_DATABASE, TEST_HMS_TABLE))
          .thenThrow(new NoSuchObjectException("db not found"));
    }
    Table hmsTable = hmsCatalogSyncClient.getTable(TEST_CATALOG_TABLE_IDENTIFIER);
    if (isTablePresent) {
      Assertions.assertEquals(table, hmsTable);
    } else {
      assertNull(hmsTable);
    }
  }

  @SneakyThrows
  @Test
  void testGetTableFailure() {
    setupCommonMocks();
    when(mockMetaStoreClient.getTable(TEST_HMS_DATABASE, TEST_HMS_TABLE))
        .thenThrow(new TException("something went wrong"));
    assertThrows(
        CatalogSyncException.class,
        () -> hmsCatalogSyncClient.getTable(TEST_CATALOG_TABLE_IDENTIFIER));
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCreateDatabase(boolean shouldFail) {
    setupCommonMocks();
    Database database = newDatabase(TEST_HMS_DATABASE);
    if (shouldFail) {
      Mockito.doThrow(new TException("something went wrong"))
          .when(mockMetaStoreClient)
          .createDatabase(database);
      assertThrows(
          CatalogSyncException.class, () -> hmsCatalogSyncClient.createDatabase(TEST_HMS_DATABASE));
    } else {
      hmsCatalogSyncClient.createDatabase(TEST_HMS_DATABASE);
      verify(mockMetaStoreClient, times(1)).createDatabase(database);
    }
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testDropTable(boolean shouldFail) {
    setupCommonMocks();
    if (shouldFail) {
      Mockito.doThrow(new TException("something went wrong"))
          .when(mockMetaStoreClient)
          .dropTable(TEST_HMS_DATABASE, TEST_HMS_TABLE);
      assertThrows(
          CatalogSyncException.class,
          () ->
              hmsCatalogSyncClient.dropTable(
                  TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER));
    } else {
      hmsCatalogSyncClient.dropTable(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
      verify(mockMetaStoreClient, times(1)).dropTable(TEST_HMS_DATABASE, TEST_HMS_TABLE);
    }
  }

  @Test
  void testCreateTable() {
    setupCommonMocks();
    hmsCatalogSyncClient.createTable(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    verify(mockIcebergHmsCatalogSyncClient, times(1))
        .createTable(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
  }

  @Test
  void testCreateTable_Failures() {
    setupCommonMocks();

    // Unsupported table format
    assertThrows(
        NotSupportedException.class,
        () ->
            hmsCatalogSyncClient.createTable(
                TEST_HUDI_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER));
    verify(mockIcebergHmsCatalogSyncClient, never()).createTable(any(), any());

    // error while creating table
    doThrow(new RuntimeException("something went wrong"))
        .when(mockIcebergHmsCatalogSyncClient)
        .createTable(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    assertThrows(
        RuntimeException.class,
        () ->
            hmsCatalogSyncClient.createTable(
                TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER));
    verify(mockIcebergHmsCatalogSyncClient, times(1))
        .createTable(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
  }

  @Test
  void testRefreshTable() {
    setupCommonMocks();
    Table hmsTable = newTable(TEST_HMS_DATABASE, TEST_HMS_TABLE);

    hmsCatalogSyncClient.refreshTable(
        TEST_ICEBERG_INTERNAL_TABLE, hmsTable, TEST_CATALOG_TABLE_IDENTIFIER);
    verify(mockIcebergHmsCatalogSyncClient, times(1))
        .refreshTable(TEST_ICEBERG_INTERNAL_TABLE, hmsTable, TEST_CATALOG_TABLE_IDENTIFIER);
  }

  @Test
  void testRefreshTable_Failures() {
    setupCommonMocks();
    Table hmsTable = newTable(TEST_HMS_DATABASE, TEST_HMS_TABLE);

    // Unsupported table format
    assertThrows(
        NotSupportedException.class,
        () ->
            hmsCatalogSyncClient.refreshTable(
                TEST_HUDI_INTERNAL_TABLE, hmsTable, TEST_CATALOG_TABLE_IDENTIFIER));
    verify(mockIcebergHmsCatalogSyncClient, never()).refreshTable(any(), any(), any());

    // error while refreshing table
    doThrow(new RuntimeException("something went wrong"))
        .when(mockIcebergHmsCatalogSyncClient)
        .refreshTable(TEST_ICEBERG_INTERNAL_TABLE, hmsTable, TEST_CATALOG_TABLE_IDENTIFIER);
    assertThrows(
        RuntimeException.class,
        () ->
            hmsCatalogSyncClient.refreshTable(
                TEST_ICEBERG_INTERNAL_TABLE, hmsTable, TEST_CATALOG_TABLE_IDENTIFIER));
    verify(mockIcebergHmsCatalogSyncClient, times(1))
        .refreshTable(TEST_ICEBERG_INTERNAL_TABLE, hmsTable, TEST_CATALOG_TABLE_IDENTIFIER);
  }

  @SneakyThrows
  @Test
  void testCreateOrReplaceTable() {
    setupCommonMocks();

    ZonedDateTime zonedDateTime =
        Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault());
    try (MockedStatic<ZonedDateTime> mockZonedDateTime = mockStatic(ZonedDateTime.class)) {
      mockZonedDateTime.when(ZonedDateTime::now).thenReturn(zonedDateTime);

      final CatalogTableIdentifier tempTableIdentifier =
          CatalogTableIdentifier.builder()
              .databaseName(TEST_HMS_DATABASE)
              .tableName(TEST_HMS_TABLE + "_temp" + ZonedDateTime.now().toEpochSecond())
              .build();

      hmsCatalogSyncClient.createOrReplaceTable(
          TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);

      verify(mockIcebergHmsCatalogSyncClient, times(1))
          .createTable(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
      verify(mockIcebergHmsCatalogSyncClient, times(1))
          .createTable(TEST_ICEBERG_INTERNAL_TABLE, tempTableIdentifier);

      verify(mockMetaStoreClient, times(1))
          .dropTable(TEST_HMS_DATABASE, TEST_CATALOG_TABLE_IDENTIFIER.getTableName());
      verify(mockMetaStoreClient, times(1))
          .dropTable(TEST_HMS_DATABASE, tempTableIdentifier.getTableName());
    }
  }
}
