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
 
package org.apache.xtable.hms;

import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
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
import java.util.Optional;
import java.util.ServiceLoader;

import lombok.SneakyThrows;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.catalog.CatalogPartitionSyncTool;
import org.apache.xtable.catalog.CatalogTableBuilder;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.catalog.ThreePartHierarchicalTableIdentifier;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.spi.sync.CatalogSyncClient;

@ExtendWith(MockitoExtension.class)
public class TestHMSCatalogSyncClient extends HMSCatalogSyncTestBase {

  @Mock private CatalogTableBuilder<Table, Table> mockTableBuilder;
  @Mock private CatalogPartitionSyncTool mockPartitionSyncTool;
  private HMSCatalogSyncClient hmsCatalogSyncClient;

  private HMSCatalogSyncClient createHMSCatalogSyncClient(boolean includePartitionSyncTool) {
    Optional<CatalogPartitionSyncTool> partitionSyncToolOpt =
        includePartitionSyncTool ? Optional.of(mockPartitionSyncTool) : Optional.empty();
    return new HMSCatalogSyncClient(
        TEST_CATALOG_CONFIG,
        mockHMSCatalogConfig,
        testConfiguration,
        mockMetaStoreClient,
        mockTableBuilder,
        partitionSyncToolOpt);
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHasDatabase(boolean isDbPresent) {
    hmsCatalogSyncClient = createHMSCatalogSyncClient(false);
    Database db = new Database(TEST_HMS_DATABASE, null, null, Collections.emptyMap());
    if (isDbPresent) {
      when(mockMetaStoreClient.getDatabase(TEST_HMS_DATABASE)).thenReturn(db);
    } else {
      when(mockMetaStoreClient.getDatabase(TEST_HMS_DATABASE))
          .thenThrow(new NoSuchObjectException("db not found"));
    }
    boolean output = hmsCatalogSyncClient.hasDatabase(TEST_CATALOG_TABLE_IDENTIFIER);
    if (isDbPresent) {
      assertTrue(output);
    } else {
      assertFalse(output);
    }
    verify(mockMetaStoreClient, times(1)).getDatabase(TEST_HMS_DATABASE);
  }

  @SneakyThrows
  @Test
  void testHasDatabaseFailure() {
    hmsCatalogSyncClient = createHMSCatalogSyncClient(false);
    when(mockMetaStoreClient.getDatabase(TEST_HMS_DATABASE))
        .thenThrow(new TException("something went wrong"));
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () -> hmsCatalogSyncClient.hasDatabase(TEST_CATALOG_TABLE_IDENTIFIER));
    assertEquals(
        String.format("Failed to get database: %s", TEST_HMS_DATABASE), exception.getMessage());
    verify(mockMetaStoreClient, times(1)).getDatabase(TEST_HMS_DATABASE);
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetTable(boolean isTablePresent) {
    hmsCatalogSyncClient = createHMSCatalogSyncClient(false);
    Table table = newTable(TEST_HMS_DATABASE, TEST_HMS_TABLE);
    if (isTablePresent) {
      when(mockMetaStoreClient.getTable(TEST_HMS_DATABASE, TEST_HMS_TABLE)).thenReturn(table);
    } else {
      when(mockMetaStoreClient.getTable(TEST_HMS_DATABASE, TEST_HMS_TABLE))
          .thenThrow(new NoSuchObjectException("db not found"));
    }
    Table hmsTable = hmsCatalogSyncClient.getTable(TEST_CATALOG_TABLE_IDENTIFIER);
    if (isTablePresent) {
      assertEquals(table, hmsTable);
    } else {
      assertNull(hmsTable);
    }
    verify(mockMetaStoreClient, times(1)).getTable(TEST_HMS_DATABASE, TEST_HMS_TABLE);
  }

  @SneakyThrows
  @Test
  void testGetTableFailure() {
    hmsCatalogSyncClient = createHMSCatalogSyncClient(false);
    when(mockMetaStoreClient.getTable(TEST_HMS_DATABASE, TEST_HMS_TABLE))
        .thenThrow(new TException("something went wrong"));
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () -> hmsCatalogSyncClient.getTable(TEST_CATALOG_TABLE_IDENTIFIER));
    assertEquals(
        String.format("Failed to get table: %s.%s", TEST_HMS_DATABASE, TEST_HMS_TABLE),
        exception.getMessage());
    verify(mockMetaStoreClient, times(1)).getTable(TEST_HMS_DATABASE, TEST_HMS_TABLE);
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCreateDatabase(boolean shouldFail) {
    hmsCatalogSyncClient = createHMSCatalogSyncClient(false);
    Database database = newDatabase(TEST_HMS_DATABASE);
    if (shouldFail) {
      Mockito.doThrow(new TException("something went wrong"))
          .when(mockMetaStoreClient)
          .createDatabase(database);
      CatalogSyncException exception =
          assertThrows(
              CatalogSyncException.class,
              () -> hmsCatalogSyncClient.createDatabase(TEST_CATALOG_TABLE_IDENTIFIER));
      assertEquals(
          String.format("Failed to create database: %s", TEST_HMS_DATABASE),
          exception.getMessage());
    } else {
      hmsCatalogSyncClient.createDatabase(TEST_CATALOG_TABLE_IDENTIFIER);
    }
    verify(mockMetaStoreClient, times(1)).createDatabase(database);
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testDropTable(boolean shouldFail) {
    hmsCatalogSyncClient = createHMSCatalogSyncClient(false);
    if (shouldFail) {
      Mockito.doThrow(new TException("something went wrong"))
          .when(mockMetaStoreClient)
          .dropTable(TEST_HMS_DATABASE, TEST_HMS_TABLE);
      CatalogSyncException exception =
          assertThrows(
              CatalogSyncException.class,
              () ->
                  hmsCatalogSyncClient.dropTable(
                      TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER));
      assertEquals(
          String.format("Failed to drop table: %s.%s", TEST_HMS_DATABASE, TEST_HMS_TABLE),
          exception.getMessage());
    } else {
      hmsCatalogSyncClient.dropTable(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    }
    verify(mockMetaStoreClient, times(1)).dropTable(TEST_HMS_DATABASE, TEST_HMS_TABLE);
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateTable_Success(boolean syncPartitions) {
    hmsCatalogSyncClient = createHMSCatalogSyncClient(syncPartitions);
    Table testTable = new Table();
    when(mockTableBuilder.getCreateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER))
        .thenReturn(testTable);
    hmsCatalogSyncClient.createTable(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    verify(mockMetaStoreClient, times(1)).createTable(testTable);
    verify(mockTableBuilder, times(1))
        .getCreateTableRequest(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    if (syncPartitions) {
      verify(mockPartitionSyncTool, times(1))
          .syncPartitions(eq(TEST_ICEBERG_INTERNAL_TABLE), eq(TEST_CATALOG_TABLE_IDENTIFIER));
    } else {
      verify(mockPartitionSyncTool, never())
          .syncPartitions(eq(TEST_ICEBERG_INTERNAL_TABLE), eq(TEST_CATALOG_TABLE_IDENTIFIER));
    }
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateTable_ErrorGettingTableInput(boolean syncPartitions) {
    hmsCatalogSyncClient = createHMSCatalogSyncClient(syncPartitions);

    // error when getting iceberg table input
    doThrow(new RuntimeException("something went wrong"))
        .when(mockTableBuilder)
        .getCreateTableRequest(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    assertThrows(
        RuntimeException.class,
        () ->
            hmsCatalogSyncClient.createTable(
                TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER));
    verify(mockTableBuilder, times(1))
        .getCreateTableRequest(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    verify(mockMetaStoreClient, never()).createTable(any());
    verify(mockPartitionSyncTool, never())
        .syncPartitions(eq(TEST_ICEBERG_INTERNAL_TABLE), eq(TEST_CATALOG_TABLE_IDENTIFIER));
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateTable_ErrorCreatingTable(boolean syncPartitions) {
    hmsCatalogSyncClient = createHMSCatalogSyncClient(syncPartitions);

    // error when creating table
    Table testTable = new Table();
    when(mockTableBuilder.getCreateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER))
        .thenReturn(testTable);
    doThrow(new TException("something went wrong"))
        .when(mockMetaStoreClient)
        .createTable(testTable);
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () ->
                hmsCatalogSyncClient.createTable(
                    TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER));
    assertEquals(
        String.format("Failed to create table: %s.%s", TEST_HMS_DATABASE, TEST_HMS_TABLE),
        exception.getMessage());
    verify(mockTableBuilder, times(1))
        .getCreateTableRequest(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    verify(mockMetaStoreClient, times(1)).createTable(testTable);
    verify(mockPartitionSyncTool, never())
        .syncPartitions(eq(TEST_ICEBERG_INTERNAL_TABLE), eq(TEST_CATALOG_TABLE_IDENTIFIER));
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRefreshTable_Success(boolean syncPartitions) {
    hmsCatalogSyncClient = createHMSCatalogSyncClient(syncPartitions);
    Table origTable = new Table();
    Table updatedTable = new Table(origTable);
    updatedTable.putToParameters(METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION_V2);
    when(mockTableBuilder.getUpdateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, origTable, TEST_CATALOG_TABLE_IDENTIFIER))
        .thenReturn(updatedTable);
    hmsCatalogSyncClient.refreshTable(
        TEST_ICEBERG_INTERNAL_TABLE, origTable, TEST_CATALOG_TABLE_IDENTIFIER);
    verify(mockMetaStoreClient, times(1))
        .alter_table(TEST_HMS_DATABASE, TEST_HMS_TABLE, updatedTable);
    verify(mockTableBuilder, times(1))
        .getUpdateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, origTable, TEST_CATALOG_TABLE_IDENTIFIER);
    if (syncPartitions) {
      verify(mockPartitionSyncTool, times(1))
          .syncPartitions(eq(TEST_ICEBERG_INTERNAL_TABLE), eq(TEST_CATALOG_TABLE_IDENTIFIER));
    } else {
      verify(mockPartitionSyncTool, never())
          .syncPartitions(eq(TEST_ICEBERG_INTERNAL_TABLE), eq(TEST_CATALOG_TABLE_IDENTIFIER));
    }
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRefreshTable_ErrorGettingUpdatedTable(boolean syncPartitions) {
    hmsCatalogSyncClient = createHMSCatalogSyncClient(syncPartitions);

    // error when getting iceberg table input
    Table testTable = new Table();
    doThrow(new RuntimeException("something went wrong"))
        .when(mockTableBuilder)
        .getUpdateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, testTable, TEST_CATALOG_TABLE_IDENTIFIER);
    assertThrows(
        RuntimeException.class,
        () ->
            hmsCatalogSyncClient.refreshTable(
                TEST_ICEBERG_INTERNAL_TABLE, testTable, TEST_CATALOG_TABLE_IDENTIFIER));
    verify(mockTableBuilder, times(1))
        .getUpdateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, testTable, TEST_CATALOG_TABLE_IDENTIFIER);
    verify(mockMetaStoreClient, never()).alter_table(any(), any(), any());
    verify(mockPartitionSyncTool, never())
        .syncPartitions(eq(TEST_ICEBERG_INTERNAL_TABLE), eq(TEST_CATALOG_TABLE_IDENTIFIER));
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRefreshTable_ErrorRefreshingTable(boolean syncPartitions) {
    hmsCatalogSyncClient = createHMSCatalogSyncClient(syncPartitions);

    // error when creating table
    Table origTable = new Table();
    Table updatedTable = new Table(origTable);
    updatedTable.putToParameters(METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION_V2);
    when(mockTableBuilder.getUpdateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, origTable, TEST_CATALOG_TABLE_IDENTIFIER))
        .thenReturn(updatedTable);
    doThrow(new TException("something went wrong"))
        .when(mockMetaStoreClient)
        .alter_table(TEST_HMS_DATABASE, TEST_HMS_TABLE, updatedTable);
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () ->
                hmsCatalogSyncClient.refreshTable(
                    TEST_ICEBERG_INTERNAL_TABLE, origTable, TEST_CATALOG_TABLE_IDENTIFIER));
    assertEquals(
        String.format("Failed to refresh table: %s.%s", TEST_HMS_DATABASE, TEST_HMS_TABLE),
        exception.getMessage());
    verify(mockTableBuilder, times(1))
        .getUpdateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, origTable, TEST_CATALOG_TABLE_IDENTIFIER);
    verify(mockMetaStoreClient, times(1))
        .alter_table(TEST_HMS_DATABASE, TEST_HMS_TABLE, updatedTable);
    verify(mockPartitionSyncTool, never())
        .syncPartitions(eq(TEST_ICEBERG_INTERNAL_TABLE), eq(TEST_CATALOG_TABLE_IDENTIFIER));
  }

  @SneakyThrows
  @Test
  void testCreateOrReplaceTable() {
    hmsCatalogSyncClient = createHMSCatalogSyncClient(false);

    ZonedDateTime zonedDateTime =
        Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault());
    try (MockedStatic<ZonedDateTime> mockZonedDateTime = mockStatic(ZonedDateTime.class)) {
      mockZonedDateTime.when(ZonedDateTime::now).thenReturn(zonedDateTime);

      String tempTableName = TEST_HMS_TABLE + "_temp" + ZonedDateTime.now().toEpochSecond();
      final ThreePartHierarchicalTableIdentifier tempTableIdentifier =
          new ThreePartHierarchicalTableIdentifier(
              TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(), tempTableName);

      Table table = newTable(TEST_HMS_DATABASE, TEST_HMS_TABLE);
      Table tempTable = newTable(TEST_HMS_DATABASE, tempTableName);

      when(mockTableBuilder.getCreateTableRequest(
              TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER))
          .thenReturn(table);
      when(mockTableBuilder.getCreateTableRequest(TEST_ICEBERG_INTERNAL_TABLE, tempTableIdentifier))
          .thenReturn(tempTable);

      hmsCatalogSyncClient.createOrReplaceTable(
          TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);

      verify(mockMetaStoreClient, times(1)).createTable(table);
      verify(mockMetaStoreClient, times(1))
          .dropTable(TEST_HMS_DATABASE, TEST_CATALOG_TABLE_IDENTIFIER.getTableName());
      verify(mockMetaStoreClient, times(1)).createTable(tempTable);
      verify(mockMetaStoreClient, times(1))
          .dropTable(TEST_HMS_DATABASE, tempTableIdentifier.getTableName());

      verify(mockTableBuilder, times(1))
          .getCreateTableRequest(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
      verify(mockTableBuilder, times(1))
          .getCreateTableRequest(TEST_ICEBERG_INTERNAL_TABLE, tempTableIdentifier);
    }
  }

  @Test
  void testLoadInstanceByServiceLoader() {
    ServiceLoader<CatalogSyncClient> loader = ServiceLoader.load(CatalogSyncClient.class);
    CatalogSyncClient catalogSyncClient = null;

    for (CatalogSyncClient instance : loader) {
      if (instance.getCatalogType().equals(CatalogType.HMS)) {
        catalogSyncClient = instance;
        break;
      }
    }
    assertNotNull(catalogSyncClient);
    assertEquals(catalogSyncClient.getClass().getName(), HMSCatalogSyncClient.class.getName());
  }
}
