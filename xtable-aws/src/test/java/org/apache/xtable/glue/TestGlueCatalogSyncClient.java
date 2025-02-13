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
 
package org.apache.xtable.glue;

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

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Optional;
import java.util.ServiceLoader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.catalog.CatalogPartitionSyncTool;
import org.apache.xtable.catalog.CatalogTableBuilder;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.hudi.catalog.HudiCatalogPartitionSyncTool;
import org.apache.xtable.model.catalog.ThreePartHierarchicalTableIdentifier;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.spi.sync.CatalogSyncClient;

import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateDatabaseResponse;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateTableResponse;
import software.amazon.awssdk.services.glue.model.Database;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableResponse;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;

@ExtendWith(MockitoExtension.class)
public class TestGlueCatalogSyncClient extends GlueCatalogSyncTestBase {

  @Mock private CatalogTableBuilder<TableInput, Table> mockTableBuilder;
  @Mock private HudiCatalogPartitionSyncTool mockPartitionSyncTool;
  private GlueCatalogSyncClient glueCatalogSyncClient;

  private GlueCatalogSyncClient createGlueCatalogSyncClient(boolean includePartitionSyncTool) {
    Optional<CatalogPartitionSyncTool> partitionSyncToolOpt =
        includePartitionSyncTool ? Optional.of(mockPartitionSyncTool) : Optional.empty();
    return new GlueCatalogSyncClient(
        catalogConfig,
        testConfiguration,
        mockGlueCatalogConfig,
        mockGlueClient,
        mockTableBuilder,
        partitionSyncToolOpt);
  }

  void setupCommonMocks() {
    when(mockGlueCatalogConfig.getCatalogId()).thenReturn(TEST_GLUE_CATALOG_ID);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHasDatabase(boolean isDbPresent) {
    setupCommonMocks();
    glueCatalogSyncClient = createGlueCatalogSyncClient(false);
    GetDatabaseRequest dbRequest = getDbRequest(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName());
    GetDatabaseResponse dbResponse =
        GetDatabaseResponse.builder()
            .database(
                Database.builder().name(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName()).build())
            .build();
    if (isDbPresent) {
      when(mockGlueClient.getDatabase(dbRequest)).thenReturn(dbResponse);
    } else {
      when(mockGlueClient.getDatabase(dbRequest))
          .thenThrow(EntityNotFoundException.builder().message("db not found").build());
    }
    boolean output = glueCatalogSyncClient.hasDatabase(TEST_CATALOG_TABLE_IDENTIFIER);
    if (isDbPresent) {
      assertTrue(output);
    } else {
      assertFalse(output);
    }
    verify(mockGlueClient, times(1)).getDatabase(dbRequest);
  }

  @Test
  void testHasDatabaseFailure() {
    setupCommonMocks();
    glueCatalogSyncClient = createGlueCatalogSyncClient(false);
    GetDatabaseRequest dbRequest = getDbRequest(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName());
    when(mockGlueClient.getDatabase(dbRequest)).thenThrow(TEST_GLUE_EXCEPTION);
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () -> glueCatalogSyncClient.hasDatabase(TEST_CATALOG_TABLE_IDENTIFIER));
    assertEquals(
        String.format("Failed to get database: %s", TEST_GLUE_DATABASE), exception.getMessage());
    verify(mockGlueClient, times(1)).getDatabase(dbRequest);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetTable(boolean isTablePresent) {
    setupCommonMocks();
    glueCatalogSyncClient = createGlueCatalogSyncClient(false);
    GetTableRequest tableRequest =
        getTableRequest(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName());
    GetTableResponse tableResponse =
        GetTableResponse.builder()
            .table(
                Table.builder()
                    .databaseName(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName())
                    .name(TEST_CATALOG_TABLE_IDENTIFIER.getTableName())
                    .build())
            .build();
    if (isTablePresent) {
      when(mockGlueClient.getTable(tableRequest)).thenReturn(tableResponse);
    } else {
      when(mockGlueClient.getTable(tableRequest))
          .thenThrow(EntityNotFoundException.builder().message("table not found").build());
    }
    Table table = glueCatalogSyncClient.getTable(TEST_CATALOG_TABLE_IDENTIFIER);
    if (isTablePresent) {
      assertNotNull(table);
      assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(), table.databaseName());
      assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getTableName(), table.name());
    } else {
      assertNull(table);
    }
    verify(mockGlueClient, times(1)).getTable(tableRequest);
  }

  @Test
  void testGetTableFailure() {
    setupCommonMocks();
    glueCatalogSyncClient = createGlueCatalogSyncClient(false);
    GetTableRequest tableRequest =
        getTableRequest(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName());
    when(mockGlueClient.getTable(tableRequest)).thenThrow(TEST_GLUE_EXCEPTION);
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () -> glueCatalogSyncClient.getTable(TEST_CATALOG_TABLE_IDENTIFIER));
    assertEquals(
        String.format("Failed to get table: %s.%s", TEST_GLUE_DATABASE, TEST_GLUE_TABLE),
        exception.getMessage());
    verify(mockGlueClient, times(1)).getTable(tableRequest);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCreateDatabase(boolean shouldFail) {
    setupCommonMocks();
    glueCatalogSyncClient = createGlueCatalogSyncClient(false);
    CreateDatabaseRequest dbRequest =
        createDbRequest(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName());
    if (shouldFail) {
      when(mockGlueClient.createDatabase(dbRequest)).thenThrow(TEST_GLUE_EXCEPTION);
      CatalogSyncException exception =
          assertThrows(
              CatalogSyncException.class,
              () -> glueCatalogSyncClient.createDatabase(TEST_CATALOG_TABLE_IDENTIFIER));
      assertEquals(
          String.format("Failed to create database: %s", TEST_GLUE_DATABASE),
          exception.getMessage());
    } else {
      when(mockGlueClient.createDatabase(dbRequest))
          .thenReturn(CreateDatabaseResponse.builder().build());
      glueCatalogSyncClient.createDatabase(TEST_CATALOG_TABLE_IDENTIFIER);
    }
    verify(mockGlueClient, times(1)).createDatabase(dbRequest);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testDropTable(boolean shouldFail) {
    setupCommonMocks();
    glueCatalogSyncClient = createGlueCatalogSyncClient(false);
    DeleteTableRequest deleteRequest =
        deleteTableRequest(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName());
    if (shouldFail) {
      when(mockGlueClient.deleteTable(deleteRequest)).thenThrow(TEST_GLUE_EXCEPTION);
      RuntimeException exception =
          assertThrows(
              RuntimeException.class,
              () ->
                  glueCatalogSyncClient.dropTable(
                      TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER));
      assertEquals(
          String.format("Failed to drop table: %s.%s", TEST_GLUE_DATABASE, TEST_GLUE_TABLE),
          exception.getMessage());
    } else {
      when(mockGlueClient.deleteTable(deleteRequest))
          .thenReturn(DeleteTableResponse.builder().build());
      glueCatalogSyncClient.dropTable(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    }
    verify(mockGlueClient, times(1)).deleteTable(deleteRequest);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateTable_Success(boolean syncPartitions) {
    setupCommonMocks();
    glueCatalogSyncClient = createGlueCatalogSyncClient(syncPartitions);
    CreateTableRequest createTableRequest =
        createTableRequest(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(), TEST_TABLE_INPUT);
    when(mockTableBuilder.getCreateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER))
        .thenReturn(TEST_TABLE_INPUT);
    when(mockGlueClient.createTable(createTableRequest))
        .thenReturn(CreateTableResponse.builder().build());
    glueCatalogSyncClient.createTable(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    verify(mockGlueClient, times(1)).createTable(createTableRequest);
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateTable_ErrorGettingTableInput(boolean syncPartitions) {
    glueCatalogSyncClient = createGlueCatalogSyncClient(syncPartitions);
    // error when getting iceberg table input
    doThrow(new RuntimeException("something went wrong"))
        .when(mockTableBuilder)
        .getCreateTableRequest(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    assertThrows(
        RuntimeException.class,
        () ->
            glueCatalogSyncClient.createTable(
                TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER));
    verify(mockTableBuilder, times(1))
        .getCreateTableRequest(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    verify(mockGlueClient, never()).createTable(any(CreateTableRequest.class));
    verify(mockPartitionSyncTool, never())
        .syncPartitions(eq(TEST_ICEBERG_INTERNAL_TABLE), eq(TEST_CATALOG_TABLE_IDENTIFIER));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCreateTable_ErrorCreatingTable(boolean syncPartitions) {
    setupCommonMocks();
    glueCatalogSyncClient = createGlueCatalogSyncClient(syncPartitions);
    // error when creating table
    CreateTableRequest createTableRequest =
        createTableRequest(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(), TEST_TABLE_INPUT);
    when(mockTableBuilder.getCreateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER))
        .thenReturn(TEST_TABLE_INPUT);
    when(mockGlueClient.createTable(createTableRequest)).thenThrow(TEST_GLUE_EXCEPTION);
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () ->
                glueCatalogSyncClient.createTable(
                    TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER));
    assertEquals(
        String.format("Failed to create table: %s.%s", TEST_GLUE_DATABASE, TEST_GLUE_TABLE),
        exception.getMessage());
    verify(mockTableBuilder, times(1))
        .getCreateTableRequest(TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    verify(mockGlueClient, times(1)).createTable(createTableRequest);
    verify(mockPartitionSyncTool, never())
        .syncPartitions(eq(TEST_ICEBERG_INTERNAL_TABLE), eq(TEST_CATALOG_TABLE_IDENTIFIER));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRefreshTable_Success(boolean syncPartitions) {
    setupCommonMocks();
    glueCatalogSyncClient = createGlueCatalogSyncClient(syncPartitions);
    UpdateTableRequest updateTableRequest =
        updateTableRequest(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(), TEST_TABLE_INPUT);
    Table glueTable = Table.builder().parameters(Collections.emptyMap()).build();
    when(mockTableBuilder.getUpdateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER))
        .thenReturn(TEST_TABLE_INPUT);
    when(mockGlueClient.updateTable(updateTableRequest))
        .thenReturn(UpdateTableResponse.builder().build());
    glueCatalogSyncClient.refreshTable(
        TEST_ICEBERG_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER);
    verify(mockGlueClient, times(1)).updateTable(updateTableRequest);
    verify(mockTableBuilder, times(1))
        .getUpdateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER);
    if (syncPartitions) {
      verify(mockPartitionSyncTool, times(1))
          .syncPartitions(eq(TEST_ICEBERG_INTERNAL_TABLE), eq(TEST_CATALOG_TABLE_IDENTIFIER));
    } else {
      verify(mockPartitionSyncTool, never())
          .syncPartitions(eq(TEST_ICEBERG_INTERNAL_TABLE), eq(TEST_CATALOG_TABLE_IDENTIFIER));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRefreshTable_ErrorCreatingTableInput(boolean syncPartitions) {
    glueCatalogSyncClient = createGlueCatalogSyncClient(syncPartitions);
    Table glueTable = Table.builder().parameters(Collections.emptyMap()).build();

    // error while refreshing table
    doThrow(new RuntimeException("something went wrong"))
        .when(mockTableBuilder)
        .getUpdateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER);
    assertThrows(
        RuntimeException.class,
        () ->
            glueCatalogSyncClient.refreshTable(
                TEST_ICEBERG_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER));
    verify(mockTableBuilder, times(1))
        .getUpdateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER);
    verify(mockGlueClient, never()).updateTable(any(UpdateTableRequest.class));
    verify(mockPartitionSyncTool, never())
        .syncPartitions(eq(TEST_ICEBERG_INTERNAL_TABLE), eq(TEST_CATALOG_TABLE_IDENTIFIER));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testRefreshTable_ErrorRefreshingTable(boolean syncPartitions) {
    setupCommonMocks();
    glueCatalogSyncClient = createGlueCatalogSyncClient(syncPartitions);
    Table glueTable = Table.builder().parameters(Collections.emptyMap()).build();

    UpdateTableRequest updateTableRequest =
        updateTableRequest(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(), TEST_TABLE_INPUT);
    when(mockTableBuilder.getUpdateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER))
        .thenReturn(TEST_TABLE_INPUT);

    // error while refreshing table
    when(mockGlueClient.updateTable(updateTableRequest)).thenThrow(TEST_GLUE_EXCEPTION);
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () ->
                glueCatalogSyncClient.refreshTable(
                    TEST_ICEBERG_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER));
    assertEquals(
        String.format("Failed to refresh table: %s.%s", TEST_GLUE_DATABASE, TEST_GLUE_TABLE),
        exception.getMessage());
    verify(mockTableBuilder, times(1))
        .getUpdateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER);
    verify(mockGlueClient, times(1)).updateTable(updateTableRequest);
    verify(mockPartitionSyncTool, never())
        .syncPartitions(eq(TEST_ICEBERG_INTERNAL_TABLE), eq(TEST_CATALOG_TABLE_IDENTIFIER));
  }

  @Test
  void testCreateOrReplaceTable() {
    setupCommonMocks();
    glueCatalogSyncClient = createGlueCatalogSyncClient(false);
    ZonedDateTime fixedDateTime = ZonedDateTime.parse("2024-10-25T10:15:30.00Z");
    try (MockedStatic<ZonedDateTime> mockZonedDateTime = mockStatic(ZonedDateTime.class)) {
      mockZonedDateTime.when(ZonedDateTime::now).thenReturn(fixedDateTime);
      String tempTableName =
          TEST_CATALOG_TABLE_IDENTIFIER.getTableName()
              + "_temp"
              + ZonedDateTime.now().toEpochSecond();
      ThreePartHierarchicalTableIdentifier tempTableIdentifier =
          new ThreePartHierarchicalTableIdentifier(
              TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(), tempTableName);
      TableInput tableInput = TableInput.builder().name(TEST_GLUE_TABLE).build();
      TableInput tempTableInput = TableInput.builder().name(tempTableName).build();
      CreateTableRequest origCreateTableRequest =
          createTableRequest(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(), tableInput);
      CreateTableRequest tempCreateTableRequest =
          createTableRequest(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(), tempTableInput);
      DeleteTableRequest origDeleteTableRequest =
          deleteTableRequest(
              TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
              TEST_CATALOG_TABLE_IDENTIFIER.getTableName());
      DeleteTableRequest tempDeleteTableRequest =
          deleteTableRequest(
              tempTableIdentifier.getDatabaseName(), tempTableIdentifier.getTableName());

      when(mockTableBuilder.getCreateTableRequest(
              TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER))
          .thenReturn(tableInput);
      when(mockTableBuilder.getCreateTableRequest(TEST_ICEBERG_INTERNAL_TABLE, tempTableIdentifier))
          .thenReturn(tempTableInput);

      glueCatalogSyncClient.createOrReplaceTable(
          TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);

      verify(mockGlueClient, times(1)).createTable(tempCreateTableRequest);
      verify(mockGlueClient, times(1)).deleteTable(tempDeleteTableRequest);
      verify(mockGlueClient, times(1)).createTable(origCreateTableRequest);
      verify(mockGlueClient, times(1)).deleteTable(origDeleteTableRequest);

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
      if (instance.getCatalogType().equals(CatalogType.GLUE)) {
        catalogSyncClient = instance;
        break;
      }
    }
    assertNotNull(catalogSyncClient);
    assertEquals(catalogSyncClient.getClass().getName(), GlueCatalogSyncClient.class.getName());
  }
}
