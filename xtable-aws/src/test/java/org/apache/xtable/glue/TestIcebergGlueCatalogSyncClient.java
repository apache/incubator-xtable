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

import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.hadoop.HadoopTables;

import org.apache.xtable.catalog.ExternalCatalogConfig;
import org.apache.xtable.conversion.TargetCatalog;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.storage.TableFormat;

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
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;

@ExtendWith(MockitoExtension.class)
public class TestIcebergGlueCatalogSyncClient extends GlueCatalogSyncClientTestBase {

  @Mock private HadoopTables mockIcebergHadoopTables;
  @Mock private BaseTable mockIcebergBaseTable;
  @Mock private TableOperations mockIcebergTableOperations;
  @Mock private TableMetadata mockIcebergTableMetadata;
  private IcebergGlueCatalogSyncClient mockIcebergGlueCatalogSyncClient;
  private static final String TEST_CATALOG_IDENTIFIER = "iceberg-aws-glue-1";
  private static final TargetCatalog mockTargetCatalog =
      TargetCatalog.builder()
          .catalogId(TEST_CATALOG_IDENTIFIER)
          .catalogTableIdentifier(TEST_CATALOG_TABLE_IDENTIFIER)
          .catalogConfig(ExternalCatalogConfig.builder().catalogImpl("").catalogName("").build())
          .build();

  private static final String ICEBERG_METADATA_FILE_LOCATION = "base-path/metadata";

  private IcebergGlueCatalogSyncClient createIcebergGlueCatalogSyncClient() {
    return new IcebergGlueCatalogSyncClient(
        mockTargetCatalog,
        mockConfiguration,
        mockGlueCatalogConfig,
        mockGlueClient,
        mockIcebergHadoopTables,
        mockGlueSchemaExtractor);
  }

  void setupCommonMocks() {
    mockIcebergGlueCatalogSyncClient = createIcebergGlueCatalogSyncClient();
    when(mockGlueCatalogConfig.getCatalogId()).thenReturn(TEST_GLUE_CATALOG_ID);
  }

  void mockIcebergHadoopTables() {
    when(mockIcebergHadoopTables.load(TEST_BASE_PATH)).thenReturn(mockIcebergBaseTable);
    mockIcebergMetadataFileLocation();
  }

  void mockIcebergMetadataFileLocation() {
    when(mockIcebergBaseTable.operations()).thenReturn(mockIcebergTableOperations);
    when(mockIcebergTableOperations.current()).thenReturn(mockIcebergTableMetadata);
    when(mockIcebergTableMetadata.metadataFileLocation()).thenReturn(ICEBERG_METADATA_FILE_LOCATION);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testHasDatabase(boolean isDbPresent) {
    setupCommonMocks();
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
    boolean output =
        mockIcebergGlueCatalogSyncClient.hasDatabase(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName());
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
    GetDatabaseRequest dbRequest = getDbRequest(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName());
    when(mockGlueClient.getDatabase(dbRequest))
        .thenThrow(new RuntimeException("something went wrong"));
    assertThrows(
        CatalogSyncException.class,
        () ->
            mockIcebergGlueCatalogSyncClient.hasDatabase(
                TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName()));
    verify(mockGlueClient, times(1)).getDatabase(dbRequest);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testGetTable(boolean isTablePresent) {
    setupCommonMocks();
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
    Table table = mockIcebergGlueCatalogSyncClient.getTable(TEST_CATALOG_TABLE_IDENTIFIER);
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
    GetTableRequest tableRequest =
        getTableRequest(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName());
    when(mockGlueClient.getTable(tableRequest))
        .thenThrow(new RuntimeException("something went wrong"));
    assertThrows(
        CatalogSyncException.class,
        () -> mockIcebergGlueCatalogSyncClient.getTable(TEST_CATALOG_TABLE_IDENTIFIER));
    verify(mockGlueClient, times(1)).getTable(tableRequest);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCreateDatabase(boolean shouldFail) {
    setupCommonMocks();
    CreateDatabaseRequest dbRequest =
        createDbRequest(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName());
    if (shouldFail) {
      when(mockGlueClient.createDatabase(dbRequest))
          .thenThrow(new RuntimeException("something went wrong"));
      assertThrows(
          CatalogSyncException.class,
          () ->
              mockIcebergGlueCatalogSyncClient.createDatabase(
                  TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName()));
    } else {
      when(mockGlueClient.createDatabase(dbRequest))
          .thenReturn(CreateDatabaseResponse.builder().build());
      mockIcebergGlueCatalogSyncClient.createDatabase(
          TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName());
    }
    verify(mockGlueClient, times(1)).createDatabase(dbRequest);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testDropTable(boolean shouldFail) {
    setupCommonMocks();
    DeleteTableRequest deleteRequest =
        deleteTableRequest(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName());
    if (shouldFail) {
      when(mockGlueClient.deleteTable(deleteRequest))
          .thenThrow(new RuntimeException("something went wrong"));
      assertThrows(
          RuntimeException.class,
          () ->
              mockIcebergGlueCatalogSyncClient.dropTable(
                  TEST_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER));
    } else {
      when(mockGlueClient.deleteTable(deleteRequest))
          .thenReturn(DeleteTableResponse.builder().build());
      mockIcebergGlueCatalogSyncClient.dropTable(TEST_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    }
    verify(mockGlueClient, times(1)).deleteTable(deleteRequest);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testCreateTable(boolean shouldFail) {
    setupCommonMocks();
    mockIcebergHadoopTables();
    when(mockGlueSchemaExtractor.toColumns(TableFormat.ICEBERG, TEST_INTERNAL_TABLE.getReadSchema()))
        .thenReturn(Collections.emptyList());
    CreateTableRequest createTableRequest =
        createTableRequest(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName(),
            mockIcebergGlueCatalogSyncClient.getTableParameters(mockIcebergBaseTable));
    if (shouldFail) {
      when(mockGlueClient.createTable(createTableRequest))
          .thenThrow(new RuntimeException("something went wrong"));
      assertThrows(
          CatalogSyncException.class,
          () ->
              mockIcebergGlueCatalogSyncClient.createTable(
                  TEST_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER));
    } else {
      when(mockGlueClient.createTable(createTableRequest))
          .thenReturn(CreateTableResponse.builder().build());
      mockIcebergGlueCatalogSyncClient.createTable(TEST_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    }
    verify(mockGlueClient, times(1)).createTable(createTableRequest);
    verify(mockGlueSchemaExtractor, times(1))
        .toColumns(TableFormat.ICEBERG, TEST_INTERNAL_TABLE.getReadSchema());
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void testRefreshTable(boolean shouldFail) {
    setupCommonMocks();
    mockIcebergHadoopTables();
    Map<String, String> glueTableParams = new HashMap<>();
    glueTableParams.put(METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION);
    Table glueTable = Table.builder().parameters(glueTableParams).build();
    Map<String, String> parameters = new HashMap<>();
    parameters.put(PREVIOUS_METADATA_LOCATION_PROP, glueTableParams.get(METADATA_LOCATION_PROP));
    parameters.putAll(mockIcebergGlueCatalogSyncClient.getTableParameters(mockIcebergBaseTable));
    when(mockGlueSchemaExtractor.toColumns(
            TableFormat.ICEBERG, TEST_INTERNAL_TABLE.getReadSchema(), glueTable))
        .thenReturn(Collections.emptyList());
    UpdateTableRequest tableReq =
        updateTableRequest(
            TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
            TEST_CATALOG_TABLE_IDENTIFIER.getTableName(),
            parameters);
    if (shouldFail) {
      when(mockGlueClient.updateTable(tableReq))
          .thenThrow(new RuntimeException("something went wrong"));
      assertThrows(
          RuntimeException.class,
          () ->
              mockIcebergGlueCatalogSyncClient.refreshTable(
                  TEST_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER));
    } else {
      when(mockGlueClient.updateTable(tableReq)).thenReturn(UpdateTableResponse.builder().build());
      mockIcebergGlueCatalogSyncClient.refreshTable(
          TEST_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER);
    }
    verify(mockGlueClient, times(1)).updateTable(tableReq);
    verify(mockGlueSchemaExtractor, times(1))
        .toColumns(TableFormat.ICEBERG, TEST_INTERNAL_TABLE.getReadSchema(), glueTable);
  }

  @Test
  void testCreateOrReplaceTable() {
    setupCommonMocks();
    mockIcebergHadoopTables();
    when(mockGlueSchemaExtractor.toColumns(TableFormat.ICEBERG, TEST_INTERNAL_TABLE.getReadSchema()))
        .thenReturn(Collections.emptyList());
    ZonedDateTime fixedDateTime = ZonedDateTime.parse("2024-10-25T10:15:30.00Z");
    try (MockedStatic<ZonedDateTime> mockZonedDateTime = mockStatic(ZonedDateTime.class)) {
      mockZonedDateTime.when(ZonedDateTime::now).thenReturn(fixedDateTime);
      CreateTableRequest mainCreateTableRequest =
          createTableRequest(
              TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
              TEST_CATALOG_TABLE_IDENTIFIER.getTableName(),
              mockIcebergGlueCatalogSyncClient.getTableParameters(mockIcebergBaseTable));
      DeleteTableRequest mainTableDeleteRequest =
          deleteTableRequest(
              TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
              TEST_CATALOG_TABLE_IDENTIFIER.getTableName());
      CreateTableRequest tempCreateTableRequest =
          createTableRequest(
              TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
              TEST_CATALOG_TABLE_IDENTIFIER.getTableName()
                  + "_temp"
                  + ZonedDateTime.now().toEpochSecond(),
              mockIcebergGlueCatalogSyncClient.getTableParameters(mockIcebergBaseTable));
      DeleteTableRequest tempTableDeleteRequest =
          deleteTableRequest(
              TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(),
              TEST_CATALOG_TABLE_IDENTIFIER.getTableName()
                  + "_temp"
                  + ZonedDateTime.now().toEpochSecond());

      when(mockGlueClient.createTable(mainCreateTableRequest))
          .thenReturn(CreateTableResponse.builder().build());
      when(mockGlueClient.createTable(tempCreateTableRequest))
          .thenReturn(CreateTableResponse.builder().build());
      when(mockGlueClient.deleteTable(mainTableDeleteRequest))
          .thenReturn(DeleteTableResponse.builder().build());
      when(mockGlueClient.deleteTable(tempTableDeleteRequest))
          .thenReturn(DeleteTableResponse.builder().build());

      mockIcebergGlueCatalogSyncClient.createOrReplaceTable(
          TEST_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);

      verify(mockGlueClient, times(1)).createTable(tempCreateTableRequest);
      verify(mockGlueClient, times(1)).deleteTable(tempTableDeleteRequest);
      verify(mockGlueClient, times(1)).createTable(mainCreateTableRequest);
      verify(mockGlueClient, times(1)).deleteTable(mainTableDeleteRequest);
      verify(mockGlueSchemaExtractor, times(2))
          .toColumns(TableFormat.ICEBERG, TEST_INTERNAL_TABLE.getReadSchema());
    }
  }

  @Test
  void testGetTableParameters() {
    mockIcebergMetadataFileLocation();
    Map<String, String> expected = new HashMap<>();
    expected.put(TABLE_TYPE_PROP, TableFormat.ICEBERG);
    expected.put(METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION);
    mockIcebergGlueCatalogSyncClient = createIcebergGlueCatalogSyncClient();
    Map<String, String> tableParameters =
        mockIcebergGlueCatalogSyncClient.getTableParameters(mockIcebergBaseTable);
    assertEquals(expected, tableParameters);
  }
}
