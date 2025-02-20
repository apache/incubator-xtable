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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.catalog.CatalogPartitionSyncOperations;
import org.apache.xtable.exception.CatalogSyncException;

import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.Partition;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableResponse;

@ExtendWith(MockitoExtension.class)
public class TestGlueCatalogPartitionSyncOperations extends GlueCatalogSyncTestBase {

  private CatalogPartitionSyncOperations gluePartitionSyncOperations;

  @BeforeEach
  public void setup() {
    setupCommonMocks();
  }

  void setupCommonMocks() {
    gluePartitionSyncOperations =
        new GlueCatalogPartitionSyncOperations(mockGlueClient, mockGlueCatalogConfig);
  }

  @Test
  void testGetAllPartitionsSuccess() {
    // Mock the first response with a next token
    GetPartitionsResponse firstResponse =
        GetPartitionsResponse.builder()
            .partitions(
                Arrays.asList(
                    Partition.builder()
                        .values(Collections.singletonList("value1"))
                        .storageDescriptor(
                            StorageDescriptor.builder().location("location1").build())
                        .build(),
                    Partition.builder()
                        .values(Collections.singletonList("value2"))
                        .storageDescriptor(
                            StorageDescriptor.builder().location("location2").build())
                        .build()))
            .nextToken("token123")
            .build();

    // Mock the second response without a next token
    GetPartitionsResponse secondResponse =
        GetPartitionsResponse.builder()
            .partitions(
                Collections.singletonList(
                    Partition.builder()
                        .values(Collections.singletonList("value3"))
                        .storageDescriptor(
                            StorageDescriptor.builder().location("location3").build())
                        .build()))
            .nextToken(null)
            .build();

    when(mockGlueClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenReturn(firstResponse)
        .thenReturn(secondResponse);

    List<org.apache.xtable.catalog.CatalogPartition> partitions =
        gluePartitionSyncOperations.getAllPartitions(TEST_CATALOG_TABLE_IDENTIFIER);

    // Validate the result
    assertEquals(3, partitions.size());

    assertEquals(Collections.singletonList("value1"), partitions.get(0).getValues());
    assertEquals("location1", partitions.get(0).getStorageLocation());

    assertEquals(Collections.singletonList("value2"), partitions.get(1).getValues());
    assertEquals("location2", partitions.get(1).getStorageLocation());

    assertEquals(Collections.singletonList("value3"), partitions.get(2).getValues());
    assertEquals("location3", partitions.get(2).getStorageLocation());

    // Verify GlueClient interactions
    verify(mockGlueClient, times(2)).getPartitions(any(GetPartitionsRequest.class));
  }

  @Test
  void testGetAllPartitionsHandlesException() {
    // Mock GlueClient to throw an exception
    when(mockGlueClient.getPartitions(any(GetPartitionsRequest.class)))
        .thenThrow(new RuntimeException("Test exception"));

    // Execute and validate exception

    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () -> gluePartitionSyncOperations.getAllPartitions(TEST_CATALOG_TABLE_IDENTIFIER));

    assertInstanceOf(RuntimeException.class, exception.getCause());

    // Verify GlueClient interactions
    verify(mockGlueClient, times(1)).getPartitions(any(GetPartitionsRequest.class));
  }

  @Test
  void testAddPartitionsToTableSuccess() {
    // Mock getTable to return a valid table
    Table mockTable =
        Table.builder()
            .databaseName(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName())
            .name(TEST_CATALOG_TABLE_IDENTIFIER.getTableName())
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();
    GetTableResponse tableResponse = GetTableResponse.builder().table(mockTable).build();
    when(mockGlueClient.getTable(any(GetTableRequest.class))).thenReturn(tableResponse);
    when(mockGlueCatalogConfig.getMaxPartitionsPerRequest()).thenReturn(100);

    // Mock glueClient to return a valid response for batchCreatePartition
    BatchCreatePartitionResponse mockResponse = mock(BatchCreatePartitionResponse.class);
    when(mockGlueClient.batchCreatePartition(any(BatchCreatePartitionRequest.class)))
        .thenReturn(mockResponse);

    // Execute the method
    gluePartitionSyncOperations.addPartitionsToTable(
        TEST_CATALOG_TABLE_IDENTIFIER,
        Collections.singletonList(
            new org.apache.xtable.catalog.CatalogPartition(
                Collections.singletonList("value1"), "location1")));

    // Verify that batchCreatePartition was called
    ArgumentCaptor<BatchCreatePartitionRequest> createPartitionRequestArgumentCaptor =
        ArgumentCaptor.forClass(BatchCreatePartitionRequest.class);
    verify(mockGlueClient, times(1))
        .batchCreatePartition(createPartitionRequestArgumentCaptor.capture());

    BatchCreatePartitionRequest request = createPartitionRequestArgumentCaptor.getValue();
    assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(), request.databaseName());
    assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getTableName(), request.tableName());
    assertEquals(1, request.partitionInputList().size());
    assertEquals("location1", request.partitionInputList().get(0).storageDescriptor().location());
    assertEquals(Collections.singletonList("value1"), request.partitionInputList().get(0).values());
  }

  @Test
  void testAddPartitionsToTableHandlesException() {
    // Mock getTable to return a valid table
    Table mockTable = mock(Table.class);
    StorageDescriptor mockSd = mock(StorageDescriptor.class);
    when(mockTable.storageDescriptor()).thenReturn(mockSd);
    GetTableResponse tableResponse = GetTableResponse.builder().table(mockTable).build();
    when(mockGlueClient.getTable(any(GetTableRequest.class))).thenReturn(tableResponse);
    when(mockGlueCatalogConfig.getMaxPartitionsPerRequest()).thenReturn(100);

    // Mock glueClient to throw an exception when batchCreatePartition is called
    when(mockGlueClient.batchCreatePartition(any(BatchCreatePartitionRequest.class)))
        .thenThrow(new RuntimeException("Test exception"));

    // Test the exception handling
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () ->
                gluePartitionSyncOperations.addPartitionsToTable(
                    TEST_CATALOG_TABLE_IDENTIFIER,
                    Collections.singletonList(
                        new org.apache.xtable.catalog.CatalogPartition(
                            Collections.singletonList("value1"), "location1"))));

    // Verify that the exception was caused by a RuntimeException
    assertInstanceOf(RuntimeException.class, exception.getCause());

    // Verify glueClient.batchCreatePartition was called
    verify(mockGlueClient, times(1)).batchCreatePartition(any(BatchCreatePartitionRequest.class));
  }

  @Test
  void testUpdatePartitionsToTableSuccess() {
    // Mock getTable to return a valid table
    Table mockTable =
        Table.builder()
            .databaseName(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName())
            .name(TEST_CATALOG_TABLE_IDENTIFIER.getTableName())
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();
    GetTableResponse tableResponse = GetTableResponse.builder().table(mockTable).build();
    when(mockGlueClient.getTable(any(GetTableRequest.class))).thenReturn(tableResponse);
    when(mockGlueCatalogConfig.getMaxPartitionsPerRequest()).thenReturn(100);

    // Prepare test partition to be updated
    org.apache.xtable.catalog.CatalogPartition partitionToUpdate =
        new org.apache.xtable.catalog.CatalogPartition(
            Collections.singletonList("value1"), "new_location");

    // Mock glueClient to return a valid response for batchUpdatePartition
    BatchUpdatePartitionResponse mockResponse = mock(BatchUpdatePartitionResponse.class);
    when(mockGlueClient.batchUpdatePartition(any(BatchUpdatePartitionRequest.class)))
        .thenReturn(mockResponse);

    // Execute the method
    gluePartitionSyncOperations.updatePartitionsToTable(
        TEST_CATALOG_TABLE_IDENTIFIER, Collections.singletonList(partitionToUpdate));

    // Verify that batchUpdatePartition was called
    ArgumentCaptor<BatchUpdatePartitionRequest> updatePartitionRequestArgumentCaptor =
        ArgumentCaptor.forClass(BatchUpdatePartitionRequest.class);
    verify(mockGlueClient, times(1))
        .batchUpdatePartition(updatePartitionRequestArgumentCaptor.capture());

    // Capture the request
    BatchUpdatePartitionRequest request = updatePartitionRequestArgumentCaptor.getValue();

    // Validate the request's properties
    assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(), request.databaseName());
    assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getTableName(), request.tableName());
    assertEquals(1, request.entries().size());
    assertEquals(
        "new_location", request.entries().get(0).partitionInput().storageDescriptor().location());
    assertEquals(
        Collections.singletonList("value1"), request.entries().get(0).partitionInput().values());
  }

  @Test
  void testUpdatePartitionsToTableHandlesException() {
    // Mock getTable to return a valid table
    Table mockTable =
        Table.builder()
            .databaseName(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName())
            .name(TEST_CATALOG_TABLE_IDENTIFIER.getTableName())
            .storageDescriptor(StorageDescriptor.builder().build())
            .build();
    GetTableResponse tableResponse = GetTableResponse.builder().table(mockTable).build();
    when(mockGlueClient.getTable(any(GetTableRequest.class))).thenReturn(tableResponse);
    when(mockGlueCatalogConfig.getMaxPartitionsPerRequest()).thenReturn(100);

    // Mock glueClient to throw an exception
    when(mockGlueClient.batchUpdatePartition(any(BatchUpdatePartitionRequest.class)))
        .thenThrow(new RuntimeException("Test exception"));

    // Execute and validate exception
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () ->
                gluePartitionSyncOperations.updatePartitionsToTable(
                    TEST_CATALOG_TABLE_IDENTIFIER,
                    Collections.singletonList(
                        new org.apache.xtable.catalog.CatalogPartition(
                            Collections.singletonList("value1"), "new_location"))));

    // Verify the exception cause
    assertInstanceOf(RuntimeException.class, exception.getCause());

    // Verify glueClient.batchUpdatePartition was called
    verify(mockGlueClient, times(1)).batchUpdatePartition(any(BatchUpdatePartitionRequest.class));
  }

  @Test
  void testDropPartitionsSuccess() {
    // Prepare a partition to drop
    org.apache.xtable.catalog.CatalogPartition partitionToDrop =
        new org.apache.xtable.catalog.CatalogPartition(
            Collections.singletonList("value1"), "location1");

    // Mock glueClient to return a valid response for batchDeletePartition
    BatchDeletePartitionResponse mockResponse = mock(BatchDeletePartitionResponse.class);
    when(mockGlueClient.batchDeletePartition(any(BatchDeletePartitionRequest.class)))
        .thenReturn(mockResponse);
    when(mockGlueCatalogConfig.getMaxPartitionsPerRequest()).thenReturn(100);

    gluePartitionSyncOperations.dropPartitions(
        TEST_CATALOG_TABLE_IDENTIFIER, Collections.singletonList(partitionToDrop));

    // Verify that batchDeletePartition was called
    ArgumentCaptor<BatchDeletePartitionRequest> deletePartitionRequestArgumentCaptor =
        ArgumentCaptor.forClass(BatchDeletePartitionRequest.class);
    verify(mockGlueClient, times(1))
        .batchDeletePartition(deletePartitionRequestArgumentCaptor.capture());

    // Capture the request
    BatchDeletePartitionRequest request = deletePartitionRequestArgumentCaptor.getValue();

    // Validate the request's properties
    assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName(), request.databaseName());
    assertEquals(TEST_CATALOG_TABLE_IDENTIFIER.getTableName(), request.tableName());
    assertEquals(1, request.partitionsToDelete().size());
    assertEquals(Collections.singletonList("value1"), request.partitionsToDelete().get(0).values());
  }

  @Test
  void testDropPartitionsHandlesException() {
    // Mock glueClient to throw an exception during batchDeletePartition
    when(mockGlueClient.batchDeletePartition(any(BatchDeletePartitionRequest.class)))
        .thenThrow(new RuntimeException("Test exception"));
    when(mockGlueCatalogConfig.getMaxPartitionsPerRequest()).thenReturn(100);

    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () ->
                gluePartitionSyncOperations.dropPartitions(
                    TEST_CATALOG_TABLE_IDENTIFIER,
                    Collections.singletonList(
                        new org.apache.xtable.catalog.CatalogPartition(
                            Collections.singletonList("value1"), "location1"))));

    // Verify the exception cause
    assertInstanceOf(RuntimeException.class, exception.getCause());

    // Verify glueClient.batchDeletePartition was called
    verify(mockGlueClient, times(1)).batchDeletePartition(any(BatchDeletePartitionRequest.class));
  }

  @Test
  void testDropPartitionsNoPartitionsToDrop() {
    // Execute the method with an empty partition list
    gluePartitionSyncOperations.dropPartitions(
        TEST_CATALOG_TABLE_IDENTIFIER, Collections.emptyList());

    // Verify that batchDeletePartition was not called
    verify(mockGlueClient, times(0)).batchDeletePartition(any(BatchDeletePartitionRequest.class));
  }

  @Test
  void testGetLastTimeSyncedPropertiesSuccess() {
    // Mock table and parameters
    Map<String, String> mockParameters = new HashMap<>();
    mockParameters.put("last_synced_time", "2023-12-01T12:00:00Z");
    mockParameters.put("last_modified_by", "user123");
    Table mockTable =
        Table.builder()
            .databaseName(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName())
            .name(TEST_CATALOG_TABLE_IDENTIFIER.getTableName())
            .storageDescriptor(StorageDescriptor.builder().build())
            .parameters(mockParameters)
            .build();
    GetTableResponse tableResponse = GetTableResponse.builder().table(mockTable).build();
    when(mockGlueClient.getTable(any(GetTableRequest.class))).thenReturn(tableResponse);

    // List of keys to look for
    List<String> lastSyncedKeys = Arrays.asList("last_synced_time", "last_modified_by");

    // Execute the method
    Map<String, String> result =
        gluePartitionSyncOperations.getTableProperties(
            TEST_CATALOG_TABLE_IDENTIFIER, lastSyncedKeys);

    // Assert the result
    assertEquals(2, result.size());
    assertEquals("2023-12-01T12:00:00Z", result.get("last_synced_time"));
    assertEquals("user123", result.get("last_modified_by"));
  }

  @Test
  void testGetLastTimeSyncedPropertiesKeyNotFound() {
    // Mock table and parameters
    Map<String, String> mockParameters =
        Collections.singletonMap("last_synced_time", "2023-12-01T12:00:00Z");
    Table mockTable =
        Table.builder()
            .databaseName(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName())
            .name(TEST_CATALOG_TABLE_IDENTIFIER.getTableName())
            .storageDescriptor(StorageDescriptor.builder().build())
            .parameters(mockParameters)
            .build();
    GetTableResponse tableResponse = GetTableResponse.builder().table(mockTable).build();
    when(mockGlueClient.getTable(any(GetTableRequest.class))).thenReturn(tableResponse);

    // List of keys to look for (including a non-existent key)
    List<String> lastSyncedKeys = Arrays.asList("last_synced_time", "non_existent_key");

    // Execute the method
    Map<String, String> result =
        gluePartitionSyncOperations.getTableProperties(
            TEST_CATALOG_TABLE_IDENTIFIER, lastSyncedKeys);

    // Assert the result
    assertEquals(1, result.size());
    assertEquals("2023-12-01T12:00:00Z", result.get("last_synced_time"));
  }

  @Test
  void testGetLastTimeSyncedPropertiesHandlesException() {
    // Mock getTable to throw an exception
    when(mockGlueClient.getTable(any(GetTableRequest.class)))
        .thenThrow(new RuntimeException("Test exception"));

    // Execute and validate exception
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () ->
                gluePartitionSyncOperations.getTableProperties(
                    TEST_CATALOG_TABLE_IDENTIFIER, Collections.singletonList("last_synced_time")));

    // Verify the exception cause
    assertInstanceOf(RuntimeException.class, exception.getCause());

    // Verify that getTable was called
    verify(mockGlueClient, times(1)).getTable(any(GetTableRequest.class));
  }

  @Test
  void testUpdateLastTimeSyncedPropertiesSuccess() {
    // Mock table and parameters
    Map<String, String> mockParameters = new HashMap<>();
    mockParameters.put("last_synced_time", "2023-12-01T12:00:00Z");
    mockParameters.put("last_modified_by", "user123");
    Table mockTable =
        Table.builder()
            .databaseName(TEST_CATALOG_TABLE_IDENTIFIER.getDatabaseName())
            .name(TEST_CATALOG_TABLE_IDENTIFIER.getTableName())
            .storageDescriptor(StorageDescriptor.builder().build())
            .parameters(mockParameters)
            .build();
    GetTableResponse tableResponse = GetTableResponse.builder().table(mockTable).build();
    when(mockGlueClient.getTable(any(GetTableRequest.class))).thenReturn(tableResponse);

    // Mock the updateTable call to verify it's triggered
    when(mockGlueClient.updateTable(any(UpdateTableRequest.class)))
        .thenReturn(UpdateTableResponse.builder().build());

    // New properties to update
    Map<String, String> newProperties = new HashMap<>();
    newProperties.put("last_synced_time", "2023-12-02T14:00:00Z");
    newProperties.put("last_modified_by", "user456");

    // Execute the method
    gluePartitionSyncOperations.updateTableProperties(TEST_CATALOG_TABLE_IDENTIFIER, newProperties);

    // Capture the UpdateTableRequest sent to the GlueClient
    ArgumentCaptor<UpdateTableRequest> captor = ArgumentCaptor.forClass(UpdateTableRequest.class);
    verify(mockGlueClient, times(1)).updateTable(captor.capture());
    UpdateTableRequest capturedRequest = captor.getValue();

    // Assert that the table update request contains the new parameters
    assertEquals(
        "2023-12-02T14:00:00Z", capturedRequest.tableInput().parameters().get("last_synced_time"));
    assertEquals("user456", capturedRequest.tableInput().parameters().get("last_modified_by"));
  }

  @Test
  void testUpdateLastTimeSyncedPropertiesHandlesException() {
    // Setup mocks to throw an exception
    when(mockGlueClient.getTable(any(GetTableRequest.class)))
        .thenThrow(new RuntimeException("Test exception"));

    // New properties to update
    Map<String, String> newProperties = new HashMap<>();
    newProperties.put("last_synced_time", "2023-12-02T14:00:00Z");

    // Execute and validate exception
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () ->
                gluePartitionSyncOperations.updateTableProperties(
                    TEST_CATALOG_TABLE_IDENTIFIER, newProperties));

    // Assert that the exception has the right cause
    assertInstanceOf(RuntimeException.class, exception.getCause());
    verify(mockGlueClient, times(1)).getTable(any(GetTableRequest.class));
  }

  @Test
  void testUpdateLastTimeSyncedPropertiesEmptyProperties() {
    // Execute the method with empty properties (nothing to update)
    gluePartitionSyncOperations.updateTableProperties(
        TEST_CATALOG_TABLE_IDENTIFIER, new HashMap<>());

    // Verify that updateTable was never called
    verify(mockGlueClient, times(0)).updateTable(any(UpdateTableRequest.class));
  }
}
