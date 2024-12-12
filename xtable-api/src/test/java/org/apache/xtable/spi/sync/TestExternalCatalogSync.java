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
 
package org.apache.xtable.spi.sync;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.sync.SyncResult;

@ExtendWith(MockitoExtension.class)
public class TestExternalCatalogSync<TABLE> {

  private final CatalogSyncClient<TABLE> mockClient1 = mock(CatalogSyncClient.class);
  private final CatalogSyncClient<TABLE> mockClient2 = mock(CatalogSyncClient.class);
  private final CatalogSyncClient<TABLE> mockClient3 = mock(CatalogSyncClient.class);
  private final CatalogSyncClient<TABLE> mockClient4 = mock(CatalogSyncClient.class);

  private final CatalogTableIdentifier tableIdentifier1 =
      CatalogTableIdentifier.builder().databaseName("database1").tableName("table1").build();
  private final CatalogTableIdentifier tableIdentifier2 =
      CatalogTableIdentifier.builder().databaseName("database2").tableName("table2").build();
  private final CatalogTableIdentifier tableIdentifier3 =
      CatalogTableIdentifier.builder().databaseName("database3").tableName("table3").build();

  @Mock TABLE mockTable;
  private final InternalTable internalTable =
      InternalTable.builder()
          .readSchema(InternalSchema.builder().name("test_schema").build())
          .partitioningFields(
              Collections.singletonList(
                  InternalPartitionField.builder()
                      .sourceField(InternalField.builder().name("partition_field").build())
                      .transformType(PartitionTransformType.VALUE)
                      .build()))
          .latestCommitTime(Instant.now().minus(10, ChronoUnit.MINUTES))
          .basePath("/tmp/test")
          .build();

  @Test
  void testSyncTable() {
    when(mockClient1.getTableIdentifier()).thenReturn(tableIdentifier1);
    when(mockClient2.getTableIdentifier()).thenReturn(tableIdentifier2);
    when(mockClient3.getTableIdentifier()).thenReturn(tableIdentifier3);

    when(mockClient1.hasDatabase("database1")).thenReturn(false);
    when(mockClient2.hasDatabase("database2")).thenReturn(true);
    when(mockClient3.hasDatabase("database3")).thenReturn(true);

    when(mockClient1.getTable(tableIdentifier1)).thenReturn(mockTable);
    when(mockClient2.getTable(tableIdentifier2)).thenReturn(null);
    when(mockClient3.getTable(tableIdentifier3)).thenReturn(mockTable);

    when(mockClient1.getStorageDescriptorLocation(any())).thenReturn("/tmp/test_changed");
    when(mockClient2.getStorageDescriptorLocation(any())).thenReturn("/tmp/test");
    when(mockClient3.getStorageDescriptorLocation(any())).thenReturn("/tmp/test");

    when(mockClient4.getCatalogId()).thenReturn("catalogId4");
    when(mockClient4.getCatalogImpl()).thenReturn("catalogImpl4");

    List<SyncResult.CatalogSyncStatus> results =
        CatalogSync.getInstance()
            .syncTable(
                Arrays.asList(mockClient1, mockClient2, mockClient3, mockClient4), internalTable)
            .getCatalogSyncStatusList();
    List<SyncResult.CatalogSyncStatus> errorStatus =
        results.stream()
            .filter(status -> status.getStatusCode().equals(SyncResult.SyncStatusCode.ERROR))
            .collect(Collectors.toList());
    assertEquals(SyncResult.SyncStatusCode.ERROR, errorStatus.get(0).getStatusCode());
    assertEquals(
        3,
        results.stream()
            .map(SyncResult.CatalogSyncStatus::getStatusCode)
            .filter(statusCode -> statusCode.equals(SyncResult.SyncStatusCode.SUCCESS))
            .count());

    verify(mockClient1, times(1)).createDatabase("database1");
    verify(mockClient1, times(1)).createOrReplaceTable(internalTable, tableIdentifier1);
    verify(mockClient2, times(1)).createTable(eq(internalTable), eq(tableIdentifier2));
    verify(mockClient3, times(1)).refreshTable(eq(internalTable), any(), eq(tableIdentifier3));
  }
}
