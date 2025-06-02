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
 
package org.apache.xtable.hudi.catalog;

import static org.apache.xtable.hudi.catalog.HudiCatalogPartitionSyncTool.LAST_COMMIT_COMPLETION_TIME_SYNC;
import static org.apache.xtable.hudi.catalog.HudiCatalogPartitionSyncTool.LAST_COMMIT_TIME_SYNC;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import lombok.SneakyThrows;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import org.apache.xtable.avro.AvroSchemaConverter;
import org.apache.xtable.catalog.CatalogPartition;
import org.apache.xtable.catalog.CatalogPartitionSyncOperations;
import org.apache.xtable.hudi.HudiTableManager;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.ThreePartHierarchicalTableIdentifier;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;

@ExtendWith(MockitoExtension.class)
public class TestHudiCatalogPartitionSyncTool {

  protected static final String HMS_DATABASE = "hms_db";
  protected static final String HMS_TABLE = "hms_table";
  protected static final String TEST_BASE_PATH = "test-base-path";

  protected static String avroSchema =
      "{\"type\":\"record\",\"name\":\"SimpleRecord\",\"namespace\":\"com.example\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"partitionKey\",\"type\":\"string\"}]}";
  protected static final InternalTable TEST_INTERNAL_TABLE_WITH_SCHEMA =
      InternalTable.builder()
          .basePath(TEST_BASE_PATH)
          .readSchema(
              AvroSchemaConverter.getInstance()
                  .toInternalSchema(new Schema.Parser().parse(avroSchema)))
          .partitioningFields(
              Collections.singletonList(
                  InternalPartitionField.builder()
                      .sourceField(
                          InternalField.builder()
                              .name("partitionKey")
                              .schema(
                                  InternalSchema.builder().dataType(InternalType.STRING).build())
                              .build())
                      .build()))
          .build();

  protected static final ThreePartHierarchicalTableIdentifier TEST_TABLE_IDENTIFIER =
      new ThreePartHierarchicalTableIdentifier(HMS_DATABASE, HMS_TABLE);

  @Mock private CatalogPartitionSyncOperations mockCatalogClient;
  @Mock private HoodieTableMetaClient mockMetaClient;
  @Mock private PartitionValueExtractor mockPartitionValueExtractor;
  @Mock private HudiTableManager mockHudiTableManager;

  private final Configuration mockConfiguration = new Configuration();
  private HudiCatalogPartitionSyncTool hudiCatalogPartitionSyncTool;

  private HudiCatalogPartitionSyncTool createMockHudiPartitionSyncTool() {
    return new HudiCatalogPartitionSyncTool(
        mockCatalogClient, mockHudiTableManager, mockPartitionValueExtractor, mockConfiguration);
  }

  private void setupCommonMocks() {
    hudiCatalogPartitionSyncTool = createMockHudiPartitionSyncTool();
  }

  @SneakyThrows
  @Test
  void testSyncAllPartitions() {
    setupCommonMocks();

    String partitionKey1 = "key1";
    String partitionKey2 = "key2";
    ZonedDateTime zonedDateTime =
        Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault());
    try (MockedStatic<ZonedDateTime> mockZonedDateTime = mockStatic(ZonedDateTime.class);
        MockedStatic<FSUtils> mockFSUtils = mockStatic(FSUtils.class)) {
      mockZonedDateTime.when(ZonedDateTime::now).thenReturn(zonedDateTime);
      List<String> mockedPartitions = Arrays.asList(partitionKey1, partitionKey2);
      mockFSUtils
          .when(
              () ->
                  FSUtils.getAllPartitionPaths(
                      any(), any(), eq(new StoragePath(TEST_BASE_PATH)), eq(true)))
          .thenReturn(mockedPartitions);
      mockFSUtils
          .when(() -> FSUtils.constructAbsolutePath(new StoragePath(TEST_BASE_PATH), partitionKey1))
          .thenReturn(new StoragePath(TEST_BASE_PATH + "/" + partitionKey1));
      mockFSUtils
          .when(() -> FSUtils.constructAbsolutePath(new StoragePath(TEST_BASE_PATH), partitionKey2))
          .thenReturn(new StoragePath(TEST_BASE_PATH + "/" + partitionKey2));
      when(mockHudiTableManager.loadTableMetaClientIfExists(TEST_BASE_PATH))
          .thenReturn(Optional.of(mockMetaClient));
      when(mockMetaClient.getBasePath()).thenReturn(new StoragePath(TEST_BASE_PATH));
      when(mockPartitionValueExtractor.extractPartitionValuesInPath(partitionKey1))
          .thenReturn(Collections.singletonList(partitionKey1));
      when(mockPartitionValueExtractor.extractPartitionValuesInPath(partitionKey2))
          .thenReturn(Collections.singletonList(partitionKey2));

      HoodieActiveTimeline mockTimeline = mock(HoodieActiveTimeline.class);
      HoodieInstant instant1 =
          new HoodieInstant(
              HoodieInstant.State.COMPLETED,
              "replacecommit",
              "100",
              "1000",
              InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
      HoodieInstant instant2 =
          new HoodieInstant(
              HoodieInstant.State.COMPLETED,
              "replacecommit",
              "101",
              "1100",
              InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
      when(mockTimeline.countInstants()).thenReturn(2);
      when(mockTimeline.lastInstant()).thenReturn(Option.of(instant2));
      when(mockTimeline.getInstantsOrderedByCompletionTime())
          .thenReturn(Stream.of(instant1, instant2));
      when(mockMetaClient.getActiveTimeline()).thenReturn(mockTimeline);

      CatalogPartition p1 =
          new CatalogPartition(Collections.singletonList(partitionKey1), partitionKey1);
      when(mockCatalogClient.getAllPartitions(TEST_TABLE_IDENTIFIER))
          .thenReturn(Collections.singletonList(p1));

      assertTrue(
          hudiCatalogPartitionSyncTool.syncPartitions(
              TEST_INTERNAL_TABLE_WITH_SCHEMA, TEST_TABLE_IDENTIFIER));

      ArgumentCaptor<List<CatalogPartition>> addPartitionsCaptor =
          ArgumentCaptor.forClass(List.class);
      verify(mockCatalogClient, times(1))
          .addPartitionsToTable(eq(TEST_TABLE_IDENTIFIER), addPartitionsCaptor.capture());
      List<CatalogPartition> addedPartitions = addPartitionsCaptor.getValue();
      assertEquals(1, addedPartitions.size());
      assertEquals(
          TEST_BASE_PATH + "/" + partitionKey2, addedPartitions.get(0).getStorageLocation());
      assertEquals(1, addedPartitions.get(0).getValues().size());
      assertEquals(partitionKey2, addedPartitions.get(0).getValues().get(0));

      verify(mockCatalogClient, times(0)).dropPartitions(any(), anyList());

      ArgumentCaptor<Map<String, String>> lastSyncedPropertiesCaptor =
          ArgumentCaptor.forClass(Map.class);
      verify(mockCatalogClient, times(1))
          .updateTableProperties(eq(TEST_TABLE_IDENTIFIER), lastSyncedPropertiesCaptor.capture());
      Map<String, String> lastSyncedProperties = lastSyncedPropertiesCaptor.getValue();
      assertEquals("101", lastSyncedProperties.get(LAST_COMMIT_TIME_SYNC));
      assertEquals("1100", lastSyncedProperties.get(LAST_COMMIT_COMPLETION_TIME_SYNC));
    }
  }

  @SneakyThrows
  @Test
  void testSyncPartitionsSinceLastSyncTime() {
    setupCommonMocks();

    String partitionKey1 = "key1";
    String partitionKey2 = "key2";
    String partitionKey3 = "key3";
    ZonedDateTime zonedDateTime =
        Instant.ofEpochMilli(System.currentTimeMillis()).atZone(ZoneId.systemDefault());
    try (MockedStatic<ZonedDateTime> mockZonedDateTime = mockStatic(ZonedDateTime.class);
        MockedStatic<FSUtils> mockFSUtils = mockStatic(FSUtils.class);
        MockedStatic<TimelineUtils> mockedTimelineUtils = mockStatic(TimelineUtils.class)) {
      mockZonedDateTime.when(ZonedDateTime::now).thenReturn(zonedDateTime);
      List<String> mockedPartitions = Arrays.asList(partitionKey1, partitionKey2);
      mockFSUtils
          .when(() -> FSUtils.getAllPartitionPaths(any(), any(), eq(TEST_BASE_PATH), eq(true)))
          .thenReturn(mockedPartitions);
      mockFSUtils
          .when(() -> FSUtils.constructAbsolutePath(new StoragePath(TEST_BASE_PATH), partitionKey2))
          .thenReturn(new StoragePath(TEST_BASE_PATH + "/" + partitionKey2));
      mockFSUtils
          .when(() -> FSUtils.constructAbsolutePath(new StoragePath(TEST_BASE_PATH), partitionKey3))
          .thenReturn(new StoragePath(TEST_BASE_PATH + "/" + partitionKey3));
      when(mockHudiTableManager.loadTableMetaClientIfExists(TEST_BASE_PATH))
          .thenReturn(Optional.of(mockMetaClient));
      when(mockMetaClient.getBasePath()).thenReturn(new StoragePath(TEST_BASE_PATH));
      when(mockPartitionValueExtractor.extractPartitionValuesInPath(partitionKey2))
          .thenReturn(Collections.singletonList(partitionKey2));
      when(mockPartitionValueExtractor.extractPartitionValuesInPath(partitionKey3))
          .thenReturn(Collections.singletonList(partitionKey3));

      HoodieActiveTimeline mockTimeline = mock(HoodieActiveTimeline.class);
      HoodieInstant instant1 =
          new HoodieInstant(
              HoodieInstant.State.COMPLETED,
              "replacecommit",
              "100",
              "1000",
              InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
      HoodieInstant instant2 =
          new HoodieInstant(
              HoodieInstant.State.COMPLETED,
              "replacecommit",
              "101",
              "1100",
              InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);

      when(mockTimeline.countInstants()).thenReturn(2);
      when(mockTimeline.lastInstant()).thenReturn(Option.of(instant2));
      when(mockTimeline.getInstantsOrderedByCompletionTime())
          .thenReturn(Stream.of(instant1, instant2));
      when(mockMetaClient.getActiveTimeline()).thenReturn(mockTimeline);

      Map<String, String> lastSyncedTimeProperties = new HashMap<>();
      lastSyncedTimeProperties.put(LAST_COMMIT_TIME_SYNC, "100");
      lastSyncedTimeProperties.put(LAST_COMMIT_COMPLETION_TIME_SYNC, "1000");
      when(mockCatalogClient.getTableProperties(
              TEST_TABLE_IDENTIFIER,
              Arrays.asList(LAST_COMMIT_TIME_SYNC, LAST_COMMIT_COMPLETION_TIME_SYNC)))
          .thenReturn(lastSyncedTimeProperties);
      when(mockTimeline.isBeforeTimelineStarts("100")).thenReturn(false);

      // prepare timeline util mocks
      mockedTimelineUtils
          .when(() -> TimelineUtils.getWrittenPartitions(any()))
          .thenReturn(Arrays.asList(partitionKey2, partitionKey3));
      mockedTimelineUtils
          .when(
              () -> TimelineUtils.getCommitsTimelineAfter(mockMetaClient, "100", Option.of("1000")))
          .thenReturn(mockTimeline);
      mockedTimelineUtils
          .when(() -> TimelineUtils.getDroppedPartitions(eq(mockMetaClient), any(), any()))
          .thenReturn(Collections.singletonList(partitionKey2));

      CatalogPartition p1 =
          new CatalogPartition(
              Collections.singletonList(partitionKey1), TEST_BASE_PATH + "/" + partitionKey1);
      CatalogPartition p2 =
          new CatalogPartition(
              Collections.singletonList(partitionKey2), TEST_BASE_PATH + "/" + partitionKey2);
      when(mockCatalogClient.getAllPartitions(TEST_TABLE_IDENTIFIER))
          .thenReturn(Arrays.asList(p1, p2));

      assertTrue(
          hudiCatalogPartitionSyncTool.syncPartitions(
              TEST_INTERNAL_TABLE_WITH_SCHEMA, TEST_TABLE_IDENTIFIER));

      // verify add partitions
      ArgumentCaptor<ThreePartHierarchicalTableIdentifier> tableIdentifierArgumentCaptor =
          ArgumentCaptor.forClass(ThreePartHierarchicalTableIdentifier.class);
      ArgumentCaptor<List<CatalogPartition>> addPartitionsCaptor =
          ArgumentCaptor.forClass(List.class);
      verify(mockCatalogClient, times(1))
          .addPartitionsToTable(
              tableIdentifierArgumentCaptor.capture(), addPartitionsCaptor.capture());
      List<CatalogPartition> addedPartitions = addPartitionsCaptor.getValue();
      assertEquals(
          TEST_TABLE_IDENTIFIER.getDatabaseName(),
          tableIdentifierArgumentCaptor.getValue().getDatabaseName());
      assertEquals(
          TEST_TABLE_IDENTIFIER.getTableName(),
          tableIdentifierArgumentCaptor.getValue().getTableName());
      assertEquals(1, addedPartitions.size());
      assertEquals(
          TEST_BASE_PATH + "/" + partitionKey3, addedPartitions.get(0).getStorageLocation());
      assertEquals(1, addedPartitions.get(0).getValues().size());
      assertEquals(partitionKey3, addedPartitions.get(0).getValues().get(0));

      // verify drop partitions
      ArgumentCaptor<List<CatalogPartition>> dropPartitionsCaptor =
          ArgumentCaptor.forClass(List.class);
      verify(mockCatalogClient, times(1))
          .dropPartitions(tableIdentifierArgumentCaptor.capture(), dropPartitionsCaptor.capture());
      List<CatalogPartition> droppedPartitions = dropPartitionsCaptor.getValue();
      assertEquals(
          TEST_TABLE_IDENTIFIER.getDatabaseName(),
          tableIdentifierArgumentCaptor.getValue().getDatabaseName());
      assertEquals(
          TEST_TABLE_IDENTIFIER.getTableName(),
          tableIdentifierArgumentCaptor.getValue().getTableName());
      assertEquals(droppedPartitions.size(), 1);
      assertEquals(
          TEST_BASE_PATH + "/" + partitionKey2, droppedPartitions.get(0).getStorageLocation());
      assertEquals(1, droppedPartitions.get(0).getValues().size());
      assertEquals(partitionKey2, droppedPartitions.get(0).getValues().get(0));

      // verify update last synced properties
      ArgumentCaptor<Map<String, String>> lastSyncedPropertiesCaptor =
          ArgumentCaptor.forClass(Map.class);
      verify(mockCatalogClient, times(1))
          .updateTableProperties(eq(TEST_TABLE_IDENTIFIER), lastSyncedPropertiesCaptor.capture());
      Map<String, String> lastSyncedProperties = lastSyncedPropertiesCaptor.getValue();
      assertEquals("101", lastSyncedProperties.get(LAST_COMMIT_TIME_SYNC));
      assertEquals("1100", lastSyncedProperties.get(LAST_COMMIT_COMPLETION_TIME_SYNC));
    }
  }
}
