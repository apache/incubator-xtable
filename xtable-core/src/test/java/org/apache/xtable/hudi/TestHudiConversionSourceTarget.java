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
 
package org.apache.xtable.hudi;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;

import org.apache.xtable.avro.AvroSchemaConverter;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.storage.DataFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;

/**
 * Unit tests that focus on the basic control flow of the HudiConversionTarget. Functional tests are
 * in {@link ITHudiConversionSourceTarget}.
 */
public class TestHudiConversionSourceTarget {
  private static final int RETENTION_IN_HOURS = 1;
  private static final int MAX_DELTA_COMMITS = 2;
  private static final Instant COMMIT_TIME = Instant.ofEpochMilli(1598644800000L);
  private static final String COMMIT = "20200828200000000";
  private static final String BASE_PATH = "test-base-path";
  private static final InternalTable TABLE =
      InternalTable.builder()
          .name("table")
          .basePath(BASE_PATH)
          .latestCommitTime(COMMIT_TIME)
          .build();

  private final BaseFileUpdatesExtractor mockBaseFileUpdatesExtractor =
      mock(BaseFileUpdatesExtractor.class);
  private final AvroSchemaConverter mockAvroSchemaConverter = mock(AvroSchemaConverter.class);
  private final HudiTableManager mockHudiTableManager = mock(HudiTableManager.class);
  private final HudiConversionTarget.CommitStateCreator mockCommitStateCreator =
      mock(HudiConversionTarget.CommitStateCreator.class);

  private HudiConversionTarget getTargetClient(HoodieTableMetaClient mockMetaClient) {
    when(mockHudiTableManager.loadTableMetaClientIfExists(BASE_PATH))
        .thenReturn(Optional.ofNullable(mockMetaClient));
    return new HudiConversionTarget(
        BASE_PATH,
        RETENTION_IN_HOURS,
        MAX_DELTA_COMMITS,
        mockBaseFileUpdatesExtractor,
        mockAvroSchemaConverter,
        mockHudiTableManager,
        mockCommitStateCreator);
  }

  @Test
  void getTableMetadataWithoutTableInitialized() {
    assertFalse(getTargetClient(null).getTableMetadata().isPresent());
  }

  @Test
  void syncSchema() {
    HudiConversionTarget targetClient = getTargetClient(null);
    HudiConversionTarget.CommitState mockCommitState =
        initMocksForBeginSync(targetClient).getLeft();
    InternalSchema input =
        InternalSchema.builder()
            .name("schema")
            .dataType(InternalType.RECORD)
            .recordKeyFields(
                Collections.singletonList(InternalField.builder().name("record_key_field").build()))
            .build();
    Schema converted = SchemaBuilder.record("record").fields().requiredInt("field").endRecord();
    when(mockAvroSchemaConverter.fromInternalSchema(input)).thenReturn(converted);
    targetClient.syncSchema(input);
    // validate that schema is set in commitState
    verify(mockCommitState).setSchema(converted);
  }

  @Test
  void syncSchemaWithRecordKeyChanged() {
    HudiConversionTarget targetClient = getTargetClient(null);
    initMocksForBeginSync(targetClient);
    List<InternalField> recordKeyFields =
        Arrays.asList(
            InternalField.builder().name("record_key_field").build(),
            InternalField.builder().name("record_key_field2").build());
    InternalSchema input =
        InternalSchema.builder()
            .name("schema")
            .dataType(InternalType.RECORD)
            .recordKeyFields(recordKeyFields)
            .build();
    Schema converted = SchemaBuilder.record("record").fields().requiredInt("field").endRecord();
    when(mockAvroSchemaConverter.fromInternalSchema(input)).thenReturn(converted);
    Exception thrownException =
        assertThrows(NotSupportedException.class, () -> targetClient.syncSchema(input));
    assertEquals(
        "Record key fields cannot be changed after creating Hudi table",
        thrownException.getMessage());
  }

  @Test
  void syncMetadata() {
    HudiConversionTarget targetClient = getTargetClient(null);
    HudiConversionTarget.CommitState mockCommitState =
        initMocksForBeginSync(targetClient).getLeft();
    TableSyncMetadata metadata = TableSyncMetadata.of(COMMIT_TIME, Collections.emptyList());
    targetClient.syncMetadata(metadata);
    // validate that metadata is set in commitState
    verify(mockCommitState).setTableSyncMetadata(metadata);
  }

  @Test
  void completeSync() {
    HudiConversionTarget targetClient = getTargetClient(null);
    HudiConversionTarget.CommitState mockCommitState =
        initMocksForBeginSync(targetClient).getLeft();
    targetClient.completeSync();
    // validate that commit is called
    verify(mockCommitState).commit();
  }

  @Test
  void syncPartitionSpecSucceedsWithoutChanges() {
    HudiConversionTarget targetClient = getTargetClient(null);
    HoodieTableMetaClient mockMetaClient = initMocksForBeginSync(targetClient).getRight();
    String field1 = "field1";
    String field2 = "field2";
    List<InternalPartitionField> inputPartitionFields =
        Arrays.asList(
            InternalPartitionField.builder()
                .sourceField(InternalField.builder().name(field1).build())
                .transformType(PartitionTransformType.VALUE)
                .build(),
            InternalPartitionField.builder()
                .sourceField(InternalField.builder().name(field2).build())
                .transformType(PartitionTransformType.VALUE)
                .build());
    HoodieTableConfig mockTableConfig = mock(HoodieTableConfig.class);
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    when(mockTableConfig.getPartitionFields()).thenReturn(Option.of(new String[] {field1, field2}));
    targetClient.syncPartitionSpec(inputPartitionFields);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "field1", "field2,field1"})
  void syncPartitionSpecFailsWithChanges(String partitionFields) {
    HudiConversionTarget targetClient = getTargetClient(null);
    HoodieTableMetaClient mockMetaClient = initMocksForBeginSync(targetClient).getRight();
    String field1 = "field1";
    String field2 = "field2";
    List<InternalPartitionField> inputPartitionFields =
        Arrays.asList(
            InternalPartitionField.builder()
                .sourceField(InternalField.builder().name(field1).build())
                .transformType(PartitionTransformType.VALUE)
                .build(),
            InternalPartitionField.builder()
                .sourceField(InternalField.builder().name(field2).build())
                .transformType(PartitionTransformType.VALUE)
                .build());
    HoodieTableConfig mockTableConfig = mock(HoodieTableConfig.class);
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    Option<String[]> existingPartitionFields =
        partitionFields.isEmpty() ? Option.empty() : Option.of(partitionFields.split(","));
    when(mockTableConfig.getPartitionFields()).thenReturn(existingPartitionFields);
    assertThrows(
        NotSupportedException.class, () -> targetClient.syncPartitionSpec(inputPartitionFields));
  }

  @Test
  void syncFilesForDiff() {
    HudiConversionTarget targetClient = getTargetClient(null);
    HudiConversionTarget.CommitState mockCommitState =
        initMocksForBeginSync(targetClient).getLeft();
    String instant = "commit";
    DataFilesDiff input = DataFilesDiff.builder().build();
    BaseFileUpdatesExtractor.ReplaceMetadata output =
        BaseFileUpdatesExtractor.ReplaceMetadata.of(
            Collections.emptyMap(), Collections.emptyList());
    when(mockCommitState.getInstantTime()).thenReturn(instant);
    when(mockBaseFileUpdatesExtractor.convertDiff(input, instant)).thenReturn(output);

    targetClient.syncFilesForDiff(input);
    // validate that replace metadata is set in commitState
    verify(mockCommitState).setReplaceMetadata(output);
  }

  @Test
  void syncFilesForSnapshot() {
    HudiConversionTarget targetClient = getTargetClient(null);
    Pair<HudiConversionTarget.CommitState, HoodieTableMetaClient> mocks =
        initMocksForBeginSync(targetClient);
    HudiConversionTarget.CommitState mockCommitState = mocks.getLeft();
    HoodieTableMetaClient mockMetaClient = mocks.getRight();
    String instant = "commit";
    List<PartitionFileGroup> input = Collections.emptyList();
    BaseFileUpdatesExtractor.ReplaceMetadata output =
        BaseFileUpdatesExtractor.ReplaceMetadata.of(
            Collections.emptyMap(), Collections.emptyList());
    when(mockCommitState.getInstantTime()).thenReturn(instant);
    when(mockBaseFileUpdatesExtractor.extractSnapshotChanges(input, mockMetaClient, instant))
        .thenReturn(output);

    targetClient.syncFilesForSnapshot(input);
    // validate that replace metadata is set in commitState
    verify(mockCommitState).setReplaceMetadata(output);
  }

  @Test
  void beginSyncForExistingTable() {
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HudiConversionTarget targetClient = getTargetClient(mockMetaClient);

    targetClient.beginSync(TABLE);
    // verify meta client timeline refreshed
    verify(mockMetaClient).reloadActiveTimeline();
    // verify existing meta client is used to create commit state
    verify(mockCommitStateCreator)
        .create(mockMetaClient, COMMIT, RETENTION_IN_HOURS, MAX_DELTA_COMMITS);
  }

  private Pair<HudiConversionTarget.CommitState, HoodieTableMetaClient> initMocksForBeginSync(
      HudiConversionTarget targetClient) {
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig mockTableConfig = mock(HoodieTableConfig.class);
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    when(mockTableConfig.getRecordKeyFields())
        .thenReturn(Option.of(new String[] {"record_key_field"}));
    when(mockHudiTableManager.initializeHudiTable(BASE_PATH, TABLE)).thenReturn(mockMetaClient);
    HudiConversionTarget.CommitState mockCommitState = mock(HudiConversionTarget.CommitState.class);
    when(mockCommitStateCreator.create(
            mockMetaClient, COMMIT, RETENTION_IN_HOURS, MAX_DELTA_COMMITS))
        .thenReturn(mockCommitState);
    targetClient.beginSync(TABLE);
    return Pair.of(mockCommitState, mockMetaClient);
  }
}
