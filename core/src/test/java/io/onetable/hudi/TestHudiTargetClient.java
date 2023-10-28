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
 
package io.onetable.hudi;

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

import io.onetable.avro.AvroSchemaConverter;
import io.onetable.exception.NotSupportedException;
import io.onetable.model.OneTable;
import io.onetable.model.OneTableMetadata;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.model.storage.OneFileGroup;

/**
 * Unit tests that focus on the basic control flow of the HudiTargetClient. Functional tests are in
 * {@link ITHudiTargetClient}.
 */
public class TestHudiTargetClient {
  private static final int RETENTION_IN_HOURS = 1;
  private static final int MAX_DELTA_COMMITS = 2;
  private static final Instant COMMIT_TIME = Instant.ofEpochMilli(1598644800000L);
  private static final String COMMIT = "20200828200000000";
  private static final String BASE_PATH = "test-base-path";
  private static final OneTable TABLE =
      OneTable.builder().name("table").basePath(BASE_PATH).latestCommitTime(COMMIT_TIME).build();

  private final BaseFileUpdatesExtractor mockBaseFileUpdatesExtractor =
      mock(BaseFileUpdatesExtractor.class);
  private final AvroSchemaConverter mockAvroSchemaConverter = mock(AvroSchemaConverter.class);
  private final HudiTableManager mockHudiTableManager = mock(HudiTableManager.class);
  private final HudiTargetClient.CommitStateCreator mockCommitStateCreator =
      mock(HudiTargetClient.CommitStateCreator.class);

  private HudiTargetClient getTargetClient(HoodieTableMetaClient mockMetaClient) {
    when(mockHudiTableManager.loadTableMetaClientIfExists(BASE_PATH))
        .thenReturn(Optional.ofNullable(mockMetaClient));
    return new HudiTargetClient(
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
    HudiTargetClient targetClient = getTargetClient(null);
    HudiTargetClient.CommitState mockCommitState = initMocksForBeginSync(targetClient).getLeft();
    OneSchema input =
        OneSchema.builder()
            .name("schema")
            .dataType(OneType.RECORD)
            .recordKeyFields(
                Collections.singletonList(OneField.builder().name("record_key_field").build()))
            .build();
    Schema converted = SchemaBuilder.record("record").fields().requiredInt("field").endRecord();
    when(mockAvroSchemaConverter.fromOneSchema(input)).thenReturn(converted);
    targetClient.syncSchema(input);
    // validate that schema is set in commitState
    verify(mockCommitState).setSchema(converted);
  }

  @Test
  void syncSchemaWithRecordKeyChanged() {
    HudiTargetClient targetClient = getTargetClient(null);
    initMocksForBeginSync(targetClient);
    List<OneField> recordKeyFields =
        Arrays.asList(
            OneField.builder().name("record_key_field").build(),
            OneField.builder().name("record_key_field2").build());
    OneSchema input =
        OneSchema.builder()
            .name("schema")
            .dataType(OneType.RECORD)
            .recordKeyFields(recordKeyFields)
            .build();
    Schema converted = SchemaBuilder.record("record").fields().requiredInt("field").endRecord();
    when(mockAvroSchemaConverter.fromOneSchema(input)).thenReturn(converted);
    Exception thrownException =
        assertThrows(NotSupportedException.class, () -> targetClient.syncSchema(input));
    assertEquals(
        "Record key fields cannot be changed after creating Hudi table",
        thrownException.getMessage());
  }

  @Test
  void syncMetadata() {
    HudiTargetClient targetClient = getTargetClient(null);
    HudiTargetClient.CommitState mockCommitState = initMocksForBeginSync(targetClient).getLeft();
    OneTableMetadata metadata = OneTableMetadata.of(COMMIT_TIME, Collections.emptyList());
    targetClient.syncMetadata(metadata);
    // validate that metadata is set in commitState
    verify(mockCommitState).setOneTableMetadata(metadata);
  }

  @Test
  void completeSync() {
    HudiTargetClient targetClient = getTargetClient(null);
    HudiTargetClient.CommitState mockCommitState = initMocksForBeginSync(targetClient).getLeft();
    targetClient.completeSync();
    // validate that commit is called
    verify(mockCommitState).commit();
  }

  @Test
  void syncPartitionSpecSucceedsWithoutChanges() {
    HudiTargetClient targetClient = getTargetClient(null);
    HoodieTableMetaClient mockMetaClient = initMocksForBeginSync(targetClient).getRight();
    String field1 = "field1";
    String field2 = "field2";
    List<OnePartitionField> inputPartitionFields =
        Arrays.asList(
            OnePartitionField.builder()
                .sourceField(OneField.builder().name(field1).build())
                .transformType(PartitionTransformType.VALUE)
                .build(),
            OnePartitionField.builder()
                .sourceField(OneField.builder().name(field2).build())
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
    HudiTargetClient targetClient = getTargetClient(null);
    HoodieTableMetaClient mockMetaClient = initMocksForBeginSync(targetClient).getRight();
    String field1 = "field1";
    String field2 = "field2";
    List<OnePartitionField> inputPartitionFields =
        Arrays.asList(
            OnePartitionField.builder()
                .sourceField(OneField.builder().name(field1).build())
                .transformType(PartitionTransformType.VALUE)
                .build(),
            OnePartitionField.builder()
                .sourceField(OneField.builder().name(field2).build())
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
    HudiTargetClient targetClient = getTargetClient(null);
    HudiTargetClient.CommitState mockCommitState = initMocksForBeginSync(targetClient).getLeft();
    String instant = "commit";
    OneDataFilesDiff input = OneDataFilesDiff.builder().build();
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
    HudiTargetClient targetClient = getTargetClient(null);
    Pair<HudiTargetClient.CommitState, HoodieTableMetaClient> mocks =
        initMocksForBeginSync(targetClient);
    HudiTargetClient.CommitState mockCommitState = mocks.getLeft();
    HoodieTableMetaClient mockMetaClient = mocks.getRight();
    String instant = "commit";
    List<OneFileGroup> input = Collections.emptyList();
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
    HudiTargetClient targetClient = getTargetClient(mockMetaClient);

    targetClient.beginSync(TABLE);
    // verify meta client timeline refreshed
    verify(mockMetaClient).reloadActiveTimeline();
    // verify existing meta client is used to create commit state
    verify(mockCommitStateCreator)
        .create(mockMetaClient, COMMIT, RETENTION_IN_HOURS, MAX_DELTA_COMMITS);
  }

  private Pair<HudiTargetClient.CommitState, HoodieTableMetaClient> initMocksForBeginSync(
      HudiTargetClient targetClient) {
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig mockTableConfig = mock(HoodieTableConfig.class);
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    when(mockTableConfig.getRecordKeyFields())
        .thenReturn(Option.of(new String[] {"record_key_field"}));
    when(mockHudiTableManager.initializeHudiTable(TABLE)).thenReturn(mockMetaClient);
    HudiTargetClient.CommitState mockCommitState = mock(HudiTargetClient.CommitState.class);
    when(mockCommitStateCreator.create(
            mockMetaClient, COMMIT, RETENTION_IN_HOURS, MAX_DELTA_COMMITS))
        .thenReturn(mockCommitState);
    targetClient.beginSync(TABLE);
    return Pair.of(mockCommitState, mockMetaClient);
  }
}
