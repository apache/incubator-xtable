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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
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
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.OneDataFilesDiff;

/**
 * Unit tests that focus on the basic control flow of the HudiTargetClient. Functional tests are in
 * {@link ITHudiTargetClient}.
 */
public class TestHudiTargetClient {
  private static final Instant COMMIT_TIME = Instant.ofEpochMilli(1696534687433L);
  private static final String COMMIT = "20231005143807433";
  private static final Configuration CONFIGURATION = new Configuration();
  private static final String BASE_PATH = "test-base-path";

  private final BaseFileUpdatesExtractor mockBaseFileUpdatesExtractor =
      mock(BaseFileUpdatesExtractor.class);
  private final AvroSchemaConverter mockAvroSchemaConverter = mock(AvroSchemaConverter.class);
  private final HudiTableManager mockHudiTableManager = mock(HudiTableManager.class);
  private final HudiTargetClient.CommitStateCreator mockCommitStateCreator =
      mock(HudiTargetClient.CommitStateCreator.class);
  private HudiTargetClient hudiTargetClient;

  @BeforeEach
  void setUp() {
    when(mockHudiTableManager.loadTableIfExists(BASE_PATH)).thenReturn(null);
    hudiTargetClient =
        new HudiTargetClient(
            BASE_PATH,
            CONFIGURATION,
            mockBaseFileUpdatesExtractor,
            mockAvroSchemaConverter,
            mockHudiTableManager,
            mockCommitStateCreator);
  }

  @Test
  void getTableMetadataWithoutTableInitialized() {
    assertFalse(hudiTargetClient.getTableMetadata().isPresent());
  }

  @Test
  void syncSchema() {
    HudiTargetClient.CommitState mockCommitState = initMocksForBeginSync().getLeft();
    OneSchema input = OneSchema.builder().name("schema").dataType(OneType.RECORD).build();
    Schema converted = SchemaBuilder.record("record").fields().requiredInt("field").endRecord();
    when(mockAvroSchemaConverter.fromOneSchema(input)).thenReturn(converted);
    hudiTargetClient.syncSchema(input);
    // validate that schema is set in commitState
    verify(mockCommitState).setSchema(converted);
  }

  @Test
  void syncMetadata() {
    HudiTargetClient.CommitState mockCommitState = initMocksForBeginSync().getLeft();
    OneTableMetadata metadata = OneTableMetadata.of(COMMIT_TIME);
    hudiTargetClient.syncMetadata(metadata);
    // validate that metadata is set in commitState
    verify(mockCommitState).setOneTableMetadata(metadata);
  }

  @Test
  void syncPartitionSpecSucceedsWithoutChanges() {
    HoodieTableMetaClient mockMetaClient = initMocksForBeginSync().getRight();
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
    hudiTargetClient.syncPartitionSpec(inputPartitionFields);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "field1", "field2,field1"})
  void syncPartitionSpecFailsWithChanges(String partitionFields) {
    HoodieTableMetaClient mockMetaClient = initMocksForBeginSync().getRight();
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
        NotSupportedException.class,
        () -> hudiTargetClient.syncPartitionSpec(inputPartitionFields));
  }

  @Test
  void syncFilesForDiff() {
    HudiTargetClient.CommitState mockCommitState = initMocksForBeginSync().getLeft();
    String instant = "commit";
    OneDataFilesDiff input = OneDataFilesDiff.builder().build();
    BaseFileUpdatesExtractor.ReplaceMetadata output =
        BaseFileUpdatesExtractor.ReplaceMetadata.of(
            Collections.emptyMap(), Collections.emptyList());
    when(mockCommitState.getInstantTime()).thenReturn(instant);
    when(mockBaseFileUpdatesExtractor.convertDiff(input, instant)).thenReturn(output);

    hudiTargetClient.syncFilesForDiff(input);
    // validate that replace metadata is set in commitState
    verify(mockCommitState).setReplaceMetadata(output);
  }

  @Test
  void syncFilesForSnapshot() {
    Pair<HudiTargetClient.CommitState, HoodieTableMetaClient> mocks = initMocksForBeginSync();
    HudiTargetClient.CommitState mockCommitState = mocks.getLeft();
    HoodieTableMetaClient mockMetaClient = mocks.getRight();
    String instant = "commit";
    OneDataFiles input = OneDataFiles.collectionBuilder().build();
    BaseFileUpdatesExtractor.ReplaceMetadata output =
        BaseFileUpdatesExtractor.ReplaceMetadata.of(
            Collections.emptyMap(), Collections.emptyList());
    when(mockCommitState.getInstantTime()).thenReturn(instant);
    when(mockBaseFileUpdatesExtractor.extractSnapshotChanges(input, mockMetaClient, instant))
        .thenReturn(output);

    hudiTargetClient.syncFilesForSnapshot(input);
    // validate that replace metadata is set in commitState
    verify(mockCommitState).setReplaceMetadata(output);
  }

  private Pair<HudiTargetClient.CommitState, HoodieTableMetaClient> initMocksForBeginSync() {
    OneTable table =
        OneTable.builder().name("table").basePath(BASE_PATH).latestCommitTime(COMMIT_TIME).build();

    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    when(mockHudiTableManager.initializeHudiTable(table)).thenReturn(mockMetaClient);
    HudiTargetClient.CommitState mockCommitState = mock(HudiTargetClient.CommitState.class);
    when(mockCommitStateCreator.create(mockMetaClient, COMMIT)).thenReturn(mockCommitState);
    hudiTargetClient.beginSync(table);
    return Pair.of(mockCommitState, mockMetaClient);
  }
}
