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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hudi.common.table.HoodieTableMetaClient;

import io.onetable.model.OneTable;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.storage.DataLayoutStrategy;
import io.onetable.model.storage.TableFormat;

public class TestHudiTableManager {

  private static final Configuration CONFIGURATION = new Configuration();
  @TempDir public static Path tempDir;
  private final String tableBasePath = tempDir.resolve(UUID.randomUUID().toString()).toString();

  private final HudiTableManager tableManager = HudiTableManager.of(CONFIGURATION);

  @ParameterizedTest
  @MethodSource("dataLayoutAndHivePartitioningEnabled")
  void validateTableInitializedCorrectly(
      DataLayoutStrategy dataLayoutStrategy, boolean expectedHivePartitioningEnabled) {
    String tableName = "testing_123";
    String field1 = "field1";
    String field2 = "field2";
    String recordKeyField = "path1.path2";
    OneSchema tableSchema =
        OneSchema.builder()
            .fields(
                Arrays.asList(
                    OneField.builder().name(field1).build(),
                    OneField.builder().name(field2).build(),
                    OneField.builder().name(recordKeyField).build()))
            .recordKeyFields(
                Collections.singletonList(OneField.builder().name(recordKeyField).build()))
            .build();
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
    OneTable table =
        OneTable.builder()
            .name(tableName)
            .partitioningFields(inputPartitionFields)
            .readSchema(tableSchema)
            // we will use the provided data path as the location so this path should be ignored
            .basePath("file://fake_path")
            .tableFormat(TableFormat.ICEBERG)
            .layoutStrategy(dataLayoutStrategy)
            .build();

    tableManager.initializeHudiTable(tableBasePath, table);

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder()
            .setBasePath(tableBasePath)
            .setConf(CONFIGURATION)
            .setLoadActiveTimelineOnLoad(false)
            .build();
    assertFalse(metaClient.getTableConfig().populateMetaFields());
    assertEquals(
        expectedHivePartitioningEnabled,
        Boolean.valueOf(metaClient.getTableConfig().getHiveStylePartitioningEnable()));
    assertEquals(
        Arrays.asList(field1, field2),
        Arrays.asList(metaClient.getTableConfig().getPartitionFields().get()));
    assertEquals(
        Arrays.asList(recordKeyField),
        Arrays.asList(metaClient.getTableConfig().getRecordKeyFields().get()));
    assertEquals(tableBasePath, metaClient.getBasePath());
    assertEquals(tableName, metaClient.getTableConfig().getTableName());
    assertEquals(
        "org.apache.hudi.keygen.ComplexKeyGenerator",
        metaClient.getTableConfig().getKeyGeneratorClassName());
  }

  public static Stream<Arguments> dataLayoutAndHivePartitioningEnabled() {
    return Stream.of(
        Arguments.of(DataLayoutStrategy.HIVE_STYLE_PARTITION, true),
        Arguments.of(DataLayoutStrategy.DIR_HIERARCHY_PARTITION_VALUES, false),
        Arguments.of(DataLayoutStrategy.FLAT, false));
  }

  @Test
  void loadExistingTable() {
    HudiTestUtil.initTableAndGetMetaClient(tableBasePath, "timestamp");
    HoodieTableMetaClient metaClient =
        tableManager.loadTableMetaClientIfExists(tableBasePath).get();
    assertTrue(metaClient.getTableConfig().populateMetaFields());
    assertEquals(
        Collections.singletonList("timestamp"),
        Arrays.asList(metaClient.getTableConfig().getPartitionFields().get()));
    assertEquals(tableBasePath, metaClient.getBasePath());
    assertEquals("test_table", metaClient.getTableConfig().getTableName());
  }

  @Test
  void loadTableThatDoesNotExist() {
    assertFalse(tableManager.loadTableMetaClientIfExists(tableBasePath).isPresent());
  }

  @ParameterizedTest
  @MethodSource("keyGeneratorTestDataProvider")
  void testKeyGenerator(
      List<OnePartitionField> partitionFields,
      List<OneField> keyFields,
      String expectedKeyGeneratorClass) {
    assertEquals(
        expectedKeyGeneratorClass,
        HudiTableManager.getKeyGeneratorClass(partitionFields, keyFields));
  }

  private static Stream<Arguments> keyGeneratorTestDataProvider() {
    OneField keyField1 = OneField.builder().name("key1").build();
    OneField keyField2 = OneField.builder().name("key2").build();
    OneField field1 = OneField.builder().name("field1").build();
    OnePartitionField field1ValuePartition =
        OnePartitionField.builder()
            .sourceField(field1)
            .transformType(PartitionTransformType.VALUE)
            .build();
    OnePartitionField field1DatePartition =
        OnePartitionField.builder()
            .sourceField(field1)
            .transformType(PartitionTransformType.YEAR)
            .build();
    OneField field2 = OneField.builder().name("field2").build();
    OnePartitionField field2ValuePartition =
        OnePartitionField.builder()
            .sourceField(field2)
            .transformType(PartitionTransformType.VALUE)
            .build();
    return Stream.of(
        Arguments.of(
            Collections.emptyList(),
            Collections.singletonList(keyField1),
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator"),
        Arguments.of(
            Collections.singletonList(field1ValuePartition),
            Arrays.asList(keyField1, keyField2),
            "org.apache.hudi.keygen.ComplexKeyGenerator"),
        Arguments.of(
            Arrays.asList(field1ValuePartition, field2ValuePartition),
            Collections.singletonList(keyField1),
            "org.apache.hudi.keygen.ComplexKeyGenerator"),
        Arguments.of(
            Collections.singletonList(field1ValuePartition),
            Collections.singletonList(keyField1),
            "org.apache.hudi.keygen.SimpleKeyGenerator"),
        Arguments.of(
            Collections.singletonList(field1DatePartition),
            Collections.singletonList(keyField1),
            "org.apache.hudi.keygen.TimestampBasedKeyGenerator"),
        Arguments.of(
            Arrays.asList(field1DatePartition, field2ValuePartition),
            Collections.singletonList(keyField1),
            "org.apache.hudi.keygen.CustomKeyGenerator"));
  }
}
