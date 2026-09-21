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
 
package org.apache.xtable.glue.table;

import static org.apache.xtable.glue.GlueCatalogSyncClient.GLUE_EXTERNAL_TABLE_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.glue.GlueCatalogSyncTestBase;

import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.SerDeInfo;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

@ExtendWith(MockitoExtension.class)
public class TestDeltaGlueCatalogTableBuilder extends GlueCatalogSyncTestBase {
  private static final String PARQUET_INPUT_FORMAT =
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
  private static final String PARQUET_OUTPUT_FORMAT =
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
  private static final String PARQUET_SERDE_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

  private static final String PARQUET_INPUT_FORMAT =
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat";
  private static final String PARQUET_OUTPUT_FORMAT =
      "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat";
  private static final String PARQUET_SERDE_CLASS =
      "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe";

  private DeltaGlueCatalogTableBuilder deltaGlueCatalogTableBuilder;

  private DeltaGlueCatalogTableBuilder createDeltaGlueCatalogSyncHelper() {
    return new DeltaGlueCatalogTableBuilder();
  }

  void setupCommonMocks() {
    deltaGlueCatalogTableBuilder = createDeltaGlueCatalogSyncHelper();
  }

  @Test
  void testGetCreateTableRequest() {
    setupCommonMocks();

    TableInput expected =
        TableInput.builder()
            .name(TEST_CATALOG_TABLE_IDENTIFIER.getTableName())
            .tableType(GLUE_EXTERNAL_TABLE_TYPE)
            .parameters(deltaGlueCatalogTableBuilder.getTableParameters())
            .storageDescriptor(getTestStorageDescriptor(DELTA_GLUE_SCHEMA))
            .partitionKeys(PARTITION_KEYS)
            .build();

    TableInput output =
        deltaGlueCatalogTableBuilder.getCreateTableRequest(
            TEST_DELTA_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    assertEquals(expected, output);
  }

  @Test
  void testGetUpdateTableInput() {
    setupCommonMocks();
    Table glueTable =
        Table.builder()
            .parameters(deltaGlueCatalogTableBuilder.getTableParameters())
            .storageDescriptor(getTestStorageDescriptor(DELTA_GLUE_SCHEMA))
            .partitionKeys(PARTITION_KEYS)
            .build();

    TableInput expected =
        TableInput.builder()
            .name(TEST_CATALOG_TABLE_IDENTIFIER.getTableName())
            .tableType(GLUE_EXTERNAL_TABLE_TYPE)
            .parameters(deltaGlueCatalogTableBuilder.getTableParameters())
            .storageDescriptor(getTestStorageDescriptor(UPDATED_DELTA_GLUE_SCHEMA))
            .partitionKeys(PARTITION_KEYS)
            .build();

    TableInput output =
        deltaGlueCatalogTableBuilder.getUpdateTableRequest(
            TEST_UPDATED_DELTA_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER);
    assertEquals(expected, output);
  }

  @Test
  void testGetUpdateTableInputPreservesExistingStorageSettings() {
    // Regression test: a from-scratch StorageDescriptor rebuild (like getCreateTableRequest
    // uses) would silently drop additionalLocations and any custom serde parameter set directly
    // in Glue outside of XTable -- refresh must instead build off the existing descriptor.
    setupCommonMocks();
    List<String> additionalLocations = Collections.singletonList("s3://base-path/extra-location");
    Map<String, String> existingSerdeParams =
        new HashMap<>(deltaGlueCatalogTableBuilder.getSerDeParameters(TEST_DELTA_INTERNAL_TABLE));
    existingSerdeParams.put("custom.serde.param", "custom-value");

    StorageDescriptor existingStorageDescriptor =
        StorageDescriptor.builder()
            .columns(DELTA_GLUE_SCHEMA)
            .location(TEST_BASE_PATH)
            .additionalLocations(additionalLocations)
            .inputFormat(PARQUET_INPUT_FORMAT)
            .outputFormat(PARQUET_OUTPUT_FORMAT)
            .serdeInfo(
                SerDeInfo.builder()
                    .serializationLibrary(PARQUET_SERDE_CLASS)
                    .parameters(existingSerdeParams)
                    .build())
            .build();
    Table glueTable =
        Table.builder()
            .parameters(deltaGlueCatalogTableBuilder.getTableParameters())
            .storageDescriptor(existingStorageDescriptor)
            .partitionKeys(PARTITION_KEYS)
            .build();

    TableInput output =
        deltaGlueCatalogTableBuilder.getUpdateTableRequest(
            TEST_UPDATED_DELTA_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER);

    StorageDescriptor outputStorageDescriptor = output.storageDescriptor();
    assertEquals(additionalLocations, outputStorageDescriptor.additionalLocations());
    assertEquals(existingSerdeParams, outputStorageDescriptor.serdeInfo().parameters());
    assertEquals(UPDATED_DELTA_GLUE_SCHEMA, outputStorageDescriptor.columns());
  }

  @Test
  void testGetUpdateTableInputRepairsLegacyStorageDescriptor() {
    // A table registered by an earlier XTable version carries no input/output format and no
    // serialization library, which is what breaks Glue Catalog Federation. Refresh must fill all
    // three in, and must leave the columns, location and existing serde parameters intact.
    setupCommonMocks();
    Map<String, String> existingSerdeParams =
        new HashMap<>(deltaGlueCatalogTableBuilder.getSerDeParameters(TEST_DELTA_INTERNAL_TABLE));
    existingSerdeParams.put("custom.serde.param", "custom-value");

    StorageDescriptor legacyStorageDescriptor =
        StorageDescriptor.builder()
            .columns(DELTA_GLUE_SCHEMA)
            .location(TEST_BASE_PATH)
            .serdeInfo(SerDeInfo.builder().parameters(existingSerdeParams).build())
            .build();
    Table glueTable =
        Table.builder()
            .parameters(deltaGlueCatalogTableBuilder.getTableParameters())
            .storageDescriptor(legacyStorageDescriptor)
            .partitionKeys(PARTITION_KEYS)
            .build();

    StorageDescriptor output =
        deltaGlueCatalogTableBuilder
            .getUpdateTableRequest(
                TEST_UPDATED_DELTA_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER)
            .storageDescriptor();

    assertEquals(PARQUET_INPUT_FORMAT, output.inputFormat());
    assertEquals(PARQUET_OUTPUT_FORMAT, output.outputFormat());
    assertEquals(PARQUET_SERDE_CLASS, output.serdeInfo().serializationLibrary());
    assertEquals(existingSerdeParams, output.serdeInfo().parameters());
    assertEquals(UPDATED_DELTA_GLUE_SCHEMA, output.columns());
    assertEquals(TEST_BASE_PATH, output.location());
  }

  @Test
  void testGetUpdateTableInputPopulatesMissingSerDeInfo() {
    // Covers the null-SerDeInfo branch of the refresh path: an existing descriptor with no
    // SerDeInfo at all gets a fresh one derived from the table.
    setupCommonMocks();
    StorageDescriptor storageDescriptorWithoutSerDeInfo =
        StorageDescriptor.builder().columns(DELTA_GLUE_SCHEMA).location(TEST_BASE_PATH).build();
    Table glueTable =
        Table.builder()
            .parameters(deltaGlueCatalogTableBuilder.getTableParameters())
            .storageDescriptor(storageDescriptorWithoutSerDeInfo)
            .partitionKeys(PARTITION_KEYS)
            .build();

    StorageDescriptor output =
        deltaGlueCatalogTableBuilder
            .getUpdateTableRequest(
                TEST_UPDATED_DELTA_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER)
            .storageDescriptor();

    assertEquals(PARQUET_INPUT_FORMAT, output.inputFormat());
    assertEquals(PARQUET_OUTPUT_FORMAT, output.outputFormat());
    assertEquals(PARQUET_SERDE_CLASS, output.serdeInfo().serializationLibrary());
    assertEquals(
        deltaGlueCatalogTableBuilder.getSerDeParameters(TEST_UPDATED_DELTA_INTERNAL_TABLE),
        output.serdeInfo().parameters());
    assertEquals(UPDATED_DELTA_GLUE_SCHEMA, output.columns());
  }

  private StorageDescriptor getTestStorageDescriptor(List<Column> columns) {
    return StorageDescriptor.builder()
        .columns(columns)
        .location(TEST_BASE_PATH)
        .inputFormat(PARQUET_INPUT_FORMAT)
        .outputFormat(PARQUET_OUTPUT_FORMAT)
        .serdeInfo(
            SerDeInfo.builder()
                .serializationLibrary(PARQUET_SERDE_CLASS)
                .parameters(
                    deltaGlueCatalogTableBuilder.getSerDeParameters(TEST_DELTA_INTERNAL_TABLE))
                .build())
        .build();
  }

  @Test
  void testGetUpdateTableInputPreservesExistingStorageSettings() {
    setupCommonMocks();
    List<String> additionalLocations = Collections.singletonList("s3://base-path/extra-location");
    Map<String, String> existingSerdeParams =
        new HashMap<>(deltaGlueCatalogTableBuilder.getSerDeParameters(TEST_DELTA_INTERNAL_TABLE));
    existingSerdeParams.put("custom.serde.param", "custom-value");

    StorageDescriptor existingStorageDescriptor =
        StorageDescriptor.builder()
            .columns(DELTA_GLUE_SCHEMA)
            .location(TEST_BASE_PATH)
            .additionalLocations(additionalLocations)
            .inputFormat(PARQUET_INPUT_FORMAT)
            .outputFormat(PARQUET_OUTPUT_FORMAT)
            .serdeInfo(
                SerDeInfo.builder()
                    .serializationLibrary(PARQUET_SERDE_CLASS)
                    .parameters(existingSerdeParams)
                    .build())
            .build();
    Table glueTable =
        Table.builder()
            .parameters(deltaGlueCatalogTableBuilder.getTableParameters())
            .storageDescriptor(existingStorageDescriptor)
            .partitionKeys(PARTITION_KEYS)
            .build();

    StorageDescriptor output =
        deltaGlueCatalogTableBuilder
            .getUpdateTableRequest(
                TEST_UPDATED_DELTA_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER)
            .storageDescriptor();

    assertEquals(additionalLocations, output.additionalLocations());
    assertEquals(existingSerdeParams, output.serdeInfo().parameters());
    assertEquals(PARQUET_INPUT_FORMAT, output.inputFormat());
    assertEquals(PARQUET_OUTPUT_FORMAT, output.outputFormat());
    assertEquals(PARQUET_SERDE_CLASS, output.serdeInfo().serializationLibrary());
    assertEquals(UPDATED_DELTA_GLUE_SCHEMA, output.columns());
  }

  @Test
  void testGetUpdateTableInputRepairsLegacyStorageDescriptor() {
    setupCommonMocks();
    Map<String, String> existingSerdeParams =
        new HashMap<>(deltaGlueCatalogTableBuilder.getSerDeParameters(TEST_DELTA_INTERNAL_TABLE));
    existingSerdeParams.put("custom.serde.param", "custom-value");

    StorageDescriptor legacyStorageDescriptor =
        StorageDescriptor.builder()
            .columns(DELTA_GLUE_SCHEMA)
            .location(TEST_BASE_PATH)
            .serdeInfo(SerDeInfo.builder().parameters(existingSerdeParams).build())
            .build();
    Table glueTable =
        Table.builder()
            .parameters(deltaGlueCatalogTableBuilder.getTableParameters())
            .storageDescriptor(legacyStorageDescriptor)
            .partitionKeys(PARTITION_KEYS)
            .build();

    StorageDescriptor output =
        deltaGlueCatalogTableBuilder
            .getUpdateTableRequest(
                TEST_UPDATED_DELTA_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER)
            .storageDescriptor();

    assertEquals(PARQUET_INPUT_FORMAT, output.inputFormat());
    assertEquals(PARQUET_OUTPUT_FORMAT, output.outputFormat());
    assertEquals(PARQUET_SERDE_CLASS, output.serdeInfo().serializationLibrary());
    assertEquals(existingSerdeParams, output.serdeInfo().parameters());
    assertEquals(UPDATED_DELTA_GLUE_SCHEMA, output.columns());
    assertEquals(TEST_BASE_PATH, output.location());
  }
}
