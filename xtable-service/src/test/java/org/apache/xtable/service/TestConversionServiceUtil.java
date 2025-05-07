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
 
package org.apache.xtable.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.SchemaParser;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.hadoop.HadoopTables;

import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.Metadata;

import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.delta.DeltaConversionSourceProvider;
import org.apache.xtable.hudi.HudiConversionSourceProvider;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.service.utils.ConversionServiceUtil;

@ExtendWith(MockitoExtension.class)
class TestConversionServiceUtil {
  private static final String DELTA_BASE_PATH = "/tmp/delta-table";
  private static final String DELTA_METADATA_JSON = "/_delta_log/000000000.json";

  private static final String HUDI_BASE_PATH = "/tmp/hudi-table";
  private static final String HUDI_METADATA_DIR = ".hoodie";
  private static final String HUDI_INSTANT_FILE = "20250506.commit";
  private static final String HUDI_SCHEMA =
      "{\n"
          + "  \"type\": \"record\",\n"
          + "  \"name\": \"user\",\n"
          + "  \"namespace\": \"example.hudi\",\n"
          + "  \"fields\": [\n"
          + "    { \"name\": \"id\", \"type\": \"string\" },\n"
          + "  ]\n"
          + "}";

  private static final String ICEBERG_BASE_PATH = "/tmp/iceberg-table";
  private static final String ICEBERG_METADATA_JSON = "/metadata/v1.json";
  private static final String ICEBERG_SCHEMA =
      "{\n"
          + "  \"type\": \"struct\",\n"
          + "  \"fields\": [\n"
          + "    { \"id\": 1, \"name\": \"id\", \"required\": true, \"type\": \"string\" },\n"
          + "  ]\n"
          + "}";

  @Mock SparkSession sparkSession;
  @Mock Configuration hadoopConf;

  private final ConversionServiceUtil util = new ConversionServiceUtil();

  @Test
  void testGetDeltaSchemaAndMetadataPath() {
    StructType testSchema = new StructType().add("id", DataTypes.IntegerType);
    Path testPath = new Path(DELTA_BASE_PATH + DELTA_METADATA_JSON);

    try (MockedStatic<DeltaLog> deltaStatic = mockStatic(DeltaLog.class)) {
      DeltaLog mockLog = mock(DeltaLog.class);
      Snapshot mockSnap = mock(Snapshot.class);
      Metadata mockMetadata = mock(Metadata.class);

      deltaStatic.when(() -> DeltaLog.forTable(sparkSession, DELTA_BASE_PATH)).thenReturn(mockLog);
      when(mockLog.snapshot()).thenReturn(mockSnap);
      when(mockSnap.metadata()).thenReturn(mockMetadata);
      when(mockMetadata.schema()).thenReturn(testSchema);
      when(mockSnap.path()).thenReturn(testPath);

      Pair<String, String> result =
          util.getDeltaSchemaAndMetadataPath(DELTA_BASE_PATH, sparkSession);
      assertEquals(testPath.toString(), result.getLeft());
      assertEquals(testSchema.json(), result.getRight());
    }
  }

  @Test
  void testGetHudiSchemaAndMetadataPath() {
    try (MockedStatic<HoodieTableMetaClient> clientStatic =
            mockStatic(HoodieTableMetaClient.class);
        MockedStatic<HoodieCommitMetadata> commitStatic = mockStatic(HoodieCommitMetadata.class)) {

      HoodieTableMetaClient.Builder builder = mock(HoodieTableMetaClient.Builder.class);
      HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);

      clientStatic.when(HoodieTableMetaClient::builder).thenReturn(builder);
      when(builder.setBasePath(HUDI_BASE_PATH)).thenReturn(builder);
      when(builder.setConf(hadoopConf)).thenReturn(builder);
      when(builder.build()).thenReturn(metaClient);

      HoodieActiveTimeline activeTimeline = mock(HoodieActiveTimeline.class);
      HoodieTimeline commitsTimeline = mock(HoodieTimeline.class);
      HoodieInstant instant = mock(HoodieInstant.class);

      when(metaClient.getActiveTimeline()).thenReturn(activeTimeline);
      when(activeTimeline.getCommitsTimeline()).thenReturn(commitsTimeline);
      when(commitsTimeline.filterCompletedInstants()).thenReturn(commitsTimeline);
      when(commitsTimeline.empty()).thenReturn(false);
      when(commitsTimeline.lastInstant()).thenReturn(Option.of(instant));

      when(metaClient.getMetaPath()).thenReturn(HUDI_METADATA_DIR);
      when(instant.getFileName()).thenReturn(HUDI_INSTANT_FILE);

      byte[] rawBytes = "data".getBytes(StandardCharsets.UTF_8);
      Option<byte[]> rawOption = Option.of(rawBytes);
      when(activeTimeline.getInstantDetails(instant)).thenReturn(rawOption);

      HoodieCommitMetadata commitMetadata = mock(HoodieCommitMetadata.class);
      commitStatic
          .when(() -> HoodieCommitMetadata.fromBytes(rawBytes, HoodieCommitMetadata.class))
          .thenReturn(commitMetadata);
      when(commitMetadata.getMetadata(HoodieCommitMetadata.SCHEMA_KEY)).thenReturn(HUDI_SCHEMA);

      Pair<String, String> result = util.getHudiSchemaAndMetadataPath(HUDI_BASE_PATH, hadoopConf);

      assertEquals(
          HUDI_BASE_PATH + "/" + HUDI_METADATA_DIR + "/" + HUDI_INSTANT_FILE, result.getLeft());
      assertEquals(HUDI_SCHEMA, result.getRight());
    }
  }

  @Test
  void testGetIcebergSchemaAndMetadataPath() {
    try (MockedConstruction<HadoopTables> tablesCons =
            mockConstruction(
                HadoopTables.class,
                (mockTables, ctx) -> {
                  BaseTable mockTable = mock(BaseTable.class);
                  TableOperations ops = mock(TableOperations.class);
                  TableMetadata meta = mock(TableMetadata.class);

                  when(mockTables.load(ICEBERG_BASE_PATH)).thenReturn(mockTable);
                  when(mockTable.operations()).thenReturn(ops);
                  when(ops.current()).thenReturn(meta);
                  when(meta.metadataFileLocation())
                      .thenReturn(ICEBERG_BASE_PATH + ICEBERG_METADATA_JSON);
                });
        MockedStatic<SchemaParser> parserStatic = mockStatic(SchemaParser.class)) {
      parserStatic.when(() -> SchemaParser.toJson(any())).thenReturn(ICEBERG_SCHEMA);

      Pair<String, String> result =
          util.getIcebergSchemaAndMetadataPath(ICEBERG_BASE_PATH, hadoopConf);

      assertEquals(ICEBERG_BASE_PATH + ICEBERG_METADATA_JSON, result.getLeft());
      assertEquals(ICEBERG_SCHEMA, result.getRight());
    }
  }

  @Test
  void testGetConversionSourceProvider() {
    ConversionSourceProvider<?> hudiProvider =
        util.getConversionSourceProvider(TableFormat.HUDI, hadoopConf);
    assertTrue(hudiProvider instanceof HudiConversionSourceProvider);

    ConversionSourceProvider<?> deltaProvider =
        util.getConversionSourceProvider(TableFormat.DELTA, hadoopConf);
    assertTrue(deltaProvider instanceof DeltaConversionSourceProvider);

    ConversionSourceProvider<?> icebergProvider =
        util.getConversionSourceProvider(TableFormat.ICEBERG, hadoopConf);
    assertTrue(icebergProvider instanceof IcebergConversionSourceProvider);

    assertThrows(
        IllegalArgumentException.class,
        () -> util.getConversionSourceProvider("SomeFormat", hadoopConf));
  }
}
