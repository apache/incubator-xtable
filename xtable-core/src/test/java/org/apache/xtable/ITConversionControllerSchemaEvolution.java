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
 
package org.apache.xtable;

import static org.apache.xtable.GenericTable.getTableName;
import static org.apache.xtable.model.storage.TableFormat.DELTA;
import static org.apache.xtable.model.storage.TableFormat.HUDI;
import static org.apache.xtable.model.storage.TableFormat.ICEBERG;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hudi.common.model.HoodieTableType;

import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.model.sync.SyncMode;

/** Schema-level concerns: UUID columns, Delta column mapping and corrupted-snapshot recovery. */
public class ITConversionControllerSchemaEvolution extends ConversionControllerTestBase {

  private static Stream<Arguments> generateTestParametersForUUID() {
    List<Arguments> arguments = new ArrayList<>();
    for (SyncMode syncMode : SyncMode.values()) {
      for (boolean isPartitioned : new boolean[] {true, false}) {
        // TODO: Add Hudi UUID support later (https://github.com/apache/incubator-xtable/issues/543)
        // Current spark parquet reader can not handle fix-size byte array with UUID logic type
        List<String> targetTableFormats = Arrays.asList(DELTA);
        arguments.add(Arguments.of(ICEBERG, targetTableFormats, syncMode, isPartitioned));
      }
    }
    return arguments.stream();
  }

  // The test content is the simplified version of testVariousOperations
  // The difference is that the data source from Iceberg contains UUID columns
  @ParameterizedTest
  @MethodSource("generateTestParametersForUUID")
  public void testVariousOperationsWithUUID(
      String sourceTableFormat,
      List<String> targetTableFormats,
      SyncMode syncMode,
      boolean isPartitioned) {
    String tableName = getTableName();
    String partitionConfig = null;
    if (isPartitioned) {
      partitionConfig = "level:VALUE";
    }
    ConversionSourceProvider<?> conversionSourceProvider =
        getConversionSourceProvider(sourceTableFormat);
    List<?> insertRecords;
    try (GenericTable table =
        GenericTable.getInstanceWithUUIDColumns(
            tableName, tempDir, sparkSession, jsc, sourceTableFormat, isPartitioned)) {
      insertRecords = table.insertRows(100);

      ConversionConfig conversionConfig =
          getTableSyncConfig(
              sourceTableFormat,
              syncMode,
              tableName,
              table,
              targetTableFormats,
              partitionConfig,
              null);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(sourceTableFormat, table, targetTableFormats, 100);

      // Upsert some records and sync again
      table.upsertRows(insertRecords.subList(0, 20));
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(sourceTableFormat, table, targetTableFormats, 100);

      table.deleteRows(insertRecords.subList(30, 50));
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(sourceTableFormat, table, targetTableFormats, 80);
      checkDatasetEquivalenceWithFilter(
          sourceTableFormat,
          table,
          targetTableFormats,
          table.getFilterQuery(),
          Collections.emptyMap());
    }
  }

  @Test
  public void testIcebergCorruptedSnapshotRecovery() throws Exception {
    String tableName = getTableName();
    ConversionSourceProvider<?> conversionSourceProvider = getConversionSourceProvider(HUDI);
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      table.insertRows(20);
      ConversionConfig conversionConfig =
          getTableSyncConfig(
              HUDI,
              SyncMode.INCREMENTAL,
              tableName,
              table,
              Collections.singletonList(ICEBERG),
              null,
              null);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      table.insertRows(10);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      table.insertRows(10);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      // corrupt last two snapshots
      Table icebergTable = new HadoopTables(jsc.hadoopConfiguration()).load(table.getBasePath());
      long currentSnapshotId = icebergTable.currentSnapshot().snapshotId();
      long previousSnapshotId = icebergTable.currentSnapshot().parentId();
      Files.delete(
          Paths.get(URI.create(icebergTable.snapshot(currentSnapshotId).manifestListLocation())));
      Files.delete(
          Paths.get(URI.create(icebergTable.snapshot(previousSnapshotId).manifestListLocation())));
      table.insertRows(10);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, Collections.singletonList(ICEBERG), 50);
    }
  }

  @Test
  public void testColumnMappingEnabledDeltaToIceberg() {
    String tableName = getTableName();
    ConversionSourceProvider<?> conversionSourceProvider = getConversionSourceProvider(DELTA);
    try (TestSparkDeltaTable table =
        TestSparkDeltaTable.forColumnMappingEnabled(tableName, tempDir, sparkSession, null)) {
      table.insertRows(20);
      ConversionController conversionController =
          new ConversionController(jsc.hadoopConfiguration());
      ConversionConfig conversionConfig =
          getTableSyncConfig(
              DELTA,
              SyncMode.INCREMENTAL,
              tableName,
              table,
              Collections.singletonList(ICEBERG),
              null,
              null);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      table.insertRows(10);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      table.insertRows(10);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(DELTA, table, Collections.singletonList(ICEBERG), 40);

      table.dropColumn("long_field");
      table.insertRows(10);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(DELTA, table, Collections.singletonList(ICEBERG), 50);

      table.renameColumn("double_field", "scores");
      table.insertRows(10);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(DELTA, table, Collections.singletonList(ICEBERG), 60);

      table.addColumn();
      table.insertRows(10);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(DELTA, table, Collections.singletonList(ICEBERG), 70);
    }
  }
}
