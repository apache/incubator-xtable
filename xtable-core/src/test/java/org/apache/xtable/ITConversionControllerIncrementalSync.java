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

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import org.apache.spark.sql.Row;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.common.model.HoodieTableType;

import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import org.apache.spark.sql.delta.DeltaLog;

import com.google.common.collect.ImmutableList;

import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.model.sync.SyncMode;

/** Incremental sync semantics: time travel, partial target sets, no-op syncs and retention. */
public class ITConversionControllerIncrementalSync extends ConversionControllerTestBase {

  @ParameterizedTest
  @ValueSource(strings = {HUDI, DELTA, ICEBERG})
  public void testTimeTravelQueries(String sourceTableFormat) throws Exception {
    String tableName = getTableName();
    try (GenericTable table =
        GenericTable.getInstance(tableName, tempDir, sparkSession, jsc, sourceTableFormat, false)) {
      table.insertRows(50);
      List<String> targetTableFormats = getOtherFormats(sourceTableFormat);
      ConversionConfig conversionConfig =
          getTableSyncConfig(
              sourceTableFormat,
              SyncMode.INCREMENTAL,
              tableName,
              table,
              targetTableFormats,
              null,
              null);
      ConversionSourceProvider<?> conversionSourceProvider =
          getConversionSourceProvider(sourceTableFormat);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      Instant instantAfterFirstSync = Instant.now();
      // sleep before starting the next commit to avoid any rounding issues
      Thread.sleep(1000);

      table.insertRows(50);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      Instant instantAfterSecondSync = Instant.now();
      // sleep before starting the next commit to avoid any rounding issues
      Thread.sleep(1000);

      table.insertRows(50);
      conversionController.sync(conversionConfig, conversionSourceProvider);

      checkDatasetEquivalence(
          sourceTableFormat,
          table,
          getTimeTravelOption(sourceTableFormat, instantAfterFirstSync),
          targetTableFormats,
          targetTableFormats.stream()
              .collect(
                  Collectors.toMap(
                      Function.identity(),
                      targetTableFormat ->
                          getTimeTravelOption(targetTableFormat, instantAfterFirstSync))),
          50);
      checkDatasetEquivalence(
          sourceTableFormat,
          table,
          getTimeTravelOption(sourceTableFormat, instantAfterSecondSync),
          targetTableFormats,
          targetTableFormats.stream()
              .collect(
                  Collectors.toMap(
                      Function.identity(),
                      targetTableFormat ->
                          getTimeTravelOption(targetTableFormat, instantAfterSecondSync))),
          100);
    }
  }

  @ParameterizedTest
  @EnumSource(value = SyncMode.class)
  public void testSyncWithSingleFormat(SyncMode syncMode) {
    String tableName = getTableName();
    ConversionSourceProvider<?> conversionSourceProvider = getConversionSourceProvider(HUDI);
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      table.insertRecords(100, true);

      ConversionConfig conversionConfigIceberg =
          getTableSyncConfig(
              HUDI, syncMode, tableName, table, ImmutableList.of(ICEBERG), null, null);
      ConversionConfig conversionConfigDelta =
          getTableSyncConfig(HUDI, syncMode, tableName, table, ImmutableList.of(DELTA), null, null);

      conversionController.sync(conversionConfigIceberg, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, Collections.singletonList(ICEBERG), 100);
      conversionController.sync(conversionConfigDelta, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, Collections.singletonList(DELTA), 100);

      table.insertRecords(100, true);
      conversionController.sync(conversionConfigIceberg, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, Collections.singletonList(ICEBERG), 200);
      conversionController.sync(conversionConfigDelta, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, Collections.singletonList(DELTA), 200);
    }
  }

  @Test
  public void testOutOfSyncIncrementalSyncs() {
    String tableName = getTableName();
    ConversionSourceProvider<?> conversionSourceProvider = getConversionSourceProvider(HUDI);
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      ConversionConfig singleTableConfig =
          getTableSyncConfig(
              HUDI, SyncMode.INCREMENTAL, tableName, table, ImmutableList.of(ICEBERG), null, null);
      ConversionConfig dualTableConfig =
          getTableSyncConfig(
              HUDI,
              SyncMode.INCREMENTAL,
              tableName,
              table,
              Arrays.asList(ICEBERG, DELTA),
              null,
              null);

      table.insertRecords(50, true);
      // sync iceberg only
      conversionController.sync(singleTableConfig, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, Collections.singletonList(ICEBERG), 50);
      // insert more records
      table.insertRecords(50, true);
      // iceberg will be an incremental sync and delta will need to bootstrap with snapshot sync
      conversionController.sync(dualTableConfig, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, Arrays.asList(ICEBERG, DELTA), 100);

      // insert more records
      table.insertRecords(50, true);
      // insert more records
      table.insertRecords(50, true);
      // incremental sync for two commits for iceberg only
      conversionController.sync(singleTableConfig, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, Collections.singletonList(ICEBERG), 200);

      // insert more records
      table.insertRecords(50, true);
      // incremental sync for one commit for iceberg and three commits for delta
      conversionController.sync(dualTableConfig, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, Arrays.asList(ICEBERG, DELTA), 250);
    }
  }

  @Test
  public void testIncrementalSyncsWithNoChangesDoesNotThrowError() {
    String tableName = getTableName();
    ConversionSourceProvider<?> conversionSourceProvider = getConversionSourceProvider(HUDI);
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      ConversionConfig dualTableConfig =
          getTableSyncConfig(
              HUDI,
              SyncMode.INCREMENTAL,
              tableName,
              table,
              Arrays.asList(ICEBERG, DELTA),
              null,
              null);

      table.insertRecords(50, true);
      ConversionController conversionController =
          new ConversionController(jsc.hadoopConfiguration());
      // sync once
      conversionController.sync(dualTableConfig, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, Arrays.asList(DELTA, ICEBERG), 50);
      // sync again
      conversionController.sync(dualTableConfig, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, Arrays.asList(DELTA, ICEBERG), 50);
    }
  }

  @Test
  public void testMetadataRetention() throws Exception {
    String tableName = getTableName();
    ConversionSourceProvider<?> conversionSourceProvider = getConversionSourceProvider(HUDI);
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      ConversionConfig conversionConfig =
          getTableSyncConfig(
              HUDI,
              SyncMode.INCREMENTAL,
              tableName,
              table,
              Arrays.asList(ICEBERG, DELTA),
              null,
              Duration.ofHours(0)); // force cleanup
      table.insertRecords(10, true);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      // later we will ensure we can still read the source table at this instant to ensure that
      // neither target cleaned up the underlying parquet files in the table
      Instant instantAfterFirstCommit = Instant.now();
      // Ensure gap between commits for time-travel query
      Thread.sleep(1000);
      // create 5 total commits to ensure Delta Log cleanup is
      IntStream.range(0, 4)
          .forEach(
              unused -> {
                table.insertRecords(10, true);
                conversionController.sync(conversionConfig, conversionSourceProvider);
              });
      // ensure that hudi rows can still be read and underlying files were not removed
      List<Row> rows =
          sparkSession
              .read()
              .format("hudi")
              .options(getTimeTravelOption(HUDI, instantAfterFirstCommit))
              .load(table.getBasePath())
              .collectAsList();
      Assertions.assertEquals(10, rows.size());
      // check snapshots retained in iceberg is under 4
      Table icebergTable = new HadoopTables().load(table.getBasePath());
      int snapshotCount =
          (int) StreamSupport.stream(icebergTable.snapshots().spliterator(), false).count();
      Assertions.assertEquals(1, snapshotCount);
      // assert that proper settings are enabled for delta log
      DeltaLog deltaLog = DeltaLog.forTable(sparkSession, table.getBasePath());
      Assertions.assertTrue(deltaLog.enableExpiredLogCleanup(deltaLog.snapshot().metadata()));
    }
  }
}
