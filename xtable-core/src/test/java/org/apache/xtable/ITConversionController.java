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
import static org.apache.xtable.hudi.HudiSourceConfig.PARTITION_FIELD_SPEC_CONFIG;
import static org.apache.xtable.hudi.HudiTestUtil.PartitionConfig;
import static org.apache.xtable.model.storage.TableFormat.DELTA;
import static org.apache.xtable.model.storage.TableFormat.HUDI;
import static org.apache.xtable.model.storage.TableFormat.ICEBERG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import lombok.Builder;
import lombok.Value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import org.apache.spark.sql.delta.DeltaLog;

import com.google.common.collect.ImmutableList;

import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.delta.DeltaConversionSourceProvider;
import org.apache.xtable.hudi.HudiConversionSourceProvider;
import org.apache.xtable.hudi.HudiTestUtil;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncMode;

public class ITConversionController {
  @TempDir public static Path tempDir;
  private static final DateTimeFormatter DATE_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("UTC"));

  private static JavaSparkContext jsc;
  private static SparkSession sparkSession;

  @BeforeAll
  public static void setupOnce() {
    SparkConf sparkConf = HudiTestUtil.getSparkConf(tempDir);
    sparkSession =
        SparkSession.builder().config(HoodieReadClient.addHoodieSupport(sparkConf)).getOrCreate();
    sparkSession
        .sparkContext()
        .hadoopConfiguration()
        .set("parquet.avro.write-old-list-structure", "false");
    jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
  }

  @AfterAll
  public static void teardown() {
    if (jsc != null) {
      jsc.close();
    }
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

  private static Stream<Arguments> testCasesWithPartitioningAndSyncModes() {
    return addBasicPartitionCases(testCasesWithSyncModes());
  }

  private static Stream<Arguments> generateTestParametersForFormatsSyncModesAndPartitioning() {
    List<Arguments> arguments = new ArrayList<>();
    for (String sourceTableFormat : Arrays.asList(HUDI, DELTA, ICEBERG)) {
      for (SyncMode syncMode : SyncMode.values()) {
        for (boolean isPartitioned : new boolean[] {true, false}) {
          arguments.add(Arguments.of(sourceTableFormat, syncMode, isPartitioned));
        }
      }
    }
    return arguments.stream();
  }

  private static Stream<Arguments> testCasesWithSyncModes() {
    return Stream.of(Arguments.of(SyncMode.INCREMENTAL), Arguments.of(SyncMode.FULL));
  }

  private ConversionSourceProvider<?> getConversionSourceProvider(String sourceTableFormat) {
    if (sourceTableFormat.equalsIgnoreCase(HUDI)) {
      ConversionSourceProvider<HoodieInstant> hudiConversionSourceProvider =
          new HudiConversionSourceProvider();
      hudiConversionSourceProvider.init(jsc.hadoopConfiguration());
      return hudiConversionSourceProvider;
    } else if (sourceTableFormat.equalsIgnoreCase(DELTA)) {
      ConversionSourceProvider<Long> deltaConversionSourceProvider =
          new DeltaConversionSourceProvider();
      deltaConversionSourceProvider.init(jsc.hadoopConfiguration());
      return deltaConversionSourceProvider;
    } else if (sourceTableFormat.equalsIgnoreCase(ICEBERG)) {
      ConversionSourceProvider<Snapshot> icebergConversionSourceProvider =
          new IcebergConversionSourceProvider();
      icebergConversionSourceProvider.init(jsc.hadoopConfiguration());
      return icebergConversionSourceProvider;
    } else {
      throw new IllegalArgumentException("Unsupported source format: " + sourceTableFormat);
    }
  }

  /*
   * This test has the following steps at a high level.
   * 1. Insert few records.
   * 2. Upsert few records.
   * 3. Delete few records.
   * 4. Insert records with new columns.
   * 5. Insert records in a new partition if table is partitioned.
   * 6. drop a partition if table is partitioned.
   * 7. Insert records in the dropped partition again if table is partitioned.
   */
  @ParameterizedTest
  @MethodSource("generateTestParametersForFormatsSyncModesAndPartitioning")
  public void testVariousOperations(
      String sourceTableFormat, SyncMode syncMode, boolean isPartitioned) {
    String tableName = getTableName();
    ConversionController conversionController = new ConversionController(jsc.hadoopConfiguration());
    List<String> targetTableFormats = getOtherFormats(sourceTableFormat);
    String partitionConfig = null;
    if (isPartitioned) {
      partitionConfig = "level:VALUE";
    }
    ConversionSourceProvider<?> conversionSourceProvider =
        getConversionSourceProvider(sourceTableFormat);
    List<?> insertRecords;
    try (GenericTable table =
        GenericTable.getInstance(
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

      // make multiple commits and then sync
      table.insertRows(100);
      table.upsertRows(insertRecords.subList(0, 20));
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(sourceTableFormat, table, targetTableFormats, 200);

      table.deleteRows(insertRecords.subList(30, 50));
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(sourceTableFormat, table, targetTableFormats, 180);
      checkDatasetEquivalenceWithFilter(
          sourceTableFormat, table, targetTableFormats, table.getFilterQuery());
    }

    try (GenericTable tableWithUpdatedSchema =
        GenericTable.getInstanceWithAdditionalColumns(
            tableName, tempDir, sparkSession, jsc, sourceTableFormat, isPartitioned)) {
      ConversionConfig conversionConfig =
          getTableSyncConfig(
              sourceTableFormat,
              syncMode,
              tableName,
              tableWithUpdatedSchema,
              targetTableFormats,
              partitionConfig,
              null);
      List<Row> insertsAfterSchemaUpdate = tableWithUpdatedSchema.insertRows(100);
      tableWithUpdatedSchema.reload();
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(sourceTableFormat, tableWithUpdatedSchema, targetTableFormats, 280);

      tableWithUpdatedSchema.deleteRows(insertsAfterSchemaUpdate.subList(60, 90));
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(sourceTableFormat, tableWithUpdatedSchema, targetTableFormats, 250);

      if (isPartitioned) {
        // Adds new partition.
        tableWithUpdatedSchema.insertRecordsForSpecialPartition(50);
        conversionController.sync(conversionConfig, conversionSourceProvider);
        checkDatasetEquivalence(sourceTableFormat, tableWithUpdatedSchema, targetTableFormats, 300);

        // Drops partition.
        tableWithUpdatedSchema.deleteSpecialPartition();
        conversionController.sync(conversionConfig, conversionSourceProvider);
        checkDatasetEquivalence(sourceTableFormat, tableWithUpdatedSchema, targetTableFormats, 250);

        // Insert records to the dropped partition again.
        tableWithUpdatedSchema.insertRecordsForSpecialPartition(50);
        conversionController.sync(conversionConfig, conversionSourceProvider);
        checkDatasetEquivalence(sourceTableFormat, tableWithUpdatedSchema, targetTableFormats, 300);
      }
    }
  }

  @ParameterizedTest
  @MethodSource("testCasesWithPartitioningAndSyncModes")
  public void testConcurrentInsertWritesInSource(
      SyncMode syncMode, PartitionConfig partitionConfig) {
    String tableName = getTableName();
    ConversionSourceProvider<?> conversionSourceProvider = getConversionSourceProvider(HUDI);
    List<String> targetTableFormats = getOtherFormats(HUDI);
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), HoodieTableType.COPY_ON_WRITE)) {
      // commit time 1 starts first but ends 2nd.
      // commit time 2 starts second but ends 1st.
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(50);
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit2 = table.generateRecords(50);
      String commitInstant1 = table.startCommit();

      String commitInstant2 = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit2, commitInstant2, true);

      ConversionConfig conversionConfig =
          getTableSyncConfig(
              HUDI,
              syncMode,
              tableName,
              table,
              targetTableFormats,
              partitionConfig.getXTableConfig(),
              null);
      ConversionController conversionController =
          new ConversionController(jsc.hadoopConfiguration());
      conversionController.sync(conversionConfig, conversionSourceProvider);

      checkDatasetEquivalence(HUDI, table, targetTableFormats, 50);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, targetTableFormats, 100);
    }
  }

  @ParameterizedTest
  @MethodSource("testCasesWithPartitioningAndSyncModes")
  public void testConcurrentInsertsAndTableServiceWrites(
      SyncMode syncMode, PartitionConfig partitionConfig) {
    HoodieTableType tableType = HoodieTableType.MERGE_ON_READ;
    ConversionSourceProvider<?> conversionSourceProvider = getConversionSourceProvider(HUDI);
    List<String> targetTableFormats = getOtherFormats(HUDI);
    String tableName = getTableName();
    try (TestSparkHudiTable table =
        TestSparkHudiTable.forStandardSchema(
            tableName, tempDir, jsc, partitionConfig.getHudiConfig(), tableType)) {
      List<HoodieRecord<HoodieAvroPayload>> insertedRecords1 = table.insertRecords(50, true);

      ConversionConfig conversionConfig =
          getTableSyncConfig(
              HUDI,
              syncMode,
              tableName,
              table,
              targetTableFormats,
              partitionConfig.getXTableConfig(),
              null);
      ConversionController conversionController =
          new ConversionController(jsc.hadoopConfiguration());
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, targetTableFormats, 50);

      table.deleteRecords(insertedRecords1.subList(0, 20), true);
      // At this point table should have 30 records but only after compaction.
      String scheduledCompactionInstant = table.onlyScheduleCompaction();

      table.insertRecords(50, true);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      Map<String, String> sourceHudiOptions =
          Collections.singletonMap("hoodie.datasource.query.type", "read_optimized");
      // Because compaction is not completed yet and read optimized query, there are 100 records.
      checkDatasetEquivalence(
          HUDI, table, sourceHudiOptions, targetTableFormats, Collections.emptyMap(), 100);

      table.insertRecords(50, true);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      // Because compaction is not completed yet and read optimized query, there are 150 records.
      checkDatasetEquivalence(
          HUDI, table, sourceHudiOptions, targetTableFormats, Collections.emptyMap(), 150);

      table.completeScheduledCompaction(scheduledCompactionInstant);
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalence(HUDI, table, targetTableFormats, 130);
    }
  }

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
      ConversionController conversionController =
          new ConversionController(jsc.hadoopConfiguration());
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

  private static List<String> getOtherFormats(String sourceTableFormat) {
    return Arrays.stream(TableFormat.values())
        .filter(format -> !format.equals(sourceTableFormat))
        .collect(Collectors.toList());
  }

  private static Stream<Arguments> provideArgsForPartitionTesting() {
    String timestampFilter =
        String.format(
            "timestamp_micros_nullable_field < timestamp_millis(%s)",
            Instant.now().truncatedTo(ChronoUnit.DAYS).minus(2, ChronoUnit.DAYS).toEpochMilli());
    String levelFilter = "level = 'INFO'";
    String nestedLevelFilter = "nested_record.level = 'INFO'";
    String severityFilter = "severity = 1";
    String timestampAndLevelFilter = String.format("%s and %s", timestampFilter, levelFilter);
    return Stream.of(
        Arguments.of(
            buildArgsForPartition(
                HUDI, Arrays.asList(ICEBERG, DELTA), "level:SIMPLE", "level:VALUE", levelFilter)),
        Arguments.of(
            buildArgsForPartition(
                DELTA, Arrays.asList(ICEBERG, HUDI), null, "level:VALUE", levelFilter)),
        Arguments.of(
            buildArgsForPartition(
                ICEBERG, Arrays.asList(DELTA, HUDI), null, "level:VALUE", levelFilter)),
        Arguments.of(
            // Delta Lake does not currently support nested partition columns
            buildArgsForPartition(
                HUDI,
                Arrays.asList(ICEBERG),
                "nested_record.level:SIMPLE",
                "nested_record.level:VALUE",
                nestedLevelFilter)),
        Arguments.of(
            buildArgsForPartition(
                HUDI,
                Arrays.asList(ICEBERG, DELTA),
                "severity:SIMPLE",
                "severity:VALUE",
                severityFilter)),
        Arguments.of(
            buildArgsForPartition(
                HUDI,
                Arrays.asList(ICEBERG, DELTA),
                "timestamp_micros_nullable_field:TIMESTAMP,level:SIMPLE",
                "timestamp_micros_nullable_field:DAY:yyyy/MM/dd,level:VALUE",
                timestampAndLevelFilter)));
  }

  @ParameterizedTest
  @MethodSource("provideArgsForPartitionTesting")
  public void testPartitionedData(TableFormatPartitionDataHolder tableFormatPartitionDataHolder) {
    String tableName = getTableName();
    String sourceTableFormat = tableFormatPartitionDataHolder.getSourceTableFormat();
    List<String> targetTableFormats = tableFormatPartitionDataHolder.getTargetTableFormats();
    Optional<String> hudiPartitionConfig = tableFormatPartitionDataHolder.getHudiSourceConfig();
    String xTablePartitionConfig = tableFormatPartitionDataHolder.getXTablePartitionConfig();
    String filter = tableFormatPartitionDataHolder.getFilter();
    ConversionSourceProvider<?> conversionSourceProvider =
        getConversionSourceProvider(sourceTableFormat);
    GenericTable table;
    if (hudiPartitionConfig.isPresent()) {
      table =
          GenericTable.getInstanceWithCustomPartitionConfig(
              tableName, tempDir, jsc, sourceTableFormat, hudiPartitionConfig.get());
    } else {
      table =
          GenericTable.getInstance(tableName, tempDir, sparkSession, jsc, sourceTableFormat, true);
    }
    try (GenericTable tableToClose = table) {
      ConversionConfig conversionConfig =
          getTableSyncConfig(
              sourceTableFormat,
              SyncMode.INCREMENTAL,
              tableName,
              table,
              targetTableFormats,
              xTablePartitionConfig,
              null);
      tableToClose.insertRows(100);
      ConversionController conversionController =
          new ConversionController(jsc.hadoopConfiguration());
      conversionController.sync(conversionConfig, conversionSourceProvider);
      // Do a second sync to force the test to read back the metadata it wrote earlier
      tableToClose.insertRows(100);
      conversionController.sync(conversionConfig, conversionSourceProvider);

      checkDatasetEquivalenceWithFilter(
          sourceTableFormat, tableToClose, targetTableFormats, filter);
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

      ConversionController conversionController =
          new ConversionController(jsc.hadoopConfiguration());
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
      ConversionController conversionController =
          new ConversionController(jsc.hadoopConfiguration());
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
  public void testIcebergCorruptedSnapshotRecovery() throws Exception {
    String tableName = getTableName();
    ConversionSourceProvider<?> conversionSourceProvider = getConversionSourceProvider(HUDI);
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      table.insertRows(20);
      ConversionController conversionController =
          new ConversionController(jsc.hadoopConfiguration());
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
      ConversionController conversionController =
          new ConversionController(jsc.hadoopConfiguration());
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

  private Map<String, String> getTimeTravelOption(String tableFormat, Instant time) {
    Map<String, String> options = new HashMap<>();
    switch (tableFormat) {
      case HUDI:
        options.put("as.of.instant", DATE_FORMAT.format(time));
        break;
      case ICEBERG:
        options.put("as-of-timestamp", String.valueOf(time.toEpochMilli()));
        break;
      case DELTA:
        options.put("timestampAsOf", DATE_FORMAT.format(time));
        break;
      default:
        throw new IllegalArgumentException("Unknown table format: " + tableFormat);
    }
    return options;
  }

  private void checkDatasetEquivalenceWithFilter(
      String sourceFormat,
      GenericTable<?, ?> sourceTable,
      List<String> targetFormats,
      String filter) {
    checkDatasetEquivalence(
        sourceFormat,
        sourceTable,
        Collections.emptyMap(),
        targetFormats,
        Collections.emptyMap(),
        null,
        filter);
  }

  private void checkDatasetEquivalence(
      String sourceFormat,
      GenericTable<?, ?> sourceTable,
      List<String> targetFormats,
      Integer expectedCount) {
    checkDatasetEquivalence(
        sourceFormat,
        sourceTable,
        Collections.emptyMap(),
        targetFormats,
        Collections.emptyMap(),
        expectedCount,
        "1 = 1");
  }

  private void checkDatasetEquivalence(
      String sourceFormat,
      GenericTable<?, ?> sourceTable,
      Map<String, String> sourceOptions,
      List<String> targetFormats,
      Map<String, Map<String, String>> targetOptions,
      Integer expectedCount) {
    checkDatasetEquivalence(
        sourceFormat,
        sourceTable,
        sourceOptions,
        targetFormats,
        targetOptions,
        expectedCount,
        "1 = 1");
  }

  private void checkDatasetEquivalence(
      String sourceFormat,
      GenericTable<?, ?> sourceTable,
      Map<String, String> sourceOptions,
      List<String> targetFormats,
      Map<String, Map<String, String>> targetOptions,
      Integer expectedCount,
      String filterCondition) {
    Dataset<Row> sourceRows =
        sparkSession
            .read()
            .options(sourceOptions)
            .format(sourceFormat.toLowerCase())
            .load(sourceTable.getBasePath())
            .orderBy(sourceTable.getOrderByColumn())
            .filter(filterCondition);
    Map<String, Dataset<Row>> targetRowsByFormat =
        targetFormats.stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    targetFormat -> {
                      Map<String, String> finalTargetOptions =
                          targetOptions.getOrDefault(targetFormat, Collections.emptyMap());
                      if (targetFormat.equals(HUDI)) {
                        finalTargetOptions = new HashMap<>(finalTargetOptions);
                        finalTargetOptions.put(HoodieMetadataConfig.ENABLE.key(), "true");
                        finalTargetOptions.put(
                            "hoodie.datasource.read.extract.partition.values.from.path", "true");
                      }
                      return sparkSession
                          .read()
                          .options(finalTargetOptions)
                          .format(targetFormat.toLowerCase())
                          .load(sourceTable.getDataPath())
                          .orderBy(sourceTable.getOrderByColumn())
                          .filter(filterCondition);
                    }));

    String[] selectColumnsArr = sourceTable.getColumnsToSelect().toArray(new String[] {});
    List<String> dataset1Rows = sourceRows.selectExpr(selectColumnsArr).toJSON().collectAsList();
    targetRowsByFormat.forEach(
        (format, targetRows) -> {
          List<String> dataset2Rows =
              targetRows.selectExpr(selectColumnsArr).toJSON().collectAsList();
          assertEquals(
              dataset1Rows.size(),
              dataset2Rows.size(),
              String.format(
                  "Datasets have different row counts when reading from Spark. Source: %s, Target: %s",
                  sourceFormat, format));
          // sanity check the count to ensure test is set up properly
          if (expectedCount != null) {
            assertEquals(expectedCount, dataset1Rows.size());
          } else {
            // if count is not known ahead of time, ensure datasets are non-empty
            assertFalse(dataset1Rows.isEmpty());
          }
          assertEquals(
              dataset1Rows,
              dataset2Rows,
              String.format(
                  "Datasets are not equivalent when reading from Spark. Source: %s, Target: %s",
                  sourceFormat, format));
        });
  }

  private static Stream<Arguments> addBasicPartitionCases(Stream<Arguments> arguments) {
    // add unpartitioned and partitioned cases
    return arguments.flatMap(
        args -> {
          Object[] unpartitionedArgs = Arrays.copyOf(args.get(), args.get().length + 1);
          unpartitionedArgs[unpartitionedArgs.length - 1] = PartitionConfig.of(null, null);
          Object[] partitionedArgs = Arrays.copyOf(args.get(), args.get().length + 1);
          partitionedArgs[partitionedArgs.length - 1] =
              PartitionConfig.of("level:SIMPLE", "level:VALUE");
          return Stream.of(
              Arguments.arguments(unpartitionedArgs), Arguments.arguments(partitionedArgs));
        });
  }

  private static TableFormatPartitionDataHolder buildArgsForPartition(
      String sourceFormat,
      List<String> targetFormats,
      String hudiPartitionConfig,
      String xTablePartitionConfig,
      String filter) {
    return TableFormatPartitionDataHolder.builder()
        .sourceTableFormat(sourceFormat)
        .targetTableFormats(targetFormats)
        .hudiSourceConfig(Optional.ofNullable(hudiPartitionConfig))
        .xTablePartitionConfig(xTablePartitionConfig)
        .filter(filter)
        .build();
  }

  @Builder
  @Value
  private static class TableFormatPartitionDataHolder {
    String sourceTableFormat;
    List<String> targetTableFormats;
    String xTablePartitionConfig;
    Optional<String> hudiSourceConfig;
    String filter;
  }

  private static ConversionConfig getTableSyncConfig(
      String sourceTableFormat,
      SyncMode syncMode,
      String tableName,
      GenericTable table,
      List<String> targetTableFormats,
      String partitionConfig,
      Duration metadataRetention) {
    Properties sourceProperties = new Properties();
    if (partitionConfig != null) {
      sourceProperties.put(PARTITION_FIELD_SPEC_CONFIG, partitionConfig);
    }
    SourceTable sourceTable =
        SourceTable.builder()
            .name(tableName)
            .formatName(sourceTableFormat)
            .basePath(table.getBasePath())
            .dataPath(table.getDataPath())
            .additionalProperties(sourceProperties)
            .build();

    List<TargetTable> targetTables =
        targetTableFormats.stream()
            .map(
                formatName ->
                    TargetTable.builder()
                        .name(tableName)
                        .formatName(formatName)
                        // set the metadata path to the data path as the default (required by Hudi)
                        .basePath(table.getDataPath())
                        .metadataRetention(metadataRetention)
                        .build())
            .collect(Collectors.toList());

    return ConversionConfig.builder()
        .sourceTable(sourceTable)
        .targetTables(targetTables)
        .syncMode(syncMode)
        .build();
  }
}
