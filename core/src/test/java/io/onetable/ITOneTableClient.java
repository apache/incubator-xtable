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
 
package io.onetable;

import static io.onetable.hudi.HudiTestUtil.PartitionConfig;
import static io.onetable.hudi.HudiTestUtil.getTableName;
import static java.util.stream.Collectors.groupingBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.avro.Schema;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import org.apache.spark.sql.delta.DeltaLog;

import com.google.common.collect.ImmutableList;

import io.onetable.client.OneTableClient;
import io.onetable.client.PerTableConfig;
import io.onetable.client.SourceClientProvider;
import io.onetable.exception.SchemaExtractorException;
import io.onetable.hudi.HudiSourceClientProvider;
import io.onetable.hudi.HudiSourceConfig;
import io.onetable.model.storage.TableFormat;
import io.onetable.model.sync.SyncMode;
import io.onetable.model.sync.SyncResult;

public class ITOneTableClient {
  @TempDir public static Path tempDir;
  private static final DateTimeFormatter DATE_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("UTC"));

  private static JavaSparkContext jsc;
  private static SparkSession sparkSession;
  private SourceClientProvider<HoodieInstant> hudiSourceClientProvider;

  @BeforeAll
  public static void setupOnce() {
    SparkConf sparkConf =
        new SparkConf()
            .setAppName("onetable-testing")
            .set("spark.serializer", KryoSerializer.class.getName())
            .set("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.default_iceberg.type", "hadoop")
            .set("spark.sql.catalog.default_iceberg.warehouse", tempDir.toString())
            .set("spark.sql.catalog.hadoop_prod", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.hadoop_prod.type", "hadoop")
            .set("spark.sql.catalog.hadoop_prod.warehouse", tempDir.toString())
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("parquet.avro.write-old-list-structure", "false")
            // Needed for ignoring not nullable constraints on nested columns in Delta.
            .set("spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled", "true")
            .set("spark.sql.shuffle.partitions", "4")
            .set("spark.default.parallelism", "4")
            .set("spark.sql.session.timeZone", "UTC")
            .set("spark.sql.iceberg.handle-timestamp-without-timezone", "true")
            .setMaster("local[4]");
    sparkSession =
        SparkSession.builder().config(HoodieReadClient.addHoodieSupport(sparkConf)).getOrCreate();
    sparkSession
        .sparkContext()
        .hadoopConfiguration()
        .set("parquet.avro.write-old-list-structure", "false");
    jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
  }

  @BeforeEach
  public void setup() {
    hudiSourceClientProvider = new HudiSourceClientProvider();
    hudiSourceClientProvider.init(jsc.hadoopConfiguration(), Collections.emptyMap());
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

  private static Stream<Arguments> testCasesWithPartitioningAndTableTypesAndSyncModes() {
    return addBasicPartitionCases(testCasesWithTableTypesAndSyncModes());
  }

  private static Stream<Arguments> testCasesWithTableTypesAndSyncModes() {
    return addTableTypeCases(testCasesWithSyncModes());
  }

  private static Stream<Arguments> testCasesWithSyncModes() {
    return addSyncModeCases(
        Stream.of(Arguments.of(Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA))));
  }

  @ParameterizedTest
  @MethodSource("testCasesWithPartitioningAndTableTypesAndSyncModes")
  public void testUpsertData(
      List<TableFormat> targetTableFormats,
      SyncMode syncMode,
      HoodieTableType tableType,
      PartitionConfig partitionConfig) {
    String tableName = getTableName();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {
      List<HoodieRecord<HoodieAvroPayload>> insertedRecords = table.insertRecords(100, true);

      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(table.getBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionFieldSpecConfig(partitionConfig.getOneTableConfig())
                      .build())
              .syncMode(syncMode)
              .build();
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 100);

      table.insertRecords(100, true);
      table.upsertRecords(insertedRecords.subList(0, 20), true);

      syncWithCompactionIfRequired(tableType, table, perTableConfig, oneTableClient);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 200);
    }
  }

  @ParameterizedTest
  @MethodSource("testCasesWithPartitioningAndTableTypesAndSyncModes")
  public void testConcurrentInsertWritesInSource(
      List<TableFormat> targetTableFormats,
      SyncMode syncMode,
      HoodieTableType tableType,
      PartitionConfig partitionConfig) {
    String tableName = getTableName();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {
      // commit time 1 starts first but ends 2nd.
      // commit time 2 starts second but ends 1st.
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(50);
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit2 = table.generateRecords(50);
      String commitInstant1 = table.startCommit();

      String commitInstant2 = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit2, commitInstant2, true);

      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(table.getBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionFieldSpecConfig(partitionConfig.getOneTableConfig())
                      .build())
              .syncMode(syncMode)
              .build();
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);

      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 50);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 100);
    }
  }

  @ParameterizedTest
  @MethodSource("testCasesWithPartitioningAndSyncModes")
  public void testConcurrentInsertsAndTableServiceWrites(
      List<TableFormat> targetTableFormats, SyncMode syncMode, PartitionConfig partitionConfig) {
    HoodieTableType tableType = HoodieTableType.MERGE_ON_READ;

    String tableName = getTableName();
    try (TestSparkHudiTable table =
        TestSparkHudiTable.forStandardSchema(
            tableName, tempDir, jsc, partitionConfig.getHudiConfig(), tableType)) {
      List<HoodieRecord<HoodieAvroPayload>> insertedRecords1 = table.insertRecords(50, true);

      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(table.getBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionFieldSpecConfig(partitionConfig.getOneTableConfig())
                      .build())
              .syncMode(syncMode)
              .build();
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 50);

      table.deleteRecords(insertedRecords1.subList(0, 20), true);
      // At this point table should have 30 records but only after compaction.
      String scheduledCompactionInstant = table.onlyScheduleCompaction();

      table.insertRecords(50, true);
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      Map<String, String> sourceHudiOptions =
          Collections.singletonMap("hoodie.datasource.query.type", "read_optimized");
      // Because compaction is not completed yet and read optimized query, there are 100 records.
      checkDatasetEquivalence(
          TableFormat.HUDI,
          sourceHudiOptions,
          targetTableFormats,
          Collections.emptyMap(),
          table.getBasePath(),
          100);

      table.insertRecords(50, true);
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      // Because compaction is not completed yet and read optimized query, there are 150 records.
      checkDatasetEquivalence(
          TableFormat.HUDI,
          sourceHudiOptions,
          targetTableFormats,
          Collections.emptyMap(),
          table.getBasePath(),
          150);

      table.completeScheduledCompaction(scheduledCompactionInstant);
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 130);
    }
  }

  @ParameterizedTest
  @MethodSource("testCasesWithPartitioningAndTableTypesAndSyncModes")
  public void testDeleteData(
      List<TableFormat> targetTableFormats,
      SyncMode syncMode,
      HoodieTableType tableType,
      PartitionConfig partitionConfig) {
    String tableName = getTableName();
    try (TestSparkHudiTable table =
        TestSparkHudiTable.forStandardSchema(
            tableName, tempDir, jsc, partitionConfig.getHudiConfig(), tableType)) {
      List<HoodieRecord<HoodieAvroPayload>> insertedRecords = table.insertRecords(50, true);

      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(table.getBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionFieldSpecConfig(partitionConfig.getOneTableConfig())
                      .build())
              .syncMode(syncMode)
              .build();
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 50);

      table.deleteRecords(insertedRecords.subList(0, 20), true);

      syncWithCompactionIfRequired(tableType, table, perTableConfig, oneTableClient);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 30);
    }
  }

  @ParameterizedTest
  @MethodSource("testCasesWithTableTypesAndSyncModes")
  public void testAddPartition(
      List<TableFormat> targetTableFormats, SyncMode syncMode, HoodieTableType tableType) {
    String tableName = getTableName();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(tableName, tempDir, "level:SIMPLE", tableType)) {
      table.insertRecords(10, "INFO", true);

      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(table.getBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder().partitionFieldSpecConfig("level:VALUE").build())
              .syncMode(syncMode)
              .build();
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 10);

      table.insertRecords(10, "WARN", true);

      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 20);
    }
  }

  @ParameterizedTest
  @MethodSource("testCasesWithTableTypesAndSyncModes")
  public void testDeletePartition(
      List<TableFormat> targetTableFormats, SyncMode syncMode, HoodieTableType tableType) {
    String tableName = getTableName();
    // Java client does not support delete partition.
    try (TestSparkHudiTable table =
        TestSparkHudiTable.forStandardSchema(tableName, tempDir, jsc, "level:SIMPLE", tableType)) {
      List<HoodieRecord<HoodieAvroPayload>> insertedRecords = table.insertRecords(100, true);
      Map<String, List<HoodieRecord>> recordsByPartition =
          insertedRecords.stream().collect(groupingBy(HoodieRecord::getPartitionPath));
      String partitionToDelete = recordsByPartition.keySet().stream().sorted().findFirst().get();

      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(table.getBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder().partitionFieldSpecConfig("level:VALUE").build())
              .syncMode(syncMode)
              .build();
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 100);

      table.deletePartition(partitionToDelete, tableType);

      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(
          TableFormat.HUDI,
          targetTableFormats,
          table.getBasePath(),
          100 - recordsByPartition.get(partitionToDelete).size());
    }
  }

  @ParameterizedTest
  @MethodSource("testCasesWithPartitioningAndTableTypesAndSyncModes")
  public void testAddColumns(
      List<TableFormat> targetTableFormats,
      SyncMode syncMode,
      HoodieTableType tableType,
      PartitionConfig partitionConfig) {
    String tableName = getTableName();
    OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
    List<HoodieRecord<HoodieAvroPayload>> insertedRecords;
    try (TestSparkHudiTable tableWithInitialSchema =
        TestSparkHudiTable.forStandardSchema(
            tableName, tempDir, jsc, partitionConfig.getHudiConfig(), tableType)) {
      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(tableWithInitialSchema.getBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionFieldSpecConfig(partitionConfig.getOneTableConfig())
                      .build())
              .syncMode(syncMode)
              .build();
      insertedRecords = tableWithInitialSchema.insertRecords(50, true);
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(
          TableFormat.HUDI, targetTableFormats, tableWithInitialSchema.getBasePath(), 50);
    }
    try (TestSparkHudiTable tableWithUpdatedSchema =
        TestSparkHudiTable.withAdditionalColumns(
            tableName, tempDir, jsc, partitionConfig.getHudiConfig(), tableType)) {
      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(tableWithUpdatedSchema.getBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionFieldSpecConfig(partitionConfig.getOneTableConfig())
                      .build())
              .syncMode(syncMode)
              .build();

      tableWithUpdatedSchema.insertRecords(50, true);
      tableWithUpdatedSchema.deleteRecords(insertedRecords.subList(0, 20), true);
      syncWithCompactionIfRequired(
          tableType, tableWithUpdatedSchema, perTableConfig, oneTableClient);
      checkDatasetEquivalence(
          TableFormat.HUDI, targetTableFormats, tableWithUpdatedSchema.getBasePath(), 80);
    }
  }

  @ParameterizedTest
  @MethodSource("testCasesWithPartitioningAndTableTypesAndSyncModes")
  public void testAddColumnsBeforeInitialSync(
      List<TableFormat> targetTableFormats,
      SyncMode syncMode,
      HoodieTableType tableType,
      PartitionConfig partitionConfig) {
    String tableName = getTableName();
    OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
    List<HoodieRecord<HoodieAvroPayload>> insertedRecords;
    // evolve the schema before the first sync
    try (TestJavaHudiTable tableWithInitialSchema =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {
      insertedRecords = tableWithInitialSchema.insertRecords(50, true);
    }
    Schema previousSchema = null;
    try (TestJavaHudiTable tableWithUpdatedSchema =
        TestJavaHudiTable.withAdditionalColumns(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {
      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(tableWithUpdatedSchema.getBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionFieldSpecConfig(partitionConfig.getOneTableConfig())
                      .build())
              .syncMode(syncMode)
              .build();
      tableWithUpdatedSchema.insertRecords(50, true);
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(
          TableFormat.HUDI, targetTableFormats, tableWithUpdatedSchema.getBasePath(), 100);
      previousSchema = tableWithUpdatedSchema.getSchema();
    }
    // Add one more column and sync
    try (TestJavaHudiTable tableWithUpdatedSchema =
        TestJavaHudiTable.withAdditionalTopLevelField(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType, previousSchema)) {
      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(tableWithUpdatedSchema.getBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionFieldSpecConfig(partitionConfig.getOneTableConfig())
                      .build())
              .syncMode(syncMode)
              .build();
      tableWithUpdatedSchema.insertRecords(50, true);
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(
          TableFormat.HUDI, targetTableFormats, tableWithUpdatedSchema.getBasePath(), 150);
    }
  }

  @ParameterizedTest
  @MethodSource("testCasesWithPartitioningAndTableTypesAndSyncModes")
  public void testCleanEvent(
      List<TableFormat> targetTableFormats,
      SyncMode syncMode,
      HoodieTableType tableType,
      PartitionConfig partitionConfig) {
    String tableName = getTableName();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {
      List<HoodieRecord<HoodieAvroPayload>> insertedRecords = table.insertRecords(50, true);

      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(table.getBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionFieldSpecConfig(partitionConfig.getOneTableConfig())
                      .build())
              .syncMode(syncMode)
              .build();
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      // sync once to establish initial OneTable state
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      table.upsertRecords(insertedRecords.subList(0, 20), true);
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        table.compact();
      }
      table.insertRecords(50, true);
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 100);
      table.clean();

      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 100);
    }
  }

  @ParameterizedTest
  @MethodSource("testCasesWithPartitioningAndTableTypesAndSyncModes")
  public void testClusteringEvent(
      List<TableFormat> targetTableFormats,
      SyncMode syncMode,
      HoodieTableType tableType,
      PartitionConfig partitionConfig) {
    String tableName = getTableName();
    try (TestSparkHudiTable table =
        TestSparkHudiTable.forStandardSchema(
            tableName, tempDir, jsc, partitionConfig.getHudiConfig(), tableType)) {
      List<HoodieRecord<HoodieAvroPayload>> insertedRecords = table.insertRecords(50, true);

      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(table.getBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionFieldSpecConfig(partitionConfig.getOneTableConfig())
                      .build())
              .syncMode(syncMode)
              .build();
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      // sync once to establish initial OneTable state
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);

      table.upsertRecords(insertedRecords.subList(0, 20), true);
      table.insertRecords(50, true);
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        table.compact();
      }
      table.cluster();
      table.insertRecords(50, true);

      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 150);
    }
  }

  @ParameterizedTest
  @MethodSource("testCasesWithPartitioningAndTableTypesAndSyncModes")
  public void testSavepointRestoreEvent(
      List<TableFormat> targetTableFormats,
      SyncMode syncMode,
      HoodieTableType tableType,
      PartitionConfig partitionConfig) {
    String tableName = getTableName();
    try (TestSparkHudiTable table =
        TestSparkHudiTable.forStandardSchema(
            tableName, tempDir, jsc, partitionConfig.getHudiConfig(), tableType)) {
      table.insertRecords(50, true);

      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(table.getBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionFieldSpecConfig(partitionConfig.getOneTableConfig())
                      .build())
              .syncMode(syncMode)
              .build();
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      // sync once to establish initial OneTable state
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);

      table.insertRecords(50, true);
      table.insertRecords(50, true);
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 150);

      table.savepointRestoreForPreviousInstant();
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 100);

      table.insertRecords(50, true);
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 150);
    }
  }

  @ParameterizedTest
  @MethodSource("testCasesWithPartitioningAndTableTypesAndSyncModes")
  public void testRollbackEvents(
      List<TableFormat> targetTableFormats,
      SyncMode syncMode,
      HoodieTableType tableType,
      PartitionConfig partitionConfig) {
    String tableName = getTableName();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {
      table.insertRecords(50, true);

      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(table.getBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionFieldSpecConfig(partitionConfig.getOneTableConfig())
                      .build())
              .syncMode(syncMode)
              .build();
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      // sync once to establish initial OneTable state
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);

      table.insertRecords(50, true);
      table.insertRecords(50, true);
      table.insertRecords(50, true);
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 200);

      table.rollback(
          table.getActiveTimeline().getCommitsTimeline().lastInstant().get().getTimestamp());
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 150);
      table.rollback(
          table.getActiveTimeline().getCommitsTimeline().lastInstant().get().getTimestamp());
      table.rollback(
          table.getActiveTimeline().getCommitsTimeline().lastInstant().get().getTimestamp());
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 50);
      table.rollback(
          table.getActiveTimeline().getCommitsTimeline().lastInstant().get().getTimestamp());
      assertThrows(
          SchemaExtractorException.class,
          () -> oneTableClient.sync(perTableConfig, hudiSourceClientProvider));
    }
  }

  @ParameterizedTest
  @MethodSource("testCasesWithSyncModes")
  public void testTimeTravelQueries(List<TableFormat> targetTableFormats, SyncMode syncMode) {
    String tableName = getTableName();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      table.insertRecords(50, true);

      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(table.getBasePath())
              .syncMode(syncMode)
              .build();
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      Instant instantAfterFirstSync = Instant.now();

      table.insertRecords(50, true);
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      Instant instantAfterSecondSync = Instant.now();

      table.insertRecords(50, true);
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);

      checkDatasetEquivalence(
          TableFormat.HUDI,
          getTimeTravelOption(TableFormat.HUDI, instantAfterFirstSync),
          targetTableFormats,
          targetTableFormats.stream()
              .collect(
                  Collectors.toMap(
                      Function.identity(),
                      targetTableFormat ->
                          getTimeTravelOption(targetTableFormat, instantAfterFirstSync))),
          table.getBasePath(),
          50);
      checkDatasetEquivalence(
          TableFormat.HUDI,
          getTimeTravelOption(TableFormat.HUDI, instantAfterSecondSync),
          targetTableFormats,
          targetTableFormats.stream()
              .collect(
                  Collectors.toMap(
                      Function.identity(),
                      targetTableFormat ->
                          getTimeTravelOption(targetTableFormat, instantAfterSecondSync))),
          table.getBasePath(),
          100);
    }
  }

  private static Stream<Arguments> provideArgsForPartitionTesting() {
    String levelFilter = "level = 'INFO'";
    String severityFilter = "severity = 1";
    return addSyncModeCases(
        Stream.of(
            Arguments.of(
                Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA),
                "level:VALUE",
                "level:SIMPLE",
                levelFilter),
            Arguments.of(
                Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA),
                "severity:VALUE",
                "severity:SIMPLE",
                severityFilter)));
  }

  @ParameterizedTest
  @MethodSource("provideArgsForPartitionTesting")
  public void testPartitionedData(
      List<TableFormat> targetTableFormats,
      String oneTablePartitionConfig,
      String hudiPartitionConfig,
      String filter,
      SyncMode syncMode) {
    String tableName = getTableName();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, hudiPartitionConfig, HoodieTableType.COPY_ON_WRITE)) {
      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(table.getBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionFieldSpecConfig(oneTablePartitionConfig)
                      .build())
              .syncMode(syncMode)
              .build();
      table.insertRecords(100, true);
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      // Do a second sync to force the test to read back the metadata it wrote earlier
      table.insertRecords(100, true);
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);

      checkDatasetEquivalenceWithFilter(
          TableFormat.HUDI, targetTableFormats, table.getBasePath(), filter);
    }
  }

  @ParameterizedTest
  @EnumSource(value = SyncMode.class)
  public void testSyncWithSingleFormat(SyncMode syncMode) {
    String tableName = getTableName();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      table.insertRecords(100, true);

      PerTableConfig perTableConfigIceberg =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(ImmutableList.of(TableFormat.ICEBERG))
              .tableBasePath(table.getBasePath())
              .syncMode(syncMode)
              .build();

      PerTableConfig perTableConfigDelta =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(ImmutableList.of(TableFormat.DELTA))
              .tableBasePath(table.getBasePath())
              .syncMode(syncMode)
              .build();

      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      oneTableClient.sync(perTableConfigIceberg, hudiSourceClientProvider);
      checkDatasetEquivalence(
          TableFormat.HUDI,
          Collections.singletonList(TableFormat.ICEBERG),
          table.getBasePath(),
          100);
      oneTableClient.sync(perTableConfigDelta, hudiSourceClientProvider);
      checkDatasetEquivalence(
          TableFormat.HUDI, Collections.singletonList(TableFormat.DELTA), table.getBasePath(), 100);

      table.insertRecords(100, true);
      oneTableClient.sync(perTableConfigIceberg, hudiSourceClientProvider);
      checkDatasetEquivalence(
          TableFormat.HUDI,
          Collections.singletonList(TableFormat.ICEBERG),
          table.getBasePath(),
          200);
      oneTableClient.sync(perTableConfigDelta, hudiSourceClientProvider);
      checkDatasetEquivalence(
          TableFormat.HUDI, Collections.singletonList(TableFormat.DELTA), table.getBasePath(), 200);
    }
  }

  @Test
  public void testSyncForInvalidPerTableConfig() {
    String tableName = getTableName();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, "level:SIMPLE", HoodieTableType.COPY_ON_WRITE)) {
      table.insertRecords(100, true);

      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(ImmutableList.of())
              .tableBasePath(table.getBasePath())
              .build();
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());

      assertThrows(
          IllegalArgumentException.class,
          () -> oneTableClient.sync(perTableConfig, hudiSourceClientProvider));
    }
  }

  @Test
  public void testOutOfSyncIncrementalSyncs() {
    String tableName = getTableName();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      PerTableConfig singleTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(ImmutableList.of(TableFormat.ICEBERG))
              .tableBasePath(table.getBasePath())
              .syncMode(SyncMode.INCREMENTAL)
              .build();

      PerTableConfig dualTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA))
              .tableBasePath(table.getBasePath())
              .syncMode(SyncMode.INCREMENTAL)
              .build();

      table.insertRecords(50, true);
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      // sync iceberg only
      oneTableClient.sync(singleTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(
          TableFormat.HUDI,
          Collections.singletonList(TableFormat.ICEBERG),
          table.getBasePath(),
          50);
      // insert more records
      table.insertRecords(50, true);
      // iceberg will be an incremental sync and delta will need to bootstrap with snapshot sync
      oneTableClient.sync(dualTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(
          TableFormat.HUDI,
          Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA),
          table.getBasePath(),
          100);

      // insert more records
      table.insertRecords(50, true);
      // insert more records
      table.insertRecords(50, true);
      // incremental sync for two commits for iceberg only
      oneTableClient.sync(singleTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(
          TableFormat.HUDI,
          Collections.singletonList(TableFormat.ICEBERG),
          table.getBasePath(),
          200);

      // insert more records
      table.insertRecords(50, true);
      // incremental sync for one commit for iceberg and three commits for delta
      oneTableClient.sync(dualTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(
          TableFormat.HUDI,
          Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA),
          table.getBasePath(),
          250);
    }
  }

  @Test
  public void testMetadataRetention() {
    String tableName = getTableName();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA))
              .tableBasePath(table.getBasePath())
              .syncMode(SyncMode.INCREMENTAL)
              .targetMetadataRetentionInHours(0) // force cleanup
              .build();
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      table.insertRecords(10, true);
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      // later we will ensure we can still read the source table at this instant to ensure that
      // neither target cleaned up the underlying parquet files in the table
      Instant instantAfterFirstCommit = Instant.now();
      // create 5 total commits to ensure Delta Log cleanup is
      IntStream.range(0, 4)
          .forEach(
              unused -> {
                table.insertRecords(10, true);
                oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
              });
      // ensure that hudi rows can still be read and underlying files were not removed
      List<Row> rows =
          sparkSession
              .read()
              .format("hudi")
              .options(getTimeTravelOption(TableFormat.HUDI, instantAfterFirstCommit))
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
      Assertions.assertTrue(deltaLog.enableExpiredLogCleanup());
    }
  }

  private void syncWithCompactionIfRequired(
      HoodieTableType tableType,
      TestAbstractHudiTable table,
      PerTableConfig perTableConfig,
      OneTableClient oneTableClient) {
    if (tableType == HoodieTableType.MERGE_ON_READ) {
      // sync once before compaction and assert no failures
      Map<TableFormat, SyncResult> syncResults =
          oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      assertNoSyncFailures(syncResults);

      table.compact();
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
    } else {
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
    }
  }

  private Map<String, String> getTimeTravelOption(TableFormat tableFormat, Instant time) {
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

  private void checkDatasetEquivalence(
      TableFormat sourceFormat,
      List<TableFormat> targetFormats,
      String basePath,
      Integer expectedCount) {
    checkDatasetEquivalence(
        sourceFormat,
        Collections.emptyMap(),
        targetFormats,
        Collections.emptyMap(),
        basePath,
        expectedCount,
        "1 = 1");
  }

  private void checkDatasetEquivalenceWithFilter(
      TableFormat sourceFormat, List<TableFormat> targetFormats, String basePath, String filter) {
    checkDatasetEquivalence(
        sourceFormat,
        Collections.emptyMap(),
        targetFormats,
        Collections.emptyMap(),
        basePath,
        null,
        filter);
  }

  private void checkDatasetEquivalence(
      TableFormat sourceFormat,
      Map<String, String> sourceOptions,
      List<TableFormat> targetFormats,
      Map<TableFormat, Map<String, String>> targetOptions,
      String basePath,
      Integer expectedCount) {
    checkDatasetEquivalence(
        sourceFormat,
        sourceOptions,
        targetFormats,
        targetOptions,
        basePath,
        expectedCount,
        "1 = 1");
  }

  private void checkDatasetEquivalence(
      TableFormat sourceFormat,
      Map<String, String> sourceOptions,
      List<TableFormat> targetFormats,
      Map<TableFormat, Map<String, String>> targetOptions,
      String basePath,
      Integer expectedCount,
      String filterCondition) {
    Dataset<Row> sourceRows =
        sparkSession
            .read()
            .options(sourceOptions)
            .format(sourceFormat.name().toLowerCase())
            .load(basePath)
            .orderBy("_hoodie_record_key")
            .filter(filterCondition);
    Map<TableFormat, Dataset<Row>> targetRowsByFormat =
        targetFormats.stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    targetFormat ->
                        sparkSession
                            .read()
                            .options(
                                targetOptions.getOrDefault(targetFormat, Collections.emptyMap()))
                            .format(targetFormat.name().toLowerCase())
                            .load(basePath)
                            .orderBy("_hoodie_record_key")
                            .filter(filterCondition)));
    final Set<String> selectColumns = getFieldNamesRemovingGeneratedColumns(sourceRows.schema());

    targetRowsByFormat
        .values()
        .forEach(
            targetRows ->
                selectColumns.addAll(getFieldNamesRemovingGeneratedColumns(targetRows.schema())));
    String[] selectColumnsArr = selectColumns.toArray(new String[] {});
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

  private Set<String> getFieldNamesRemovingGeneratedColumns(StructType schema) {
    return Arrays.stream(schema.fields())
        .map(StructField::name)
        .filter(name -> !name.startsWith("onetable_partition_col_"))
        .collect(Collectors.toSet());
  }

  private void assertNoSyncFailures(Map<TableFormat, SyncResult> results) {
    Assertions.assertTrue(
        results.values().stream()
            .noneMatch(
                result -> result.getStatus().getStatusCode() != SyncResult.SyncStatusCode.SUCCESS));
  }

  private static Stream<Arguments> addSyncModeCases(Stream<Arguments> arguments) {
    return arguments.flatMap(
        args -> {
          Object[] snapshotArgs = Arrays.copyOf(args.get(), args.get().length + 1);
          snapshotArgs[snapshotArgs.length - 1] = SyncMode.FULL;
          Object[] incrementalArgs = Arrays.copyOf(args.get(), args.get().length + 1);
          incrementalArgs[incrementalArgs.length - 1] = SyncMode.INCREMENTAL;
          return Stream.of(Arguments.arguments(snapshotArgs), Arguments.arguments(incrementalArgs));
        });
  }

  private static Stream<Arguments> addTableTypeCases(Stream<Arguments> arguments) {
    return arguments.flatMap(
        args -> {
          Object[] morArgs = Arrays.copyOf(args.get(), args.get().length + 1);
          morArgs[morArgs.length - 1] = HoodieTableType.MERGE_ON_READ;
          Object[] cowArgs = Arrays.copyOf(args.get(), args.get().length + 1);
          cowArgs[cowArgs.length - 1] = HoodieTableType.COPY_ON_WRITE;
          return Stream.of(Arguments.arguments(morArgs), Arguments.arguments(cowArgs));
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
}
