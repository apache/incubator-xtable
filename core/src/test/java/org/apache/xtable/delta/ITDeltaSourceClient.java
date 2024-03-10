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
 
package org.apache.xtable.delta;

import static org.junit.jupiter.api.Assertions.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.xtable.GenericTable;
import org.apache.xtable.TestSparkDeltaTable;
import org.apache.xtable.ValidationTestHelper;
import org.apache.xtable.client.PerTableConfig;
import org.apache.xtable.client.PerTableConfigImpl;
import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.OneSnapshot;
import org.apache.xtable.model.OneTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.schema.OneField;
import org.apache.xtable.model.schema.OnePartitionField;
import org.apache.xtable.model.schema.OneSchema;
import org.apache.xtable.model.schema.OneType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.schema.SchemaCatalog;
import org.apache.xtable.model.schema.SchemaVersion;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.OneDataFile;
import org.apache.xtable.model.storage.OneFileGroup;
import org.apache.xtable.model.storage.TableFormat;

public class ITDeltaSourceClient {

  private static final OneField COL1_INT_FIELD =
      OneField.builder()
          .name("col1")
          .schema(
              OneSchema.builder().name("integer").dataType(OneType.INT).isNullable(true).build())
          .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
          .build();
  private static final ColumnStat COL1_COLUMN_STAT =
      ColumnStat.builder()
          .field(COL1_INT_FIELD)
          .range(Range.vector(1, 1))
          .numNulls(0)
          .numValues(1)
          .totalSize(0)
          .build();

  private static final OneField COL2_INT_FIELD =
      OneField.builder()
          .name("col2")
          .schema(
              OneSchema.builder().name("integer").dataType(OneType.INT).isNullable(true).build())
          .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
          .build();
  private static final ColumnStat COL2_COLUMN_STAT =
      ColumnStat.builder()
          .field(COL2_INT_FIELD)
          .range(Range.vector(2, 2))
          .numNulls(0)
          .numValues(1)
          .totalSize(0)
          .build();

  @TempDir private static Path tempDir;
  private static SparkSession sparkSession;

  private DeltaSourceClientProvider clientProvider;

  @BeforeAll
  public static void setupOnce() {
    sparkSession =
        SparkSession.builder()
            .appName("TestDeltaTable")
            .master("local[4]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.serializer", KryoSerializer.class.getName())
            .getOrCreate();
  }

  @AfterAll
  public static void teardown() {
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

  @BeforeEach
  void setUp() {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "file:///");

    clientProvider = new DeltaSourceClientProvider();
    clientProvider.init(hadoopConf, null);
  }

  @Test
  void getCurrentSnapshotNonPartitionedTest() throws URISyntaxException {
    // Table name
    final String tableName = GenericTable.getTableName();
    final Path basePath = tempDir.resolve(tableName);
    // Create table with a single row using Spark
    sparkSession.sql(
        "CREATE TABLE `"
            + tableName
            + "` USING DELTA LOCATION '"
            + basePath
            + "' AS SELECT * FROM VALUES (1, 2)");
    // Create Delta source client
    PerTableConfig tableConfig =
        PerTableConfigImpl.builder()
            .tableName(tableName)
            .tableBasePath(basePath.toString())
            .targetTableFormats(Collections.singletonList(TableFormat.ICEBERG))
            .build();
    DeltaSourceClient client = clientProvider.getSourceClientInstance(tableConfig);
    // Get current snapshot
    OneSnapshot snapshot = client.getCurrentSnapshot();
    // Validate table
    List<OneField> fields = Arrays.asList(COL1_INT_FIELD, COL2_INT_FIELD);
    validateTable(
        snapshot.getTable(),
        tableName,
        TableFormat.DELTA,
        OneSchema.builder().name("struct").dataType(OneType.RECORD).fields(fields).build(),
        DataLayoutStrategy.FLAT,
        "file:" + basePath,
        Collections.emptyList());
    // Validate schema catalog
    SchemaCatalog oneSchemaCatalog = snapshot.getSchemaCatalog();
    validateSchemaCatalog(
        oneSchemaCatalog,
        Collections.singletonMap(new SchemaVersion(1, ""), snapshot.getTable().getReadSchema()));
    // Validate data files
    List<ColumnStat> columnStats = Arrays.asList(COL1_COLUMN_STAT, COL2_COLUMN_STAT);
    Assertions.assertEquals(1, snapshot.getPartitionedDataFiles().size());
    validatePartitionDataFiles(
        OneFileGroup.builder()
            .files(
                Collections.singletonList(
                    OneDataFile.builder()
                        .physicalPath("file:/fake/path")
                        .fileFormat(FileFormat.APACHE_PARQUET)
                        .partitionValues(Collections.emptyList())
                        .fileSizeBytes(684)
                        .recordCount(1)
                        .columnStats(columnStats)
                        .build()))
            .partitionValues(Collections.emptyList())
            .build(),
        snapshot.getPartitionedDataFiles().get(0));
  }

  @Test
  void getCurrentSnapshotPartitionedTest() throws URISyntaxException {
    // Table name
    final String tableName = GenericTable.getTableName();
    final Path basePath = tempDir.resolve(tableName);
    // Create table with a single row using Spark
    sparkSession.sql(
        "CREATE TABLE `"
            + tableName
            + "` USING DELTA PARTITIONED BY (part_col)\n"
            + "LOCATION '"
            + basePath
            + "' AS SELECT 'SingleValue' AS part_col, 1 AS col1, 2 AS col2");
    // Create Delta source client
    PerTableConfig tableConfig =
        PerTableConfigImpl.builder()
            .tableName(tableName)
            .tableBasePath(basePath.toString())
            .targetTableFormats(Collections.singletonList(TableFormat.ICEBERG))
            .build();
    DeltaSourceClient client = clientProvider.getSourceClientInstance(tableConfig);
    // Get current snapshot
    OneSnapshot snapshot = client.getCurrentSnapshot();
    // Validate table
    OneField partCol =
        OneField.builder()
            .name("part_col")
            .schema(
                OneSchema.builder()
                    .name("string")
                    .dataType(OneType.STRING)
                    .isNullable(true)
                    .build())
            .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
            .build();
    List<OneField> fields = Arrays.asList(partCol, COL1_INT_FIELD, COL2_INT_FIELD);
    validateTable(
        snapshot.getTable(),
        tableName,
        TableFormat.DELTA,
        OneSchema.builder().name("struct").dataType(OneType.RECORD).fields(fields).build(),
        DataLayoutStrategy.HIVE_STYLE_PARTITION,
        "file:" + basePath,
        Collections.singletonList(
            OnePartitionField.builder()
                .sourceField(partCol)
                .transformType(PartitionTransformType.VALUE)
                .build()));
    // Validate schema catalog
    SchemaCatalog oneSchemaCatalog = snapshot.getSchemaCatalog();
    validateSchemaCatalog(
        oneSchemaCatalog,
        Collections.singletonMap(new SchemaVersion(1, ""), snapshot.getTable().getReadSchema()));
    // Validate data files
    List<ColumnStat> columnStats = Arrays.asList(COL1_COLUMN_STAT, COL2_COLUMN_STAT);
    Assertions.assertEquals(1, snapshot.getPartitionedDataFiles().size());
    List<PartitionValue> partitionValue =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(
                    OnePartitionField.builder()
                        .sourceField(partCol)
                        .transformType(PartitionTransformType.VALUE)
                        .build())
                .range(Range.scalar("SingleValue"))
                .build());
    validatePartitionDataFiles(
        OneFileGroup.builder()
            .partitionValues(partitionValue)
            .files(
                Collections.singletonList(
                    OneDataFile.builder()
                        .physicalPath("file:/fake/path")
                        .fileFormat(FileFormat.APACHE_PARQUET)
                        .partitionValues(partitionValue)
                        .fileSizeBytes(684)
                        .recordCount(1)
                        .columnStats(columnStats)
                        .build()))
            .build(),
        snapshot.getPartitionedDataFiles().get(0));
  }

  @Disabled("Requires Spark 3.4.0+")
  @Test
  void getCurrentSnapshotGenColPartitionedTest() {
    // Table name
    final String tableName = GenericTable.getTableName();
    final Path basePath = tempDir.resolve(tableName);
    // Create table with a single row using Spark
    sparkSession.sql(
        "CREATE TABLE `"
            + tableName
            + "` (id BIGINT, event_time TIMESTAMP, day INT GENERATED ALWAYS AS (DATE_FORMAT(event_time, 'YYYY-MM-dd')))"
            + " USING DELTA LOCATION '"
            + basePath
            + "'");
    sparkSession.sql(
        "INSERT INTO TABLE `"
            + tableName
            + "` VALUES(1, CAST('2012-02-12 00:12:34' AS TIMESTAMP))");
    // Create Delta source client
    PerTableConfig tableConfig =
        PerTableConfigImpl.builder()
            .tableName(tableName)
            .tableBasePath(basePath.toString())
            .targetTableFormats(Collections.singletonList(TableFormat.ICEBERG))
            .build();
    DeltaSourceClient client = clientProvider.getSourceClientInstance(tableConfig);
    // Get current snapshot
    OneSnapshot snapshot = client.getCurrentSnapshot();
    // TODO: Complete and enable test (see https://github.com/onetable-io/onetable/issues/90)
  }

  @ParameterizedTest
  @MethodSource("testWithPartitionToggle")
  public void testInsertsUpsertsAndDeletes(boolean isPartitioned) {
    String tableName = GenericTable.getTableName();
    TestSparkDeltaTable testSparkDeltaTable =
        new TestSparkDeltaTable(
            tableName, tempDir, sparkSession, isPartitioned ? "yearOfBirth" : null, false);
    List<List<String>> allActiveFiles = new ArrayList<>();
    List<TableChange> allTableChanges = new ArrayList<>();
    List<Row> rows = testSparkDeltaTable.insertRows(50);
    Long timestamp1 = testSparkDeltaTable.getLastCommitTimestamp();
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    List<Row> rows1 = testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.upsertRows(rows.subList(0, 20));
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.deleteRows(rows1.subList(0, 20));
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());
    PerTableConfig tableConfig =
        PerTableConfigImpl.builder()
            .tableName(testSparkDeltaTable.getTableName())
            .tableBasePath(testSparkDeltaTable.getBasePath())
            .targetTableFormats(Arrays.asList(TableFormat.HUDI, TableFormat.ICEBERG))
            .build();
    DeltaSourceClient deltaSourceClient = clientProvider.getSourceClientInstance(tableConfig);
    assertEquals(180L, testSparkDeltaTable.getNumRows());
    OneSnapshot oneSnapshot = deltaSourceClient.getCurrentSnapshot();

    if (isPartitioned) {
      validateDeltaPartitioning(oneSnapshot);
    }
    ValidationTestHelper.validateOneSnapshot(
        oneSnapshot, allActiveFiles.get(allActiveFiles.size() - 1));
    // Get changes in incremental format.
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
            .build();
    CommitsBacklog<Long> commitsBacklog =
        deltaSourceClient.getCommitsBacklog(instantsForIncrementalSync);
    for (Long version : commitsBacklog.getCommitsToProcess()) {
      TableChange tableChange = deltaSourceClient.getTableChangeForCommit(version);
      allTableChanges.add(tableChange);
    }
    ValidationTestHelper.validateTableChanges(allActiveFiles, allTableChanges);
  }

  @Test
  public void testsShowingVacuumHasNoEffectOnIncrementalSync() {
    boolean isPartitioned = true;
    String tableName = GenericTable.getTableName();
    TestSparkDeltaTable testSparkDeltaTable =
        new TestSparkDeltaTable(
            tableName, tempDir, sparkSession, isPartitioned ? "yearOfBirth" : null, false);
    // Insert 50 rows to 2018 partition.
    List<Row> commit1Rows = testSparkDeltaTable.insertRowsForPartition(50, 2018);
    Long timestamp1 = testSparkDeltaTable.getLastCommitTimestamp();
    PerTableConfig tableConfig =
        PerTableConfigImpl.builder()
            .tableName(testSparkDeltaTable.getTableName())
            .tableBasePath(testSparkDeltaTable.getBasePath())
            .targetTableFormats(Arrays.asList(TableFormat.HUDI, TableFormat.ICEBERG))
            .build();
    DeltaSourceClient deltaSourceClient = clientProvider.getSourceClientInstance(tableConfig);
    OneSnapshot snapshotAfterCommit1 = deltaSourceClient.getCurrentSnapshot();
    List<String> allActivePaths = ValidationTestHelper.getAllFilePaths(snapshotAfterCommit1);
    assertEquals(1, allActivePaths.size());
    String activePathAfterCommit1 = allActivePaths.get(0);

    // Upsert all rows inserted before, so all files are replaced.
    testSparkDeltaTable.upsertRows(commit1Rows.subList(0, 50));

    // Insert 50 rows to different (2020) partition.
    testSparkDeltaTable.insertRowsForPartition(50, 2020);

    // Run vacuum. This deletes all older files from commit1 of 2018 partition.
    testSparkDeltaTable.runVacuum();

    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
            .build();
    deltaSourceClient = clientProvider.getSourceClientInstance(tableConfig);
    CommitsBacklog<Long> instantCurrentCommitState =
        deltaSourceClient.getCommitsBacklog(instantsForIncrementalSync);
    boolean areFilesRemoved = false;
    for (Long version : instantCurrentCommitState.getCommitsToProcess()) {
      TableChange tableChange = deltaSourceClient.getTableChangeForCommit(version);
      areFilesRemoved = areFilesRemoved | checkIfFileIsRemoved(activePathAfterCommit1, tableChange);
    }
    assertTrue(areFilesRemoved);
    assertTrue(deltaSourceClient.isIncrementalSyncSafeFrom(Instant.ofEpochMilli(timestamp1)));
    // Table doesn't have instant of this older commit, hence it is not safe.
    Instant instantAsOfHourAgo = Instant.now().minus(1, ChronoUnit.HOURS);
    assertFalse(deltaSourceClient.isIncrementalSyncSafeFrom(instantAsOfHourAgo));
  }

  @ParameterizedTest
  @MethodSource("testWithPartitionToggle")
  public void testVacuum(boolean isPartitioned) {
    String tableName = GenericTable.getTableName();
    TestSparkDeltaTable testSparkDeltaTable =
        new TestSparkDeltaTable(
            tableName, tempDir, sparkSession, isPartitioned ? "yearOfBirth" : null, false);
    List<List<String>> allActiveFiles = new ArrayList<>();
    List<TableChange> allTableChanges = new ArrayList<>();
    List<Row> rows = testSparkDeltaTable.insertRows(50);
    Long timestamp1 = testSparkDeltaTable.getLastCommitTimestamp();
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.deleteRows(rows.subList(0, 20));
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.runVacuum();
    // vacuum has two commits, one for start and one for end, hence adding twice.
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    PerTableConfig tableConfig =
        PerTableConfigImpl.builder()
            .tableName(testSparkDeltaTable.getTableName())
            .tableBasePath(testSparkDeltaTable.getBasePath())
            .targetTableFormats(Arrays.asList(TableFormat.HUDI, TableFormat.ICEBERG))
            .build();
    DeltaSourceClient deltaSourceClient = clientProvider.getSourceClientInstance(tableConfig);
    assertEquals(130L, testSparkDeltaTable.getNumRows());
    OneSnapshot oneSnapshot = deltaSourceClient.getCurrentSnapshot();
    if (isPartitioned) {
      validateDeltaPartitioning(oneSnapshot);
    }
    ValidationTestHelper.validateOneSnapshot(
        oneSnapshot, allActiveFiles.get(allActiveFiles.size() - 1));
    // Get changes in incremental format.
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
            .build();
    CommitsBacklog<Long> commitsBacklog =
        deltaSourceClient.getCommitsBacklog(instantsForIncrementalSync);
    for (Long version : commitsBacklog.getCommitsToProcess()) {
      TableChange tableChange = deltaSourceClient.getTableChangeForCommit(version);
      allTableChanges.add(tableChange);
    }
    ValidationTestHelper.validateTableChanges(allActiveFiles, allTableChanges);
  }

  @ParameterizedTest
  @MethodSource("testWithPartitionToggle")
  public void testAddColumns(boolean isPartitioned) {
    String tableName = GenericTable.getTableName();
    TestSparkDeltaTable testSparkDeltaTable =
        new TestSparkDeltaTable(
            tableName, tempDir, sparkSession, isPartitioned ? "yearOfBirth" : null, true);
    List<List<String>> allActiveFiles = new ArrayList<>();
    List<TableChange> allTableChanges = new ArrayList<>();
    List<Row> rows = testSparkDeltaTable.insertRows(50);
    Long timestamp1 = testSparkDeltaTable.getLastCommitTimestamp();
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    PerTableConfig tableConfig =
        PerTableConfigImpl.builder()
            .tableName(testSparkDeltaTable.getTableName())
            .tableBasePath(testSparkDeltaTable.getBasePath())
            .targetTableFormats(Arrays.asList(TableFormat.HUDI, TableFormat.ICEBERG))
            .build();
    DeltaSourceClient deltaSourceClient = clientProvider.getSourceClientInstance(tableConfig);
    assertEquals(150L, testSparkDeltaTable.getNumRows());
    OneSnapshot oneSnapshot = deltaSourceClient.getCurrentSnapshot();
    if (isPartitioned) {
      validateDeltaPartitioning(oneSnapshot);
    }
    ValidationTestHelper.validateOneSnapshot(
        oneSnapshot, allActiveFiles.get(allActiveFiles.size() - 1));
    // Get changes in incremental format.
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
            .build();
    CommitsBacklog<Long> commitsBacklog =
        deltaSourceClient.getCommitsBacklog(instantsForIncrementalSync);
    for (Long version : commitsBacklog.getCommitsToProcess()) {
      TableChange tableChange = deltaSourceClient.getTableChangeForCommit(version);
      allTableChanges.add(tableChange);
    }
    ValidationTestHelper.validateTableChanges(allActiveFiles, allTableChanges);
  }

  @Test
  public void testDropPartition() {
    String tableName = GenericTable.getTableName();
    TestSparkDeltaTable testSparkDeltaTable =
        new TestSparkDeltaTable(tableName, tempDir, sparkSession, "yearOfBirth", false);
    List<List<String>> allActiveFiles = new ArrayList<>();
    List<TableChange> allTableChanges = new ArrayList<>();

    List<Row> rows = testSparkDeltaTable.insertRows(50);
    Long timestamp1 = testSparkDeltaTable.getLastCommitTimestamp();
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    List<Row> rows1 = testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    List<Row> allRows = new ArrayList<>();
    allRows.addAll(rows);
    allRows.addAll(rows1);

    Map<Integer, List<Row>> rowsByPartition = testSparkDeltaTable.getRowsByPartition(allRows);
    Integer partitionValueToDelete = rowsByPartition.keySet().stream().findFirst().get();
    testSparkDeltaTable.deletePartition(partitionValueToDelete);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    // Insert few records for deleted partition again to make it interesting.
    testSparkDeltaTable.insertRowsForPartition(20, partitionValueToDelete);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    PerTableConfig tableConfig =
        PerTableConfigImpl.builder()
            .tableName(testSparkDeltaTable.getTableName())
            .tableBasePath(testSparkDeltaTable.getBasePath())
            .targetTableFormats(Arrays.asList(TableFormat.HUDI, TableFormat.ICEBERG))
            .build();
    DeltaSourceClient deltaSourceClient = clientProvider.getSourceClientInstance(tableConfig);
    assertEquals(
        120 - rowsByPartition.get(partitionValueToDelete).size(), testSparkDeltaTable.getNumRows());
    OneSnapshot oneSnapshot = deltaSourceClient.getCurrentSnapshot();

    validateDeltaPartitioning(oneSnapshot);
    ValidationTestHelper.validateOneSnapshot(
        oneSnapshot, allActiveFiles.get(allActiveFiles.size() - 1));
    // Get changes in incremental format.
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
            .build();
    CommitsBacklog<Long> commitsBacklog =
        deltaSourceClient.getCommitsBacklog(instantsForIncrementalSync);
    for (Long version : commitsBacklog.getCommitsToProcess()) {
      TableChange tableChange = deltaSourceClient.getTableChangeForCommit(version);
      allTableChanges.add(tableChange);
    }
    ValidationTestHelper.validateTableChanges(allActiveFiles, allTableChanges);
  }

  @ParameterizedTest
  @MethodSource("testWithPartitionToggle")
  public void testOptimizeAndClustering(boolean isPartitioned) {
    String tableName = GenericTable.getTableName();
    TestSparkDeltaTable testSparkDeltaTable =
        new TestSparkDeltaTable(
            tableName, tempDir, sparkSession, isPartitioned ? "yearOfBirth" : null, false);
    List<List<String>> allActiveFiles = new ArrayList<>();
    List<TableChange> allTableChanges = new ArrayList<>();
    List<Row> rows = testSparkDeltaTable.insertRows(50);
    Long timestamp1 = testSparkDeltaTable.getLastCommitTimestamp();
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.runCompaction();
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.runClustering();
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    PerTableConfig tableConfig =
        PerTableConfigImpl.builder()
            .tableName(testSparkDeltaTable.getTableName())
            .tableBasePath(testSparkDeltaTable.getBasePath())
            .targetTableFormats(Arrays.asList(TableFormat.HUDI, TableFormat.ICEBERG))
            .build();
    DeltaSourceClient deltaSourceClient = clientProvider.getSourceClientInstance(tableConfig);
    assertEquals(250L, testSparkDeltaTable.getNumRows());
    OneSnapshot oneSnapshot = deltaSourceClient.getCurrentSnapshot();
    if (isPartitioned) {
      validateDeltaPartitioning(oneSnapshot);
    }
    ValidationTestHelper.validateOneSnapshot(
        oneSnapshot, allActiveFiles.get(allActiveFiles.size() - 1));
    // Get changes in incremental format.
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
            .build();
    CommitsBacklog<Long> commitsBacklog =
        deltaSourceClient.getCommitsBacklog(instantsForIncrementalSync);
    for (Long version : commitsBacklog.getCommitsToProcess()) {
      TableChange tableChange = deltaSourceClient.getTableChangeForCommit(version);
      allTableChanges.add(tableChange);
    }
    ValidationTestHelper.validateTableChanges(allActiveFiles, allTableChanges);
  }

  private void validateDeltaPartitioning(OneSnapshot oneSnapshot) {
    List<OnePartitionField> partitionFields = oneSnapshot.getTable().getPartitioningFields();
    assertEquals(1, partitionFields.size());
    OnePartitionField partitionField = partitionFields.get(0);
    assertEquals("birthDate", partitionField.getSourceField().getName());
    assertEquals(PartitionTransformType.YEAR, partitionField.getTransformType());
  }

  private static void validateTable(
      OneTable oneTable,
      String tableName,
      String tableFormat,
      OneSchema readSchema,
      DataLayoutStrategy dataLayoutStrategy,
      String basePath,
      List<OnePartitionField> partitioningFields) {
    Assertions.assertEquals(tableName, oneTable.getName());
    Assertions.assertEquals(tableFormat, oneTable.getTableFormat());
    Assertions.assertEquals(readSchema, oneTable.getReadSchema());
    Assertions.assertEquals(dataLayoutStrategy, oneTable.getLayoutStrategy());
    Assertions.assertEquals(basePath, oneTable.getBasePath());
    Assertions.assertEquals(partitioningFields, oneTable.getPartitioningFields());
  }

  private void validateSchemaCatalog(
      SchemaCatalog oneSchemaCatalog, Map<SchemaVersion, OneSchema> schemas) {
    Assertions.assertEquals(schemas, oneSchemaCatalog.getSchemas());
  }

  private void validatePartitionDataFiles(
      OneFileGroup expectedPartitionFiles, OneFileGroup actualPartitionFiles)
      throws URISyntaxException {
    assertEquals(
        expectedPartitionFiles.getPartitionValues(), actualPartitionFiles.getPartitionValues());
    validateDataFiles(expectedPartitionFiles.getFiles(), actualPartitionFiles.getFiles());
  }

  private void validateDataFiles(List<OneDataFile> expectedFiles, List<OneDataFile> actualFiles)
      throws URISyntaxException {
    Assertions.assertEquals(expectedFiles.size(), actualFiles.size());
    for (int i = 0; i < expectedFiles.size(); i++) {
      OneDataFile expected = expectedFiles.get(i);
      OneDataFile actual = actualFiles.get(i);
      validatePropertiesDataFile(expected, actual);
    }
  }

  private void validatePropertiesDataFile(OneDataFile expected, OneDataFile actual)
      throws URISyntaxException {
    Assertions.assertEquals(expected.getSchemaVersion(), actual.getSchemaVersion());
    Assertions.assertTrue(
        Paths.get(new URI(actual.getPhysicalPath()).getPath()).isAbsolute(),
        () -> "path == " + actual.getPhysicalPath() + " is not absolute");
    Assertions.assertEquals(expected.getFileFormat(), actual.getFileFormat());
    Assertions.assertEquals(expected.getPartitionValues(), actual.getPartitionValues());
    Assertions.assertEquals(expected.getFileSizeBytes(), actual.getFileSizeBytes());
    Assertions.assertEquals(expected.getRecordCount(), actual.getRecordCount());
    Instant now = Instant.now();
    long minRange = now.minus(1, ChronoUnit.HOURS).toEpochMilli();
    long maxRange = now.toEpochMilli();
    Assertions.assertTrue(
        actual.getLastModified() > minRange && actual.getLastModified() <= maxRange,
        () ->
            "last modified == "
                + actual.getLastModified()
                + " is expected between "
                + minRange
                + " and "
                + maxRange);
    Assertions.assertEquals(expected.getColumnStats(), actual.getColumnStats());
  }

  private static Stream<Arguments> testWithPartitionToggle() {
    return Stream.of(Arguments.of(false), Arguments.of(true));
  }

  private boolean checkIfFileIsRemoved(String activePath, TableChange tableChange) {
    Set<String> filePathsRemoved =
        tableChange.getFilesDiff().getFilesRemoved().stream()
            .map(oneDf -> oneDf.getPhysicalPath())
            .collect(Collectors.toSet());
    return filePathsRemoved.contains(activePath);
  }
}
