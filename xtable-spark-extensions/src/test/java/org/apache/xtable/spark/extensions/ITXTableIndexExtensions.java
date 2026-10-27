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
 
package org.apache.xtable.spark.extensions;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.Spark3Util;

import scala.Option;
import scala.collection.JavaConverters;

import org.apache.xtable.catalog.TableFormatUtils;
import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.hudi.HudiTargetConfig;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.model.sync.SyncStatusCode;
import org.apache.xtable.spark.extensions.optimizer.IndexPruningRule;
import org.apache.xtable.spark.extensions.optimizer.IndexPruningRule$;
import org.apache.xtable.spark.extensions.optimizer.StagedScans;

/**
 * Runs the index rule end to end against Iceberg tables in a Hadoop catalog, with indexes that an
 * XTable sync builds, and checks every query against the same query without the rule.
 */
public class ITXTableIndexExtensions {
  private static String TABLE;

  @TempDir public static Path tempDir;

  private static SparkSession spark;

  @BeforeAll
  public static void setupOnce() {
    SparkConf sparkConf =
        new SparkConf()
            .setAppName("xtable-spark-extensions-testing")
            .setMaster("local[2]")
            .set(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                    + XTableSparkSessionExtensions.class.getName())
            .set("spark.sql.catalog.cat", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.cat.type", "hadoop")
            .set("spark.sql.catalog.cat.warehouse", tempDir.toUri().toString())
            .set("spark.sql.shuffle.partitions", "2")
            .set("spark.sql.adaptive.enabled", "false")
            .set("spark.sql.session.timeZone", "UTC")
            .set("spark.ui.enabled", "false")
            // Hudi, which stores the index, needs Kryo
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            // a lookup always pays off in these small tables
            .set(XTableIndexConf.MinCandidateFiles(), "0")
            .set(XTableIndexConf.MinCandidateBytes(), "0");
    spark =
        SparkSession.builder().config(HoodieReadClient.addHoodieSupport(sparkConf)).getOrCreate();
    spark
        .sparkContext()
        .hadoopConfiguration()
        .set("parquet.avro.write-old-list-structure", "false");
  }

  @AfterAll
  public static void teardown() {
    if (spark != null) {
      spark.close();
    }
  }

  @BeforeEach
  public void newTableName() {
    // a dropped table can leave its index behind, so every test uses a new table
    TABLE = "cat.db.events_" + UUID.randomUUID().toString().replace("-", "_");
  }

  @AfterEach
  public void dropTable() {
    spark.sql("DROP TABLE IF EXISTS " + TABLE + " PURGE");
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void indexLifecycle(boolean partitioned) throws Exception {
    createTable(partitioned);
    appendRows(0, 1600, 4);

    List<String> emails = emails(7, 123, 1599);
    // a table without an index is read as usual
    assertDoesNotUseIndex("SELECT * FROM " + TABLE + " WHERE email = '" + emails.get(0) + "'");

    syncIndex("email");
    assertUsesIndex("SELECT * FROM " + TABLE + " WHERE email = '" + emails.get(0) + "'");
    assertUsesIndex("SELECT * FROM " + TABLE + " WHERE '" + emails.get(0) + "' = email");
    assertUsesIndex("SELECT * FROM " + TABLE + " WHERE email IN " + inList(emails));
    // more than 10 values become an InSet
    assertUsesIndex(
        "SELECT * FROM " + TABLE + " WHERE email IN " + inList(emails(LongStream.range(0, 20))));
    assertUsesIndex(
        "SELECT id FROM " + TABLE + " WHERE email IN " + inList(emails) + " AND id > 100");
    // the filter on the indexed column need not come first
    assertUsesIndex(
        "SELECT id FROM " + TABLE + " WHERE id = 7 AND email = '" + emails.get(0) + "'");
    assertReturnsNothingWithoutScan("SELECT * FROM " + TABLE + " WHERE email = 'missing'");
    // an OR of other columns cannot use the index
    assertDoesNotUseIndex(
        "SELECT * FROM " + TABLE + " WHERE email = '" + emails.get(0) + "' OR id = 5");
    // a column without an index
    assertDoesNotUseIndex("SELECT * FROM " + TABLE + " WHERE id = 7");

    // queries of an older snapshot do not use the index
    long snapshotId = icebergTable().currentSnapshot().snapshotId();
    assertDoesNotUseIndex(
        "SELECT * FROM "
            + TABLE
            + " VERSION AS OF "
            + snapshotId
            + " WHERE email = '"
            + emails.get(0)
            + "'");
    // nor do queries with the rule disabled
    withConf(
        XTableIndexConf.PruningEnabled(),
        "false",
        () ->
            assertDoesNotUseIndex("SELECT * FROM " + TABLE + " WHERE email IN " + inList(emails)));
    // nor do scans below the size thresholds
    withConf(
        XTableIndexConf.MinCandidateFiles(),
        "100000",
        () ->
            withConf(
                XTableIndexConf.MinCandidateBytes(),
                String.valueOf(Long.MAX_VALUE),
                () ->
                    assertDoesNotUseIndex(
                        "SELECT * FROM " + TABLE + " WHERE email IN " + inList(emails))));

    // new rows make the index stale until the next sync
    appendRows(1600, 1700, 2);
    List<String> newEmails = emails(7, 1650);
    assertDoesNotUseIndex("SELECT * FROM " + TABLE + " WHERE email IN " + inList(newEmails));
    syncIndex("email");
    assertUsesIndex("SELECT * FROM " + TABLE + " WHERE email IN " + inList(newEmails));

    // deleted rows are gone after a sync
    spark.sql("DELETE FROM " + TABLE + " WHERE id < 10");
    syncIndex("email");
    assertReturnsNothingWithoutScan(
        "SELECT * FROM " + TABLE + " WHERE email IN " + inList(emails(7, 8)));
    assertUsesIndex("SELECT * FROM " + TABLE + " WHERE email IN " + inList(emails(8, 123)));

    // without the Hudi metadata the table has no index
    deleteIndex();
    assertDoesNotUseIndex("SELECT * FROM " + TABLE + " WHERE email = '" + emails.get(1) + "'");

    // a sync builds the index again, also on another column, without a new snapshot
    syncIndex("id");
    assertUsesIndex("SELECT * FROM " + TABLE + " WHERE id IN (20, 900, 1650)");
    assertDoesNotUseIndex("SELECT * FROM " + TABLE + " WHERE email = '" + emails.get(1) + "'");

    // staged scans are released once their queries end
    spark.sparkContext().listenerBus().waitUntilEmpty();
    assertTrue(StagedScans.liveSetIds().isEmpty());
  }

  @Test
  void dataFrameReadsUseIndex() throws Exception {
    createTable(false);
    appendRows(0, 400, 4);
    syncIndex("email");
    List<String> emails = emails(7, 123);

    assertUsesIndex(() -> spark.table(TABLE).where(col("email").isin(emails.toArray())));
    assertUsesIndex(
        () ->
            spark.read().format("iceberg").load(TABLE).where(col("email").equalTo(emails.get(0))));
    // a read of an older snapshot by option does not use the index
    long snapshotId = icebergTable().currentSnapshot().snapshotId();
    assertDoesNotUseIndex(
        () ->
            spark
                .read()
                .format("iceberg")
                .option("snapshot-id", snapshotId)
                .load(TABLE)
                .where(col("email").equalTo(emails.get(0))));
  }

  @Test
  void incrementalReadsDoNotUseIndex() throws Exception {
    createTable(false);
    appendRows(0, 400, 4);
    long firstSnapshot = icebergTable().currentSnapshot().snapshotId();
    appendRows(400, 800, 4);
    long secondSnapshot = icebergTable().currentSnapshot().snapshotId();
    syncIndex("email");

    // row 7 is in the first snapshot, so a read of the changes after it does not return the row
    String email = emails(7).get(0);
    Supplier<Dataset<Row>> changes =
        () ->
            spark
                .read()
                .format("iceberg")
                .option("start-snapshot-id", firstSnapshot)
                .option("end-snapshot-id", secondSnapshot)
                .load(TABLE)
                .where(col("email").equalTo(email));
    assertDoesNotUseIndex(changes);
    assertTrue(changes.get().collectAsList().isEmpty());
    String newEmail = emails(407).get(0);
    assertDoesNotUseIndex(
        () ->
            spark
                .read()
                .format("iceberg")
                .option("start-snapshot-id", firstSnapshot)
                .load(TABLE)
                .where(col("email").equalTo(newEmail)));
  }

  /**
   * A sync that completes between the freshness check and the lookup points the index at files that
   * the candidates of the checked snapshot do not have. The rule must then read the table as usual
   * instead of reading no file.
   */
  @Test
  void syncDuringLookupKeepsTheRows() throws Exception {
    createTable(false);
    appendRows(0, 400, 4);
    syncIndex("email");
    String email = emails(7).get(0);
    String sql = "SELECT id FROM " + TABLE + " WHERE email = '" + email + "'";
    // another row in the file of row 7, whose copy-on-write delete rewrites the file and keeps row
    // 7
    long neighbor =
        spark
            .sql(
                "SELECT id FROM "
                    + TABLE
                    + " WHERE id <> 7 AND _file = (SELECT _file FROM "
                    + TABLE
                    + " WHERE id = 7) LIMIT 1")
            .collectAsList()
            .get(0)
            .getLong(0);

    IndexPruningRule$.MODULE$.beforeLookup_$eq(
        () -> {
          IndexPruningRule$.MODULE$.beforeLookup_$eq(() -> null);
          spark.sql("DELETE FROM " + TABLE + " WHERE id = " + neighbor);
          syncIndex("email");
          return null;
        });
    try {
      Dataset<Row> query = spark.sql(sql);
      // The rule must not intersect the candidates of the checked snapshot with the files of the
      // newer index, which share no file and would read nothing. A later optimizer iteration may
      // use the index again, once the table and the index are at the same snapshot.
      assertFalse(
          leaves(query.queryExecution().optimizedPlan()).stream()
              .allMatch(leaf -> leaf instanceof LocalRelation),
          "expected a scan when the index changes during the lookup");
      assertEquals(
          Collections.singletonList(7L),
          query.collectAsList().stream().map(row -> row.getLong(0)).collect(Collectors.toList()));
    } finally {
      IndexPruningRule$.MODULE$.beforeLookup_$eq(() -> null);
    }
    // the next query sees the synced index
    assertUsesIndex(sql);
  }

  /**
   * Iceberg fetches the staged tasks of a scan only when Spark plans its partitions, so planning
   * other queries must not remove them. Once the registry is full, new scans read the table as
   * usual.
   */
  @Test
  void stagedScansStayLiveUntilTheirQueryEnds() throws Exception {
    createTable(false);
    appendRows(0, 400, 4);
    syncIndex("email");
    String email = emails(7).get(0);
    Dataset<Row> query = spark.sql("SELECT id FROM " + TABLE + " WHERE email = '" + email + "'");
    assertEquals(1, stagedSetIds(query.queryExecution().optimizedPlan()).size());

    List<FileScanTask> fillerTasks = new ArrayList<>();
    try (CloseableIterable<FileScanTask> tasks = icebergTable().newScan().planFiles()) {
      fillerTasks.add(tasks.iterator().next());
    }
    List<String> fillers = new ArrayList<>();
    while (StagedScans.liveSetIds().size() < StagedScans.MaxLiveSets()) {
      fillers.add(
          StagedScans.stage(icebergTable(), JavaConverters.asScalaBuffer(fillerTasks).toSeq())
              .get());
    }
    try {
      assertDoesNotUseIndex("SELECT * FROM " + TABLE + " WHERE email = '" + email + "'");
      assertEquals(
          Collections.singletonList(7L),
          query.collectAsList().stream().map(row -> row.getLong(0)).collect(Collectors.toList()));
    } finally {
      fillers.forEach(StagedScans::release);
    }
  }

  /** Builds or updates the index of the column the way an XTable sync job does. */
  private static void syncIndex(String column) {
    Table table = icebergTable();
    String dataPath = dataPath(table);
    SourceTable sourceTable =
        XTableIndex.toSourceTable(
            table.name(),
            table.location(),
            dataPath,
            Option.empty(),
            Option.empty(),
            scala.collection.immutable.Map$.MODULE$.<String, String>empty());
    Properties properties = new Properties();
    properties.setProperty(HudiTargetConfig.HUDI_TABLE_VERSION, "9");
    properties.setProperty(
        HudiTargetConfig.EXECUTION_ENGINE, HudiTargetConfig.ExecutionEngine.SPARK.getConfigValue());
    properties.setProperty(HudiTargetConfig.SECONDARY_INDEX_COLUMN, column);
    TargetTable targetTable =
        TargetTable.builder()
            .name(sourceTable.getName())
            .basePath(dataPath)
            .formatName(TableFormat.HUDI)
            .additionalProperties(properties)
            .build();
    ConversionConfig conversionConfig =
        ConversionConfig.builder()
            .sourceTable(sourceTable)
            .targetTables(Collections.singletonList(targetTable))
            .build();
    IcebergConversionSourceProvider sourceProvider = new IcebergConversionSourceProvider();
    sourceProvider.init(spark.sparkContext().hadoopConfiguration());
    SyncResult syncResult =
        new ConversionController(spark.sparkContext().hadoopConfiguration())
            .sync(conversionConfig, sourceProvider)
            .get(TableFormat.HUDI);
    // no result means that the table has no commit the index has not seen
    if (syncResult != null) {
      assertEquals(
          SyncStatusCode.SUCCESS,
          syncResult.getTableFormatSyncStatus().getStatusCode(),
          syncResult.getErrorMessage());
    }
  }

  /** Deletes the Hudi metadata that holds the index, and keeps the data files. */
  private static void deleteIndex() throws Exception {
    org.apache.hadoop.fs.Path metaPath =
        new org.apache.hadoop.fs.Path(
            dataPath(icebergTable()), HoodieTableMetaClient.METAFOLDER_NAME);
    FileSystem fs = metaPath.getFileSystem(spark.sparkContext().hadoopConfiguration());
    assertTrue(fs.delete(metaPath, true));
  }

  private static String dataPath(Table table) {
    return TableFormatUtils.getTableDataLocation(
        TableFormat.ICEBERG, table.location(), table.properties());
  }

  private static void createTable(boolean partitioned) {
    spark.sql(
        "CREATE TABLE "
            + TABLE
            + " (id BIGINT, email STRING, level STRING, score DOUBLE) USING iceberg"
            + (partitioned ? " PARTITIONED BY (level)" : "")
            + " TBLPROPERTIES ('format-version'='2', 'write.distribution-mode'='none',"
            + " 'write.delete.mode'='copy-on-write')");
  }

  /** Appends rows with ids in [from, to) as several files whose email ranges overlap. */
  private static void appendRows(long from, long to, int files) throws Exception {
    spark
        .range(from, to)
        .selectExpr(
            "id",
            "md5(CAST(id AS STRING)) AS email",
            "CONCAT('level_', CAST(id % 3 AS STRING)) AS level",
            "CAST(id AS DOUBLE) / 7 AS score")
        .repartition(files)
        .sortWithinPartitions("level")
        .writeTo(TABLE)
        .append();
  }

  /**
   * Checks that the query reads only the files that hold its keys, and returns the same rows as
   * without the index.
   */
  private static void assertUsesIndex(String sql) {
    assertUsesIndex(
        sql,
        () -> spark.sql(sql),
        () -> spark.sql(sql.replaceFirst("SELECT .*? FROM", "SELECT _file FROM")),
        // the index selects files by the key filter alone, so other filters can drop rows of them
        !sql.contains(" AND "));
  }

  /** Checks a DataFrame read that filters the table without a projection. */
  private static void assertUsesIndex(Supplier<Dataset<Row>> newQuery) {
    assertUsesIndex(
        newQuery.get().queryExecution().analyzed().toString(),
        newQuery,
        () -> newQuery.get().select("_file"),
        true);
  }

  private static void assertUsesIndex(
      String description,
      Supplier<Dataset<Row>> newQuery,
      Supplier<Dataset<Row>> newFileQuery,
      boolean readsOnlyFilesWithMatches) {
    Dataset<Row> query = newQuery.get();
    List<String> setIds = stagedSetIds(query.queryExecution().optimizedPlan());
    assertEquals(1, setIds.size(), "expected the index to select the files of: " + description);
    Set<String> stagedFiles =
        ScanTaskSetManager.get().<FileScanTask>fetchTasks(icebergTable(), setIds.get(0)).stream()
            .map(task -> IndexPruningRule.normalizePath(task.file().location()))
            .collect(Collectors.toSet());
    List<Row> expected = withoutIndex(newQuery);
    Set<String> filesWithMatches = new java.util.HashSet<>();
    withoutIndex(newFileQuery)
        .forEach(row -> filesWithMatches.add(IndexPruningRule.normalizePath(row.getString(0))));
    if (readsOnlyFilesWithMatches) {
      assertEquals(filesWithMatches, stagedFiles, "files read for: " + description);
    } else {
      assertTrue(stagedFiles.containsAll(filesWithMatches), "files read for: " + description);
      assertTrue(stagedFiles.size() < dataFileCount(), "files read for: " + description);
    }
    assertEquals(expected, sorted(query.collectAsList()), "rows of: " + description);
  }

  private static void assertDoesNotUseIndex(String sql) {
    assertDoesNotUseIndex(sql, () -> spark.sql(sql));
  }

  private static void assertDoesNotUseIndex(Supplier<Dataset<Row>> newQuery) {
    assertDoesNotUseIndex(newQuery.get().queryExecution().analyzed().toString(), newQuery);
  }

  private static void assertDoesNotUseIndex(String description, Supplier<Dataset<Row>> newQuery) {
    Dataset<Row> query = newQuery.get();
    assertTrue(
        stagedSetIds(query.queryExecution().optimizedPlan()).isEmpty(),
        "expected a full scan for: " + description);
    assertEquals(withoutIndex(newQuery), sorted(query.collectAsList()), "rows of: " + description);
  }

  private static void assertReturnsNothingWithoutScan(String sql) {
    Dataset<Row> query = spark.sql(sql);
    List<LogicalPlan> leaves = leaves(query.queryExecution().optimizedPlan());
    assertTrue(
        leaves.stream().allMatch(leaf -> leaf instanceof LocalRelation),
        "expected no scan for: " + sql);
    assertTrue(query.collectAsList().isEmpty());
  }

  /** The rows of the query without the index, sorted. */
  private static List<Row> withoutIndex(Supplier<Dataset<Row>> newQuery) {
    List<Row> rows = new ArrayList<>();
    withConf(
        XTableIndexConf.PruningEnabled(),
        "false",
        () -> rows.addAll(sorted(newQuery.get().collectAsList())));
    return rows;
  }

  private static long dataFileCount() {
    return spark.sql("SELECT * FROM " + TABLE + ".files").count();
  }

  private static List<String> stagedSetIds(LogicalPlan plan) {
    return leaves(plan).stream()
        .filter(leaf -> leaf instanceof DataSourceV2ScanRelation)
        .map(
            leaf ->
                ((DataSourceV2ScanRelation) leaf)
                    .relation()
                    .options()
                    .get(StagedScans.ScanTaskSetIdOption()))
        .filter(setId -> setId != null)
        .collect(Collectors.toList());
  }

  private static List<LogicalPlan> leaves(LogicalPlan plan) {
    return JavaConverters.seqAsJavaList(plan.collectLeaves());
  }

  private static List<Row> sorted(List<Row> rows) {
    List<Row> sorted = new ArrayList<>(rows);
    sorted.sort((left, right) -> left.toString().compareTo(right.toString()));
    return sorted;
  }

  private static List<String> emails(long... ids) {
    return emails(LongStream.of(ids));
  }

  private static List<String> emails(LongStream ids) {
    return ids.mapToObj(
            id -> spark.sql("SELECT md5('" + id + "')").collectAsList().get(0).getString(0))
        .collect(Collectors.toList());
  }

  private static String inList(List<String> values) {
    return values.stream()
        .map(value -> "'" + value + "'")
        .collect(Collectors.joining(", ", "(", ")"));
  }

  private static Table icebergTable() {
    try {
      return Spark3Util.loadIcebergTable(spark, TABLE);
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  private static void withConf(String key, String value, Runnable action) {
    String previous = spark.conf().getOption(key).getOrElse(() -> null);
    spark.conf().set(key, value);
    try {
      action.run();
    } finally {
      if (previous == null) {
        spark.conf().unset(key);
      } else {
        spark.conf().set(key, previous);
      }
    }
  }
}
