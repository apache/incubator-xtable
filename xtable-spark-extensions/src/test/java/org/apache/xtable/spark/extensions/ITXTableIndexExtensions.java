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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.IndexAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchIndexException;
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

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.spark.ScanTaskSetManager;
import org.apache.iceberg.spark.Spark3Util;

import scala.collection.JavaConverters;

import org.apache.xtable.spark.extensions.optimizer.IndexPruningRule;
import org.apache.xtable.spark.extensions.optimizer.StagedScans;

/**
 * Runs the index commands and the index rule end to end against Iceberg tables in a Hadoop catalog,
 * and checks every query against the same query without the rule.
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

    assertThrows(
        IllegalArgumentException.class,
        () -> spark.sql("CREATE INDEX score_idx ON " + TABLE + " USING xtable (score)"),
        "double columns are not supported");
    spark.sql("CREATE INDEX email_idx ON " + TABLE + " USING xtable (email)");
    assertEquals("email", tableProperties().get("xtable.index.email_idx.column"));
    spark.sql("CREATE INDEX IF NOT EXISTS email_idx ON " + TABLE + " USING xtable (email)");
    assertThrows(
        IndexAlreadyExistsException.class,
        () -> spark.sql("CREATE INDEX email_idx ON " + TABLE + " USING xtable (email)"));
    assertThrows(
        IllegalArgumentException.class,
        () -> spark.sql("CREATE INDEX email_idx2 ON " + TABLE + " USING xtable (email)"),
        "a table has at most one index");
    assertThrows(
        IllegalArgumentException.class,
        () -> spark.sql("CREATE INDEX id_idx ON " + TABLE + " USING xtable (id)"),
        "a table has at most one index");

    List<String> emails = emails(7, 123, 1599);
    assertUsesIndex("SELECT * FROM " + TABLE + " WHERE email = '" + emails.get(0) + "'");
    assertUsesIndex("SELECT * FROM " + TABLE + " WHERE '" + emails.get(0) + "' = email");
    assertUsesIndex("SELECT * FROM " + TABLE + " WHERE email IN " + inList(emails));
    // more than 10 values become an InSet
    assertUsesIndex(
        "SELECT * FROM " + TABLE + " WHERE email IN " + inList(emails(LongStream.range(0, 20))));
    assertUsesIndex(
        "SELECT id FROM " + TABLE + " WHERE email IN " + inList(emails) + " AND id > 100");
    assertReturnsNothingWithoutScan("SELECT * FROM " + TABLE + " WHERE email = 'missing'");
    // an OR of other columns cannot use the index
    assertDoesNotUseIndex(
        "SELECT * FROM " + TABLE + " WHERE email = '" + emails.get(0) + "' OR id = 5");

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

    // new rows make the index stale until it is refreshed
    appendRows(1600, 1700, 2);
    List<String> newEmails = emails(7, 1650);
    assertDoesNotUseIndex("SELECT * FROM " + TABLE + " WHERE email IN " + inList(newEmails));
    spark.sql("REFRESH INDEX email_idx ON " + TABLE);
    assertUsesIndex("SELECT * FROM " + TABLE + " WHERE email IN " + inList(newEmails));

    // deleted rows are gone after a refresh
    spark.sql("DELETE FROM " + TABLE + " WHERE id < 10");
    spark.sql("REFRESH INDEX email_idx ON " + TABLE);
    assertReturnsNothingWithoutScan(
        "SELECT * FROM " + TABLE + " WHERE email IN " + inList(emails(7, 8)));
    assertUsesIndex("SELECT * FROM " + TABLE + " WHERE email IN " + inList(emails(8, 123)));

    spark.sql("DROP INDEX email_idx ON " + TABLE);
    assertFalse(tableProperties().containsKey("xtable.index.email_idx.column"));
    assertDoesNotUseIndex("SELECT * FROM " + TABLE + " WHERE email = '" + emails.get(1) + "'");
    spark.sql("DROP INDEX IF EXISTS email_idx ON " + TABLE);
    assertThrows(NoSuchIndexException.class, () -> spark.sql("DROP INDEX email_idx ON " + TABLE));
    assertThrows(
        NoSuchIndexException.class, () -> spark.sql("REFRESH INDEX email_idx ON " + TABLE));

    // after a drop, the table can be indexed on another column, without a new snapshot
    spark.sql("CREATE INDEX id_idx ON " + TABLE + " USING xtable (id)");
    assertUsesIndex("SELECT * FROM " + TABLE + " WHERE id IN (20, 900, 1650)");
    assertDoesNotUseIndex("SELECT * FROM " + TABLE + " WHERE email = '" + emails.get(1) + "'");
    spark.sql("DROP INDEX id_idx ON " + TABLE);

    // and the dropped index can be created again
    spark.sql("CREATE INDEX email_idx ON " + TABLE + " USING xtable (email)");
    assertUsesIndex("SELECT * FROM " + TABLE + " WHERE email = '" + emails.get(1) + "'");

    // staged scans are released once their queries end
    spark.sparkContext().listenerBus().waitUntilEmpty();
    assertTrue(StagedScans.liveSetIds().isEmpty());
  }

  @Test
  void otherIndexTypesAndStatementsAreLeftToSpark() throws Exception {
    createTable(false);
    appendRows(0, 10, 1);
    assertThrows(
        Exception.class,
        () -> spark.sql("CREATE INDEX lucene_idx ON " + TABLE + " USING lucene (email)"));
    spark.sql("REFRESH TABLE " + TABLE);
    assertEquals(10, spark.sql("SELECT * FROM " + TABLE).count());
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
    Dataset<Row> query = spark.sql(sql);
    List<String> setIds = stagedSetIds(query.queryExecution().optimizedPlan());
    assertEquals(1, setIds.size(), "expected the index to select the files of: " + sql);
    Set<String> stagedFiles =
        ScanTaskSetManager.get().<FileScanTask>fetchTasks(icebergTable(), setIds.get(0)).stream()
            .map(task -> IndexPruningRule.normalizePath(task.file().location()))
            .collect(Collectors.toSet());
    List<Row> expected = withoutIndex(sql);
    Set<String> filesWithMatches = filesWithMatches(sql);
    if (sql.contains(" AND ")) {
      // the index selects files by the key filter alone, so other filters can drop rows of them
      assertTrue(stagedFiles.containsAll(filesWithMatches), "files read for: " + sql);
      assertTrue(stagedFiles.size() < dataFileCount(), "files read for: " + sql);
    } else {
      assertEquals(filesWithMatches, stagedFiles, "files read for: " + sql);
    }
    assertEquals(expected, sorted(query.collectAsList()), "rows of: " + sql);
  }

  private static void assertDoesNotUseIndex(String sql) {
    Dataset<Row> query = spark.sql(sql);
    assertTrue(
        stagedSetIds(query.queryExecution().optimizedPlan()).isEmpty(),
        "expected a full scan for: " + sql);
    assertEquals(withoutIndex(sql), sorted(query.collectAsList()), "rows of: " + sql);
  }

  private static void assertReturnsNothingWithoutScan(String sql) {
    Dataset<Row> query = spark.sql(sql);
    List<LogicalPlan> leaves = leaves(query.queryExecution().optimizedPlan());
    assertTrue(
        leaves.stream().allMatch(leaf -> leaf instanceof LocalRelation),
        "expected no scan for: " + sql);
    assertTrue(query.collectAsList().isEmpty());
  }

  private static List<Row> withoutIndex(String sql) {
    List<Row> rows = new ArrayList<>();
    withConf(
        XTableIndexConf.PruningEnabled(),
        "false",
        () -> rows.addAll(sorted(spark.sql(sql).collectAsList())));
    return rows;
  }

  /** The data files that hold at least one row of the query, found without the index. */
  private static Set<String> filesWithMatches(String sql) {
    Set<String> files = new java.util.HashSet<>();
    withConf(
        XTableIndexConf.PruningEnabled(),
        "false",
        () ->
            spark
                .sql(sql.replaceFirst("SELECT .*? FROM", "SELECT _file FROM"))
                .collectAsList()
                .forEach(row -> files.add(IndexPruningRule.normalizePath(row.getString(0)))));
    return files;
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

  private static Map<String, String> tableProperties() {
    return icebergTable().properties();
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
