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
 
package org.apache.xtable.spark;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;

import org.apache.xtable.model.metadata.TableSyncMetadata;

/**
 * The basic user experience: an unchanged Iceberg pipeline (catalog config, DDL, INSERT INTO,
 * writeTo().append(), SELECT) plus the plugin jar and one config, and the writes go through Hudi
 * while the same catalog table stays readable by plain Iceberg.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ITHudiIcebergSparkPlugin {
  private static Path tempDir;
  private static SparkSession spark;
  private static String warehouse;

  @BeforeAll
  static void startSpark() throws Exception {
    tempDir = Files.createTempDirectory("hudi-iceberg-plugin");
    warehouse = tempDir.resolve("warehouse").toString();
    SparkConf conf =
        new SparkConf()
            .setMaster("local[2]")
            .setAppName("hudi-iceberg-plugin-it")
            // the two configs the user adds
            .set("spark.plugins", "org.apache.spark.HudiIcebergPlugin")
            .set("spark.serializer", HudiIcebergConf.KRYO_SERIALIZER)
            // the user's existing Iceberg setup, untouched
            .set(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .set("spark.sql.catalog.hcat", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.hcat.type", "hadoop")
            .set("spark.sql.catalog.hcat.warehouse", warehouse)
            // a second catalog over the same warehouse that opts out: plain Iceberg reads
            .set("spark.sql.catalog.plain", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.plain.type", "hadoop")
            .set("spark.sql.catalog.plain.warehouse", warehouse)
            .set("spark.sql.catalog.plain.hudi.enabled", "false")
            // a plain reader over the same table; no caching so it behaves like a separate engine
            .set("spark.sql.catalog.plain.cache-enabled", "false")
            // a catalog that does not manage new tables by default: adopt/eject per table
            .set("spark.sql.catalog.lazy", "org.apache.iceberg.spark.SparkCatalog")
            .set("spark.sql.catalog.lazy.type", "hadoop")
            .set("spark.sql.catalog.lazy.warehouse", warehouse)
            .set("spark.sql.catalog.lazy.hudi.manage-new-tables", "false")
            .set("spark.sql.shuffle.partitions", "2")
            .set("spark.ui.enabled", "false");
    spark = SparkSession.builder().config(conf).getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @Test
  @Order(1)
  void pluginRewritesTheConfiguration() {
    assertEquals("0.5.0-SNAPSHOT", spark.conf().get(HudiIcebergConf.VERSION));
    // SparkSession.builder().config(...) re-applies the builder's catalog entries over the
    // SparkContext conf; the session extension rewrites them again while the analyzer is built
    spark.sql("SELECT 1").collect();
    assertEquals(HudiSparkCatalog.class.getName(), spark.conf().get("spark.sql.catalog.hcat"));
    assertEquals(
        "org.apache.iceberg.spark.SparkCatalog", spark.conf().get("spark.sql.catalog.plain"));
    // the session conf shows the builder's static options; the SparkContext conf is what
    // SparkSession.getOrCreate read the extensions from
    assertTrue(
        spark
            .sparkContext()
            .conf()
            .get("spark.sql.extensions")
            .endsWith(HudiIcebergExtensions.class.getName()));
  }

  @Test
  @Order(2)
  void insertIntoGoesThroughHudiAndStaysReadableAsIceberg() throws Exception {
    spark.sql("CREATE DATABASE IF NOT EXISTS hcat.db");
    spark.sql(
        "CREATE TABLE hcat.db.events (id INT, name STRING, region STRING) USING iceberg "
            + "PARTITIONED BY (region)");
    spark.sql("INSERT INTO hcat.db.events VALUES (1, 'a', 'us'), (2, 'b', 'eu')");

    Path basePath = Paths.get(warehouse, "db", "events", "data");
    assertTrue(
        Files.isDirectory(basePath.resolve(".hoodie")), "Hudi table initialized at " + basePath);
    assertEquals(Arrays.asList("1:a:us", "2:b:eu"), readViaCatalog("hcat"));
    assertEquals(Arrays.asList("1:a:us", "2:b:eu"), readViaCatalog("plain"));

    Table table = loadWithPlainIceberg("db", "events");
    Snapshot snapshot = table.currentSnapshot();
    assertNotNull(snapshot, "Iceberg snapshot published to the catalog");
    assertTrue(snapshot.summary().containsKey(TableSyncMetadata.XTABLE_METADATA));
    assertEquals(1, table.currentSnapshot().summary().get("added-data-files").isEmpty() ? 0 : 1);
    List<String> columns =
        table.schema().columns().stream().map(c -> c.name()).collect(Collectors.toList());
    assertEquals(Arrays.asList("id", "name", "region"), columns, "no _hoodie_* columns leak");
    assertTrue(table.spec().isPartitioned());
  }

  @Test
  @Order(3)
  void secondInsertAndDataFrameAppendAccumulate() throws Exception {
    spark.sql("INSERT INTO hcat.db.events VALUES (3, 'c', 'us')");
    spark
        .createDataFrame(
            Arrays.asList(org.apache.spark.sql.RowFactory.create(4, "d", "apac")),
            spark.table("hcat.db.events").schema())
        .writeTo("hcat.db.events")
        .append();
    assertEquals(Arrays.asList("1:a:us", "2:b:eu", "3:c:us", "4:d:apac"), readViaCatalog("hcat"));
    assertEquals(Arrays.asList("1:a:us", "2:b:eu", "3:c:us", "4:d:apac"), readViaCatalog("plain"));
    Table table = loadWithPlainIceberg("db", "events");
    long snapshots = 0;
    for (Snapshot ignored : table.snapshots()) {
      snapshots++;
    }
    assertEquals(3, snapshots, "one Iceberg snapshot per Hudi commit");
  }

  @Test
  @Order(4)
  void explainShowsTheHudiRoute() {
    String plan =
        spark
            .sql("EXPLAIN INSERT INTO hcat.db.events VALUES (9, 'z', 'us')")
            .collectAsList()
            .get(0)
            .getString(0);
    assertTrue(plan.contains("HudiIcebergWrite"), plan);
    assertFalse(plan.contains("AppendData"), plan);
  }

  @Test
  @Order(5)
  void tableOnTheOptedOutCatalogUsesNativeIceberg() throws Exception {
    spark.sql("CREATE TABLE plain.db.native (id INT) USING iceberg");
    spark.sql("INSERT INTO plain.db.native VALUES (1)");
    assertFalse(Files.exists(Paths.get(warehouse, "db", "native", "data", ".hoodie")));
    assertEquals(1, spark.sql("SELECT * FROM plain.db.native").count());
  }

  @Test
  @Order(6)
  void insertOverwriteReplacesOnlyTheTouchedPartitions() throws Exception {
    spark.sql("SET spark.sql.sources.partitionOverwriteMode=dynamic");
    spark.sql("INSERT OVERWRITE hcat.db.events VALUES (10, 'x', 'us')");
    spark.sql("RESET spark.sql.sources.partitionOverwriteMode");
    assertEquals(Arrays.asList("2:b:eu", "4:d:apac", "10:x:us"), readViaCatalog("hcat"));
    assertEquals(Arrays.asList("2:b:eu", "4:d:apac", "10:x:us"), readViaCatalog("plain"));
  }

  @Test
  @Order(7)
  void identifierFieldsMakeInsertsUpserts() throws Exception {
    spark.sql(
        "CREATE TABLE hcat.db.users (id INT NOT NULL, name STRING, ts BIGINT) USING iceberg "
            + "TBLPROPERTIES ('hudi.ordering-field'='ts')");
    spark.sql("ALTER TABLE hcat.db.users SET IDENTIFIER FIELDS id");
    spark.sql("INSERT INTO hcat.db.users VALUES (1, 'a', 1), (2, 'b', 1)");
    spark.sql("INSERT INTO hcat.db.users VALUES (1, 'a2', 2), (3, 'c', 2)");
    List<String> rows =
        spark.sql("SELECT id, name, ts FROM plain.db.users ORDER BY id").collectAsList().stream()
            .map(r -> r.getInt(0) + ":" + r.getString(1) + ":" + r.getLong(2))
            .collect(Collectors.toList());
    assertEquals(Arrays.asList("1:a2:2", "2:b:1", "3:c:2"), rows);
  }

  @Test
  @Order(8)
  void updateAndDeleteOnKeyedTable() {
    spark.sql("UPDATE hcat.db.users SET name = 'b2', ts = 3 WHERE id = 2");
    assertEquals(Arrays.asList("1:a2:2", "2:b2:3", "3:c:2"), readUsers());
    spark.sql("DELETE FROM hcat.db.users WHERE id = 3");
    assertEquals(Arrays.asList("1:a2:2", "2:b2:3"), readUsers());
  }

  @Test
  @Order(9)
  void mergeIntoKeyedTable() {
    usersSource("src", Arrays.asList(new Object[] {2, "b3", 4L}, new Object[] {4, "d", 4L}));
    spark.sql(
        "MERGE INTO hcat.db.users t USING src s ON t.id = s.id "
            + "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *");
    assertEquals(Arrays.asList("1:a2:2", "2:b3:4", "4:d:4"), readUsers());

    usersSource("src2", Arrays.asList(new Object[] {1, "a3", 5L}, new Object[] {5, "e", 5L}));
    spark.sql(
        "MERGE INTO hcat.db.users t USING src2 s ON t.id = s.id "
            + "WHEN MATCHED THEN UPDATE SET name = s.name, ts = s.ts");
    assertEquals(Arrays.asList("1:a3:5", "2:b3:4", "4:d:4"), readUsers());

    usersSource("src3", Arrays.asList(new Object[] {4, "zz", 9L}, new Object[] {6, "f", 6L}));
    spark.sql(
        "MERGE INTO hcat.db.users t USING src3 s ON s.id = t.id "
            + "WHEN NOT MATCHED THEN INSERT *");
    assertEquals(Arrays.asList("1:a3:5", "2:b3:4", "4:d:4", "6:f:6"), readUsers());
  }

  @Test
  @Order(10)
  void rowLevelWriteOnKeylessTableIsRejectedNotSilentlyBypassed() {
    Exception e =
        org.junit.jupiter.api.Assertions.assertThrows(
            Exception.class, () -> spark.sql("DELETE FROM hcat.db.events WHERE id = 10"));
    assertTrue(e.getMessage().contains("not routed through Hudi"), e.getMessage());
    assertEquals(Arrays.asList("2:b:eu", "4:d:apac", "10:x:us"), readViaCatalog("hcat"));
  }

  @Test
  @Order(11)
  void rewriteDataFilesRunsHudiClustering() {
    spark.sql("INSERT INTO hcat.db.events VALUES (11, 'y', 'us')");
    spark.sql("INSERT INTO hcat.db.events VALUES (12, 'w', 'us')");
    assertEquals(3, visibleFiles("us"));
    long snapshotsBefore = spark.sql("SELECT * FROM hcat.db.events.snapshots").count();

    List<Row> result =
        spark.sql("CALL hcat.system.rewrite_data_files(table => 'db.events')").collectAsList();
    assertEquals(1, result.size());
    assertTrue(result.get(0).getInt(0) >= 3, "files rewritten: " + result.get(0));

    assertEquals(1, visibleFiles("us"));
    assertEquals(
        Arrays.asList("2:b:eu", "4:d:apac", "10:x:us", "11:y:us", "12:w:us"),
        readViaCatalog("plain"));
    assertEquals(snapshotsBefore + 1, spark.sql("SELECT * FROM hcat.db.events.snapshots").count());

    spark.sql("CALL hcat.system.expire_snapshots(table => 'db.events')").collectAsList();
    assertEquals(snapshotsBefore + 1, spark.sql("SELECT * FROM hcat.db.events.snapshots").count());
  }

  @Test
  @Order(12)
  void adoptAndEjectThroughTheTableProperty() {
    spark.sql("CREATE TABLE lazy.db.adopted (id INT) USING iceberg");
    Path hoodie = Paths.get(warehouse, "db", "adopted", "data", ".hoodie");
    assertFalse(Files.exists(hoodie), "not managed until adopted");

    // adopt: from here on writes go through Hudi
    spark.sql("ALTER TABLE lazy.db.adopted SET TBLPROPERTIES ('hudi.managed' = 'true')");
    spark.sql("INSERT INTO lazy.db.adopted VALUES (1)");
    spark.sql("INSERT INTO lazy.db.adopted VALUES (2)");
    assertTrue(Files.isDirectory(hoodie), "adopted: Hudi table initialized");

    // eject: a plain Iceberg table again, Iceberg's own writer takes over
    spark.sql("ALTER TABLE lazy.db.adopted UNSET TBLPROPERTIES ('hudi.managed')");
    spark.sql("INSERT INTO lazy.db.adopted VALUES (3)");

    // re-adopt with a natively written file in place: Hudi ignores files it did not write
    spark.sql("ALTER TABLE lazy.db.adopted SET TBLPROPERTIES ('hudi.managed' = 'true')");
    spark.sql("INSERT INTO lazy.db.adopted VALUES (4)");
    List<Integer> ids =
        spark.sql("SELECT id FROM plain.db.adopted ORDER BY id").collectAsList().stream()
            .map(r -> r.getInt(0))
            .collect(Collectors.toList());
    assertEquals(Arrays.asList(1, 2, 3, 4), ids);
  }

  @Test
  @Order(13)
  void staticPartitionOverwrite() {
    spark.sql("INSERT OVERWRITE hcat.db.events PARTITION (region = 'eu') VALUES (20, 'q')");
    assertEquals(
        Arrays.asList("4:d:apac", "10:x:us", "11:y:us", "12:w:us", "20:q:eu"),
        readViaCatalog("plain"));
  }

  @Test
  @Order(14)
  void mergeOnReadTableWritesDeletionVectors() {
    spark.sql(
        "CREATE TABLE hcat.db.mor (id INT NOT NULL, name STRING, ts BIGINT) USING iceberg "
            + "TBLPROPERTIES ('write.merge.mode'='merge-on-read', 'write.update.mode'='merge-on-read', "
            + "'write.delete.mode'='merge-on-read', 'hudi.ordering-field'='ts')");
    spark.sql("ALTER TABLE hcat.db.mor SET IDENTIFIER FIELDS id");
    assertEquals(
        "3",
        spark
            .sql("SHOW TBLPROPERTIES hcat.db.mor ('format-version')")
            .collectAsList()
            .get(0)
            .getString(1));
    spark.sql("INSERT INTO hcat.db.mor VALUES (1, 'a', 1), (2, 'b', 1), (3, 'c', 1)");
    spark.sql("UPDATE hcat.db.mor SET name = 'b2', ts = 2 WHERE id = 2");
    spark.sql("DELETE FROM hcat.db.mor WHERE id = 3");
    usersSource("morsrc", Arrays.asList(new Object[] {1, "a2", 3L}, new Object[] {4, "d", 3L}));
    spark.sql(
        "MERGE INTO hcat.db.mor t USING morsrc s ON t.id = s.id "
            + "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *");
    List<String> rows =
        spark.sql("SELECT id, name, ts FROM plain.db.mor ORDER BY id").collectAsList().stream()
            .map(r -> r.getInt(0) + ":" + r.getString(1) + ":" + r.getLong(2))
            .collect(Collectors.toList());
    assertEquals(Arrays.asList("1:a2:3", "2:b2:2", "4:d:3"), rows);
    long deleteFiles = spark.sql("SELECT * FROM plain.db.mor.delete_files").count();
    assertTrue(
        deleteFiles > 0, "updates and deletes land as Iceberg delete files (deletion vectors)");
    assertEquals(
        Arrays.asList("PUFFIN"),
        spark
            .sql("SELECT DISTINCT file_format FROM plain.db.mor.delete_files")
            .collectAsList()
            .stream()
            .map(r -> r.getString(0))
            .collect(Collectors.toList()));

    spark.sql("CALL hcat.system.rewrite_position_delete_files(table => 'db.mor')").collectAsList();
    assertEquals(
        0,
        spark.sql("SELECT * FROM plain.db.mor.delete_files").count(),
        "compaction folds the deletion vectors away");
    rows =
        spark.sql("SELECT id, name, ts FROM plain.db.mor ORDER BY id").collectAsList().stream()
            .map(r -> r.getInt(0) + ":" + r.getString(1) + ":" + r.getLong(2))
            .collect(Collectors.toList());
    assertEquals(Arrays.asList("1:a2:3", "2:b2:2", "4:d:3"), rows);
  }

  private static void usersSource(String view, List<Object[]> rows) {
    spark
        .createDataFrame(
            rows.stream().map(org.apache.spark.sql.RowFactory::create).collect(Collectors.toList()),
            new org.apache.spark.sql.types.StructType()
                .add("id", "int", false)
                .add("name", "string")
                .add("ts", "long"))
        .createOrReplaceTempView(view);
  }

  private static List<String> readUsers() {
    return spark.sql("SELECT id, name, ts FROM plain.db.users ORDER BY id").collectAsList().stream()
        .map(r -> r.getInt(0) + ":" + r.getString(1) + ":" + r.getLong(2))
        .collect(Collectors.toList());
  }

  private static long visibleFiles(String region) {
    return spark
        .sql("SELECT file_path FROM hcat.db.events.files WHERE partition.region = '" + region + "'")
        .count();
  }

  private static List<String> readViaCatalog(String catalog) {
    return spark
        .sql("SELECT id, name, region FROM " + catalog + ".db.events ORDER BY id")
        .collectAsList()
        .stream()
        .map(r -> r.getInt(0) + ":" + r.getString(1) + ":" + r.getString(2))
        .collect(Collectors.toList());
  }

  private static Table loadWithPlainIceberg(String db, String name) {
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), warehouse);
    return catalog.loadTable(TableIdentifier.of(db, name));
  }
}
