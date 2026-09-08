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
