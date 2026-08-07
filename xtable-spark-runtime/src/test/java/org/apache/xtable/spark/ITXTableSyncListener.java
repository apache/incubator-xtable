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

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.client.HoodieReadClient;

import org.apache.xtable.hudi.HudiTestUtil;

/**
 * End-to-end test: a Hudi write through the Spark datasource triggers {@link XTableSyncListener},
 * which synchronizes the table to Delta and Iceberg in the same driver JVM.
 */
class ITXTableSyncListener {

  @TempDir static java.nio.file.Path tempDir;

  private SparkSession spark;

  @AfterEach
  void stopSpark() {
    if (spark != null) {
      spark.stop();
      spark = null;
    }
    SparkSession.clearActiveSession();
    SparkSession.clearDefaultSession();
  }

  @Test
  void hudiWriteSyncsToDeltaAndIceberg() throws Exception {
    String basePath = tempDir.resolve("orders").toUri().toString();

    SparkConf sparkConf =
        HudiTestUtil.getSparkConf(tempDir)
            .set("spark.sql.queryExecutionListeners", XTableSyncListener.class.getName())
            .set("spark.xtable.tables", "orders")
            .set("spark.xtable.orders.basePath", basePath)
            .set("spark.xtable.orders.sourceFormat", "HUDI")
            .set("spark.xtable.orders.targets", "DELTA,ICEBERG");
    spark =
        SparkSession.builder().config(HoodieReadClient.addHoodieSupport(sparkConf)).getOrCreate();

    List<Row> rows =
        Arrays.asList(
            RowFactory.create("1", "alice", 1L),
            RowFactory.create("2", "bob", 2L),
            RowFactory.create("3", "carol", 3L),
            RowFactory.create("4", "dave", 4L),
            RowFactory.create("5", "erin", 5L));
    StructType schema =
        new StructType()
            .add("id", DataTypes.StringType, false)
            .add("name", DataTypes.StringType, true)
            .add("ts", DataTypes.LongType, false);

    // Write through the Hudi Spark datasource so the write flows through a QueryExecution and fires
    // the listener (unlike the write-client-based test helpers).
    spark
        .createDataFrame(rows, schema)
        .write()
        .format("hudi")
        .option("hoodie.table.name", "orders")
        .option("hoodie.datasource.write.recordkey.field", "id")
        .option("hoodie.datasource.write.precombine.field", "ts")
        .option("hoodie.datasource.write.partitionpath.field", "")
        .option(
            "hoodie.datasource.write.keygenerator.class",
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
        .option("hoodie.datasource.write.operation", "insert")
        .mode(SaveMode.Append)
        .save(basePath);

    // Callback delivery is async even though the sync itself runs inline, so wait for the targets.
    waitForTargets(basePath);

    Set<String> expectedIds = rows.stream().map(r -> r.getString(0)).collect(Collectors.toSet());
    assertEquals(expectedIds, idsFrom(basePath, "delta"));
    assertEquals(expectedIds, idsFrom(basePath, "iceberg"));
  }

  private Set<String> idsFrom(String basePath, String format) {
    Dataset<Row> df = spark.read().format(format).load(basePath);
    return df.select("id").collectAsList().stream()
        .map(r -> r.getString(0))
        .collect(Collectors.toSet());
  }

  private static void waitForTargets(String basePath) throws InterruptedException {
    File local = new File(java.net.URI.create(basePath));
    File deltaLog = new File(local, "_delta_log");
    File icebergMetadata = new File(local, "metadata");
    long deadline = System.currentTimeMillis() + 120_000;
    while (System.currentTimeMillis() < deadline) {
      if (hasDeltaCommit(deltaLog) && hasIcebergMetadata(icebergMetadata)) {
        return;
      }
      Thread.sleep(2_000);
    }
    throw new AssertionError("Timed out waiting for Delta/Iceberg metadata under " + basePath);
  }

  private static boolean hasDeltaCommit(File deltaLog) {
    File[] files = deltaLog.listFiles((dir, name) -> name.endsWith(".json"));
    return files != null && files.length > 0;
  }

  private static boolean hasIcebergMetadata(File metadataDir) {
    File[] files = metadataDir.listFiles((dir, name) -> name.endsWith("metadata.json"));
    return files != null && files.length > 0;
  }
}
