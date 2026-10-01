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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;

import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import org.apache.xtable.hudi.HudiTestUtil;

/**
 * The deletion-vector model driven by the Spark write client rather than the Java client: the
 * update decomposition, small-file gating and compaction hook live in engine-specific Hudi code, so
 * the Spark path must produce the same Iceberg view as the Java path.
 */
class ITIcebergSparkMergeOnReadDeletionVectors {

  @TempDir public static Path tempDir;

  private static SparkSession sparkSession;
  private static JavaSparkContext jsc;

  @BeforeAll
  static void setupOnce() {
    sparkSession = SparkSession.builder().config(HudiTestUtil.getSparkConf(tempDir)).getOrCreate();
    jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
  }

  @AfterAll
  static void teardown() {
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

  private static Properties tableProperties() {
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.TABLE_FORMAT.key(), "ICEBERG");
    properties.put(
        HoodieTableConfig.VERSION.key(), String.valueOf(HoodieTableVersion.EIGHT.versionCode()));
    properties.put(HoodieMetadataConfig.ENABLE.key(), "false");
    properties.put("hoodie.write.updates.as.deletes.and.inserts", "true");
    properties.put("hoodie.index.type", "SIMPLE");
    properties.put("xtable.iceberg.format-version", "3");
    return properties;
  }

  @Test
  void sparkWriterProducesDeletionVectors() throws Exception {
    String tableName = "mor_dv_spark";
    try (TestSparkHudiTable table =
        TestSparkHudiTable.forStandardSchema(
            tableName, tempDir, jsc, null, HoodieTableType.MERGE_ON_READ, tableProperties())) {
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(50, true);
      Table icebergTable = new HadoopTables(new Configuration()).load(table.getBasePath());
      assertEquals(50, readKeys(icebergTable).size());

      table.upsertRecords(inserts.subList(0, 10), true);
      icebergTable.refresh();
      ITIcebergMergeOnReadDeletionVectors.assertDeletionVectors(icebergTable, 10);
      assertEquals(50, readKeys(icebergTable).size(), "updates must not add or lose rows");

      table.deleteRecords(inserts.subList(10, 15), true);
      icebergTable.refresh();
      assertEquals(45, readKeys(icebergTable).size(), "deleted rows must disappear");

      table.compact();
      icebergTable.refresh();
      assertEquals(45, readKeys(icebergTable).size(), "compaction must not change the merged view");
      ITIcebergMergeOnReadDeletionVectors.assertNoDeletionVectors(icebergTable, "compaction");
    }
  }

  private Set<String> readKeys(Table icebergTable) {
    List<Row> rows =
        sparkSession
            .read()
            .format("iceberg")
            .load(icebergTable.location())
            .select("key")
            .collectAsList();
    Set<String> keys = new HashSet<>();
    rows.forEach(row -> keys.add(row.getString(0)));
    assertEquals(rows.size(), keys.size(), "Iceberg reads must not expose duplicate keys");
    return keys;
  }
}
