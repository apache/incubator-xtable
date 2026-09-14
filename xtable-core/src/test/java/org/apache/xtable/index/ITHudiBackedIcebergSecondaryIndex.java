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
 
package org.apache.xtable.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.client.HoodieReadClient;

import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;

import org.apache.xtable.TestIcebergTable;
import org.apache.xtable.hudi.HudiTestUtil;

/**
 * Builds a Hudi backed secondary index for an Iceberg table and checks that lookups resolve to the
 * same file and row position as Iceberg's {@code _file} and {@code _pos} metadata columns.
 */
public class ITHudiBackedIcebergSecondaryIndex {
  private static final String INDEXED_COLUMN = "id";
  private static final String PARTITION_COLUMN = "level";

  @TempDir public static java.nio.file.Path tempDir;

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

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = PARTITION_COLUMN)
  void syncAndLookup(String partitionField) {
    String tableName = "test_table_" + UUID.randomUUID().toString().replace("-", "_");
    try (TestIcebergTable table =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, partitionField, tempDir, jsc.hadoopConfiguration())) {
      List<Record> records = new ArrayList<>(table.insertRows(100));
      Table icebergTable = table.getIcebergTable();
      HudiBackedIcebergSecondaryIndex index =
          new HudiBackedIcebergSecondaryIndex(icebergTable, sparkSession, new Properties());
      assertFalse(index.doesIndexExist(INDEXED_COLUMN));

      index.syncIndex(icebergTable, INDEXED_COLUMN);
      assertTrue(index.doesIndexExist(INDEXED_COLUMN));
      assertLookupMatchesIceberg(table, index, records, partitionField != null);

      // a second batch of files is added to the index by an incremental sync
      records.addAll(table.insertRows(50));
      icebergTable.refresh();
      index.syncIndex(icebergTable, INDEXED_COLUMN);
      assertLookupMatchesIceberg(table, index, records, partitionField != null);
    }
  }

  private void assertLookupMatchesIceberg(
      TestIcebergTable table,
      HudiBackedIcebergSecondaryIndex index,
      List<Record> records,
      boolean partitioned) {
    List<String> keys =
        records.stream()
            .map(record -> record.getField(INDEXED_COLUMN).toString())
            .collect(Collectors.toList());
    // keys that are not in the table must not produce a result
    keys.add("missing-key-1");
    keys.add("missing-key-2");
    List<IndexLookupResult> lookupResults =
        index
            .lookup(table.getIcebergTable(), jsc.parallelize(keys, 2).rdd(), INDEXED_COLUMN)
            .toJavaRDD()
            .collect();
    assertEquals(records.size(), lookupResults.size());

    Map<String, Pair<String, Long>> expectedLocations =
        sparkSession
            .read()
            .format("iceberg")
            .load(table.getBasePath())
            .selectExpr(INDEXED_COLUMN, "_file", "_pos")
            .collectAsList()
            .stream()
            .collect(
                Collectors.toMap(
                    row -> row.getString(0),
                    row -> Pair.of(new Path(row.getString(1)).toUri().getPath(), row.getLong(2))));
    Map<String, Record> recordsByKey =
        records.stream()
            .collect(
                Collectors.toMap(
                    record -> record.getField(INDEXED_COLUMN).toString(), Function.identity()));
    for (IndexLookupResult lookupResult : lookupResults) {
      Pair<String, Long> expected = expectedLocations.get(lookupResult.getKey());
      assertEquals(expected.getLeft(), new Path(lookupResult.getFile()).toUri().getPath());
      assertEquals(expected.getRight(), lookupResult.getPosition());
      if (partitioned) {
        assertEquals(
            recordsByKey.get(lookupResult.getKey()).getField(PARTITION_COLUMN).toString(),
            lookupResult.getPartition().getUTF8String(0).toString());
      } else {
        assertNull(lookupResult.getPartition());
      }
    }
  }
}
