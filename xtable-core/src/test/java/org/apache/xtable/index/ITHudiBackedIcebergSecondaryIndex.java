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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
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
      assertLookupMatchesIceberg(table, index, partitionField != null, Collections.emptyList());

      // a second batch of files is added to the index by an incremental sync
      records.addAll(table.insertRows(50));
      icebergTable.refresh();
      index.syncIndex(icebergTable, INDEXED_COLUMN);
      assertLookupMatchesIceberg(table, index, partitionField != null, Collections.emptyList());

      // updating rows rewrites the files that hold them, so the index must resolve the updated keys
      // to their new file and row position instead of the ones the previous sync recorded
      table.upsertRows(records.subList(0, 30));
      icebergTable.refresh();
      index.syncIndex(icebergTable, INDEXED_COLUMN);
      assertLookupMatchesIceberg(table, index, partitionField != null, Collections.emptyList());

      // deleting rows must drop their keys from the index, and must not strand the rows that are
      // rewritten alongside them
      List<Record> deletedRecords = new ArrayList<>(records.subList(30, 50));
      List<String> deletedKeys =
          deletedRecords.stream()
              .map(record -> record.getField(INDEXED_COLUMN).toString())
              .collect(Collectors.toList());
      table.deleteRows(deletedRecords);
      records.removeAll(deletedRecords);
      icebergTable.refresh();
      index.syncIndex(icebergTable, INDEXED_COLUMN);
      assertLookupMatchesIceberg(table, index, partitionField != null, deletedKeys);
    }
  }

  /**
   * Looks every key currently in the Iceberg table up in the index and checks the result against
   * Iceberg's own {@code _file}, {@code _pos} and partition values. Expectations are read from the
   * table rather than from the records the test wrote, so this stays correct after rows are updated
   * or deleted. {@code keysThatMustNotResolve} are looked up as well and must return nothing.
   */
  private void assertLookupMatchesIceberg(
      TestIcebergTable table,
      HudiBackedIcebergSecondaryIndex index,
      boolean partitioned,
      List<String> keysThatMustNotResolve) {
    Map<String, Pair<String, Long>> expectedLocations = new HashMap<>();
    Map<String, String> expectedPartitions = new HashMap<>();
    sparkSession
        .read()
        .format("iceberg")
        .load(table.getBasePath())
        .selectExpr(INDEXED_COLUMN, "_file", "_pos", PARTITION_COLUMN)
        .collectAsList()
        .forEach(
            row -> {
              expectedLocations.put(
                  row.getString(0),
                  Pair.of(new Path(row.getString(1)).toUri().getPath(), row.getLong(2)));
              expectedPartitions.put(row.getString(0), row.getString(3));
            });

    List<String> keys = new ArrayList<>(expectedLocations.keySet());
    // keys that are not in the table must not produce a result
    keys.add("missing-key-1");
    keys.add("missing-key-2");
    keys.addAll(keysThatMustNotResolve);
    List<IndexLookupResult> lookupResults =
        index
            .lookup(table.getIcebergTable(), jsc.parallelize(keys, 2).rdd(), INDEXED_COLUMN)
            .toJavaRDD()
            .collect();
    assertEquals(expectedLocations.size(), lookupResults.size());

    Set<String> resolvedKeys =
        lookupResults.stream().map(IndexLookupResult::getKey).collect(Collectors.toSet());
    keysThatMustNotResolve.forEach(key -> assertFalse(resolvedKeys.contains(key)));

    for (IndexLookupResult lookupResult : lookupResults) {
      Pair<String, Long> expected = expectedLocations.get(lookupResult.getKey());
      assertNotNull(expected, "index returned a key that is not in the table");
      assertEquals(expected.getLeft(), new Path(lookupResult.getFile()).toUri().getPath());
      assertEquals(expected.getRight(), lookupResult.getPosition());
      if (partitioned) {
        assertEquals(
            expectedPartitions.get(lookupResult.getKey()),
            lookupResult.getPartition().getUTF8String(0).toString());
      } else {
        assertNull(lookupResult.getPartition());
      }
    }
  }
}
