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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
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

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;

import org.apache.xtable.hudi.HudiTestUtil;

/**
 * End to end proof of the merge-on-read model under the Iceberg pluggable table format: an upsert
 * routes each update as a positional delete plus an insert into a different file group (the Hudi
 * writer's hoodie.write.updates.as.deletes.and.inserts mode), and the deltacommit lands in Iceberg
 * as a single row delta of new data files plus format-version 3 deletion vectors. Iceberg readers
 * then see exactly the merged view without a sync job or a compaction in between.
 */
class ITIcebergMergeOnReadDeletionVectors {

  @TempDir public static Path tempDir;

  private static SparkSession sparkSession;

  @BeforeAll
  static void setupOnce() {
    sparkSession = SparkSession.builder().config(HudiTestUtil.getSparkConf(tempDir)).getOrCreate();
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
    // The deletion-vector model: updates decompose into positional deletes plus inserts, all
    // inserts go to base files (no log routing), and the simple index supplies record positions.
    properties.put("hoodie.write.updates.as.deletes.and.inserts", "true");
    properties.put("hoodie.index.type", "SIMPLE");
    properties.put("hoodie.parquet.small.file.limit", "0");
    // Deletion vectors require an Iceberg format-version 3 table.
    properties.put("xtable.iceberg.format-version", "3");
    return properties;
  }

  @Test
  void upsertsAndDeletesProduceDeletionVectors() throws Exception {
    String tableName = "mor_deletion_vectors";
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.MERGE_ON_READ, tableProperties())) {

      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(50, true);
      Table icebergTable = new HadoopTables(new Configuration()).load(table.getBasePath());
      assertEquals(
          3,
          ((BaseTable) icebergTable).operations().current().formatVersion(),
          "the Iceberg table must be created at format version 3");
      assertEquals(50, readKeys(icebergTable).size());

      // First update round: 10 records move to new file groups, tombstoned positionally.
      table.upsertRecords(inserts.subList(0, 10), true);
      icebergTable.refresh();
      assertDeletionVectors(icebergTable, 10);
      Set<String> keysAfterFirstUpdate = readKeys(icebergTable);
      assertEquals(50, keysAfterFirstUpdate.size(), "updates must not add or lose rows");

      // Second update round on a different subset: the new deletion vector for each base file
      // must supersede the previous one (merged positions, old vector removed).
      table.upsertRecords(inserts.subList(10, 20), true);
      icebergTable.refresh();
      assertDeletionVectors(icebergTable, 20);
      assertEquals(50, readKeys(icebergTable).size());

      // Pure deletes ride the same positional path.
      table.deleteRecords(inserts.subList(20, 25), true);
      icebergTable.refresh();
      Set<String> keysAfterDelete = readKeys(icebergTable);
      assertEquals(45, keysAfterDelete.size(), "deleted rows must disappear from Iceberg reads");
      inserts.subList(20, 25).stream()
          .map(HoodieRecord::getRecordKey)
          .forEach(key -> assertFalse(keysAfterDelete.contains(key), "deleted key still visible"));

      // Compaction rewrites the tombstoned base files; the merged view must be unchanged.
      table.compact();
      icebergTable.refresh();
      assertEquals(45, readKeys(icebergTable).size(), "compaction must not change the merged view");
    }
  }

  @Test
  void repeatedUpdatesOfSameKeysStayConsistent() throws Exception {
    String tableName = "mor_dv_repeated_updates";
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.MERGE_ON_READ, tableProperties())) {
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(50, true);
      Table icebergTable = new HadoopTables(new Configuration()).load(table.getBasePath());

      // The same ten keys move to a new file group on every round; each round adds one deletion
      // vector for the file group the keys previously lived in.
      List<HoodieRecord<HoodieAvroPayload>> latest = inserts.subList(0, 10);
      for (int round = 1; round <= 3; round++) {
        latest = table.upsertRecords(latest, true);
        icebergTable.refresh();
        assertDeletionVectors(icebergTable, 10L * round);
        assertEquals(
            50,
            readKeys(icebergTable).size(),
            "round " + round + " must neither add nor lose rows");
      }
    }
  }

  @Test
  void rollbackOfDeletionVectorDeltacommitRevertsIceberg() throws Exception {
    String tableName = "mor_dv_rollback";
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.MERGE_ON_READ, tableProperties())) {
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(20, true);
      table.upsertRecords(inserts.subList(0, 5), true);
      Table icebergTable = new HadoopTables(new Configuration()).load(table.getBasePath());
      assertDeletionVectors(icebergTable, 5);

      String updateInstant =
          table
              .getMetaClient()
              .reloadActiveTimeline()
              .filterCompletedInstants()
              .lastInstant()
              .get()
              .requestedTime();
      assertTrue(table.getWriteClient().rollback(updateInstant), "rollback must succeed");

      icebergTable.refresh();
      Set<String> keys = readKeys(icebergTable);
      assertEquals(20, keys.size(), "rollback must restore the pre-update row set");
      long remainingDeleteFiles = 0;
      try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().planFiles()) {
        for (FileScanTask task : tasks) {
          remainingDeleteFiles += task.deletes().size();
        }
      }
      assertEquals(0, remainingDeleteFiles, "rollback must remove the deletion vector");
    }
  }

  /**
   * Asserts each data file carries at most one deletion vector and the vectors cover the expected
   * number of deleted rows in total.
   */
  private void assertDeletionVectors(Table icebergTable, long expectedDeletedRows)
      throws Exception {
    Map<String, List<DeleteFile>> deletesByDataFile = new HashMap<>();
    try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().planFiles()) {
      tasks.forEach(task -> deletesByDataFile.put(task.file().path().toString(), task.deletes()));
    }
    long deletedRows = 0;
    boolean sawDeletionVector = false;
    for (Map.Entry<String, List<DeleteFile>> entry : deletesByDataFile.entrySet()) {
      assertTrue(
          entry.getValue().size() <= 1,
          "a data file must have at most one deletion vector: " + entry.getKey());
      for (DeleteFile deleteFile : entry.getValue()) {
        assertEquals(FileFormat.PUFFIN, deleteFile.format(), "deletes must be deletion vectors");
        deletedRows += deleteFile.recordCount();
        sawDeletionVector = true;
      }
    }
    assertTrue(sawDeletionVector, "expected at least one deletion vector");
    assertEquals(expectedDeletedRows, deletedRows, "deletion vectors must cover all deleted rows");
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
