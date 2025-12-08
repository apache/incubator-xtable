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
 
package org.apache.xtable.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.xtable.TestPaimonTable;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.InternalDataFile;

public class TestPaimonDataFileExtractor {
  private static final PaimonDataFileExtractor extractor = PaimonDataFileExtractor.getInstance();
  private static final PaimonSchemaExtractor schemaExtractor = PaimonSchemaExtractor.getInstance();

  @TempDir private Path tempDir;
  private TestPaimonTable testTable;
  private FileStoreTable paimonTable;
  private InternalSchema testSchema;

  @Test
  void testToInternalDataFilesWithUnpartitionedTable() {
    createUnpartitionedTable();
    InternalSchema schema = schemaExtractor.toInternalSchema(testTable.getPaimonTable().schema());
    assertEquals(1, schema.getRecordKeyFields().size());

    // Insert some data to create files
    testTable.insertRows(5);

    List<InternalDataFile> result =
        extractor.toInternalDataFiles(
            paimonTable, paimonTable.snapshotManager().latestSnapshot(), schema);

    assertNotNull(result);
    assertFalse(result.isEmpty());

    InternalDataFile dataFile = result.get(0);
    assertNotNull(dataFile.getPhysicalPath());
    assertTrue(dataFile.getPhysicalPath().contains("bucket-"));
    assertTrue(dataFile.getFileSizeBytes() > 0);
    assertEquals(5, dataFile.getRecordCount());
    assertEquals(0, dataFile.getPartitionValues().size());
  }

  @Test
  void testToInternalDataFilesWithPartitionedTable() {
    createPartitionedTable();
    InternalSchema schema = schemaExtractor.toInternalSchema(testTable.getPaimonTable().schema());

    // Insert some data to create files
    testTable.insertRows(5);

    List<InternalDataFile> result =
        extractor.toInternalDataFiles(
            paimonTable, paimonTable.snapshotManager().latestSnapshot(), schema);

    assertNotNull(result);
    assertFalse(result.isEmpty());

    InternalDataFile dataFile = result.get(0);
    assertNotNull(dataFile.getPhysicalPath());
    assertTrue(dataFile.getPhysicalPath().contains("bucket-"));
    assertTrue(dataFile.getFileSizeBytes() > 0);
    assertEquals(5, dataFile.getRecordCount());
    assertNotNull(dataFile.getPartitionValues());
  }

  @Test
  void testToInternalDataFilesWithTableWithPrimaryKeys() {
    createTableWithPrimaryKeys();
    InternalSchema schema = schemaExtractor.toInternalSchema(testTable.getPaimonTable().schema());

    // Insert some data to create files
    testTable.insertRows(5);

    // Get the latest snapshot
    List<InternalDataFile> result =
        extractor.toInternalDataFiles(
            paimonTable, paimonTable.snapshotManager().latestSnapshot(), schema);

    assertNotNull(result);
    assertFalse(result.isEmpty());

    InternalDataFile dataFile = result.get(0);
    assertNotNull(dataFile.getPhysicalPath());
    assertTrue(dataFile.getFileSizeBytes() > 0);
    assertEquals(5, dataFile.getRecordCount());
  }

  @Test
  void testPhysicalPathFormat() {
    createUnpartitionedTable();
    InternalSchema schema = schemaExtractor.toInternalSchema(testTable.getPaimonTable().schema());

    // Insert data
    testTable.insertRows(2);

    List<InternalDataFile> result =
        extractor.toInternalDataFiles(
            paimonTable, paimonTable.snapshotManager().latestSnapshot(), schema);

    assertFalse(result.isEmpty());

    for (InternalDataFile dataFile : result) {
      String path = dataFile.getPhysicalPath();
      assertTrue(path.contains("bucket-"));
      assertTrue(path.endsWith(".orc") || path.endsWith(".parquet"));
    }
  }

  @Test
  void testColumnStatsUnpartitioned() {
    createUnpartitionedTable();
    InternalSchema schema = schemaExtractor.toInternalSchema(testTable.getPaimonTable().schema());

    List<GenericRow> rows = testTable.insertRows(10);

    List<InternalDataFile> result =
        extractor.toInternalDataFiles(
            paimonTable, paimonTable.snapshotManager().latestSnapshot(), schema);

    assertFalse(result.isEmpty());
    InternalDataFile dataFile = result.get(0);
    List<ColumnStat> stats = dataFile.getColumnStats();
    assertFalse(stats.isEmpty());

    // Verify "id" stats (INT)
    int minId = rows.stream().map(r -> r.getInt(0)).min(Integer::compareTo).get();
    int maxId = rows.stream().map(r -> r.getInt(0)).max(Integer::compareTo).get();
    ColumnStat idStat =
        stats.stream().filter(s -> s.getField().getName().equals("id")).findFirst().get();
    assertEquals(Range.vector(minId, maxId), idStat.getRange());
    assertEquals(0, idStat.getNumNulls());

    // Verify "value" stats (DOUBLE)
    double minValue = rows.stream().map(r -> r.getDouble(2)).min(Double::compareTo).get();
    double maxValue = rows.stream().map(r -> r.getDouble(2)).max(Double::compareTo).get();
    ColumnStat valueStat =
        stats.stream().filter(s -> s.getField().getName().equals("value")).findFirst().get();
    assertEquals(Range.vector(minValue, maxValue), valueStat.getRange());
    assertEquals(0, valueStat.getNumNulls());

    // Verify "name" stats (STRING)
    // Verify "created_at" stats (TIMESTAMP)
    // Verify "updated_at" stats (TIMESTAMP)
    // Verify "is_active" stats (BOOLEAN)
    // Verify "description" stats (VARCHAR(255))

  }

  private void createUnpartitionedTable() {
    testTable =
        (TestPaimonTable)
            TestPaimonTable.createTable("test_table", null, tempDir, new Configuration(), false);
    paimonTable = testTable.getPaimonTable();
  }

  private void createPartitionedTable() {
    testTable =
        (TestPaimonTable)
            TestPaimonTable.createTable("test_table", "level", tempDir, new Configuration(), false);
    paimonTable = testTable.getPaimonTable();
  }

  private void createTableWithPrimaryKeys() {
    testTable =
        (TestPaimonTable)
            TestPaimonTable.createTable("test_table", null, tempDir, new Configuration(), false);
    paimonTable = testTable.getPaimonTable();
  }
}
