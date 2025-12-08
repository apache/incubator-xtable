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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.table.FileStoreTable;
import org.apache.xtable.GenericTable;
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

    // Verify "name" stats (STRING)
    String minName =
        rows.stream().map(r -> r.getString(1).toString()).min(String::compareTo).get();
    String maxName =
        rows.stream().map(r -> r.getString(1).toString()).max(String::compareTo).get();
    ColumnStat nameStat =
        stats.stream().filter(s -> s.getField().getName().equals("name")).findFirst().get();
    assertEquals(Range.vector(minName, maxName), nameStat.getRange());
    assertEquals(0, nameStat.getNumNulls());

    // Verify "value" stats (DOUBLE)
    double minValue = rows.stream().map(r -> r.getDouble(2)).min(Double::compareTo).get();
    double maxValue = rows.stream().map(r -> r.getDouble(2)).max(Double::compareTo).get();
    ColumnStat valueStat =
        stats.stream().filter(s -> s.getField().getName().equals("value")).findFirst().get();
    assertEquals(Range.vector(minValue, maxValue), valueStat.getRange());
    assertEquals(0, valueStat.getNumNulls());

    // Verify "created_at" stats (TIMESTAMP)
    Instant minCreatedAt =
        rows.stream()
            .map(r -> r.getTimestamp(3, 9).toInstant())
            .min(Instant::compareTo)
            .get();
    Instant maxCreatedAt =
        rows.stream()
            .map(r -> r.getTimestamp(3, 9).toInstant())
            .max(Instant::compareTo)
            .get();
    ColumnStat createdAtStat =
        stats.stream().filter(s -> s.getField().getName().equals("created_at")).findFirst().get();
    assertEquals(
        Range.vector(
            minCreatedAt.toEpochMilli() * 1000 + minCreatedAt.getNano() / 1000,
            maxCreatedAt.toEpochMilli() * 1000 + maxCreatedAt.getNano() / 1000),
        createdAtStat.getRange());
    assertEquals(0, createdAtStat.getNumNulls());

    // Verify "updated_at" stats (TIMESTAMP)
    Instant minUpdatedAt =
        rows.stream()
            .map(r -> r.getTimestamp(4, 9).toInstant())
            .min(Instant::compareTo)
            .get();
    Instant maxUpdatedAt =
        rows.stream()
            .map(r -> r.getTimestamp(4, 9).toInstant())
            .max(Instant::compareTo)
            .get();
    ColumnStat updatedAtStat =
        stats.stream().filter(s -> s.getField().getName().equals("updated_at")).findFirst().get();
    assertEquals(
        Range.vector(
            minUpdatedAt.toEpochMilli() * 1000 + minUpdatedAt.getNano() / 1000,
            maxUpdatedAt.toEpochMilli() * 1000 + maxUpdatedAt.getNano() / 1000),
        updatedAtStat.getRange());
    assertEquals(0, updatedAtStat.getNumNulls());

    // Verify "is_active" stats (BOOLEAN)
    ColumnStat isActiveStat =
        stats.stream().filter(s -> s.getField().getName().equals("is_active")).findFirst().get();
    assertEquals(Range.vector(false, true), isActiveStat.getRange());
    assertEquals(0, isActiveStat.getNumNulls());

    // Verify "description" stats (VARCHAR(255))
    String minDescription =
        rows.stream().map(r -> r.getString(6).toString()).min(String::compareTo).get();
    String maxDescription =
        rows.stream().map(r -> r.getString(6).toString()).max(String::compareTo).get();
    ColumnStat descriptionStat =
        stats.stream().filter(s -> s.getField().getName().equals("description")).findFirst().get();
    assertEquals(Range.vector(minDescription, maxDescription), descriptionStat.getRange());
    assertEquals(0, descriptionStat.getNumNulls());
  }

  @Test
  void testColumnStatsPartitionedTable() {
    createPartitionedTable();
    InternalSchema schema = schemaExtractor.toInternalSchema(testTable.getPaimonTable().schema());

    List<GenericRow> rows = testTable.insertRows(10);

    List<InternalDataFile> result =
        extractor.toInternalDataFiles(
            paimonTable, paimonTable.snapshotManager().latestSnapshot(), schema);

    assertFalse(result.isEmpty());
    InternalDataFile dataFile = result.get(0);
    List<ColumnStat> stats = dataFile.getColumnStats();
    assertFalse(stats.isEmpty());

    assertEquals("id", stats.get(0).getField().getName());
    assertEquals("name", stats.get(1).getField().getName());
    assertEquals("value", stats.get(2).getField().getName());
    assertEquals("created_at", stats.get(3).getField().getName());
    assertEquals("updated_at", stats.get(4).getField().getName());
    assertEquals("is_active", stats.get(5).getField().getName());
    assertEquals("description", stats.get(6).getField().getName());
    assertEquals("level", stats.get(7).getField().getName());

    assertEquals(Range.scalar(GenericTable.LEVEL_VALUES.get(0)), stats.get(7).getRange());
  }

  // TODO: test with millis, micros and nanos timestamp
  // TODO: test with decimal
  // TODO: test long string field truncation
  // TODO: test with date field
  // TODO: test null counts & value counts


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
