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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.Snapshot;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.xtable.TestPaimonTable;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFilesDiff;

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
  void testExtractFilesDiffWithNewFiles() {
    createUnpartitionedTable();
    InternalSchema schema = schemaExtractor.toInternalSchema(testTable.getPaimonTable().schema());

    // Insert initial data
    testTable.insertRows(5);
    Snapshot firstSnapshot = paimonTable.snapshotManager().latestSnapshot();
    assertNotNull(firstSnapshot);

    // Insert more data to create a second snapshot
    testTable.insertRows(3);
    Snapshot secondSnapshot = paimonTable.snapshotManager().latestSnapshot();
    assertNotNull(secondSnapshot);

    InternalFilesDiff filesDiff = extractor.extractFilesDiff(paimonTable, secondSnapshot, schema);

    // Verify we have replaced the single file on this setup
    assertNotNull(filesDiff);
    assertNotNull(filesDiff.getFilesAdded());
    assertEquals(1, filesDiff.getFilesAdded().size());
    // Note: Even for inserts, Paimon tables with primary keys (which all test tables have)
    // may have removed files due to compaction. The compaction merges files, so old files are
    // removed
    // and new compacted files are added. This is expected behavior.
    assertNotNull(filesDiff.getFilesRemoved());
    assertEquals(1, filesDiff.getFilesRemoved().size());
  }

  @Test
  void testExtractFilesDiffWithPartitionedTable() {
    createPartitionedTable();
    InternalSchema schema = schemaExtractor.toInternalSchema(testTable.getPaimonTable().schema());

    // Insert initial data
    testTable.insertRows(5);
    Snapshot firstSnapshot = paimonTable.snapshotManager().latestSnapshot();
    assertNotNull(firstSnapshot);

    // Insert more data
    testTable.insertRows(3);
    Snapshot secondSnapshot = paimonTable.snapshotManager().latestSnapshot();
    assertNotNull(secondSnapshot);

    InternalFilesDiff filesDiff = extractor.extractFilesDiff(paimonTable, secondSnapshot, schema);

    // Verify we have added files with partition values
    assertNotNull(filesDiff);
    assertTrue(filesDiff.getFilesAdded().size() > 0);

    for (InternalDataFile file : filesDiff.dataFilesAdded()) {
      assertNotNull(file.getPartitionValues());
    }
  }

  @Test
  void testExtractFilesDiffWithTableWithPrimaryKeys() {
    createTableWithPrimaryKeys();
    InternalSchema schema = schemaExtractor.toInternalSchema(testTable.getPaimonTable().schema());

    // Insert initial data
    testTable.insertRows(5);
    Snapshot firstSnapshot = paimonTable.snapshotManager().latestSnapshot();
    assertNotNull(firstSnapshot);

    // Insert more data to create compaction
    testTable.insertRows(3);
    Snapshot secondSnapshot = paimonTable.snapshotManager().latestSnapshot();
    assertNotNull(secondSnapshot);

    InternalFilesDiff filesDiff = extractor.extractFilesDiff(paimonTable, secondSnapshot, schema);

    // Verify the diff is returned (size may vary based on compaction)
    assertNotNull(filesDiff);
    assertNotNull(filesDiff.getFilesAdded());
    assertNotNull(filesDiff.getFilesRemoved());
  }

  @Test
  void testExtractFilesDiffForFirstSnapshot() {
    createUnpartitionedTable();
    InternalSchema schema = schemaExtractor.toInternalSchema(testTable.getPaimonTable().schema());

    // Insert data to create first snapshot
    testTable.insertRows(5);
    Snapshot firstSnapshot = paimonTable.snapshotManager().latestSnapshot();
    assertNotNull(firstSnapshot);

    InternalFilesDiff filesDiff = extractor.extractFilesDiff(paimonTable, firstSnapshot, schema);

    // First snapshot should only have added files
    assertNotNull(filesDiff);
    assertTrue(filesDiff.getFilesAdded().size() > 0);
    assertEquals(0, filesDiff.getFilesRemoved().size());
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
