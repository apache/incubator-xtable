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
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.xtable.TestPaimonTable;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.storage.InternalDataFile;

public class TestPaimonDataFileExtractor {
  private static final PaimonDataFileExtractor extractor = PaimonDataFileExtractor.getInstance();

  @TempDir private Path tempDir;
  private TestPaimonTable testTable;
  private FileStoreTable paimonTable;
  private InternalSchema testSchema;

  @Test
  void testToInternalDataFilesWithUnpartitionedTable() {
    createUnpartitionedTable();

    // Insert some data to create files
    testTable.insertRows(5);

    List<InternalDataFile> result =
        extractor.toInternalDataFiles(
            paimonTable, paimonTable.snapshotManager().latestSnapshot(), testSchema);

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

    // Insert some data to create files
    testTable.insertRows(5);

    List<InternalDataFile> result =
        extractor.toInternalDataFiles(
            paimonTable, paimonTable.snapshotManager().latestSnapshot(), testSchema);

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

    // Insert some data to create files
    testTable.insertRows(5);

    // Get the latest snapshot
    List<InternalDataFile> result =
        extractor.toInternalDataFiles(
            paimonTable, paimonTable.snapshotManager().latestSnapshot(), testSchema);

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

    // Insert data
    testTable.insertRows(2);

    List<InternalDataFile> result =
        extractor.toInternalDataFiles(
            paimonTable, paimonTable.snapshotManager().latestSnapshot(), testSchema);

    assertFalse(result.isEmpty());

    for (InternalDataFile dataFile : result) {
      String path = dataFile.getPhysicalPath();
      assertTrue(path.contains("bucket-"));
      assertTrue(path.endsWith(".orc") || path.endsWith(".parquet"));
    }
  }

  @Test
  void testColumnStatsAreEmpty() {
    createUnpartitionedTable();

    testTable.insertRows(1);

    List<InternalDataFile> result =
        extractor.toInternalDataFiles(
            paimonTable, paimonTable.snapshotManager().latestSnapshot(), testSchema);

    assertFalse(result.isEmpty());
    for (InternalDataFile dataFile : result) {
      assertEquals(0, dataFile.getColumnStats().size());
    }
  }

  private void createUnpartitionedTable() {
    testTable =
        (TestPaimonTable)
            TestPaimonTable.createTable("test_table", null, tempDir, new Configuration(), false);
    paimonTable = testTable.getPaimonTable();
    testSchema =
        InternalSchema.builder().build(); // empty schema won't matter for non-partitioned tables
  }

  private void createPartitionedTable() {
    testTable =
        (TestPaimonTable)
            TestPaimonTable.createTable("test_table", "level", tempDir, new Configuration(), false);
    paimonTable = testTable.getPaimonTable();

    // just the partition field matters for this test
    InternalField partitionField =
        InternalField.builder()
            .name("level")
            .schema(InternalSchema.builder().dataType(InternalType.STRING).build())
            .build();

    testSchema = InternalSchema.builder().fields(Collections.singletonList(partitionField)).build();
  }

  private void createTableWithPrimaryKeys() {
    testTable =
        (TestPaimonTable)
            TestPaimonTable.createTable("test_table", null, tempDir, new Configuration(), false);
    paimonTable = testTable.getPaimonTable();
    testSchema =
        InternalSchema.builder().build(); // empty schema won't matter for non-partitioned tables
  }
}
