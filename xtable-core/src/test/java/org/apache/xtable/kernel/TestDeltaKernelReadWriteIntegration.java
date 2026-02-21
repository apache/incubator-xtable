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
 
package org.apache.xtable.kernel;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;

import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.sync.TableFormatSync;

/**
 * Comprehensive end-to-end integration test for Delta Kernel read and write operations.
 *
 * <p>This test validates: 1. Writing data to Delta tables using DeltaKernelConversionTarget 2.
 * Reading data from Delta tables using DeltaKernelConversionSource 3. Round-trip data integrity
 * (write → read → validate) 4. Partitioned tables 5. Incremental updates (add/remove files) 6. Time
 * travel (version-based reads) 7. Empty table handling
 */
public class TestDeltaKernelReadWriteIntegration {
  private static final Random RANDOM = new Random();
  private static final Instant LAST_COMMIT_TIME = Instant.now();

  @TempDir public Path tempDir;
  private Engine engine;

  @BeforeEach
  public void setup() {
    Configuration hadoopConf = new Configuration();
    engine = DefaultEngine.create(hadoopConf);
  }

  /**
   * Test 1: Basic Write and Read Validates that data written to Delta can be read back correctly.
   */
  @Test
  public void testBasicWriteAndRead() throws Exception {
    String tableName = "test_basic_" + UUID.randomUUID();
    Path basePath = tempDir.resolve(tableName);
    Files.createDirectories(basePath);

    // === WRITE PHASE ===
    InternalSchema schema = createSimpleSchema();
    DeltaKernelConversionTarget writer = createWriter(tableName, basePath);

    // Create test data files
    InternalDataFile file1 = createDataFile(1, Collections.emptyList(), basePath);
    InternalDataFile file2 = createDataFile(2, Collections.emptyList(), basePath);

    // Write data to Delta table
    InternalTable writeTable = createInternalTable(tableName, basePath, schema, null);
    InternalSnapshot snapshot = buildSnapshot(writeTable, "0", file1, file2);
    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(writer), snapshot);

    // Verify Delta table was created
    assertTrue(Files.exists(basePath.resolve("_delta_log")), "Delta log directory should exist");

    // === READ PHASE ===
    DeltaKernelConversionSource reader = createReader(tableName, basePath);

    // Read current table metadata
    InternalTable readTable = reader.getCurrentTable();
    assertNotNull(readTable, "Should be able to read table");
    assertEquals(tableName, readTable.getName());

    // Normalize paths for comparison (handle file:// prefix differences)
    String expectedPath = basePath.toString();
    String actualPath = readTable.getBasePath().replace("file://", "").replace("file:", "");
    assertTrue(
        actualPath.endsWith(expectedPath) || actualPath.equals(expectedPath),
        "Base path should match. Expected: " + expectedPath + ", Actual: " + actualPath);

    // Verify schema
    InternalSchema readSchema = readTable.getReadSchema();
    assertNotNull(readSchema);
    assertEquals(schema.getFields().size(), readSchema.getFields().size());

    // Read current snapshot
    InternalSnapshot readSnapshot = reader.getCurrentSnapshot();
    assertNotNull(readSnapshot);

    // Extract data files from partition groups (files with same partition values are grouped)
    List<InternalDataFile> dataFiles = extractDataFiles(readSnapshot);
    assertEquals(2, dataFiles.size(), "Should have 2 files in snapshot");
    assertTrue(dataFiles.stream().anyMatch(f -> f.getFileSizeBytes() == file1.getFileSizeBytes()));
    assertTrue(dataFiles.stream().anyMatch(f -> f.getFileSizeBytes() == file2.getFileSizeBytes()));
  }

  /**
   * Test 2: Partitioned Table Write and Read Validates partition handling in both write and read
   * operations.
   */
  @Test
  public void testPartitionedTableRoundTrip() throws Exception {
    String tableName = "test_partitioned_" + UUID.randomUUID();
    Path basePath = tempDir.resolve(tableName);
    Files.createDirectories(basePath);

    DeltaKernelConversionTarget writer = createWriter(tableName, basePath);
    DeltaKernelConversionSource reader = createReader(tableName, basePath);

    // Define partition field
    InternalPartitionField partitionField =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("string_field")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();

    // === WRITE PHASE ===
    InternalSchema schema = createSimpleSchema();
    InternalTable table =
        createInternalTable(tableName, basePath, schema, Collections.singletonList(partitionField));

    // Create partitioned data files
    List<PartitionValue> partition1 =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(partitionField)
                .range(Range.scalar("category_a"))
                .build());
    List<PartitionValue> partition2 =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(partitionField)
                .range(Range.scalar("category_b"))
                .build());

    InternalDataFile file1 = createDataFile(1, partition1, basePath);
    InternalDataFile file2 = createDataFile(2, partition1, basePath);
    InternalDataFile file3 = createDataFile(3, partition2, basePath);

    InternalSnapshot snapshot = buildSnapshot(table, "0", file1, file2, file3);
    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(writer), snapshot);

    // === READ PHASE ===
    InternalTable readTable = reader.getCurrentTable();

    // Verify partitioning
    assertNotNull(readTable.getPartitioningFields());
    assertEquals(1, readTable.getPartitioningFields().size());
    assertEquals(
        "string_field", readTable.getPartitioningFields().get(0).getSourceField().getName());

    // Verify all files are present
    InternalSnapshot readSnapshot = reader.getCurrentSnapshot();
    List<InternalDataFile> dataFiles = extractDataFiles(readSnapshot);
    assertEquals(3, dataFiles.size(), "Should have all 3 partitioned files");

    // Verify partition columns in Delta metadata
    Table deltaTable = Table.forPath(engine, basePath.toString());
    Snapshot deltaSnapshot = deltaTable.getLatestSnapshot(engine);
    SnapshotImpl snapshotImpl = (SnapshotImpl) deltaSnapshot;
    Set<String> partitionColumns = snapshotImpl.getMetadata().getPartitionColNames();
    assertEquals(1, partitionColumns.size());
    assertTrue(partitionColumns.contains("string_field"));
  }

  /**
   * Test 3: Incremental Updates (Add/Remove Files) Validates that incremental changes are properly
   * handled.
   */
  @Test
  public void testIncrementalUpdates() throws Exception {
    String tableName = "test_incremental_" + UUID.randomUUID();
    Path basePath = tempDir.resolve(tableName);
    Files.createDirectories(basePath);

    DeltaKernelConversionTarget writer = createWriter(tableName, basePath);
    DeltaKernelConversionSource reader = createReader(tableName, basePath);

    InternalSchema schema = createSimpleSchema();
    InternalTable table = createInternalTable(tableName, basePath, schema, null);

    // === SNAPSHOT 1: Initial files ===
    InternalDataFile file1 = createDataFile(1, Collections.emptyList(), basePath);
    InternalDataFile file2 = createDataFile(2, Collections.emptyList(), basePath);
    InternalSnapshot snapshot1 = buildSnapshot(table, "0", file1, file2);
    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(writer), snapshot1);

    InternalSnapshot read1 = reader.getCurrentSnapshot();
    assertEquals(2, extractDataFiles(read1).size(), "Should have 2 files after first snapshot");

    // === SNAPSHOT 2: Remove file1, keep file2, add file3 ===
    InternalDataFile file3 = createDataFile(3, Collections.emptyList(), basePath);
    InternalSnapshot snapshot2 = buildSnapshot(table, "1", file2, file3);
    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(writer), snapshot2);

    InternalSnapshot read2 = reader.getCurrentSnapshot();
    List<InternalDataFile> files2 = extractDataFiles(read2);
    assertEquals(2, files2.size(), "Should have 2 files after second snapshot");

    // Verify correct files are present
    assertTrue(
        files2.stream().anyMatch(f -> f.getFileSizeBytes() == file2.getFileSizeBytes()),
        "file2 should be present");
    assertTrue(
        files2.stream().anyMatch(f -> f.getFileSizeBytes() == file3.getFileSizeBytes()),
        "file3 should be present");
    assertFalse(
        files2.stream().anyMatch(f -> f.getFileSizeBytes() == file1.getFileSizeBytes()),
        "file1 should be removed");

    // === SNAPSHOT 3: Replace all files ===
    InternalDataFile file4 = createDataFile(4, Collections.emptyList(), basePath);
    InternalSnapshot snapshot3 = buildSnapshot(table, "2", file4);
    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(writer), snapshot3);

    InternalSnapshot read3 = reader.getCurrentSnapshot();
    List<InternalDataFile> files3 = extractDataFiles(read3);
    assertEquals(1, files3.size(), "Should have only 1 file after third snapshot");
    assertEquals(file4.getFileSizeBytes(), files3.get(0).getFileSizeBytes());
  }

  /** Test 4: Read at Specific Version (Time Travel) Validates version-based reading. */
  @Test
  public void testReadAtVersion() throws Exception {
    String tableName = "test_versioned_" + UUID.randomUUID();
    Path basePath = tempDir.resolve(tableName);
    Files.createDirectories(basePath);

    DeltaKernelConversionTarget writer = createWriter(tableName, basePath);
    DeltaKernelConversionSource reader = createReader(tableName, basePath);

    InternalSchema schema = createSimpleSchema();
    InternalTable table = createInternalTable(tableName, basePath, schema, null);

    // Write version 0
    InternalDataFile file1 = createDataFile(1, Collections.emptyList(), basePath);
    InternalSnapshot snapshot0 = buildSnapshot(table, "0", file1);
    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(writer), snapshot0);

    // Write version 1
    InternalDataFile file2 = createDataFile(2, Collections.emptyList(), basePath);
    InternalSnapshot snapshot1 = buildSnapshot(table, "1", file1, file2);
    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(writer), snapshot1);

    // Write version 2
    InternalDataFile file3 = createDataFile(3, Collections.emptyList(), basePath);
    InternalSnapshot snapshot2 = buildSnapshot(table, "2", file2, file3);
    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(writer), snapshot2);

    // Read at version 0 (should have only file1)
    InternalTable tableV0 = reader.getTable(0L);
    assertNotNull(tableV0);

    // Read at version 1 (should have file1 and file2)
    InternalTable tableV1 = reader.getTable(1L);
    assertNotNull(tableV1);

    // Read latest version (should have file2 and file3)
    InternalSnapshot latestSnapshot = reader.getCurrentSnapshot();
    List<InternalDataFile> latestFiles = extractDataFiles(latestSnapshot);
    assertEquals(2, latestFiles.size());

    // Verify latest version doesn't have file1
    assertFalse(
        latestFiles.stream().anyMatch(f -> f.getFileSizeBytes() == file1.getFileSizeBytes()),
        "Latest version should not have file1");
  }

  /** Test 5: Empty Table Creation and Read Validates handling of empty tables. */
  @Test
  public void testEmptyTableRoundTrip() throws Exception {
    String tableName = "test_empty_" + UUID.randomUUID();
    Path basePath = tempDir.resolve(tableName);
    Files.createDirectories(basePath);

    DeltaKernelConversionTarget writer = createWriter(tableName, basePath);
    DeltaKernelConversionSource reader = createReader(tableName, basePath);

    // Write empty table with just schema
    InternalSchema schema = createSimpleSchema();
    InternalTable table = createInternalTable(tableName, basePath, schema, null);
    InternalSnapshot emptySnapshot = buildSnapshot(table, "0"); // No files

    TableFormatSync.getInstance().syncSnapshot(Collections.singletonList(writer), emptySnapshot);

    // Read back
    InternalTable readTable = reader.getCurrentTable();
    assertNotNull(readTable);
    assertEquals(schema.getFields().size(), readTable.getReadSchema().getFields().size());

    InternalSnapshot readSnapshot = reader.getCurrentSnapshot();
    assertNotNull(readSnapshot);
    assertEquals(0, readSnapshot.getPartitionedDataFiles().size(), "Should have no files");
  }

  // ==================== Helper Methods ====================

  private DeltaKernelConversionTarget createWriter(String tableName, Path basePath) {
    return new DeltaKernelConversionTarget(
        TargetTable.builder()
            .name(tableName)
            .basePath(basePath.toString())
            .metadataRetention(Duration.of(1, ChronoUnit.HOURS))
            .formatName(TableFormat.DELTA)
            .build(),
        engine);
  }

  private DeltaKernelConversionSource createReader(String tableName, Path basePath) {
    return DeltaKernelConversionSource.builder()
        .basePath(basePath.toString())
        .tableName(tableName)
        .engine(engine)
        .build();
  }

  private InternalSchema createSimpleSchema() {
    Map<InternalSchema.MetadataKey, Object> timestampMetadata = new HashMap<>();
    timestampMetadata.put(
        InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MILLIS);

    return InternalSchema.builder()
        .dataType(InternalType.RECORD)
        .name("test_schema")
        .fields(
            Arrays.asList(
                InternalField.builder()
                    .name("id")
                    .schema(
                        InternalSchema.builder()
                            .name("long")
                            .dataType(InternalType.LONG)
                            .isNullable(false)
                            .build())
                    .build(),
                InternalField.builder()
                    .name("string_field")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .isNullable(true)
                            .build())
                    .build(),
                InternalField.builder()
                    .name("int_field")
                    .schema(
                        InternalSchema.builder()
                            .name("int")
                            .dataType(InternalType.INT)
                            .isNullable(true)
                            .build())
                    .build(),
                InternalField.builder()
                    .name("timestamp_field")
                    .schema(
                        InternalSchema.builder()
                            .name("timestamp")
                            .dataType(InternalType.TIMESTAMP)
                            .isNullable(true)
                            .metadata(timestampMetadata)
                            .build())
                    .build()))
        .isNullable(false)
        .build();
  }

  private InternalTable createInternalTable(
      String tableName,
      Path basePath,
      InternalSchema schema,
      List<InternalPartitionField> partitionFields) {
    return InternalTable.builder()
        .name(tableName)
        .basePath(basePath.toUri().toString())
        .layoutStrategy(DataLayoutStrategy.FLAT)
        .tableFormat(TableFormat.HUDI)
        .readSchema(schema)
        .partitioningFields(partitionFields)
        .latestCommitTime(LAST_COMMIT_TIME)
        .build();
  }

  private InternalSnapshot buildSnapshot(
      InternalTable table, String sourceIdentifier, InternalDataFile... dataFiles) {
    return InternalSnapshot.builder()
        .table(table)
        .partitionedDataFiles(PartitionFileGroup.fromFiles(Arrays.asList(dataFiles)))
        .sourceIdentifier(sourceIdentifier)
        .build();
  }

  private InternalDataFile createDataFile(
      int index, List<PartitionValue> partitionValues, Path basePath) {
    try {
      Path filePath = basePath.resolve("data_" + index + ".parquet");
      Files.createFile(filePath);

      String physicalPath = new org.apache.hadoop.fs.Path(filePath.toUri()).toString();

      return InternalDataFile.builder()
          .fileFormat(FileFormat.APACHE_PARQUET)
          .fileSizeBytes(1000 + index) // Unique size for identification
          .physicalPath(physicalPath)
          .recordCount(100)
          .partitionValues(partitionValues)
          .columnStats(Collections.emptyList())
          .lastModified(Instant.now().toEpochMilli())
          .build();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create test data file", e);
    }
  }

  private List<InternalDataFile> extractDataFiles(InternalSnapshot snapshot) {
    List<InternalDataFile> files = new ArrayList<>();
    for (PartitionFileGroup group : snapshot.getPartitionedDataFiles()) {
      files.addAll(group.getDataFiles());
    }
    return files;
  }
}
