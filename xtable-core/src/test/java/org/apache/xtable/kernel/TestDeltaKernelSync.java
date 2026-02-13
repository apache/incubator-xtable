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
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.ScanImpl;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.utils.CloseableIterator;

import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;
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
 * Validates that Delta Kernel tables are properly created/updated using
 * DeltaKernelConversionTarget. Tests partitioning, schema evolution, and metadata sync without
 * Spark SQL dependencies.
 */
public class TestDeltaKernelSync {
  private static final Random RANDOM = new Random();
  private static final Instant LAST_COMMIT_TIME = Instant.ofEpochSecond(1000);

  @TempDir public Path tempDir;
  private DeltaKernelConversionTarget conversionTarget;
  private Path basePath;
  private String tableName;
  private Engine engine;

  @BeforeEach
  public void setup() throws IOException {
    tableName = "test-" + UUID.randomUUID();
    basePath = tempDir.resolve(tableName);
    Files.createDirectories(basePath);

    Configuration hadoopConf = new Configuration();
    engine = DefaultEngine.create(hadoopConf);

    conversionTarget =
        new DeltaKernelConversionTarget(
            TargetTable.builder()
                .name(tableName)
                .basePath(basePath.toString())
                .metadataRetention(Duration.of(1, ChronoUnit.HOURS))
                .formatName(TableFormat.DELTA)
                .build(),
            engine);
  }

  @Test
  public void testCreateSnapshotControlFlow() throws Exception {
    InternalSchema schema1 = getInternalSchema();
    List<InternalField> fields2 = new ArrayList<>(schema1.getFields());
    fields2.add(
        InternalField.builder()
            .name("float_field")
            .schema(
                InternalSchema.builder()
                    .name("float")
                    .dataType(InternalType.FLOAT)
                    .isNullable(true)
                    .build())
            .build());
    InternalSchema schema2 = getInternalSchema().toBuilder().fields(fields2).build();
    InternalTable table1 = getInternalTable(tableName, basePath, schema1, null, LAST_COMMIT_TIME);
    InternalTable table2 = getInternalTable(tableName, basePath, schema2, null, LAST_COMMIT_TIME);

    InternalDataFile dataFile1 = getDataFile(1, Collections.emptyList(), basePath);
    InternalDataFile dataFile2 = getDataFile(2, Collections.emptyList(), basePath);
    InternalDataFile dataFile3 = getDataFile(3, Collections.emptyList(), basePath);

    InternalSnapshot snapshot1 = buildSnapshot(table1, "0", dataFile1, dataFile2);
    InternalSnapshot snapshot2 = buildSnapshot(table2, "1", dataFile2, dataFile3);

    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot1);
    validateDeltaTable(basePath, new HashSet<>(Arrays.asList(dataFile1, dataFile2)));

    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot2);
    validateDeltaTable(basePath, new HashSet<>(Arrays.asList(dataFile2, dataFile3)));
  }

  @Test
  public void testFileRemovalWithCheckpoint() throws Exception {
    // This test does 11 syncs to trigger checkpoint creation (happens at 10th commit)
    // and verifies that file removal works correctly after checkpoint exists
    String checkpointTableName = "test_table_checkpoint_" + UUID.randomUUID();
    Path checkpointTestPath = tempDir.resolve(checkpointTableName);
    Files.createDirectories(checkpointTestPath);

    InternalSchema schema = getInternalSchema();
    InternalTable checkpointTable =
        getInternalTable(checkpointTableName, checkpointTestPath, schema, null, LAST_COMMIT_TIME);

    DeltaKernelConversionTarget checkpointTarget =
        new DeltaKernelConversionTarget(
            TargetTable.builder()
                .name(checkpointTableName)
                .basePath(checkpointTestPath.toString())
                .metadataRetention(Duration.of(1, ChronoUnit.HOURS))
                .formatName(TableFormat.DELTA)
                .build(),
            engine);

    System.out.println("=== Starting 10 syncs to trigger checkpoint ===");

    // Do 10 syncs to trigger checkpoint creation
    for (int i = 0; i < 10; i++) {
      InternalDataFile file1 = getDataFile(i * 2 + 1, Collections.emptyList(), checkpointTestPath);
      InternalDataFile file2 = getDataFile(i * 2 + 2, Collections.emptyList(), checkpointTestPath);

      InternalSnapshot snapshot = buildSnapshot(checkpointTable, String.valueOf(i), file1, file2);
      TableFormatSync.getInstance()
          .syncSnapshot(Collections.singletonList(checkpointTarget), snapshot);

      System.out.println("Completed sync " + (i + 1) + " of 10");
    }

    System.out.println("=== 10 syncs complete. Checkpoint should be created at version 10 ===");

    // 11th sync: This triggers checkpoint creation at version 10
    InternalDataFile file21 = getDataFile(21, Collections.emptyList(), checkpointTestPath);
    InternalDataFile file22 = getDataFile(22, Collections.emptyList(), checkpointTestPath);
    InternalSnapshot snapshot11 = buildSnapshot(checkpointTable, "10", file21, file22);

    System.out.println("=== Doing 11th sync (creates checkpoint at version 10) ===");
    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(checkpointTarget), snapshot11);

    // Sleep briefly to ensure checkpoint file system operations complete
    Thread.sleep(100);

    // 12th sync: NOW checkpoint exists and can be used to detect file removals
    InternalDataFile file23 = getDataFile(23, Collections.emptyList(), checkpointTestPath);
    InternalDataFile file24 = getDataFile(24, Collections.emptyList(), checkpointTestPath);
    InternalSnapshot snapshot12 = buildSnapshot(checkpointTable, "11", file23, file24);

    System.out.println("=== Doing 12th sync (should use checkpoint to remove file21/file22) ===");
    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(checkpointTarget), snapshot12);

    // Validate: Should only have file23 and file24 (file21/file22 should be removed)
    System.out.println("=== Validating: only file23 and file24 should remain ===");
    validateDeltaTable(checkpointTestPath, new HashSet<>(Arrays.asList(file23, file24)));
  }

  @Test
  public void testPrimitiveFieldPartitioning() throws Exception {
    InternalSchema schema = getInternalSchema();
    InternalPartitionField internalPartitionField =
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
    InternalTable table =
        getInternalTable(
            tableName,
            basePath,
            schema,
            Collections.singletonList(internalPartitionField),
            LAST_COMMIT_TIME);

    List<PartitionValue> partitionValues1 =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(internalPartitionField)
                .range(Range.scalar("level"))
                .build());
    List<PartitionValue> partitionValues2 =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(internalPartitionField)
                .range(Range.scalar("warning"))
                .build());
    InternalDataFile dataFile1 = getDataFile(1, partitionValues1, basePath);
    InternalDataFile dataFile2 = getDataFile(2, partitionValues1, basePath);
    InternalDataFile dataFile3 = getDataFile(3, partitionValues2, basePath);

    InternalSnapshot snapshot1 = buildSnapshot(table, "0", dataFile1, dataFile2, dataFile3);

    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot1);

    // Validate all files are present
    validateDeltaTable(basePath, new HashSet<>(Arrays.asList(dataFile1, dataFile2, dataFile3)));

    // Verify partition columns are set
    Table deltaTable = Table.forPath(engine, basePath.toString());
    Snapshot snapshot = deltaTable.getLatestSnapshot(engine);
    SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
    Set<String> partitionColumns = snapshotImpl.getMetadata().getPartitionColNames();
    assertEquals(1, partitionColumns.size());
    assertTrue(partitionColumns.contains("string_field"));
  }

  @Test
  public void testMultipleFieldPartitioning() throws Exception {
    InternalSchema schema = getInternalSchema();
    InternalPartitionField internalPartitionField1 =
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
    InternalPartitionField internalPartitionField2 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("int_field")
                    .schema(InternalSchema.builder().name("int").dataType(InternalType.INT).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    InternalTable table =
        getInternalTable(
            tableName,
            basePath,
            schema,
            Arrays.asList(internalPartitionField1, internalPartitionField2),
            LAST_COMMIT_TIME);

    List<PartitionValue> partitionValues1 =
        Arrays.asList(
            PartitionValue.builder()
                .partitionField(internalPartitionField1)
                .range(Range.scalar("level"))
                .build(),
            PartitionValue.builder()
                .partitionField(internalPartitionField2)
                .range(Range.scalar(10))
                .build());
    List<PartitionValue> partitionValues2 =
        Arrays.asList(
            PartitionValue.builder()
                .partitionField(internalPartitionField1)
                .range(Range.scalar("level"))
                .build(),
            PartitionValue.builder()
                .partitionField(internalPartitionField2)
                .range(Range.scalar(20))
                .build());
    List<PartitionValue> partitionValues3 =
        Arrays.asList(
            PartitionValue.builder()
                .partitionField(internalPartitionField1)
                .range(Range.scalar("warning"))
                .build(),
            PartitionValue.builder()
                .partitionField(internalPartitionField2)
                .range(Range.scalar(20))
                .build());

    InternalDataFile dataFile1 = getDataFile(1, partitionValues1, basePath);
    InternalDataFile dataFile2 = getDataFile(2, partitionValues2, basePath);
    InternalDataFile dataFile3 = getDataFile(3, partitionValues3, basePath);

    InternalSnapshot snapshot1 = buildSnapshot(table, "0", dataFile1, dataFile2, dataFile3);
    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot1);
    validateDeltaTable(basePath, new HashSet<>(Arrays.asList(dataFile1, dataFile2, dataFile3)));

    // Verify partition columns
    Table deltaTable = Table.forPath(engine, basePath.toString());
    Snapshot snapshot = deltaTable.getLatestSnapshot(engine);
    SnapshotImpl snapshotImpl = (SnapshotImpl) snapshot;
    Set<String> partitionColumns = snapshotImpl.getMetadata().getPartitionColNames();
    assertEquals(2, partitionColumns.size());
    assertTrue(partitionColumns.contains("string_field"));
    assertTrue(partitionColumns.contains("int_field"));
  }

  @Test
  @Disabled("Disabled due to tags not present in commitinfo")
  public void testSourceTargetIdMapping() throws Exception {
    InternalSchema baseSchema = getInternalSchema();
    InternalTable sourceTable =
        getInternalTable("source_table", basePath, baseSchema, null, LAST_COMMIT_TIME);

    InternalDataFile sourceDataFile1 = getDataFile(101, Collections.emptyList(), basePath);
    InternalDataFile sourceDataFile2 = getDataFile(102, Collections.emptyList(), basePath);
    InternalDataFile sourceDataFile3 = getDataFile(103, Collections.emptyList(), basePath);

    InternalSnapshot sourceSnapshot1 =
        buildSnapshot(sourceTable, "0", sourceDataFile1, sourceDataFile2);
    InternalSnapshot sourceSnapshot2 =
        buildSnapshot(sourceTable, "1", sourceDataFile2, sourceDataFile3);

    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), sourceSnapshot1);
    Optional<String> mappedTargetId1 =
        conversionTarget.getTargetCommitIdentifier(sourceSnapshot1.getSourceIdentifier());
    validateDeltaTable(basePath, new HashSet<>(Arrays.asList(sourceDataFile1, sourceDataFile2)));
    assertTrue(mappedTargetId1.isPresent());
    assertEquals("0", mappedTargetId1.get());

    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), sourceSnapshot2);
    Optional<String> mappedTargetId2 =
        conversionTarget.getTargetCommitIdentifier(sourceSnapshot2.getSourceIdentifier());
    validateDeltaTable(basePath, new HashSet<>(Arrays.asList(sourceDataFile2, sourceDataFile3)));
    assertTrue(mappedTargetId2.isPresent());
    assertEquals("1", mappedTargetId2.get());

    Optional<String> unmappedTargetId = conversionTarget.getTargetCommitIdentifier("s3");
    assertFalse(unmappedTargetId.isPresent());
  }

  @Test
  public void testGetTargetCommitIdentifierWithNullSourceIdentifier() throws Exception {
    InternalSchema baseSchema = getInternalSchema();
    InternalTable internalTable =
        getInternalTable("source_table", basePath, baseSchema, null, LAST_COMMIT_TIME);
    InternalDataFile sourceDataFile = getDataFile(101, Collections.emptyList(), basePath);
    InternalSnapshot snapshot = buildSnapshot(internalTable, "0", sourceDataFile);

    // Mock the snapshot sync process
    conversionTarget.beginSync(internalTable);
    TableSyncMetadata tableSyncMetadata =
        TableSyncMetadata.of(
            internalTable.getLatestCommitTime(), new ArrayList<>(snapshot.getPendingCommits()));
    conversionTarget.syncMetadata(tableSyncMetadata);
    conversionTarget.syncSchema(internalTable.getReadSchema());
    conversionTarget.syncPartitionSpec(internalTable.getPartitioningFields());
    conversionTarget.syncFilesForSnapshot(snapshot.getPartitionedDataFiles());
    conversionTarget.completeSync();

    // No crash should happen during the process
    Optional<String> unmappedTargetId = conversionTarget.getTargetCommitIdentifier("0");
    // The targetIdentifier is expected to not be found
    assertFalse(unmappedTargetId.isPresent());
  }

  @Test
  public void testGetTableMetadata() throws Exception {
    InternalSchema schema = getInternalSchema();
    InternalTable table = getInternalTable(tableName, basePath, schema, null, LAST_COMMIT_TIME);
    InternalDataFile dataFile = getDataFile(1, Collections.emptyList(), basePath);
    InternalSnapshot snapshot = buildSnapshot(table, "0", dataFile);

    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot);

    Optional<TableSyncMetadata> metadata = conversionTarget.getTableMetadata();
    assertTrue(metadata.isPresent());
    assertNotNull(metadata.get().getLastInstantSynced());
  }

  private void validateDeltaTable(Path basePath, Set<InternalDataFile> expectedFiles)
      throws IOException {
    Table table = Table.forPath(engine, basePath.toString());
    assertNotNull(table);

    Snapshot snapshot = table.getLatestSnapshot(engine);
    assertNotNull(snapshot);

    // Scan all files
    ScanImpl scan = (ScanImpl) snapshot.getScanBuilder().build();
    CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(engine, false);

    Map<String, InternalDataFile> pathToFile =
        expectedFiles.stream()
            .collect(Collectors.toMap(InternalDataFile::getPhysicalPath, Function.identity()));

    int count = 0;
    while (scanFiles.hasNext()) {
      FilteredColumnarBatch batch = scanFiles.next();
      CloseableIterator<Row> rows = batch.getRows();

      while (rows.hasNext()) {
        Row scanFileRow = rows.next();
        AddFile addFile =
            new AddFile(scanFileRow.getStruct(scanFileRow.getSchema().indexOf("add")));

        String fullPath =
            new org.apache.hadoop.fs.Path(basePath.resolve(addFile.getPath()).toUri()).toString();
        InternalDataFile expected = pathToFile.get(fullPath);
        assertNotNull(expected, "Unexpected file in Delta table: " + fullPath);
        assertEquals(addFile.getSize(), expected.getFileSizeBytes());
        count++;
      }
    }

    assertEquals(
        expectedFiles.size(), count, "Number of files from Delta scan don't match expectation");
  }

  private InternalSnapshot buildSnapshot(
      InternalTable table, String sourceIdentifier, InternalDataFile... dataFiles) {
    return InternalSnapshot.builder()
        .table(table)
        .partitionedDataFiles(PartitionFileGroup.fromFiles(Arrays.asList(dataFiles)))
        .sourceIdentifier(sourceIdentifier)
        .build();
  }

  private InternalTable getInternalTable(
      String tableName,
      Path basePath,
      InternalSchema schema,
      List<InternalPartitionField> partitionFields,
      Instant lastCommitTime) {
    return InternalTable.builder()
        .name(tableName)
        .basePath(basePath.toUri().toString())
        .layoutStrategy(DataLayoutStrategy.FLAT)
        .tableFormat(TableFormat.HUDI)
        .readSchema(schema)
        .partitioningFields(partitionFields)
        .latestCommitTime(lastCommitTime)
        .build();
  }

  private InternalDataFile getDataFile(
      int index, List<PartitionValue> partitionValues, Path basePath) {
    // Create actual physical file so Delta Kernel can reference it
    try {
      Path filePath = basePath.resolve("physical" + index + ".parquet");
      Files.createFile(filePath);

      String physicalPath = new org.apache.hadoop.fs.Path(filePath.toUri()).toString();

      return InternalDataFile.builder()
          .fileFormat(FileFormat.APACHE_PARQUET)
          .fileSizeBytes(RANDOM.nextInt(10000))
          .physicalPath(physicalPath)
          .recordCount(RANDOM.nextInt(10000))
          .partitionValues(partitionValues)
          .columnStats(Collections.emptyList())
          .lastModified(Instant.now().toEpochMilli())
          .build();
    } catch (IOException e) {
      throw new RuntimeException("Failed to create test data file", e);
    }
  }

  private InternalSchema getInternalSchema() {
    Map<InternalSchema.MetadataKey, Object> timestampMetadata = new HashMap<>();
    timestampMetadata.put(
        InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MILLIS);
    return InternalSchema.builder()
        .dataType(InternalType.RECORD)
        .name("top_level_schema")
        .fields(
            Arrays.asList(
                InternalField.builder()
                    .name("long_field")
                    .schema(
                        InternalSchema.builder()
                            .name("long")
                            .dataType(InternalType.LONG)
                            .isNullable(true)
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
                            .name("time")
                            .dataType(InternalType.TIMESTAMP)
                            .isNullable(true)
                            .metadata(timestampMetadata)
                            .build())
                    .build()))
        .isNullable(false)
        .build();
  }

  @Test
  public void testTimestampNtz() throws Exception {
    InternalSchema schema1 = getInternalSchemaWithTimestampNtz();
    List<InternalField> fields2 = new ArrayList<>(schema1.getFields());
    fields2.add(
        InternalField.builder()
            .name("float_field")
            .schema(
                InternalSchema.builder()
                    .name("float")
                    .dataType(InternalType.FLOAT)
                    .isNullable(true)
                    .build())
            .build());
    InternalSchema schema2 = getInternalSchema().toBuilder().fields(fields2).build();
    InternalTable table1 = getInternalTable(tableName, basePath, schema1, null, LAST_COMMIT_TIME);
    InternalTable table2 = getInternalTable(tableName, basePath, schema2, null, LAST_COMMIT_TIME);

    InternalDataFile dataFile1 = getDataFile(1, Collections.emptyList(), basePath);
    InternalDataFile dataFile2 = getDataFile(2, Collections.emptyList(), basePath);
    InternalDataFile dataFile3 = getDataFile(3, Collections.emptyList(), basePath);

    InternalSnapshot snapshot1 = buildSnapshot(table1, "0", dataFile1, dataFile2);
    InternalSnapshot snapshot2 = buildSnapshot(table2, "1", dataFile2, dataFile3);

    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot1);
    validateDeltaTable(basePath, new HashSet<>(Arrays.asList(dataFile1, dataFile2)));

    TableFormatSync.getInstance()
        .syncSnapshot(Collections.singletonList(conversionTarget), snapshot2);
    validateDeltaTable(basePath, new HashSet<>(Arrays.asList(dataFile2, dataFile3)));
  }

  private InternalSchema getInternalSchemaWithTimestampNtz() {
    Map<InternalSchema.MetadataKey, Object> timestampMetadata = new HashMap<>();
    timestampMetadata.put(
        InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MICROS);
    List<InternalField> fields = new ArrayList<>(getInternalSchema().getFields());
    fields.add(
        InternalField.builder()
            .name("timestamp_ntz_field")
            .schema(
                InternalSchema.builder()
                    .name("time_ntz")
                    .dataType(InternalType.TIMESTAMP_NTZ)
                    .isNullable(true)
                    .metadata(timestampMetadata)
                    .build())
            .build());
    return getInternalSchema().toBuilder().fields(fields).build();
  }
}
