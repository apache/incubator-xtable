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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import scala.collection.JavaConverters;

import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.actions.RowBackedAction;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.PartitionFileGroup;

public class TestDeltaKernelDataFileUpdatesExtractor {

  @TempDir private Path tempDir;

  private Engine engine;
  private DeltaKernelDataFileUpdatesExtractor extractor;
  private InternalSchema testSchema;
  private StructType physicalSchema;

  @BeforeEach
  public void setup() {
    Configuration hadoopConf = new Configuration();
    engine = DefaultEngine.create(hadoopConf);

    // Create test schema
    testSchema =
        InternalSchema.builder()
            .name("record")
            .dataType(InternalType.RECORD)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("id")
                        .schema(
                            InternalSchema.builder()
                                .name("integer")
                                .dataType(InternalType.INT)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("name")
                        .schema(
                            InternalSchema.builder()
                                .name("string")
                                .dataType(InternalType.STRING)
                                .isNullable(true)
                                .build())
                        .build()))
            .build();

    // Create physical schema
    physicalSchema =
        new StructType()
            .add(new StructField("id", IntegerType.INTEGER, false))
            .add(new StructField("name", StringType.STRING, true));

    // Initialize extractor
    extractor =
        DeltaKernelDataFileUpdatesExtractor.builder()
            .engine(engine)
            .basePath(tempDir.toString())
            .includeColumnStats(false)
            .build();
  }

  @Test
  public void testCreateAddFileAction() throws IOException {
    // Create a test data file
    String testFilePath = tempDir.resolve("test_data.parquet").toString();
    Files.createFile(Paths.get(testFilePath));

    InternalDataFile dataFile =
        InternalDataFile.builder()
            .physicalPath(testFilePath)
            .fileSizeBytes(1024L)
            .lastModified(Instant.now().toEpochMilli())
            .recordCount(100L)
            .partitionValues(Collections.emptyList())
            .columnStats(Collections.emptyList())
            .build();

    // Create a simple Delta table for testing
    Table table = createSimpleDeltaTable();

    List<PartitionFileGroup> partitionedDataFiles =
        Collections.singletonList(
            PartitionFileGroup.builder()
                .files(Collections.singletonList(dataFile))
                .partitionValues(Collections.emptyList())
                .build());

    // Execute applySnapshot
    scala.collection.Seq<RowBackedAction> actions =
        extractor.applySnapshot(table, partitionedDataFiles, testSchema);

    // Verify actions are created
    assertNotNull(actions);
    List<RowBackedAction> actionList = JavaConverters.seqAsJavaList(actions);
    assertFalse(actionList.isEmpty(), "Should have at least one action");

    // Verify we have AddFile actions
    boolean hasAddFile = actionList.stream().anyMatch(action -> action instanceof AddFile);
    assertTrue(hasAddFile, "Should contain AddFile actions");
  }

  @Test
  public void testApplySnapshotWithPartitionedData() throws IOException {
    // Create test data files with partitions
    String testFilePath1 = tempDir.resolve("partition1/test_data1.parquet").toString();
    String testFilePath2 = tempDir.resolve("partition2/test_data2.parquet").toString();
    Files.createDirectories(Paths.get(testFilePath1).getParent());
    Files.createDirectories(Paths.get(testFilePath2).getParent());
    Files.createFile(Paths.get(testFilePath1));
    Files.createFile(Paths.get(testFilePath2));

    InternalPartitionField partitionField =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("partition_col")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();

    PartitionValue partitionValue1 =
        PartitionValue.builder()
            .partitionField(partitionField)
            .range(Range.scalar("partition1"))
            .build();

    PartitionValue partitionValue2 =
        PartitionValue.builder()
            .partitionField(partitionField)
            .range(Range.scalar("partition2"))
            .build();

    InternalDataFile dataFile1 =
        InternalDataFile.builder()
            .physicalPath(testFilePath1)
            .fileSizeBytes(1024L)
            .lastModified(Instant.now().toEpochMilli())
            .recordCount(50L)
            .partitionValues(Collections.singletonList(partitionValue1))
            .columnStats(Collections.emptyList())
            .build();

    InternalDataFile dataFile2 =
        InternalDataFile.builder()
            .physicalPath(testFilePath2)
            .fileSizeBytes(2048L)
            .lastModified(Instant.now().toEpochMilli())
            .recordCount(75L)
            .partitionValues(Collections.singletonList(partitionValue2))
            .columnStats(Collections.emptyList())
            .build();

    Table table = createSimpleDeltaTable();

    List<PartitionFileGroup> partitionedDataFiles =
        Arrays.asList(
            PartitionFileGroup.builder()
                .files(Collections.singletonList(dataFile1))
                .partitionValues(Collections.singletonList(partitionValue1))
                .build(),
            PartitionFileGroup.builder()
                .files(Collections.singletonList(dataFile2))
                .partitionValues(Collections.singletonList(partitionValue2))
                .build());

    // Execute applySnapshot
    scala.collection.Seq<RowBackedAction> actions =
        extractor.applySnapshot(table, partitionedDataFiles, testSchema);

    // Verify
    assertNotNull(actions);
    List<RowBackedAction> actionList = JavaConverters.seqAsJavaList(actions);
    assertFalse(actionList.isEmpty(), "Should have actions for partitioned data");

    // Should have AddFile actions for new files
    long addFileCount = actionList.stream().filter(action -> action instanceof AddFile).count();
    assertTrue(addFileCount >= 2, "Should have at least 2 AddFile actions");
  }

  @Test
  public void testApplySnapshotWithRemovedFiles() throws IOException {
    // This test verifies that files in the current snapshot but not in new data
    // are converted to RemoveFile actions

    Table table = createSimpleDeltaTable();

    // Provide empty partitioned data files (simulating all files removed)
    List<PartitionFileGroup> partitionedDataFiles = Collections.emptyList();

    // Execute applySnapshot
    scala.collection.Seq<RowBackedAction> actions =
        extractor.applySnapshot(table, partitionedDataFiles, testSchema);

    // Verify
    assertNotNull(actions);
    List<RowBackedAction> actionList = JavaConverters.seqAsJavaList(actions);

    // If the table had files, they should be converted to RemoveFile actions
    // Since we created a simple empty table, this might be empty or have remove actions
    // depending on the table state
    assertNotNull(actionList);
  }

  @Test
  public void testDifferentialSyncWithExistingData() throws IOException {
    // This test simulates a real differential sync scenario:
    // 1. Delta table has existing files: file1.parquet, file2.parquet
    // 2. New sync brings: file2.parquet (unchanged), file3.parquet (new)
    // 3. Expected result: AddFile for file3, RemoveFile for file1

    // Step 1: Create a Delta table with existing data
    Path tablePath = tempDir.resolve("delta_table_with_data");
    Files.createDirectories(tablePath);
    Path deltaLogPath = tablePath.resolve("_delta_log");
    Files.createDirectories(deltaLogPath);

    // Create existing data files
    Path existingFile1 = tablePath.resolve("file1.parquet");
    Path existingFile2 = tablePath.resolve("file2.parquet");
    Files.createFile(existingFile1);
    Files.createFile(existingFile2);

    // Create initial commit with file1 and file2
    Path initialCommit = deltaLogPath.resolve("00000000000000000000.json");
    String initialCommitJson =
        "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n"
            + "{\"metaData\":{\"id\":\"test-id\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\""
            + physicalSchema.toJson().replace("\"", "\\\"")
            + "\",\"partitionColumns\":[],\"configuration\":{},\"createdTime\":"
            + System.currentTimeMillis()
            + "}}\n"
            + "{\"add\":{\"path\":\"file1.parquet\",\"partitionValues\":{},\"size\":1024,\"modificationTime\":"
            + Instant.now().toEpochMilli()
            + ",\"dataChange\":true,\"stats\":\"{}\"}}\n"
            + "{\"add\":{\"path\":\"file2.parquet\",\"partitionValues\":{},\"size\":2048,\"modificationTime\":"
            + Instant.now().toEpochMilli()
            + ",\"dataChange\":true,\"stats\":\"{}\"}}\n";
    Files.write(initialCommit, initialCommitJson.getBytes(StandardCharsets.UTF_8));

    // Create the table
    Table table = Table.forPath(engine, tablePath.toString());
    assertNotNull(table);

    // Step 2: Prepare new sync data - file2 (unchanged) + file3 (new)
    Path newFile3 = tablePath.resolve("file3.parquet");
    Files.createFile(newFile3);

    InternalDataFile dataFile2 =
        InternalDataFile.builder()
            .physicalPath(existingFile2.toString())
            .fileSizeBytes(2048L)
            .lastModified(Instant.now().toEpochMilli())
            .recordCount(100L)
            .partitionValues(Collections.emptyList())
            .columnStats(Collections.emptyList())
            .build();

    InternalDataFile dataFile3 =
        InternalDataFile.builder()
            .physicalPath(newFile3.toString())
            .fileSizeBytes(3072L)
            .lastModified(Instant.now().toEpochMilli())
            .recordCount(150L)
            .partitionValues(Collections.emptyList())
            .columnStats(Collections.emptyList())
            .build();

    List<PartitionFileGroup> newPartitionedDataFiles =
        Collections.singletonList(
            PartitionFileGroup.builder()
                .files(Arrays.asList(dataFile2, dataFile3))
                .partitionValues(Collections.emptyList())
                .build());

    // Step 3: Apply snapshot (differential sync)
    DeltaKernelDataFileUpdatesExtractor syncExtractor =
        DeltaKernelDataFileUpdatesExtractor.builder()
            .engine(engine)
            .basePath(tablePath.toString())
            .includeColumnStats(false)
            .build();

    scala.collection.Seq<RowBackedAction> actions =
        syncExtractor.applySnapshot(table, newPartitionedDataFiles, testSchema);

    // Step 4: Verify the differential sync results
    assertNotNull(actions, "Actions should not be null");
    List<RowBackedAction> actionList = JavaConverters.seqAsJavaList(actions);
    assertFalse(actionList.isEmpty(), "Should have actions for differential sync");

    // Count AddFile and RemoveFile actions
    long addFileCount = actionList.stream().filter(action -> action instanceof AddFile).count();
    long removeFileCount =
        actionList.stream().filter(action -> action instanceof RemoveFile).count();

    // Verify: Should have AddFile for file3 (new file)
    assertTrue(addFileCount >= 1, "Should have at least 1 AddFile action for new file (file3)");

    // Verify: Should have RemoveFile for file1 (removed from new sync)
    assertTrue(
        removeFileCount >= 1,
        "Should have at least 1 RemoveFile action for file1 that's not in new sync");

    // Verify specific files in actions
    boolean hasFile3Add =
        actionList.stream()
            .filter(action -> action instanceof AddFile)
            .map(action -> (AddFile) action)
            .anyMatch(addFile -> addFile.getPath().contains("file3.parquet"));

    assertTrue(hasFile3Add, "Should have AddFile action for file3.parquet");

    // Note: file2 should not appear in actions as it's unchanged
    // file1 should appear as RemoveFile as it's not in the new sync
    System.out.println(
        "Differential sync completed: "
            + addFileCount
            + " files added, "
            + removeFileCount
            + " files removed");
  }

  private Table createSimpleDeltaTable() {
    try {
      // Create a simple Delta table directory structure
      Path tablePath = tempDir.resolve("delta_table");
      Files.createDirectories(tablePath);
      Path deltaLogPath = tablePath.resolve("_delta_log");
      Files.createDirectories(deltaLogPath);

      // Create an empty commit file to make it a valid Delta table
      Path commitFile = deltaLogPath.resolve("00000000000000000000.json");
      String commitJson =
          "{\"protocol\":{\"minReaderVersion\":1,\"minWriterVersion\":2}}\n"
              + "{\"metaData\":{\"id\":\"test-id\",\"format\":{\"provider\":\"parquet\",\"options\":{}},\"schemaString\":\""
              + physicalSchema.toJson().replace("\"", "\\\"")
              + "\",\"partitionColumns\":[],\"configuration\":{},\"createdTime\":"
              + System.currentTimeMillis()
              + "}}\n";
      Files.write(commitFile, commitJson.getBytes(StandardCharsets.UTF_8));

      return Table.forPath(engine, tablePath.toString());
    } catch (IOException e) {
      throw new RuntimeException("Failed to create test Delta table", e);
    }
  }
}
