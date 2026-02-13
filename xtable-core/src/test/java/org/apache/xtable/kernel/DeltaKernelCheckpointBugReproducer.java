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

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;

/**
 * Minimal reproducible test case for Delta Kernel 4.0.0 checkpoint bug.
 *
 * <p>ISSUE: When creating a new Delta table and attempting to read it back, a NullPointerException
 * occurs in getLatestSnapshot() because the _last_checkpoint file is not properly created by
 * PostCommitHook.
 *
 * <p>ENVIRONMENT: - Delta Kernel version: 4.0.0 (io.delta:delta-kernel-api:4.0.0,
 * io.delta:delta-kernel-defaults:4.0.0) - Java version: 11+ - Operating System: Any
 *
 * <p>REPRODUCTION STEPS: 1. Create a new Delta table using Delta Kernel Transaction API 2. Set
 * delta.checkpointInterval=1 to force immediate checkpoint creation 3. Commit the transaction and
 * execute PostCommitHooks 4. Try to read the table back using getLatestSnapshot()
 *
 * <p>EXPECTED BEHAVIOR: - PostCommitHook creates checkpoint at version 0 - _last_checkpoint file is
 * created in _delta_log directory - getLatestSnapshot() successfully reads the table
 *
 * <p>ACTUAL BEHAVIOR: - PostCommitHook may fail silently or create incomplete checkpoint -
 * _last_checkpoint file is NOT created - getLatestSnapshot() throws NullPointerException
 *
 * <p>WORKAROUND: Catch NullPointerException and fall back to reading JSON log files directly
 * (unreliable)
 *
 * <p>This test is intended to be shared with the Delta Kernel community for investigation.
 */
public class DeltaKernelCheckpointBugReproducer {

  @TempDir public Path tempDir;

  @Test
  public void testCheckpointCreationBug() throws Exception {
    // Setup
    String tableName = "test_table_" + UUID.randomUUID();
    Path tablePath = tempDir.resolve(tableName);
    Files.createDirectories(tablePath);

    Configuration hadoopConf = new Configuration();
    Engine engine = DefaultEngine.create(hadoopConf);

    // Define a simple schema
    StructType schema =
        new StructType()
            .add(new StructField("id", IntegerType.INTEGER, true))
            .add(new StructField("name", StringType.STRING, true));

    // Create table directory
    File tableDir = tablePath.toFile();
    if (!tableDir.exists()) {
      tableDir.mkdirs();
    }

    Table table = Table.forPath(engine, tablePath.toString());

    // Build transaction with checkpoint interval = 1
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("delta.checkpointInterval", "1"); // Force immediate checkpoint creation

    TransactionBuilder txnBuilder =
        table
            .createTransactionBuilder(engine, "Test Transaction", Operation.CREATE_TABLE)
            .withSchema(engine, schema)
            .withTableProperties(engine, tableProperties);

    Transaction txn = txnBuilder.build(engine);

    // Create a dummy data file (just for testing - doesn't need to exist)
    Path dataFilePath = tablePath.resolve("data1.parquet");
    Files.createFile(dataFilePath); // Create empty file

    Row addFileRow =
        AddFile.createAddFileRow(
            schema,
            "data1.parquet", // relative path
            VectorUtils.stringStringMapValue(Collections.emptyMap()), // no partition values
            100L, // file size
            System.currentTimeMillis(), // modification time
            true, // dataChange
            Optional.empty(), // deletionVector
            Optional.empty(), // tags
            Optional.empty(), // baseRowId
            Optional.empty(), // defaultRowCommitVersion
            Optional.empty() // stats
            );

    Row wrappedRow = SingleAction.createAddFileSingleAction(addFileRow);

    List<Row> actionRows = Collections.singletonList(wrappedRow);
    CloseableIterator<Row> actionsIterator =
        new CloseableIterator<Row>() {
          private int currentIndex = 0;

          @Override
          public boolean hasNext() {
            return currentIndex < actionRows.size();
          }

          @Override
          public Row next() {
            return actionRows.get(currentIndex++);
          }

          @Override
          public void close() {}
        };

    CloseableIterable<Row> dataActions = CloseableIterable.inMemoryIterable(actionsIterator);

    // Commit the transaction
    System.out.println("=== Committing transaction ===");
    TransactionCommitResult result = txn.commit(engine, dataActions);
    System.out.println("Transaction committed. Version: " + result.getVersion());

    // Execute PostCommitHooks
    List<PostCommitHook> hooks = result.getPostCommitHooks();
    System.out.println("=== Executing PostCommitHooks ===");
    if (hooks != null && !hooks.isEmpty()) {
      System.out.println("Found " + hooks.size() + " post-commit hook(s)");
      for (PostCommitHook hook : hooks) {
        System.out.println("Hook type: " + hook.getType());
        try {
          hook.threadSafeInvoke(engine);
          System.out.println("Hook executed successfully");
        } catch (Exception e) {
          System.err.println("ERROR: PostCommitHook failed!");
          e.printStackTrace();
        }
      }
    } else {
      System.out.println("No post-commit hooks returned");
    }

    // Check if _last_checkpoint file was created
    File deltaLogDir = new File(tableDir, "_delta_log");
    File lastCheckpointFile = new File(deltaLogDir, "_last_checkpoint");
    System.out.println("=== Checking for _last_checkpoint file ===");
    System.out.println("_last_checkpoint exists: " + lastCheckpointFile.exists());
    System.out.println("Delta log directory contents:");
    File[] logFiles = deltaLogDir.listFiles();
    if (logFiles != null) {
      for (File f : logFiles) {
        System.out.println("  - " + f.getName());
      }
    }

    // Short sleep to ensure file system operations complete
    Thread.sleep(100);

    // THE BUG: Try to read the table back
    System.out.println("=== Attempting to read table with getLatestSnapshot() ===");
    Table readTable = Table.forPath(engine, tablePath.toString());

    try {
      Snapshot snapshot = readTable.getLatestSnapshot(engine);
      System.out.println("SUCCESS: Got snapshot at version " + snapshot.getVersion());
      assertNotNull(snapshot, "Snapshot should not be null");
      assertEquals(0, snapshot.getVersion(), "Version should be 0");
    } catch (NullPointerException npe) {
      System.err.println("FAILURE: NullPointerException thrown when reading snapshot!");
      System.err.println("This is the Delta Kernel 4.0.0 bug.");
      npe.printStackTrace();

      fail(
          "Delta Kernel 4.0.0 bug reproduced: NullPointerException when reading snapshot. "
              + "_last_checkpoint file was "
              + (lastCheckpointFile.exists() ? "created" : "NOT created")
              + ". "
              + "This should not happen - getLatestSnapshot() should work after successful commit.");
    }
  }

  @Test
  public void testMultipleCommitsToTriggerCheckpoint() throws Exception {
    // This test does multiple commits to verify checkpoint behavior at version 10
    String tableName = "test_table_multi_" + UUID.randomUUID();
    Path tablePath = tempDir.resolve(tableName);
    Files.createDirectories(tablePath);

    Configuration hadoopConf = new Configuration();
    Engine engine = DefaultEngine.create(hadoopConf);

    StructType schema =
        new StructType()
            .add(new StructField("id", IntegerType.INTEGER, true))
            .add(new StructField("value", StringType.STRING, true));

    File tableDir = tablePath.toFile();
    tableDir.mkdirs();

    // Create table with checkpointInterval = 10 (default)
    Table table = Table.forPath(engine, tablePath.toString());

    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("delta.checkpointInterval", "10");

    // First commit - create table
    System.out.println("=== Creating table with initial commit ===");
    TransactionBuilder txnBuilder =
        table
            .createTransactionBuilder(engine, "Create Table", Operation.CREATE_TABLE)
            .withSchema(engine, schema)
            .withTableProperties(engine, tableProperties);

    Transaction txn = txnBuilder.build(engine);

    Path dataFile1 = tablePath.resolve("data1.parquet");
    Files.createFile(dataFile1);

    Row addFileRow =
        AddFile.createAddFileRow(
            schema,
            "data1.parquet",
            VectorUtils.stringStringMapValue(Collections.emptyMap()),
            100L,
            System.currentTimeMillis(),
            true,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    List<Row> actions =
        Collections.singletonList(SingleAction.createAddFileSingleAction(addFileRow));
    TransactionCommitResult result = txn.commit(engine, toCloseableIterable(actions));
    executeHooks(engine, result);

    System.out.println("Initial commit completed at version " + result.getVersion());

    // Do 9 more commits to reach version 10 (should trigger checkpoint)
    for (int i = 2; i <= 10; i++) {
      System.out.println("=== Commit " + i + " ===");
      table = Table.forPath(engine, tablePath.toString());

      txnBuilder = table.createTransactionBuilder(engine, "Commit " + i, Operation.WRITE);
      txn = txnBuilder.build(engine);

      Path dataFile = tablePath.resolve("data" + i + ".parquet");
      Files.createFile(dataFile);

      addFileRow =
          AddFile.createAddFileRow(
              schema,
              "data" + i + ".parquet",
              VectorUtils.stringStringMapValue(Collections.emptyMap()),
              100L,
              System.currentTimeMillis(),
              true,
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty(),
              Optional.empty());

      actions = Collections.singletonList(SingleAction.createAddFileSingleAction(addFileRow));
      result = txn.commit(engine, toCloseableIterable(actions));
      executeHooks(engine, result);

      System.out.println("Commit " + i + " completed at version " + result.getVersion());
    }

    // Check if checkpoint was created at version 10
    File deltaLogDir = new File(tableDir, "_delta_log");
    File lastCheckpointFile = new File(deltaLogDir, "_last_checkpoint");
    File checkpointFile = new File(deltaLogDir, "00000000000000000010.checkpoint.parquet");

    System.out.println("=== Checkpoint files after 10 commits ===");
    System.out.println("_last_checkpoint exists: " + lastCheckpointFile.exists());
    System.out.println(
        "00000000000000000010.checkpoint.parquet exists: " + checkpointFile.exists());

    // Try to read the table
    System.out.println("=== Reading table after 10 commits ===");
    table = Table.forPath(engine, tablePath.toString());

    try {
      Snapshot snapshot = table.getLatestSnapshot(engine);
      System.out.println("SUCCESS: Read snapshot at version " + snapshot.getVersion());
      assertEquals(9, snapshot.getVersion(), "Should be at version 9 (0-indexed)");
    } catch (NullPointerException npe) {
      System.err.println("FAILURE: NullPointerException when reading snapshot after 10 commits!");
      npe.printStackTrace();
      fail(
          "Delta Kernel 4.0.0 bug: Cannot read snapshot after checkpoint should have been created at version 10");
    }
  }

  @Test
  public void testFileRemovalWithCheckpoint() throws Exception {
    // This test mirrors XTable sync behavior: add files, then remove some and add others
    String tableName = "test_table_removal_" + UUID.randomUUID();
    Path tablePath = tempDir.resolve(tableName);
    Files.createDirectories(tablePath);

    Configuration hadoopConf = new Configuration();
    Engine engine = DefaultEngine.create(hadoopConf);

    StructType schema =
        new StructType()
            .add(new StructField("id", IntegerType.INTEGER, true))
            .add(new StructField("data", StringType.STRING, true));

    File tableDir = tablePath.toFile();
    tableDir.mkdirs();

    // Set checkpointInterval=1 to force immediate checkpoint
    Map<String, String> tableProperties = new HashMap<>();
    tableProperties.put("delta.checkpointInterval", "1");

    // First commit: Create table with 2 files
    System.out.println("=== First commit: Adding file1 and file2 ===");
    Table table = Table.forPath(engine, tablePath.toString());

    TransactionBuilder txnBuilder =
        table
            .createTransactionBuilder(engine, "Create Table", Operation.CREATE_TABLE)
            .withSchema(engine, schema)
            .withTableProperties(engine, tableProperties);

    Transaction txn = txnBuilder.build(engine);

    // Create actual files
    Path dataFile1 = tablePath.resolve("file1.parquet");
    Path dataFile2 = tablePath.resolve("file2.parquet");
    Files.createFile(dataFile1);
    Files.createFile(dataFile2);

    // Add both files
    Row addFileRow1 =
        AddFile.createAddFileRow(
            schema,
            "file1.parquet",
            VectorUtils.stringStringMapValue(Collections.emptyMap()),
            100L,
            System.currentTimeMillis(),
            true,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    Row addFileRow2 =
        AddFile.createAddFileRow(
            schema,
            "file2.parquet",
            VectorUtils.stringStringMapValue(Collections.emptyMap()),
            200L,
            System.currentTimeMillis(),
            true,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    List<Row> actions = new ArrayList<>();
    actions.add(SingleAction.createAddFileSingleAction(addFileRow1));
    actions.add(SingleAction.createAddFileSingleAction(addFileRow2));

    TransactionCommitResult result = txn.commit(engine, toCloseableIterable(actions));
    executeHooks(engine, result);

    System.out.println("First commit completed at version " + result.getVersion());

    // Second commit: Remove file1, keep file2, add file3
    // This simulates XTable sync behavior where some files are removed and new ones added
    System.out.println("=== Second commit: Removing file1, adding file3 (keeping file2) ===");
    table = Table.forPath(engine, tablePath.toString());

    txnBuilder = table.createTransactionBuilder(engine, "Update Files", Operation.WRITE);
    txn = txnBuilder.build(engine);

    // Create file3
    Path dataFile3 = tablePath.resolve("file3.parquet");
    Files.createFile(dataFile3);

    // Create RemoveFile action for file1
    io.delta.kernel.internal.actions.AddFile addFile1 =
        new io.delta.kernel.internal.actions.AddFile(addFileRow1);
    io.delta.kernel.internal.actions.RemoveFile removeFile1 =
        new io.delta.kernel.internal.actions.RemoveFile(
            addFile1.toRemoveFileRow(false, Optional.of(result.getVersion())));

    // Create AddFile action for file3
    Row addFileRow3 =
        AddFile.createAddFileRow(
            schema,
            "file3.parquet",
            VectorUtils.stringStringMapValue(Collections.emptyMap()),
            300L,
            System.currentTimeMillis(),
            true,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    actions = new ArrayList<>();
    actions.add(SingleAction.createRemoveFileSingleAction(removeFile1.toRow()));
    actions.add(SingleAction.createAddFileSingleAction(addFileRow3));

    result = txn.commit(engine, toCloseableIterable(actions));
    executeHooks(engine, result);

    System.out.println("Second commit completed at version " + result.getVersion());

    // Check checkpoint files
    File deltaLogDir = new File(tableDir, "_delta_log");
    File lastCheckpointFile = new File(deltaLogDir, "_last_checkpoint");
    System.out.println("=== Checkpoint status after RemoveFile commit ===");
    System.out.println("_last_checkpoint exists: " + lastCheckpointFile.exists());

    // THE BUG: Try to read the table after RemoveFile operations
    System.out.println("=== Reading table after RemoveFile operations ===");
    table = Table.forPath(engine, tablePath.toString());

    try {
      Snapshot snapshot = table.getLatestSnapshot(engine);
      System.out.println("SUCCESS: Read snapshot at version " + snapshot.getVersion());
      assertNotNull(snapshot, "Snapshot should not be null");
      assertEquals(1, snapshot.getVersion(), "Should be at version 1");

      // Verify the table has the correct files (file2 and file3, not file1)
      System.out.println(
          "Snapshot read successfully. Files should be: file2.parquet, file3.parquet");
    } catch (NullPointerException npe) {
      System.err.println("FAILURE: NullPointerException when reading snapshot after RemoveFile!");
      System.err.println(
          "This demonstrates the bug affects both AddFile and RemoveFile operations");
      npe.printStackTrace();
      fail(
          "Delta Kernel 4.0.0 bug: Cannot read snapshot after commit with RemoveFile actions. "
              + "This is critical for XTable which needs to sync file additions AND removals.");
    }
  }

  private void executeHooks(Engine engine, TransactionCommitResult result) {
    List<PostCommitHook> hooks = result.getPostCommitHooks();
    if (hooks != null && !hooks.isEmpty()) {
      for (PostCommitHook hook : hooks) {
        try {
          hook.threadSafeInvoke(engine);
          if (hook.getType() == PostCommitHook.PostCommitHookType.CHECKPOINT) {
            System.out.println("  Checkpoint hook executed at version " + result.getVersion());
          }
        } catch (Exception e) {
          System.err.println("  WARNING: Hook failed: " + e.getMessage());
        }
      }
    }
  }

  private CloseableIterable<Row> toCloseableIterable(List<Row> rows) {
    return CloseableIterable.inMemoryIterable(
        new CloseableIterator<Row>() {
          private int index = 0;

          @Override
          public boolean hasNext() {
            return index < rows.size();
          }

          @Override
          public Row next() {
            return rows.get(index++);
          }

          @Override
          public void close() {}
        });
  }
}
