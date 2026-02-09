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

import java.time.Instant;
import java.util.*;

import lombok.Getter;
import lombok.Setter;

import scala.collection.Seq;

import com.google.common.annotations.VisibleForTesting;

import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.RowBackedAction;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.sync.ConversionTarget;

public class DeltaKernelConversionTarget implements ConversionTarget {
  private static final int MIN_READER_VERSION = 1;
  // gets access to generated columns.
  private static final int MIN_WRITER_VERSION = 4;

  private DeltaKernelSchemaExtractor schemaExtractor;
  private DeltaKernelPartitionExtractor partitionExtractor;
  private DeltaKernelDataFileUpdatesExtractor dataKernelFileUpdatesExtractor;

  private String tableName;
  private String basePath;
  private long logRetentionInHours;
  private DeltaKernelConversionTarget.TransactionState transactionState;
  private Engine engine;

  public DeltaKernelConversionTarget() {}

  public DeltaKernelConversionTarget(TargetTable targetTable, Engine engine) {
    this(
        targetTable.getBasePath(),
        targetTable.getName(),
        targetTable.getMetadataRetention().toHours(),
        engine,
        DeltaKernelSchemaExtractor.getInstance(),
        DeltaKernelPartitionExtractor.getInstance(),
        DeltaKernelDataFileUpdatesExtractor.builder().build());
  }

  @VisibleForTesting
  DeltaKernelConversionTarget(
      String tableDataPath,
      String tableName,
      long logRetentionInHours,
      Engine engine,
      DeltaKernelSchemaExtractor schemaExtractor,
      DeltaKernelPartitionExtractor partitionExtractor,
      DeltaKernelDataFileUpdatesExtractor dataKernelFileUpdatesExtractor) {

    _init(
        tableDataPath,
        tableName,
        logRetentionInHours,
        engine,
        schemaExtractor,
        partitionExtractor,
        dataKernelFileUpdatesExtractor);
  }

  private void _init(
      String tableDataPath,
      String tableName,
      long logRetentionInHours,
      Engine engine,
      DeltaKernelSchemaExtractor schemaExtractor,
      DeltaKernelPartitionExtractor partitionExtractor,
      DeltaKernelDataFileUpdatesExtractor dataFileUpdatesExtractor) {
    this.basePath = tableDataPath;
    Table table = Table.forPath(engine, this.basePath);
    this.schemaExtractor = schemaExtractor;
    this.partitionExtractor = partitionExtractor;
    this.dataKernelFileUpdatesExtractor = dataFileUpdatesExtractor;
    this.engine = engine;
    this.tableName = tableName;
    this.logRetentionInHours = logRetentionInHours;
  }

  @Override
  public void init(TargetTable targetTable, org.apache.hadoop.conf.Configuration configuration) {
    // Create Delta Kernel Engine from Hadoop Configuration
    Engine engine = io.delta.kernel.defaults.engine.DefaultEngine.create(configuration);

    // Initialize with the engine and target table
    _init(
        targetTable.getBasePath(),
        targetTable.getName(),
        targetTable.getMetadataRetention().toHours(),
        engine,
        DeltaKernelSchemaExtractor.getInstance(),
        DeltaKernelPartitionExtractor.getInstance(),
        DeltaKernelDataFileUpdatesExtractor.builder()
            .engine(engine)
            .basePath(targetTable.getBasePath())
            .includeColumnStats(false)
            .build());
  }

  @Override
  public void beginSync(InternalTable table) {
    this.transactionState =
        new DeltaKernelConversionTarget.TransactionState(
            engine, tableName, table.getLatestCommitTime(), logRetentionInHours);
  }

  @Override
  public void syncSchema(InternalSchema schema) {
    transactionState.setLatestSchema(schema);
  }

  @Override
  public void syncPartitionSpec(List<InternalPartitionField> partitionSpec) {
    if (partitionSpec != null) {
      Map<String, StructField> spec =
          partitionExtractor.convertToDeltaPartitionFormat(partitionSpec);
      for (Map.Entry<String, StructField> e : spec.entrySet()) {
        transactionState.getPartitionColumns().add(e.getKey());
        if (e.getValue() != null
            && transactionState.getLatestSchema().fields().stream()
                .noneMatch(field -> field.getName().equals(e.getValue().getName()))) {
          // add generated columns to schema.
          transactionState.addColumn(e.getValue());
        }
      }
    }
  }

  @Override
  public void syncMetadata(TableSyncMetadata metadata) {
    transactionState.setMetadata(metadata);
  }

  @Override
  public void syncFilesForSnapshot(List<PartitionFileGroup> partitionedDataFiles) {
    Table table = Table.forPath(engine, basePath);
    transactionState.setActions(
        dataKernelFileUpdatesExtractor.applySnapshot(
            table, partitionedDataFiles, transactionState.getLatestSchemaInternal()));
  }

  @Override
  public void syncFilesForDiff(InternalFilesDiff internalFilesDiff) {
    Table table = Table.forPath(engine, basePath);
    transactionState.setActions(
        dataKernelFileUpdatesExtractor.applyDiff(
            internalFilesDiff,
            transactionState.getLatestSchemaInternal(),
            table.getPath(engine).toString(),
            table.getLatestSnapshot(engine).getSchema()));
  }

  @Override
  public void completeSync() {
    transactionState.commitTransaction();
    transactionState = null;
  }

  @Override
  public Optional<TableSyncMetadata> getTableMetadata() {
    Table table = Table.forPath(engine, basePath);
    io.delta.kernel.Snapshot snapshot = table.getLatestSnapshot(engine);

    // Cast to SnapshotImpl to access internal getMetadata() method
    Metadata metadata = ((SnapshotImpl) snapshot).getMetadata();

    // Get configuration from metadata
    Map<String, String> configuration = metadata.getConfiguration();
    String metadataJson = configuration.get(TableSyncMetadata.XTABLE_METADATA);

    return TableSyncMetadata.fromJson(metadataJson);
  }

  @Override
  public String getTableFormat() {
    return TableFormat.DELTA;
  }

  @Override
  public Optional<String> getTargetCommitIdentifier(String sourceIdentifier) {
    Table table = Table.forPath(engine, basePath);
    io.delta.kernel.Snapshot currentSnapshot = table.getLatestSnapshot(engine);

    // Cast to TableImpl to access getChanges API
    io.delta.kernel.internal.TableImpl tableImpl = (io.delta.kernel.internal.TableImpl) table;

    // Request COMMITINFO actions to read commit metadata
    java.util.Set<io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction> actionSet =
        new java.util.HashSet<>();
    actionSet.add(io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction.COMMITINFO);

    // Get changes from version 0 to current version
    try (io.delta.kernel.utils.CloseableIterator<io.delta.kernel.data.ColumnarBatch> iter =
        tableImpl.getChanges(engine, 0, currentSnapshot.getVersion(), actionSet)) {

      while (iter.hasNext()) {
        io.delta.kernel.data.ColumnarBatch batch = iter.next();
        int commitInfoIndex =
            batch
                .getSchema()
                .indexOf(
                    io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction.COMMITINFO.colName);

        try (io.delta.kernel.utils.CloseableIterator<io.delta.kernel.data.Row> rows =
            batch.getRows()) {

          while (rows.hasNext()) {
            io.delta.kernel.data.Row row = rows.next();

            // Get version (first column)
            long version = row.getLong(0);

            // Check if CommitInfo exists
            if (row.isNullAt(commitInfoIndex)) {
              continue;
            }

            // Get CommitInfo row
            io.delta.kernel.data.Row commitInfoRow = row.getStruct(commitInfoIndex);

            // Get tags from CommitInfo (tags is a MapValue)
            int tagsIndex = commitInfoRow.getSchema().indexOf("tags");
            if (tagsIndex == -1 || commitInfoRow.isNullAt(tagsIndex)) {
              continue;
            }

            io.delta.kernel.data.MapValue tags = commitInfoRow.getMap(tagsIndex);

            // Search for XTABLE_METADATA key in tags
            // Use Delta Kernel's MapValue API: getKeys() and getValues() return ColumnVectors
            io.delta.kernel.data.ColumnVector keys = tags.getKeys();
            io.delta.kernel.data.ColumnVector values = tags.getValues();
            int tagSize = tags.getSize();
            for (int i = 0; i < tagSize; i++) {
              String key = keys.getString(i);

              if (TableSyncMetadata.XTABLE_METADATA.equals(key)) {
                String metadataJson = values.getString(i);

                // Parse metadata and check source identifier
                try {
                  Optional<TableSyncMetadata> optionalMetadata =
                      TableSyncMetadata.fromJson(metadataJson);

                  if (optionalMetadata.isPresent()) {
                    TableSyncMetadata metadata = optionalMetadata.get();
                    if (sourceIdentifier.equals(metadata.getSourceIdentifier())) {
                      return Optional.of(String.valueOf(version));
                    }
                  }
                } catch (Exception e) {
                  // Log and continue to next commit
                  System.err.println(
                      "Failed to parse commit metadata for version "
                          + version
                          + ": "
                          + e.getMessage());
                }
                break;
              }
            }
          }
        }
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to read commit history", e);
    }

    return Optional.empty();
  }

  private class TransactionState {
    private final Instant commitTime;
    private final Engine engine;
    private final long retentionInHours;
    @Getter private final List<String> partitionColumns;
    private final String tableName;
    @Getter private StructType latestSchema;
    @Getter private InternalSchema latestSchemaInternal;
    @Setter private TableSyncMetadata metadata;
    @Setter private Seq<RowBackedAction> actions;

    private TransactionState(
        Engine engine, String tableName, Instant latestCommitTime, long retentionInHours) {
      this.engine = engine;
      this.commitTime = latestCommitTime;
      this.partitionColumns = new ArrayList<>();
      this.tableName = tableName;
      this.retentionInHours = retentionInHours;

      // Check if table exists to get current schema
      if (checkTableExists()) {
        Table table = Table.forPath(engine, basePath);
        this.latestSchema = table.getLatestSnapshot(engine).getSchema();
      } else {
        // For new tables, schema will be set by syncSchema()
        this.latestSchema = null;
      }
    }

    private void addColumn(StructField field) {
      latestSchema = latestSchema.add(field);
      latestSchemaInternal = schemaExtractor.toInternalSchema(latestSchema);
    }

    private void setLatestSchema(InternalSchema schema) {
      this.latestSchemaInternal = schema;
      this.latestSchema = schemaExtractor.fromInternalSchema(schema);
    }

    private void commitTransaction() {
      // Check if table exists
      boolean tableExists = checkTableExists();

      Table table;
      io.delta.kernel.Operation operation;

      if (!tableExists) {
        // For new tables, use CREATE_TABLE operation
        operation = io.delta.kernel.Operation.CREATE_TABLE;
        // Create table directory structure
        java.io.File tableDir = new java.io.File(basePath);
        if (!tableDir.exists()) {
          tableDir.mkdirs();
        }
        table = Table.forPath(engine, basePath);
      } else {
        // For existing tables, use WRITE operation
        operation = io.delta.kernel.Operation.WRITE;
        table = Table.forPath(engine, basePath);
      }

      // Build transaction with schema, partition columns, and table properties
      io.delta.kernel.TransactionBuilder txnBuilder =
          table.createTransactionBuilder(engine, "XTable Delta Sync", operation);

      // Set schema and partition columns only for new tables
      // For existing tables, schema evolution is handled by adding Metadata actions manually
      // (Delta Kernel 4.0.0 doesn't support schema evolution via withSchema)
      if (!tableExists) {
        txnBuilder = txnBuilder.withSchema(engine, latestSchema);

        if (!partitionColumns.isEmpty()) {
          txnBuilder = txnBuilder.withPartitionColumns(engine, partitionColumns);
        }
      }

      // Set table properties (configuration)
      Map<String, String> tableProperties = getConfigurationsForDeltaSync(tableExists);
      txnBuilder = txnBuilder.withTableProperties(engine, tableProperties);

      // Build the transaction
      io.delta.kernel.Transaction txn = txnBuilder.build(engine);

      // Get transaction state
      io.delta.kernel.data.Row transactionState = txn.getTransactionState(engine);

      // Convert actions to Row format
      // Note: We don't use generateAppendActions here because our AddFile actions
      // already have partition values embedded. generateAppendActions would require
      // us to provide partition values via DataWriteContext, which doesn't work well
      // when different files have different partition values.
      List<io.delta.kernel.data.Row> allActionRows = new ArrayList<>();

      // Check if schema has changed for existing tables - if so, add Metadata action
      if (tableExists) {
        io.delta.kernel.Snapshot currentSnapshot = table.getLatestSnapshot(engine);
        io.delta.kernel.types.StructType currentSchema = currentSnapshot.getSchema();

        // Compare schemas by comparing field names and types
        // Schema changed if: different number of fields OR any field differs
        boolean schemaChanged = (currentSchema.fields().size() != latestSchema.fields().size());

        if (!schemaChanged) {
          // Same number of fields - check if any field differs
          // Create maps for easier comparison
          java.util.Map<String, StructField> currentFieldsMap = new java.util.HashMap<>();
          for (StructField field : currentSchema.fields()) {
            currentFieldsMap.put(field.getName(), field);
          }

          for (StructField newField : latestSchema.fields()) {
            StructField currentField = currentFieldsMap.get(newField.getName());
            if (currentField == null
                || !currentField.getDataType().equivalent(newField.getDataType())) {
              schemaChanged = true;
              break;
            }
          }
        }

        if (schemaChanged) {
          // Get current metadata and create new one with updated schema
          io.delta.kernel.internal.SnapshotImpl snapshotImpl =
              (io.delta.kernel.internal.SnapshotImpl) currentSnapshot;
          io.delta.kernel.internal.actions.Metadata currentMetadata = snapshotImpl.getMetadata();
          io.delta.kernel.internal.actions.Metadata newMetadata =
              currentMetadata.withNewSchema(latestSchema);

          // Add metadata action to the BEGINNING of the actions list
          // Metadata actions should come first in Delta log entries
          io.delta.kernel.data.Row metadataRow =
              io.delta.kernel.internal.actions.SingleAction.createMetadataSingleAction(
                  newMetadata.toRow());
          allActionRows.add(0, metadataRow);
        }
      }

      scala.collection.Iterator<RowBackedAction> actionsIterator = actions.iterator();
      while (actionsIterator.hasNext()) {
        RowBackedAction action = actionsIterator.next();

        if (action instanceof io.delta.kernel.internal.actions.AddFile) {
          // AddFile actions already have partition values - wrap in SingleAction format
          io.delta.kernel.internal.actions.AddFile addFile =
              (io.delta.kernel.internal.actions.AddFile) action;
          io.delta.kernel.data.Row wrappedRow =
              io.delta.kernel.internal.actions.SingleAction.createAddFileSingleAction(
                  addFile.toRow());
          allActionRows.add(wrappedRow);
        } else if (action instanceof io.delta.kernel.internal.actions.RemoveFile) {
          // RemoveFile actions - wrap in SingleAction format
          io.delta.kernel.internal.actions.RemoveFile removeFile =
              (io.delta.kernel.internal.actions.RemoveFile) action;
          io.delta.kernel.data.Row wrappedRow =
              io.delta.kernel.internal.actions.SingleAction.createRemoveFileSingleAction(
                  removeFile.toRow());
          allActionRows.add(wrappedRow);
        }
      }

      // Create iterable for commit
      io.delta.kernel.utils.CloseableIterator<io.delta.kernel.data.Row> allActionsIterator =
          new io.delta.kernel.utils.CloseableIterator<io.delta.kernel.data.Row>() {
            private int currentIndex = 0;

            @Override
            public boolean hasNext() {
              return currentIndex < allActionRows.size();
            }

            @Override
            public io.delta.kernel.data.Row next() {
              return allActionRows.get(currentIndex++);
            }

            @Override
            public void close() {
              // No resources to close
            }
          };

      // Commit the transaction with properly formatted actions (both AddFile and RemoveFile)
      io.delta.kernel.utils.CloseableIterable<io.delta.kernel.data.Row> dataActions =
          io.delta.kernel.utils.CloseableIterable.inMemoryIterable(allActionsIterator);

      try {
        io.delta.kernel.TransactionCommitResult result = txn.commit(engine, dataActions);
        System.out.println("Transaction committed successfully. Version: " + result.getVersion());

        // Execute PostCommitHooks (the correct way to create checkpoints in Delta Kernel)
        // This properly creates both the checkpoint file AND the _last_checkpoint metadata file
        // Reference: Delta Kernel examples (CreateTableAndInsertData.java)
        java.util.List<io.delta.kernel.hook.PostCommitHook> hooks = result.getPostCommitHooks();
        if (hooks != null && !hooks.isEmpty()) {
          System.out.println("Executing " + hooks.size() + " post-commit hooks");
          for (io.delta.kernel.hook.PostCommitHook hook : hooks) {
            System.out.println("Hook type: " + hook.getType());
            try {
              System.out.println("Invoking hook...");
              hook.threadSafeInvoke(engine);
              System.out.println("Hook invoked successfully");
              if (hook.getType()
                  == io.delta.kernel.hook.PostCommitHook.PostCommitHookType.CHECKPOINT) {
                System.out.println(
                    "Checkpoint created via PostCommitHook at version " + result.getVersion());
              }
            } catch (java.io.IOException hookEx) {
              // Log but don't fail - post-commit hooks are optimizations
              System.err.println("Warning: PostCommitHook failed: " + hookEx.getMessage());
              hookEx.printStackTrace();
            } catch (Exception hookEx) {
              System.err.println(
                  "Warning: PostCommitHook failed with unexpected exception: "
                      + hookEx.getMessage());
              hookEx.printStackTrace();
            }
          }
        } else {
          System.out.println("No post-commit hooks returned (checkpoint not needed yet)");
        }

        // Verify table was created
        boolean exists = checkTableExists();
        System.out.println("Delta log exists after commit: " + exists);
        if (!exists) {
          System.err.println("WARNING: Delta log not found at basePath: " + basePath);
          // Try to find where it was actually created
          String tablePath = table.getPath(engine).toString();
          System.err.println("Table path from Delta Kernel: " + tablePath);
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(
            "Failed to commit Delta Kernel transaction: " + e.getMessage(), e);
      }

      // NOTE: Delta Kernel API limitations compared to Delta Standalone:
      // - Commit tags (like XTABLE_METADATA in commitInfo.tags) are not yet supported
      // - Operation type metadata (like DeltaOperations.Update) is simplified to
      // Operation.WRITE/CREATE_TABLE
      // - The commit timestamp is managed by Delta Kernel automatically
    }

    private boolean checkTableExists() {
      try {
        // Handle both regular paths and file:// URIs
        java.io.File tableDir;
        if (basePath.startsWith("file:")) {
          tableDir = new java.io.File(java.net.URI.create(basePath));
        } else {
          tableDir = new java.io.File(basePath);
        }
        java.io.File deltaLogDir = new java.io.File(tableDir, "_delta_log");
        boolean exists = deltaLogDir.exists() && deltaLogDir.isDirectory();
        return exists;
      } catch (Exception e) {
        return false;
      }
    }

    private Map<String, String> getConfigurationsForDeltaSync(boolean tableExists) {
      Map<String, String> configMap = new HashMap<>();

      // NOTE: Protocol versions (minReaderVersion, minWriterVersion) cannot be set via
      // table properties in Delta Kernel. They are managed by the Transaction API based
      // on the features used (e.g., partition columns, generated columns).

      // Store XTable metadata in table configuration
      configMap.put(TableSyncMetadata.XTABLE_METADATA, metadata.toJson());

      // Sets retention for the Delta Log
      // Note: Delta Kernel may not support all Delta Lake configuration keys yet
      configMap.put(
          "delta.logRetentionDuration", String.format("interval %d hours", retentionInHours));

      // Force checkpoint creation on every commit to ensure _last_checkpoint
      // file is created immediately, preventing NullPointerException when reading
      // new Delta tables with Delta Kernel 4.0.0
      configMap.put("delta.checkpointInterval", "1");

      return configMap;
    }

    private String getFileFormat() {
      if (actions.iterator().hasNext()) {
        // Set file format based on action
        RowBackedAction action = actions.iterator().next();
        String path = null;

        if (action instanceof io.delta.kernel.internal.actions.AddFile) {
          path = ((io.delta.kernel.internal.actions.AddFile) action).getPath();
        } else if (action instanceof io.delta.kernel.internal.actions.RemoveFile) {
          path = ((io.delta.kernel.internal.actions.RemoveFile) action).getPath();
        }

        if (path != null) {
          if (path.contains(".parquet")) {
            return "parquet";
          } else if (path.contains(".orc")) {
            return "orc";
          }
          throw new NotSupportedException("File format is not supported for delta sync");
        }
      }

      // Fallback to existing table metadata
      Table table = Table.forPath(engine, basePath);
      io.delta.kernel.Snapshot snapshot = table.getLatestSnapshot(engine);
      io.delta.kernel.internal.SnapshotImpl snapshotImpl =
          (io.delta.kernel.internal.SnapshotImpl) snapshot;
      io.delta.kernel.internal.actions.Metadata metadata = snapshotImpl.getMetadata();

      // Return format provider name from metadata
      return metadata.getFormat().getProvider();
    }

    private Map<String, String> getCommitTags() {
      return Collections.singletonMap(TableSyncMetadata.XTABLE_METADATA, metadata.toJson());
    }
  }
}
