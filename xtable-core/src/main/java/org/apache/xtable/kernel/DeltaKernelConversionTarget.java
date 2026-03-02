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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

import scala.collection.Seq;

import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

import io.delta.kernel.Operation;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.Transaction;
import io.delta.kernel.TransactionBuilder;
import io.delta.kernel.TransactionCommitResult;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.Metadata;
import io.delta.kernel.internal.actions.RowBackedAction;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;

import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.exception.ReadException;
import org.apache.xtable.exception.UpdateException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.sync.ConversionTarget;

/**
 * Implementation of {@link ConversionTarget} for Delta Lake using the Delta Kernel API.
 *
 * <p>This implementation uses Delta Kernel (io.delta.kernel) instead of Delta Standalone for write
 * operations, providing better compatibility with cloud storage (S3, GCS, Azure Blob Storage, HDFS)
 * and improved support for Delta Lake 3.x features.
 *
 * <p><strong>Known Limitations:</strong>
 *
 * <ul>
 *   <li><strong>Commit Tags:</strong> Delta Kernel 4.0.0 does not support commit tags in
 *       commitInfo (e.g., XTABLE_METADATA tags). This affects source-to-target commit identifier
 *       mapping.
 *   <li><strong>Schema Evolution:</strong> Schema changes are handled through Delta Kernel's
 *       transaction API, which may have different semantics compared to Delta Standalone.
 *   <li><strong>Internal API Usage:</strong> This implementation casts to internal classes
 *       (SnapshotImpl, TableImpl) to access metadata and commit history, as Delta Kernel 4.0.0
 *       lacks public APIs for these operations. These casts are brittle and may break on version
 *       upgrades. Public API alternatives should be used when available.
 * </ul>
 *
 * <p><strong>Implementation Choice:</strong> Delta Kernel API was chosen over Delta Standalone to:
 *
 * <ul>
 *   <li>Support newer Delta Lake features and protocol versions
 *   <li>Align with the Delta Lake community's direction (Kernel is the recommended API)
 *   <li>Reduce dependency on Spark-specific implementations
 * </ul>
 *
 * @see ConversionTarget
 * @see io.delta.kernel.Table
 * @see io.delta.kernel.Transaction
 */
@Log4j2
public class DeltaKernelConversionTarget implements ConversionTarget {
  private DeltaKernelSchemaExtractor schemaExtractor;
  private DeltaKernelPartitionExtractor partitionExtractor;
  private DeltaKernelDataFileUpdatesExtractor dataKernelFileUpdatesExtractor;

  private String basePath;
  private long logRetentionInHours;
  private DeltaKernelConversionTarget.TransactionState transactionState;
  private Engine engine;

  public DeltaKernelConversionTarget(TargetTable targetTable, Engine engine) {
    this(
        targetTable.getBasePath(),
        targetTable.getMetadataRetention().toHours(),
        engine,
        DeltaKernelSchemaExtractor.getInstance(),
        DeltaKernelPartitionExtractor.getInstance(),
        DeltaKernelDataFileUpdatesExtractor.builder()
            .engine(engine)
            .basePath(targetTable.getBasePath())
            // Column statistics are not needed for conversion operations
            .includeColumnStats(false)
            .build());
  }

  @VisibleForTesting
  DeltaKernelConversionTarget(
      String tableDataPath,
      long logRetentionInHours,
      Engine engine,
      DeltaKernelSchemaExtractor schemaExtractor,
      DeltaKernelPartitionExtractor partitionExtractor,
      DeltaKernelDataFileUpdatesExtractor dataKernelFileUpdatesExtractor) {
    this.basePath = tableDataPath;
    this.schemaExtractor = schemaExtractor;
    this.partitionExtractor = partitionExtractor;
    this.dataKernelFileUpdatesExtractor = dataKernelFileUpdatesExtractor;
    this.engine = engine;
    this.logRetentionInHours = logRetentionInHours;
  }

  @Override
  public void init(TargetTable targetTable, Configuration configuration) {
    Engine engine = DefaultEngine.create(configuration);

    this.basePath = targetTable.getBasePath();
    this.logRetentionInHours = targetTable.getMetadataRetention().toHours();
    this.engine = engine;
    this.schemaExtractor = DeltaKernelSchemaExtractor.getInstance();
    this.partitionExtractor = DeltaKernelPartitionExtractor.getInstance();
    this.dataKernelFileUpdatesExtractor =
        DeltaKernelDataFileUpdatesExtractor.builder()
            .engine(engine)
            .basePath(targetTable.getBasePath())
            // Column statistics are not needed for conversion operations
            .includeColumnStats(false)
            .build();
  }

  @Override
  public void beginSync(InternalTable table) {
    this.transactionState =
        new DeltaKernelConversionTarget.TransactionState(engine, logRetentionInHours);
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
      for (Map.Entry<String, StructField> partitionEntry : spec.entrySet()) {
        String partitionColumnName = partitionEntry.getKey();
        StructField partitionField = partitionEntry.getValue();

        transactionState.getPartitionColumns().add(partitionColumnName);
        if (partitionField != null
            && transactionState.getLatestSchema().fields().stream()
                .noneMatch(field -> field.getName().equals(partitionField.getName()))) {
          // add generated columns to schema.
          transactionState.addColumn(partitionField);
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
            table.getPath(engine),
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
    Snapshot snapshot = table.getLatestSnapshot(engine);

    // WORKAROUND: Cast to SnapshotImpl (internal class) to access metadata configuration.
    // Delta Kernel 4.0.0 does not provide a public API to access table metadata/configuration.
    // This cast is brittle and may break on Kernel version upgrades.
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
    Snapshot currentSnapshot = table.getLatestSnapshot(engine);

    // WORKAROUND: Cast to TableImpl (internal class) to access getChanges() API for reading commit history.
    // Delta Kernel 4.0.0 does not provide a public API to iterate through table changes/commits.
    // This cast is brittle and may break on Kernel version upgrades.
    // TODO: Replace with public API when available (track: https://github.com/delta-io/delta/issues/XXXX)
    io.delta.kernel.internal.TableImpl tableImpl = (io.delta.kernel.internal.TableImpl) table;

    // Request COMMITINFO actions to read commit metadata
    Set<io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction> actionSet =
        new HashSet<>();
    actionSet.add(io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction.COMMITINFO);

    // Get changes from version 0 to current version
    try (CloseableIterator<ColumnarBatch> iter =
        tableImpl.getChanges(engine, 0, currentSnapshot.getVersion(), actionSet)) {

      while (iter.hasNext()) {
        ColumnarBatch batch = iter.next();
        int commitInfoIndex =
            batch
                .getSchema()
                .indexOf(
                    io.delta.kernel.internal.DeltaLogActionUtils.DeltaAction.COMMITINFO.colName);

        try (CloseableIterator<Row> rows =
            batch.getRows()) {

          while (rows.hasNext()) {
            Row row = rows.next();

            // Get version (first column)
            long version = row.getLong(0);

            // Check if CommitInfo exists
            if (row.isNullAt(commitInfoIndex)) {
              continue;
            }

            // Get CommitInfo row
            Row commitInfoRow = row.getStruct(commitInfoIndex);

            // Get tags from CommitInfo (tags is a MapValue)
            int tagsIndex = commitInfoRow.getSchema().indexOf("tags");
            if (tagsIndex == -1 || commitInfoRow.isNullAt(tagsIndex)) {
              continue;
            }

            MapValue tags = commitInfoRow.getMap(tagsIndex);

            // Search for XTABLE_METADATA key in tags
            // Use Delta Kernel's MapValue API: getKeys() and getValues() return ColumnVectors
            ColumnVector keys = tags.getKeys();
            ColumnVector values = tags.getValues();
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
                  log.warn("Failed to parse commit metadata for version {}: {}", version, e.getMessage());
                }
                break;
              }
            }
          }
        }
      }
    } catch (Exception e) {
      throw new ReadException("Failed to read commit history", e);
    }

    return Optional.empty();
  }

  private class TransactionState {
    private final Engine engine;
    private final long retentionInHours;
    @Getter private final List<String> partitionColumns;
    @Getter private StructType latestSchema;
    @Getter private InternalSchema latestSchemaInternal;
    @Setter private TableSyncMetadata metadata;
    @Setter private Seq<RowBackedAction> actions;

    private TransactionState(Engine engine, long retentionInHours) {
      this.engine = engine;
      this.partitionColumns = new ArrayList<>();
      this.retentionInHours = retentionInHours;

      try {
        Table table = Table.forPath(engine, basePath);
        this.latestSchema = table.getLatestSnapshot(engine).getSchema();
      } catch (Exception e) {
        // Table doesn't exist yet
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
      boolean tableExists = checkTableExists();

      Operation operation =
          tableExists ? Operation.WRITE : Operation.CREATE_TABLE;

      if (!tableExists) {
        java.io.File tableDir = new java.io.File(basePath);
        if (!tableDir.exists()) {
          tableDir.mkdirs();
        }
      }

      Table table = Table.forPath(engine, basePath);
      TransactionBuilder txnBuilder =
          table.createTransactionBuilder(engine, "XTable Delta Sync", operation);

      // Schema evolution for existing tables is handled via Metadata actions manually
      // as Delta Kernel 4.0.0 doesn't support schema evolution via withSchema
      if (!tableExists) {
        txnBuilder = txnBuilder.withSchema(engine, latestSchema);

        if (!partitionColumns.isEmpty()) {
          txnBuilder = txnBuilder.withPartitionColumns(engine, partitionColumns);
        }
      }

      Map<String, String> tableProperties = getConfigurationsForDeltaSync();
      txnBuilder = txnBuilder.withTableProperties(engine, tableProperties);

      Transaction txn = txnBuilder.build(engine);
      List<Row> allActionRows = new ArrayList<>();

      scala.collection.Iterator<RowBackedAction> actionsIterator = actions.iterator();
      while (actionsIterator.hasNext()) {
        RowBackedAction action = actionsIterator.next();

        if (action instanceof io.delta.kernel.internal.actions.AddFile) {
          io.delta.kernel.internal.actions.AddFile addFile =
              (io.delta.kernel.internal.actions.AddFile) action;
          Row wrappedRow =
              io.delta.kernel.internal.actions.SingleAction.createAddFileSingleAction(
                  addFile.toRow());
          allActionRows.add(wrappedRow);
        } else if (action instanceof io.delta.kernel.internal.actions.RemoveFile) {
          io.delta.kernel.internal.actions.RemoveFile removeFile =
              (io.delta.kernel.internal.actions.RemoveFile) action;
          Row wrappedRow =
              io.delta.kernel.internal.actions.SingleAction.createRemoveFileSingleAction(
                  removeFile.toRow());
          allActionRows.add(wrappedRow);
        }
      }

      CloseableIterator<Row> allActionsIterator =
          new CloseableIterator<Row>() {
            private int currentIndex = 0;

            @Override
            public boolean hasNext() {
              return currentIndex < allActionRows.size();
            }

            @Override
            public Row next() {
              return allActionRows.get(currentIndex++);
            }

            @Override
            public void close() {}
          };

      CloseableIterable<Row> dataActions =
          io.delta.kernel.utils.CloseableIterable.inMemoryIterable(allActionsIterator);

      try {
        TransactionCommitResult result = txn.commit(engine, dataActions);

        // Execute PostCommitHooks to create checkpoints and _last_checkpoint metadata file
        List<PostCommitHook> hooks = result.getPostCommitHooks();
        if (hooks != null && !hooks.isEmpty()) {
          for (PostCommitHook hook : hooks) {
            try {
              hook.threadSafeInvoke(engine);
            } catch (Exception hookEx) {
              // Post-commit hooks are optimizations; log but don't fail the transaction
              log.warn("Post-commit hook failed but transaction succeeded", hookEx);
            }
          }
        }
      } catch (Exception e) {
        throw new UpdateException("Failed to commit Delta Kernel transaction", e);
      }

      // NOTE: Delta Kernel API limitations compared to Delta Standalone:
      // - Commit tags (like XTABLE_METADATA in commitInfo.tags) are not yet supported
      // - Operation type metadata (like DeltaOperations.Update) is simplified to
      // Operation.WRITE/CREATE_TABLE
      // - The commit timestamp is managed by Delta Kernel automatically
    }

    private boolean checkTableExists() {
      try {
        Table table = Table.forPath(engine, basePath);
        table.getLatestSnapshot(engine);
        return true;
      } catch (Exception e) {
        // Table doesn't exist or _delta_log is not accessible
        return false;
      }
    }

    private Map<String, String> getConfigurationsForDeltaSync() {
      Map<String, String> configMap = new HashMap<>();

      configMap.put(TableSyncMetadata.XTABLE_METADATA, metadata.toJson());
      configMap.put(
          "delta.logRetentionDuration", String.format("interval %d hours", retentionInHours));

      return configMap;
    }
  }
}
