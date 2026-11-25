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
 
package org.apache.xtable.iceberg;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.iceberg.ExpireSnapshots;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.CleanableFailure;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.MappedFields;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.types.Types;

import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.sync.ConversionTarget;

@Log4j2
public class IcebergConversionTarget implements ConversionTarget {
  private static final String METADATA_DIR_PATH = "/metadata/";
  private static final String DELETION_VECTOR_FILE_EXTENSION = ".puffin";
  private IcebergSchemaExtractor schemaExtractor;
  private IcebergSchemaSync schemaSync;
  private IcebergPartitionSpecExtractor partitionSpecExtractor;
  private IcebergPartitionSpecSync partitionSpecSync;
  private IcebergDataFileUpdatesSync dataFileUpdatesExtractor;
  private IcebergTableManager tableManager;
  private String basePath;
  private TableIdentifier tableIdentifier;
  private IcebergCatalogConfig catalogConfig;
  private Configuration configuration;
  private Duration metadataRetention;
  private Transaction transaction;
  private Table table;
  private InternalTable internalTableState;
  private TableSyncMetadata tableSyncMetadata;
  /** Null when the next files diff carries no deletion vectors and is applied as an overwrite. */
  private Map<String, ? extends Collection<Long>> stagedDeletedPositions;

  private Collection<List<PartitionValue>> stagedPartitionsOfDataFiles;

  private boolean stagedPositionsReplaceDeletionVectors;

  /** Deletion vector files the staged row delta wrote, which only its commit makes referenced. */
  private Set<String> uncommittedDeletionVectorFiles = Collections.emptySet();

  private boolean stagedDeleteOperation;

  /** Null when every data file the next files diff references has to be in the table. */
  private Predicate<String> stagedDataFilesWithoutLiveRows;

  private boolean stagedRewrite;

  public IcebergConversionTarget() {}

  IcebergConversionTarget(
      TargetTable targetTable,
      Configuration configuration,
      IcebergSchemaExtractor schemaExtractor,
      IcebergSchemaSync schemaSync,
      IcebergPartitionSpecExtractor partitionSpecExtractor,
      IcebergPartitionSpecSync partitionSpecSync,
      IcebergDataFileUpdatesSync dataFileUpdatesExtractor,
      IcebergTableManager tableManager) {
    _init(
        targetTable,
        configuration,
        schemaExtractor,
        schemaSync,
        partitionSpecExtractor,
        partitionSpecSync,
        dataFileUpdatesExtractor,
        tableManager);
  }

  private void _init(
      TargetTable targetTable,
      Configuration configuration,
      IcebergSchemaExtractor schemaExtractor,
      IcebergSchemaSync schemaSync,
      IcebergPartitionSpecExtractor partitionSpecExtractor,
      IcebergPartitionSpecSync partitionSpecSync,
      IcebergDataFileUpdatesSync dataFileUpdatesExtractor,
      IcebergTableManager tableManager) {
    this.schemaExtractor = schemaExtractor;
    this.schemaSync = schemaSync;
    this.partitionSpecExtractor = partitionSpecExtractor;
    this.partitionSpecSync = partitionSpecSync;
    this.dataFileUpdatesExtractor = dataFileUpdatesExtractor;
    String tableName = targetTable.getName();
    this.basePath = targetTable.getBasePath();
    this.configuration = configuration;
    this.metadataRetention = targetTable.getMetadataRetention();
    String[] namespace = targetTable.getNamespace();
    this.tableIdentifier =
        namespace == null
            ? TableIdentifier.of(tableName)
            : TableIdentifier.of(Namespace.of(namespace), tableName);
    this.tableManager = tableManager;
    this.catalogConfig = (IcebergCatalogConfig) targetTable.getCatalogConfig();

    if (tableManager.tableExists(catalogConfig, tableIdentifier, basePath)) {
      // Load the table state if it already exists
      this.table = tableManager.getTable(catalogConfig, tableIdentifier, basePath);
    }
    // Clear any corrupted state before using the conversion target
    rollbackCorruptCommits();
  }

  @Override
  public void init(TargetTable targetTable, Configuration configuration) {
    _init(
        targetTable,
        configuration,
        IcebergSchemaExtractor.getInstance(),
        IcebergSchemaSync.getInstance(),
        IcebergPartitionSpecExtractor.getInstance(),
        IcebergPartitionSpecSync.getInstance(),
        IcebergDataFileUpdatesSync.of(
            IcebergColumnStatsConverter.getInstance(),
            IcebergPartitionValueConverter.getInstance()),
        IcebergTableManager.of(configuration));
  }

  @Override
  public void beginSync(InternalTable internalTable) {
    initializeTableIfRequired(internalTable);
    transaction = table.newTransaction();
    internalTableState = internalTable;
  }

  private void initializeTableIfRequired(InternalTable internalTable) {
    if (table == null) {
      table =
          tableManager.getOrCreateTable(
              catalogConfig,
              tableIdentifier,
              basePath,
              schemaExtractor.toIceberg(internalTable.getReadSchema()),
              partitionSpecExtractor.toIceberg(
                  internalTable.getPartitioningFields(),
                  schemaExtractor.toIceberg(internalTable.getReadSchema())));
    }
  }

  /**
   * Create a name mapping from the given schema, use storage names in {@link
   * IcebergSchemaExtractor#getIdToStorageName()}, if any exist, in place of field names, and apply
   * the updated mapping to the table.
   *
   * <p>This method should only be called when the name mapping is not set, or when field IDs are
   * provided in the source schema
   *
   * @param schema the {@link Schema} from which to create the name mapping
   */
  private void createAndSetNameMapping(Schema schema) {
    NameMapping mapping = MappingUtil.create(schema);
    MappedFields updatedMappedFields =
        updateNameMapping(mapping.asMappedFields(), schemaExtractor.getIdToStorageName());
    setNameMapping(withRowLineage(NameMapping.of(updatedMappedFields), schema));
  }

  private void setNameMapping(NameMapping mapping) {
    transaction
        .updateProperties()
        .set(TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(mapping))
        .commit();
  }

  /**
   * Adds the row lineage columns of a format version 3 table to the name mapping under their
   * reserved field ids, so that a data file without field ids that stores them, as a rewrite of a
   * data file does, is read with the stored values rather than with values derived from the file's
   * first row id and sequence number. Iceberg applies a name mapping only to a file without any
   * field ids.
   *
   * @return the given mapping when it needs no change
   */
  private NameMapping withRowLineage(NameMapping mapping, Schema schema) {
    if (TableUtil.formatVersion(table) < 3) {
      return mapping;
    }
    List<MappedField> fields = new ArrayList<>(mapping.asMappedFields().fields());
    for (Types.NestedField column :
        Arrays.asList(MetadataColumns.ROW_ID, MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER)) {
      if (mapping.find(column.fieldId()) == null
          && mapping.find(column.name()) == null
          && schema.findField(column.name()) == null) {
        fields.add(MappedField.of(column.fieldId(), column.name()));
      }
    }
    return fields.size() == mapping.asMappedFields().fields().size()
        ? mapping
        : NameMapping.of(fields);
  }

  private MappedFields updateNameMapping(
      MappedFields mapping, Map<Integer, String> idToStorageName) {
    if (mapping == null) {
      return null;
    }
    List<MappedField> fieldResults = new ArrayList<>();
    for (MappedField field : mapping.fields()) {
      // Files are written with the storage name, so map only that; the logical name can collide
      // with another field's storage name after renames.
      Set<String> fieldNames =
          idToStorageName.containsKey(field.id())
              ? Collections.singleton(idToStorageName.get(field.id()))
              : new HashSet<>(field.names());
      MappedFields nestedMapping = updateNameMapping(field.nestedMapping(), idToStorageName);
      fieldResults.add(MappedField.of(field.id(), fieldNames, nestedMapping));
    }
    return MappedFields.of(fieldResults);
  }

  @Override
  public void syncSchema(InternalSchema schema) {
    Schema latestSchema = schemaExtractor.toIceberg(schema);
    if (!transaction.table().properties().containsKey(TableProperties.DEFAULT_NAME_MAPPING)) {
      // The table's own schema has the field ids the data files are read by. latestSchema numbers
      // the columns of a source without field ids in order, which differs from the table's ids
      // once a column was dropped or added. The schema update below maintains the mapping.
      createAndSetNameMapping(transaction.table().schema());
    }
    if (!transaction.table().schema().sameSchema(latestSchema)) {
      boolean hasFieldIds =
          schema.getAllFields().stream().anyMatch(field -> field.getFieldId() != null);
      if (hasFieldIds) {
        // If field IDs are provided in the source schema, manually update the name mapping to
        // ensure the IDs match the correct fields.
        createAndSetNameMapping(latestSchema);
        // There is no clean way to sync the schema with the provided field IDs using the
        // transaction API so we commit the current transaction and interact directly with
        // the operations API.
        transaction.commitTransaction();
        schemaSync.syncWithProvidedIds(latestSchema, table);
        // Start a new transaction for remaining operations
        table.refresh();
        transaction = table.newTransaction();
      } else {
        schemaSync.sync(transaction.table().schema(), latestSchema, transaction);
      }
    }
    NameMapping mapping =
        NameMappingParser.fromJson(
            transaction.table().properties().get(TableProperties.DEFAULT_NAME_MAPPING));
    NameMapping mappingWithRowLineage = withRowLineage(mapping, transaction.table().schema());
    if (mappingWithRowLineage != mapping) {
      setNameMapping(mappingWithRowLineage);
    }
  }

  @Override
  public void syncPartitionSpec(List<InternalPartitionField> partitionSpec) {
    PartitionSpec latestPartitionSpec =
        partitionSpecExtractor.toIceberg(partitionSpec, transaction.table().schema());
    partitionSpecSync.sync(table.spec(), latestPartitionSpec, transaction);
  }

  @Override
  public void syncMetadata(TableSyncMetadata metadata) {
    tableSyncMetadata = metadata;

    UpdateProperties updateProperties = transaction.updateProperties();
    updateProperties.set(TableSyncMetadata.XTABLE_METADATA, metadata.toJson());
    if (!table.properties().containsKey(TableProperties.WRITE_DATA_LOCATION)) {
      // Required for a consistent write location when writing back to the table as Iceberg
      updateProperties.set(TableProperties.WRITE_DATA_LOCATION, basePath);
    }
    if (!Boolean.parseBoolean(
        table
            .properties()
            .getOrDefault(
                TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, Boolean.FALSE.toString()))) {
      // Helps control the number of metadata files for frequently updated tables
      updateProperties.set(
          TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, Boolean.TRUE.toString());
    }
    updateProperties.commit();
  }

  @Override
  public void syncFilesForSnapshot(List<PartitionFileGroup> partitionedDataFiles) {
    dataFileUpdatesExtractor.applySnapshot(
        table,
        internalTableState,
        transaction,
        partitionedDataFiles,
        transaction.table().schema(),
        transaction.table().spec(),
        tableSyncMetadata);
  }

  /**
   * Stages row positions deleted from data files for the next {@link #syncFilesForDiff}, which then
   * commits the diff and the positions as one row delta, see {@link
   * IcebergDataFileUpdatesSync#applyRowDelta}. An empty map still selects the row delta, for a diff
   * on a table that carries deletion vectors.
   *
   * @param partitionsOfDataFiles the partitions holding the data files the positions are deleted
   *     from, to which the lookup of their current deletion vectors is limited
   */
  public void stageDeletionVectors(
      Map<String, ? extends Collection<Long>> deletedPositionsByDataFile,
      Collection<List<PartitionValue>> partitionsOfDataFiles,
      boolean replaceDeletionVectors) {
    this.stagedDeletedPositions = deletedPositionsByDataFile;
    this.stagedPartitionsOfDataFiles = partitionsOfDataFiles;
    this.stagedPositionsReplaceDeletionVectors = replaceDeletionVectors;
  }

  /**
   * Marks the next {@link #syncFilesForDiff} as the diff of a source commit that deletes rows, so
   * that it is published as an Iceberg delete even when it changes no files.
   */
  public void stageDeleteOperation() {
    this.stagedDeleteOperation = true;
  }

  /**
   * Tells the next {@link #syncFilesForDiff} which data files the source holds no live rows of. A
   * row delta removes a data file whose rows a delete removes in full, so a later diff of the
   * source can still reference it; the diff leaves such a file out when the table does not hold it,
   * see {@link IcebergDataFileUpdatesSync#applyRowDelta} and {@link
   * IcebergDataFileUpdatesSync#applyRewrite}.
   *
   * @param withoutLiveRows whether the source holds no live rows of the data file at a path
   */
  public void stageDataFilesWithoutLiveRows(Predicate<String> withoutLiveRows) {
    this.stagedDataFilesWithoutLiveRows = withoutLiveRows;
  }

  /**
   * Marks the next {@link #syncFilesForDiff} as the diff of a source commit that rewrites data
   * files and keeps every row, as a compaction or a clustering does, so that it is published as an
   * Iceberg replace, see {@link IcebergDataFileUpdatesSync#applyRewrite}. A diff that Iceberg
   * cannot express as a rewrite is published as any other.
   */
  public void stageRewrite() {
    this.stagedRewrite = true;
  }

  @Override
  public void syncFilesForDiff(InternalFilesDiff internalFilesDiff) {
    if (stagedRewrite) {
      Set<String> deletionVectorFiles =
          dataFileUpdatesExtractor.applyRewrite(
              table,
              transaction,
              internalFilesDiff,
              stagedDeletedPositions == null ? Collections.emptyMap() : stagedDeletedPositions,
              stagedDataFilesWithoutLiveRows,
              transaction.table().schema(),
              transaction.table().spec(),
              tableSyncMetadata);
      if (deletionVectorFiles != null) {
        uncommittedDeletionVectorFiles = deletionVectorFiles;
        return;
      }
    }
    if (stagedDeletedPositions != null) {
      uncommittedDeletionVectorFiles =
          dataFileUpdatesExtractor.applyRowDelta(
              table,
              transaction,
              internalFilesDiff,
              stagedDeletedPositions,
              stagedPartitionsOfDataFiles,
              stagedPositionsReplaceDeletionVectors,
              stagedDeleteOperation,
              stagedDataFilesWithoutLiveRows,
              transaction.table().schema(),
              transaction.table().spec(),
              tableSyncMetadata);
      return;
    }
    dataFileUpdatesExtractor.applyDiff(
        transaction,
        internalFilesDiff,
        stagedDeleteOperation,
        transaction.table().schema(),
        transaction.table().spec(),
        tableSyncMetadata);
  }

  @Override
  public void completeSync() {
    try {
      if (!metadataRetention.isNegative()) {
        transaction
            .expireSnapshots()
            .expireOlderThan(Instant.now().minus(metadataRetention).toEpochMilli())
            .deleteWith(this::safeDelete)
            .cleanExpiredFiles(true)
            .commit();
      }
      transaction.commitTransaction();
    } catch (RuntimeException e) {
      deleteUncommittedDeletionVectorFiles(e);
      throw e;
    }
    resetTransactionState();
  }

  /**
   * Deletes the deletion vector files of a sync whose commit failed, since no snapshot references
   * them. They are kept when the commit may have landed, as Iceberg decides for its own metadata
   * files: when the outcome is unknown, or when the catalog requires strict cleanup and the failure
   * does not mark the commit as failed.
   */
  private void deleteUncommittedDeletionVectorFiles(RuntimeException commitFailure) {
    if (uncommittedDeletionVectorFiles.isEmpty()) {
      return;
    }
    boolean mayHaveCommitted =
        commitFailure instanceof CommitStateUnknownException
            || (((HasTableOperations) table).operations().requireStrictCleanup()
                && !(commitFailure instanceof CleanableFailure));
    if (mayHaveCommitted) {
      log.warn(
          "Keeping deletion vector files {} of table {}, since its commit may have landed",
          uncommittedDeletionVectorFiles,
          tableIdentifier);
    } else {
      IcebergDataFileUpdatesSync.deleteUncommittedFiles(table.io(), uncommittedDeletionVectorFiles);
    }
    uncommittedDeletionVectorFiles = Collections.emptySet();
  }

  /**
   * Data files belong to the source table and are never deleted here. Deletion vector files are
   * written by this target next to them, so they are deleted once no retained snapshot references
   * them.
   */
  private void safeDelete(String file) {
    if (file.startsWith(new Path(basePath) + METADATA_DIR_PATH)
        || file.endsWith(DELETION_VECTOR_FILE_EXTENSION)) {
      table.io().deleteFile(file);
    }
  }

  @Override
  public Optional<TableSyncMetadata> getTableMetadata() {
    if (table == null) {
      return Optional.empty();
    }
    return TableSyncMetadata.fromJson(table.properties().get(TableSyncMetadata.XTABLE_METADATA));
  }

  @Override
  public String getTableFormat() {
    return TableFormat.ICEBERG;
  }

  @Override
  public Optional<String> getTargetCommitIdentifier(String sourceIdentifier) {
    for (Snapshot snapshot : table.snapshots()) {
      Map<String, String> summary = snapshot.summary();
      String sourceMetadataJson = summary.get(TableSyncMetadata.XTABLE_METADATA);
      if (sourceMetadataJson == null) {
        continue;
      }

      try {
        Optional<TableSyncMetadata> optionalMetadata =
            TableSyncMetadata.fromJson(sourceMetadataJson);
        if (!optionalMetadata.isPresent()) {
          continue;
        }

        TableSyncMetadata metadata = optionalMetadata.get();
        if (sourceIdentifier.equals(metadata.getSourceIdentifier())) {
          return Optional.of(String.valueOf(snapshot.snapshotId()));
        }
      } catch (Exception e) {
        log.warn("Failed to parse parse snapshot metadata for {}", snapshot.snapshotId(), e);
      }
    }
    return Optional.empty();
  }

  /**
   * Expires the given snapshots and ends the sync. Requires {@link #beginSync} to have run. Passing
   * an empty list is a no-op rather than an empty metadata commit, since callers driven by Hudi
   * archival reach this on every round.
   *
   * @param snapshotIds snapshots to expire
   */
  public void expireSnapshotIds(List<Long> snapshotIds) {
    if (snapshotIds.isEmpty()) {
      // Nothing to expire, so end the sync without writing a metadata version that changes nothing.
      resetTransactionState();
      return;
    }
    ExpireSnapshots expireSnapshots = transaction.expireSnapshots().deleteWith(this::safeDelete);
    for (Long snapshotId : snapshotIds) {
      expireSnapshots.expireSnapshotId(snapshotId);
    }
    expireSnapshots.commit();
    transaction.commitTransaction();
    resetTransactionState();
  }

  private void resetTransactionState() {
    transaction = null;
    internalTableState = null;
    tableSyncMetadata = null;
    stagedDeletedPositions = null;
    stagedPartitionsOfDataFiles = null;
    uncommittedDeletionVectorFiles = Collections.emptySet();
    stagedDeleteOperation = false;
    stagedDataFilesWithoutLiveRows = null;
    stagedRewrite = false;
  }

  private void rollbackCorruptCommits() {
    if (table == null) {
      // there is no existing table so exit early
      return;
    }
    // There are cases when using the HadoopTables API where the table metadata json is updated but
    // the manifest file is not written, causing corruption. This is a workaround to fix the issue.
    if (catalogConfig == null && table.currentSnapshot() != null) {
      boolean validSnapshot = false;
      while (!validSnapshot) {
        try {
          table.currentSnapshot().allManifests(table.io());
          validSnapshot = true;
        } catch (NotFoundException ex) {
          Snapshot currentSnapshot = table.currentSnapshot();
          log.warn(
              "Corrupt snapshot detected for table: {} snapshotId: {}. Rolling back to previous snapshot: {}.",
              tableIdentifier,
              currentSnapshot.snapshotId(),
              currentSnapshot.parentId());
          // if we need to rollback, we must also clear the last sync state since that sync is no
          // longer considered valid. This will force InternalTable to fall back to a snapshot sync
          // in the subsequent sync round.
          table.manageSnapshots().rollbackTo(currentSnapshot.parentId()).commit();
          Transaction transaction = table.newTransaction();
          transaction.updateProperties().remove(TableSyncMetadata.XTABLE_METADATA).commit();
          transaction.commitTransaction();
        }
      }
    }
  }
}
