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

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NotFoundException;

import org.apache.xtable.conversion.PerTableConfig;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.DataFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.sync.ConversionTarget;

@Log4j2
public class IcebergConversionTarget implements ConversionTarget {
  private static final String METADATA_DIR_PATH = "/metadata/";
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
  private int snapshotRetentionInHours;
  private Transaction transaction;
  private Table table;
  private InternalTable internalTableState;

  public IcebergConversionTarget() {}

  IcebergConversionTarget(
      PerTableConfig perTableConfig,
      Configuration configuration,
      IcebergSchemaExtractor schemaExtractor,
      IcebergSchemaSync schemaSync,
      IcebergPartitionSpecExtractor partitionSpecExtractor,
      IcebergPartitionSpecSync partitionSpecSync,
      IcebergDataFileUpdatesSync dataFileUpdatesExtractor,
      IcebergTableManager tableManager) {
    _init(
        perTableConfig,
        configuration,
        schemaExtractor,
        schemaSync,
        partitionSpecExtractor,
        partitionSpecSync,
        dataFileUpdatesExtractor,
        tableManager);
  }

  private void _init(
      PerTableConfig perTableConfig,
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
    String tableName = perTableConfig.getTableName();
    this.basePath = perTableConfig.getTableBasePath();
    this.configuration = configuration;
    this.snapshotRetentionInHours = perTableConfig.getTargetMetadataRetentionInHours();
    String[] namespace = perTableConfig.getNamespace();
    this.tableIdentifier =
        namespace == null
            ? TableIdentifier.of(tableName)
            : TableIdentifier.of(Namespace.of(namespace), tableName);
    this.tableManager = tableManager;
    this.catalogConfig = (IcebergCatalogConfig) perTableConfig.getIcebergCatalogConfig();

    if (tableManager.tableExists(catalogConfig, tableIdentifier, basePath)) {
      // Load the table state if it already exists
      this.table = tableManager.getTable(catalogConfig, tableIdentifier, basePath);
    }
    // Clear any corrupted state before using the conversion target
    rollbackCorruptCommits();
  }

  @Override
  public void init(PerTableConfig perTableConfig, Configuration configuration) {
    _init(
        perTableConfig,
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

  @Override
  public void syncSchema(InternalSchema schema) {
    Schema latestSchema = schemaExtractor.toIceberg(schema);
    schemaSync.sync(transaction.table().schema(), latestSchema, transaction);
  }

  @Override
  public void syncPartitionSpec(List<InternalPartitionField> partitionSpec) {
    PartitionSpec latestPartitionSpec =
        partitionSpecExtractor.toIceberg(partitionSpec, transaction.table().schema());
    partitionSpecSync.sync(table.spec(), latestPartitionSpec, transaction);
  }

  @Override
  public void syncMetadata(TableSyncMetadata metadata) {
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
        transaction.table().spec());
  }

  @Override
  public void syncFilesForDiff(DataFilesDiff dataFilesDiff) {
    dataFileUpdatesExtractor.applyDiff(
        transaction, dataFilesDiff, transaction.table().schema(), transaction.table().spec());
  }

  @Override
  public void completeSync() {
    transaction
        .expireSnapshots()
        .expireOlderThan(
            Instant.now().minus(snapshotRetentionInHours, ChronoUnit.HOURS).toEpochMilli())
        .deleteWith(this::safeDelete) // ensures that only metadata files are deleted
        .cleanExpiredFiles(true)
        .commit();
    transaction.commitTransaction();
    transaction = null;
    internalTableState = null;
  }

  private void safeDelete(String file) {
    if (file.startsWith(new Path(basePath) + METADATA_DIR_PATH)) {
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
