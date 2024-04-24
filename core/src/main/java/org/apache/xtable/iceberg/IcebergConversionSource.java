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

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import lombok.*;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;

import org.apache.xtable.conversion.PerTableConfig;
import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.storage.*;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.spi.extractor.ConversionSource;

@Log4j2
@Builder
public class IcebergConversionSource implements ConversionSource<Snapshot> {
  @NonNull private final Configuration hadoopConf;
  @NonNull private final PerTableConfig sourceTableConfig;

  @Getter(lazy = true, value = AccessLevel.PACKAGE)
  private final Table sourceTable = initSourceTable();

  @Getter(lazy = true, value = AccessLevel.PACKAGE)
  private final FileIO tableOps = initTableOps();

  @Builder.Default
  private final IcebergPartitionValueConverter partitionConverter =
      IcebergPartitionValueConverter.getInstance();

  @Builder.Default
  private final IcebergDataFileExtractor dataFileExtractor =
      IcebergDataFileExtractor.builder().build();

  private Table initSourceTable() {
    IcebergTableManager tableManager = IcebergTableManager.of(hadoopConf);
    String[] namespace = sourceTableConfig.getNamespace();
    String tableName = sourceTableConfig.getTableName();
    TableIdentifier tableIdentifier =
        namespace == null
            ? TableIdentifier.of(tableName)
            : TableIdentifier.of(Namespace.of(namespace), tableName);
    return tableManager.getTable(
        (IcebergCatalogConfig) sourceTableConfig.getIcebergCatalogConfig(),
        tableIdentifier,
        sourceTableConfig.getTableBasePath());
  }

  private FileIO initTableOps() {
    return new HadoopFileIO(hadoopConf);
  }

  @Override
  public InternalTable getTable(Snapshot snapshot) {
    Table iceTable = getSourceTable();
    Schema iceSchema = iceTable.schemas().get(snapshot.schemaId());
    IcebergSchemaExtractor schemaExtractor = IcebergSchemaExtractor.getInstance();
    InternalSchema irSchema = schemaExtractor.fromIceberg(iceSchema);

    // TODO select snapshot specific partition spec
    IcebergPartitionSpecExtractor partitionExtractor = IcebergPartitionSpecExtractor.getInstance();
    List<InternalPartitionField> irPartitionFields =
        partitionExtractor.fromIceberg(iceTable.spec(), iceSchema, irSchema);
    // Data layout is hive storage for partitioned by default. See
    // (https://github.com/apache/iceberg/blob/main/docs/aws.md)
    DataLayoutStrategy dataLayoutStrategy =
        irPartitionFields.size() > 0
            ? DataLayoutStrategy.HIVE_STYLE_PARTITION
            : DataLayoutStrategy.FLAT;
    return InternalTable.builder()
        .tableFormat(TableFormat.ICEBERG)
        .basePath(iceTable.location())
        .name(iceTable.name())
        .partitioningFields(irPartitionFields)
        .latestCommitTime(Instant.ofEpochMilli(snapshot.timestampMillis()))
        .readSchema(irSchema)
        .layoutStrategy(dataLayoutStrategy)
        .build();
  }

  @Override
  public InternalSnapshot getCurrentSnapshot() {
    Table iceTable = getSourceTable();

    Snapshot currentSnapshot = iceTable.currentSnapshot();
    InternalTable irTable = getTable(currentSnapshot);

    TableScan scan = iceTable.newScan().useSnapshot(currentSnapshot.snapshotId());
    PartitionSpec partitionSpec = iceTable.spec();
    List<PartitionFileGroup> partitionedDataFiles;
    try (CloseableIterable<FileScanTask> files = scan.planFiles()) {
      List<InternalDataFile> irFiles = new ArrayList<>();
      for (FileScanTask fileScanTask : files) {
        DataFile file = fileScanTask.file();
        InternalDataFile irDataFile = fromIceberg(file, partitionSpec, irTable);
        irFiles.add(irDataFile);
      }
      partitionedDataFiles = PartitionFileGroup.fromFiles(irFiles);
    } catch (IOException e) {
      throw new ReadException("Failed to fetch current snapshot files from Iceberg source", e);
    }

    return InternalSnapshot.builder()
        .version(String.valueOf(currentSnapshot.snapshotId()))
        .table(irTable)
        .partitionedDataFiles(partitionedDataFiles)
        .build();
  }

  private InternalDataFile fromIceberg(
      DataFile file, PartitionSpec partitionSpec, InternalTable internalTable) {
    List<PartitionValue> partitionValues =
        partitionConverter.toXTable(internalTable, file.partition(), partitionSpec);
    return dataFileExtractor.fromIceberg(file, partitionValues, internalTable.getReadSchema());
  }

  @Override
  public TableChange getTableChangeForCommit(Snapshot snapshot) {
    FileIO fileIO = getTableOps();
    Table iceTable = getSourceTable();
    PartitionSpec partitionSpec = iceTable.spec();
    InternalTable irTable = getTable(snapshot);

    Set<InternalDataFile> dataFilesAdded =
        StreamSupport.stream(snapshot.addedDataFiles(fileIO).spliterator(), false)
            .map(dataFile -> fromIceberg(dataFile, partitionSpec, irTable))
            .collect(Collectors.toSet());

    Set<InternalDataFile> dataFilesRemoved =
        StreamSupport.stream(snapshot.removedDataFiles(fileIO).spliterator(), false)
            .map(dataFile -> fromIceberg(dataFile, partitionSpec, irTable))
            .collect(Collectors.toSet());

    DataFilesDiff filesDiff =
        DataFilesDiff.builder().filesAdded(dataFilesAdded).filesRemoved(dataFilesRemoved).build();

    InternalTable table = getTable(snapshot);
    return TableChange.builder().tableAsOfChange(table).filesDiff(filesDiff).build();
  }

  @Override
  public CommitsBacklog<Snapshot> getCommitsBacklog(InstantsForIncrementalSync lastSyncInstant) {

    long epochMilli = lastSyncInstant.getLastSyncInstant().toEpochMilli();
    Table iceTable = getSourceTable();

    // There are two ways to fetch Iceberg table's change log; 1) fetch the history using .history()
    // method and 2) fetch the snapshots using .snapshots() method and traverse the snapshots in
    // reverse chronological order. The issue with #1 is that if transactions are involved, the
    // history tracks only the last snapshot of a multi-snapshot transaction. As a result the
    // timeline generated for sync would be incomplete. Hence, #2 is used.

    Snapshot pendingSnapshot = iceTable.currentSnapshot();
    if (pendingSnapshot.timestampMillis() <= epochMilli) {
      // Even the latest snapshot was committed before the lastSyncInstant. No new commits were made
      // and no new snapshots need to be synced. Return empty state.
      return CommitsBacklog.<Snapshot>builder().build();
    }

    List<Snapshot> snapshots = new ArrayList<>();
    while (pendingSnapshot != null && pendingSnapshot.timestampMillis() > epochMilli) {
      snapshots.add(pendingSnapshot);
      pendingSnapshot =
          pendingSnapshot.parentId() != null ? iceTable.snapshot(pendingSnapshot.parentId()) : null;
    }
    // reverse the list to process the oldest snapshot first
    Collections.reverse(snapshots);
    return CommitsBacklog.<Snapshot>builder().commitsToProcess(snapshots).build();
  }

  /*
   * Following checks are to be performed:
   * 1. Check if snapshot at or before the provided instant exists.
   * 2. Check if expiring of snapshots has impacted the provided instant.
   */
  @Override
  public boolean isIncrementalSyncSafeFrom(Instant instant) {
    long timeInMillis = instant.toEpochMilli();
    Table iceTable = getSourceTable();
    boolean doesInstantOfAgeExists = false;
    Long targetSnapshotId = null;
    for (Snapshot snapshot : iceTable.snapshots()) {
      if (snapshot.timestampMillis() <= timeInMillis) {
        doesInstantOfAgeExists = true;
        targetSnapshotId = snapshot.snapshotId();
      } else {
        break;
      }
    }
    if (!doesInstantOfAgeExists) {
      return false;
    }
    // Go from latest snapshot until targetSnapshotId through parent reference.
    // nothing has to be null in this chain to guarantee safety of incremental sync.
    Long currentSnapshotId = iceTable.currentSnapshot().snapshotId();
    while (currentSnapshotId != null && currentSnapshotId != targetSnapshotId) {
      Snapshot currentSnapshot = iceTable.snapshot(currentSnapshotId);
      if (currentSnapshot == null) {
        // The snapshot is expired.
        return false;
      }
      currentSnapshotId = currentSnapshot.parentId();
    }
    return true;
  }

  @Override
  public void close() {
    getTableOps().close();
  }
}
