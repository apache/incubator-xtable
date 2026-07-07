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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import lombok.extern.log4j.Log4j2;

import org.apache.paimon.Snapshot;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.extractor.ConversionSource;

@Log4j2
public class PaimonConversionSource implements ConversionSource<Snapshot> {

  private final FileStoreTable paimonTable;
  private final SchemaManager schemaManager;
  private final SnapshotManager snapshotManager;

  private final PaimonDataFileExtractor dataFileExtractor = PaimonDataFileExtractor.getInstance();
  private final PaimonSchemaExtractor schemaExtractor = PaimonSchemaExtractor.getInstance();
  private final PaimonPartitionExtractor partitionSpecExtractor =
      PaimonPartitionExtractor.getInstance();

  public PaimonConversionSource(FileStoreTable paimonTable) {
    this.paimonTable = paimonTable;
    this.schemaManager = paimonTable.schemaManager();
    this.snapshotManager = paimonTable.snapshotManager();
  }

  @Override
  public InternalTable getTable(Snapshot snapshot) {
    TableSchema paimonSchema = schemaManager.schema(snapshot.schemaId());
    InternalSchema internalSchema = schemaExtractor.toInternalSchema(paimonSchema);

    List<String> partitionKeys = paimonTable.partitionKeys();
    List<InternalPartitionField> partitioningFields =
        partitionSpecExtractor.toInternalPartitionFields(partitionKeys, internalSchema);

    return InternalTable.builder()
        .name(paimonTable.name())
        .tableFormat(TableFormat.PAIMON)
        .readSchema(internalSchema)
        .layoutStrategy(DataLayoutStrategy.HIVE_STYLE_PARTITION)
        .basePath(paimonTable.location().toString())
        .partitioningFields(partitioningFields)
        .latestCommitTime(Instant.ofEpochMilli(snapshot.timeMillis()))
        .latestMetadataPath(snapshotManager.snapshotPath(snapshot.id()).toString())
        .build();
  }

  @Override
  public InternalTable getCurrentTable() {
    Snapshot snapshot = getLastSnapshot();
    return getTable(snapshot);
  }

  @Override
  public InternalSnapshot getCurrentSnapshot() {
    Snapshot snapshot = getLastSnapshot();
    InternalTable internalTable = getTable(snapshot);
    InternalSchema internalSchema = internalTable.getReadSchema();
    List<InternalDataFile> dataFiles =
        dataFileExtractor.toInternalDataFiles(paimonTable, snapshot, internalSchema);

    return InternalSnapshot.builder()
        .table(internalTable)
        .version(Long.toString(snapshot.timeMillis()))
        .partitionedDataFiles(PartitionFileGroup.fromFiles(dataFiles))
        .sourceIdentifier(getCommitIdentifier(snapshot))
        .build();
  }

  private Snapshot getLastSnapshot() {
    SnapshotManager snapshotManager = paimonTable.snapshotManager();
    Snapshot snapshot = snapshotManager.latestSnapshot();
    if (snapshot == null) {
      throw new ReadException("No snapshots found for table " + paimonTable.name());
    }
    return snapshot;
  }

  @Override
  public TableChange getTableChangeForCommit(Snapshot snapshot) {
    InternalTable tableAtSnapshot = getTable(snapshot);
    InternalSchema internalSchema = tableAtSnapshot.getReadSchema();

    InternalFilesDiff filesDiff =
        dataFileExtractor.extractFilesDiff(paimonTable, snapshot, internalSchema);

    return TableChange.builder()
        .tableAsOfChange(tableAtSnapshot)
        .filesDiff(filesDiff)
        .sourceIdentifier(getCommitIdentifier(snapshot))
        .build();
  }

  @Override
  public CommitsBacklog<Snapshot> getCommitsBacklog(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    Instant lastSyncInstant = instantsForIncrementalSync.getLastSyncInstant();
    long lastSyncTimeMillis = lastSyncInstant.toEpochMilli();

    log.info(
        "Getting commits backlog for Paimon table {} from instant {}",
        paimonTable.name(),
        lastSyncInstant);

    Iterator<Snapshot> snapshotIterator;
    try {
      snapshotIterator = snapshotManager.snapshots();
    } catch (IOException e) {
      throw new ReadException("Could not iterate over the Paimon snapshot list", e);
    }

    List<Snapshot> snapshotsToProcess = new ArrayList<>();
    while (snapshotIterator.hasNext()) {
      Snapshot snapshot = snapshotIterator.next();
      // Only include snapshots committed after the last sync
      if (snapshot.timeMillis() > lastSyncTimeMillis) {
        snapshotsToProcess.add(snapshot);
        log.debug(
            "Including snapshot {} (time={}, commitId={}) in backlog",
            snapshot.id(),
            snapshot.timeMillis(),
            snapshot.commitIdentifier());
      }
    }

    log.info("Found {} snapshots to process for incremental sync", snapshotsToProcess.size());

    return CommitsBacklog.<Snapshot>builder()
        .commitsToProcess(snapshotsToProcess)
        .inFlightInstants(Collections.emptyList())
        .build();
  }

  @Override
  public boolean isIncrementalSyncSafeFrom(Instant instant) {
    long timeInMillis = instant.toEpochMilli();

    Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
    Long latestSnapshotId = snapshotManager.latestSnapshotId();
    if (earliestSnapshotId == null || latestSnapshotId == null) {
      log.warn("No snapshots found in table {}", paimonTable.name());
      return false;
    }

    Snapshot earliestSnapshot = snapshotManager.snapshot(earliestSnapshotId);
    Snapshot latestSnapshot = snapshotManager.snapshot(latestSnapshotId);

    // Check 1: If instant is in the future (after latest snapshot), return false
    if (timeInMillis > latestSnapshot.timeMillis()) {
      log.warn(
          "Instant {} is in the future. Latest snapshot {} has time {}",
          instant,
          latestSnapshot.id(),
          latestSnapshot.timeMillis());
      return false;
    }

    // Check 2: Has snapshot expiration affected this instant?
    // If the earliest snapshot is after the requested instant,
    // then snapshots have been expired and we can't do incremental sync
    if (earliestSnapshot.timeMillis() > timeInMillis) {
      log.warn(
          "Incremental sync is not safe from instant {}. "
              + "Earliest available snapshot {} (time={}) is newer than the requested instant. "
              + "Snapshots may have been expired.",
          instant,
          earliestSnapshot.id(),
          earliestSnapshot.timeMillis());
      return false;
    }

    // Check 3: Verify a snapshot exists at or before the instant
    if (earliestSnapshot.timeMillis() <= timeInMillis) {
      log.info(
          "Incremental sync is safe from instant {} for table {}", instant, paimonTable.name());
      return true;
    }

    log.warn("No snapshot found at or before instant {} for table {}", instant, paimonTable.name());
    return false;
  }

  @Override
  public String getCommitIdentifier(Snapshot snapshot) {
    return Long.toString(snapshot.commitIdentifier());
  }

  @Override
  public void close() throws IOException {}
}
