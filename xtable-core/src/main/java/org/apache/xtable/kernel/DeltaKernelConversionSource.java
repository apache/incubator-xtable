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

import java.io.IOException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.actions.RowBackedAction;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.utils.CloseableIterator;

import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.spi.extractor.ConversionSource;
import org.apache.xtable.spi.extractor.DataFileIterator;

@Slf4j
@Builder
public class DeltaKernelConversionSource implements ConversionSource<Long> {

  @Builder.Default
  private final DeltaKernelDataFileExtractor dataFileExtractor =
      DeltaKernelDataFileExtractor.builder().build();

  @Builder.Default
  private final DeltaKernelActionsConverter actionsConverter =
      DeltaKernelActionsConverter.getInstance();

  private final String basePath;
  private final String tableName;
  private final Engine engine;

  @Builder.Default
  private final DeltaKernelTableExtractor tableExtractor =
      DeltaKernelTableExtractor.builder().build();

  private Optional<DeltaKernelIncrementalChangesState> deltaKernelIncrementalChangesState =
      Optional.empty();

  @Override
  public InternalTable getTable(Long version) {
    try {
      Table table = Table.forPath(engine, basePath);
      Snapshot snapshot = table.getSnapshotAsOfVersion(engine, version);
      return tableExtractor.table(table, snapshot, engine, tableName, basePath);
    } catch (Exception e) {
      throw new ReadException("Failed to get table at version " + version, e);
    }
  }

  @Override
  public InternalTable getCurrentTable() {
    Table table = Table.forPath(engine, basePath);
    Snapshot snapshot = table.getLatestSnapshot(engine);
    return getTable(snapshot.getVersion());
  }

  @Override
  public InternalSnapshot getCurrentSnapshot() {
    Table table_snapshot = Table.forPath(engine, basePath);
    Snapshot snapshot = table_snapshot.getLatestSnapshot(engine);
    InternalTable table = getTable(snapshot.getVersion());
    return InternalSnapshot.builder()
        .table(table)
        .partitionedDataFiles(
            getInternalDataFiles(snapshot, table_snapshot, engine, table.getReadSchema()))
        .sourceIdentifier(getCommitIdentifier(snapshot.getVersion()))
        .build();
  }

  @Override
  public TableChange getTableChangeForCommit(Long versionNumber) {
    Table table = Table.forPath(engine, basePath);
    Snapshot snapshot = table.getSnapshotAsOfVersion(engine, versionNumber);
    InternalTable tableAtVersion =
        tableExtractor.table(table, snapshot, engine, tableName, basePath);
    Map<String, InternalDataFile> addedFiles = new HashMap<>();
    Map<String, InternalDataFile> removedFiles = new HashMap<>();
    String provider = ((SnapshotImpl) snapshot).getMetadata().getFormat().getProvider();
    FileFormat fileFormat = actionsConverter.convertToFileFormat(provider);

    List<RowBackedAction> actionsForVersion = getChangesState().getActionsForVersion(versionNumber);

    for (RowBackedAction action : actionsForVersion) {
      if (action instanceof AddFile) {
        AddFile addFile = (AddFile) action;
        Map<String, String> partitionValues = VectorUtils.toJavaMap(addFile.getPartitionValues());
        InternalDataFile dataFile =
            actionsConverter.convertAddActionToInternalDataFile(
                addFile,
                table,
                fileFormat,
                tableAtVersion.getPartitioningFields(),
                tableAtVersion.getReadSchema().getFields(),
                true,
                DeltaKernelPartitionExtractor.getInstance(),
                DeltaKernelStatsExtractor.getInstance(),
                partitionValues);
        addedFiles.put(dataFile.getPhysicalPath(), dataFile);
      } else if (action instanceof RemoveFile) {
        RemoveFile removeFile = (RemoveFile) action;
        Map<String, String> partitionValues =
            removeFile
                .getPartitionValues()
                .map(VectorUtils::<String, String>toJavaMap)
                .orElse(Collections.emptyMap());
        InternalDataFile dataFile =
            actionsConverter.convertRemoveActionToInternalDataFile(
                removeFile,
                table,
                fileFormat,
                tableAtVersion.getPartitioningFields(),
                DeltaKernelPartitionExtractor.getInstance(),
                partitionValues);
        removedFiles.put(dataFile.getPhysicalPath(), dataFile);
      }
    }

    InternalFilesDiff internalFilesDiff =
        InternalFilesDiff.builder()
            .filesAdded(addedFiles.values())
            .filesRemoved(removedFiles.values())
            .build();
    return TableChange.builder()
        .tableAsOfChange(tableAtVersion)
        .filesDiff(internalFilesDiff)
        .sourceIdentifier(getCommitIdentifier(versionNumber))
        .build();
  }

  @Override
  public CommitsBacklog<Long> getCommitsBacklog(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    Table table = Table.forPath(engine, basePath);
    Snapshot snapshot =
        table.getSnapshotAsOfTimestamp(
            engine, Timestamp.from(instantsForIncrementalSync.getLastSyncInstant()).getTime());

    long versionNumberAtLastSyncInstant = snapshot.getVersion();
    resetState(versionNumberAtLastSyncInstant + 1, engine, table);
    return CommitsBacklog.<Long>builder()
        .commitsToProcess(getChangesState().getVersionsInSortedOrder())
        .build();
  }

  @Override
  public boolean isIncrementalSyncSafeFrom(Instant instant) {
    try {
      Table table = Table.forPath(engine, basePath);
      Snapshot snapshot = table.getSnapshotAsOfTimestamp(engine, Timestamp.from(instant).getTime());

      // There is a chance earliest commit of the table is returned if the instant is before the
      // earliest commit of the table, hence the additional check.
      Instant deltaCommitInstant = Instant.ofEpochMilli(snapshot.getTimestamp(engine));
      if (deltaCommitInstant.isAfter(instant)) {
        log.info(
            "No commit found at or before instant {}. Earliest available commit is at {}",
            instant,
            deltaCommitInstant);
        return false;
      }

      long versionAtInstant = snapshot.getVersion();
      long currentVersion = table.getLatestSnapshot(engine).getVersion();

      // Verify that we can actually access commit files from this version to current version.
      // This will fail if VACUUM has removed the necessary commit files.
      Set<DeltaLogActionUtils.DeltaAction> actionSet = new HashSet<>();
      actionSet.add(DeltaLogActionUtils.DeltaAction.ADD);
      actionSet.add(DeltaLogActionUtils.DeltaAction.REMOVE);

      TableImpl tableImpl = (TableImpl) table;
      // Attempt to get changes - this will throw if commit files are missing
      try (CloseableIterator<ColumnarBatch> changesIterator =
          tableImpl.getChanges(engine, versionAtInstant, currentVersion, actionSet)) {
        // Test if we can access at least the first batch
        if (changesIterator.hasNext()) {
          // Successfully verified we can access commit files
          return true;
        } else {
          // No changes available (edge case: versionAtInstant == currentVersion)
          return true;
        }
      }
    } catch (Exception e) {
      // Commit files have been vacuumed or are otherwise inaccessible
      log.info(
          "Cannot perform incremental sync from instant {} due to missing or inaccessible commit files: {}",
          instant,
          e.getMessage());
      return false;
    }
  }

  @Override
  public String getCommitIdentifier(Long commit) {
    return String.valueOf(commit);
  }

  private void resetState(long versionToStartFrom, Engine engine, Table table) {
    deltaKernelIncrementalChangesState =
        Optional.of(
            DeltaKernelIncrementalChangesState.builder()
                .engine(engine)
                .table(table)
                .versionToStartFrom(versionToStartFrom)
                .endVersion(table.getLatestSnapshot(engine).getVersion())
                .build());
  }

  private List<PartitionFileGroup> getInternalDataFiles(
      Snapshot snapshot, Table table, Engine engine, InternalSchema schema) {
    try (DataFileIterator fileIterator =
        dataFileExtractor.iterator(snapshot, table, engine, schema)) {

      List<InternalDataFile> dataFiles = new ArrayList<>();
      fileIterator.forEachRemaining(dataFiles::add);
      return PartitionFileGroup.fromFiles(dataFiles);
    } catch (Exception e) {
      throw new ReadException("Failed to iterate through Delta data files", e);
    }
  }

  @Override
  public void close() throws IOException {}

  private DeltaKernelIncrementalChangesState getChangesState() {
    return deltaKernelIncrementalChangesState.orElseThrow(
        () -> new IllegalStateException("DeltaIncrementalChangesState is not initialized"));
  }
}
