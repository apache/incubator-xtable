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
import java.util.*;

import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.util.FileNames;
import lombok.Builder;

import org.apache.hadoop.conf.Configuration;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.replay.ActionsIterator;
import io.delta.kernel.internal.actions.SingleAction;
import io.delta.kernel.internal.util.FileNames.DeltaLogFileType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import io.delta.kernel.internal.fs.Path;


import org.apache.spark.sql.delta.DeltaHistoryManager;
import org.apache.xtable.delta.*;
import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.*;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.spi.extractor.ConversionSource;
import org.apache.xtable.spi.extractor.DataFileIterator;
import scala.Option;

@Builder
public class DeltaKernelConversionSource implements ConversionSource<Long> {

  @Builder.Default
  private final DeltaKernelDataFileExtractor dataFileExtractor =
      DeltaKernelDataFileExtractor.builder().build();
  @Builder.Default
  private final DeltaKernelActionsConverter actionsConverter = DeltaKernelActionsConverter.getInstance();

  private final String basePath;
  private final String tableName;
  private final Engine engine;

  private final StructType actionSchema = SingleAction.FULL_SCHEMA;
  //  private final DeltaKernelTableExtractor tableExtractor;

  @Builder.Default
  private final DeltaKernelTableExtractor tableExtractor =
      DeltaKernelTableExtractor.builder().build();
  private Optional<DeltaIncrementalChangesState> deltaIncrementalChangesState = Optional.empty();

  @Override
  public InternalTable getTable(Long version) {
    Configuration hadoopConf = new Configuration();
    try {
      Engine engine = DefaultEngine.create(hadoopConf);
      Table table = Table.forPath(engine, basePath);
      Snapshot snapshot = table.getSnapshotAsOfVersion(engine, version);
      return tableExtractor.table(table, snapshot, engine, tableName, basePath);
    } catch (Exception e) {
      throw new ReadException("Failed to get table at version " + version, e);
    }
  }

  @Override
  public InternalTable getCurrentTable() {
    Configuration hadoopConf = new Configuration();
    Engine engine = DefaultEngine.create(hadoopConf);
    Table table = Table.forPath(engine, basePath);
    Snapshot snapshot = table.getLatestSnapshot(engine);
    return getTable(snapshot.getVersion());
  }

  @Override
  public InternalSnapshot getCurrentSnapshot() {
    Configuration hadoopConf = new Configuration();
    Engine engine = DefaultEngine.create(hadoopConf);
    Table table_snapshot = Table.forPath(engine, basePath);
    Snapshot snapshot = table_snapshot.getLatestSnapshot(engine);
    InternalTable table = getTable(snapshot.getVersion());
    return InternalSnapshot.builder()
        .table(table)
        .partitionedDataFiles(getInternalDataFiles(snapshot, table.getReadSchema()))
        .sourceIdentifier(getCommitIdentifier(snapshot.getVersion()))
        .build();
  }

  @Override
  public TableChange getTableChangeForCommit(Long versionNumber) {
    Configuration hadoopConf = new Configuration();
    Engine engine = DefaultEngine.create(hadoopConf);
    Table table = Table.forPath(engine, basePath);
    Snapshot snapshot = table.getSnapshotAsOfVersion(engine, versionNumber);
    InternalTable tableAtVersion = tableExtractor.table(table, snapshot, engine, tableName, basePath);
    Map<String, InternalDataFile> addedFiles = new HashMap<>();
    String provider = ((SnapshotImpl) snapshot).getMetadata().getFormat().getProvider();
    FileFormat fileFormat =
            actionsConverter.convertToFileFormat(provider);
    List<FileStatus> files = DeltaLogActionUtils.listDeltaLogFilesAsIter(
            engine,
            Collections.singleton(FileNames.DeltaLogFileType.COMMIT),
            new Path(basePath),
            versionNumber,
            Optional.of(versionNumber),
            false
    ).toInMemoryList();

    List<Row> actions = new ArrayList<>();
    ActionsIterator actionsIterator = new ActionsIterator(engine, files, actionSchema, Optional.empty());
    while (actionsIterator.hasNext()) {
      // Each ActionWrapper may wrap a batch of rows (actions)
      CloseableIterator<Row> scanFileRows = actionsIterator.next().getColumnarBatch().getRows();
      while (scanFileRows.hasNext()) {
        Row scanFileRow = scanFileRows.next();
        if (scanFileRow instanceof AddFile){
          Map<String, String> partitionValues =
                  InternalScanFileUtils.getPartitionValues(scanFileRow);
//    List<Action> actionsForVersion = getChangesState().getActionsForVersion(versionNumber);
        InternalDataFile dataFile =
                actionsConverter.convertAddActionToInternalDataFile(
                        (AddFile) scanFileRow,
                        snapshot,
                        fileFormat,
                        tableAtVersion.getPartitioningFields(),
                        tableAtVersion.getReadSchema().getFields(),
                        true,
                        DeltaKernelPartitionExtractor.getInstance(),
                        DeltaKernelStatsExtractor.getInstance(),
                        partitionValues
                        );
        addedFiles.put(dataFile.getPhysicalPath(), dataFile);
      }
    }}


    InternalFilesDiff internalFilesDiff =
            InternalFilesDiff.builder()
                    .filesAdded(addedFiles.values())
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
    return null;
//    DeltaHistoryManager.Commit deltaCommitAtLastSyncInstant =
//            deltaLog.
//                    .getActiveCommitAtTime(
//                            Timestamp.from(instantsForIncrementalSync.getLastSyncInstant()), true, false, true);
//    long versionNumberAtLastSyncInstant = deltaCommitAtLastSyncInstant.version();
//    resetState(versionNumberAtLastSyncInstant + 1);
//    return CommitsBacklog.<Long>builder()
//            .commitsToProcess(getChangesState().getVersionsInSortedOrder())
//            .build();
  }

  @Override
  public boolean isIncrementalSyncSafeFrom(Instant instant) {
    return false;
  }

  @Override
  public String getCommitIdentifier(Long aLong) {
    return "";
  }

  private List<PartitionFileGroup> getInternalDataFiles(
      io.delta.kernel.Snapshot snapshot, InternalSchema schema) {
    try (DataFileIterator fileIterator = dataFileExtractor.iterator(snapshot, schema)) {

      List<InternalDataFile> dataFiles = new ArrayList<>();
      fileIterator.forEachRemaining(dataFiles::add);
      return PartitionFileGroup.fromFiles(dataFiles);
    } catch (Exception e) {
      throw new ReadException("Failed to iterate through Delta data files", e);
    }
  }

  @Override
  public void close() throws IOException {}

  private DeltaIncrementalChangesState getChangesState() {
    return deltaIncrementalChangesState.orElseThrow(
            () -> new IllegalStateException("DeltaIncrementalChangesState is not initialized"));
  }
}
