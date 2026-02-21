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

import java.util.*;
import java.util.stream.Stream;

import lombok.Builder;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.ScanImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.actions.RowBackedAction;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

import org.apache.xtable.collectors.CustomCollectors;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.*;
import org.apache.xtable.paths.PathUtils;

@Builder
public class DeltaKernelDataFileUpdatesExtractor {
  @Builder.Default
  private final DeltaKernelStatsExtractor deltaStatsExtractor =
      DeltaKernelStatsExtractor.getInstance();

  @Builder.Default
  private final DeltaKernelPartitionExtractor deltaKernelPartitionExtractor =
      DeltaKernelPartitionExtractor.getInstance();

  @Builder.Default
  private final DeltaKernelDataFileExtractor dataFileExtractor =
      DeltaKernelDataFileExtractor.builder().build();

  private final Engine engine;
  private final String basePath;
  private final boolean includeColumnStats;

  public Seq<RowBackedAction> applySnapshot(
      Table table, List<PartitionFileGroup> partitionedDataFiles, InternalSchema tableSchema) {

    // all files in the current delta snapshot are potential candidates for remove actions, i.e. if
    // the file is not present in the new snapshot (addedFiles) then the file is considered removed
    Map<String, RowBackedAction> previousFiles = new HashMap<>();
    StructType physicalSchema;

    // Check if table exists by checking if _delta_log directory exists
    boolean tableExists = checkTableExists(table.getPath(engine).toString());

    if (tableExists) {
      Snapshot snapshot = table.getLatestSnapshot(engine);

      ScanImpl myScan = (ScanImpl) snapshot.getScanBuilder().build();
      CloseableIterator<FilteredColumnarBatch> scanFiles =
          myScan.getScanFiles(engine, includeColumnStats);

      while (scanFiles.hasNext()) {
        FilteredColumnarBatch scanFileColumnarBatch = scanFiles.next();
        CloseableIterator<Row> batchRows = scanFileColumnarBatch.getRows();

        // Process ALL rows in this batch
        while (batchRows.hasNext()) {
          Row scanFileRow = batchRows.next();
          int addIndex = scanFileRow.getSchema().indexOf("add");

          if (addIndex >= 0 && !scanFileRow.isNullAt(addIndex)) {
            AddFile addFile = new AddFile(scanFileRow.getStruct(addIndex));
            RemoveFile removeFile =
                new RemoveFile(addFile.toRemoveFileRow(false, Optional.of(snapshot.getVersion())));
            String fullPath =
                DeltaKernelActionsConverter.getFullPathToFile(removeFile.getPath(), table);
            previousFiles.put(fullPath, removeFile);
          }
        }
      }
      physicalSchema = snapshot.getSchema();
    } else {
      // Table doesn't exist yet - no previous files to remove
      // Convert InternalSchema to StructType for physical schema
      DeltaKernelSchemaExtractor schemaExtractor = DeltaKernelSchemaExtractor.getInstance();
      physicalSchema = schemaExtractor.fromInternalSchema(tableSchema);
    }

    FilesDiff<InternalFile, RowBackedAction> diff =
        InternalFilesDiff.findNewAndRemovedFiles(partitionedDataFiles, previousFiles);

    return applyDiff(
        diff.getFilesAdded(),
        diff.getFilesRemoved(),
        tableSchema,
        table.getPath(engine),
        physicalSchema);
  }

  private boolean checkTableExists(String tablePath) {
    try {
      // Handle both regular paths and file:// URIs
      java.io.File tableDir;
      if (tablePath.startsWith("file:")) {
        tableDir = new java.io.File(java.net.URI.create(tablePath));
      } else {
        tableDir = new java.io.File(tablePath);
      }
      java.io.File deltaLogDir = new java.io.File(tableDir, "_delta_log");
      return deltaLogDir.exists() && deltaLogDir.isDirectory();
    } catch (Exception e) {
      return false;
    }
  }

  public Seq<RowBackedAction> applyDiff(
      InternalFilesDiff internalFilesDiff,
      InternalSchema tableSchema,
      String tableBasePath,
      StructType physicalSchema) {
    List<RowBackedAction> removeActions =
        internalFilesDiff.dataFilesRemoved().stream()
            .flatMap(dFile -> createAddFileAction(dFile, tableBasePath, physicalSchema))
            .map(addFile -> new RemoveFile(addFile.toRemoveFileRow(false, Optional.empty())))
            .collect(CustomCollectors.toList(internalFilesDiff.dataFilesRemoved().size()));
    return applyDiff(
        internalFilesDiff.dataFilesAdded(),
        removeActions,
        tableSchema,
        tableBasePath,
        physicalSchema);
  }

  private Seq<RowBackedAction> applyDiff(
      Set<? extends InternalFile> filesAdded,
      Collection<RowBackedAction> removeFileActions,
      InternalSchema tableSchema,
      String tableBasePath,
      StructType physicalSchema) {
    Stream<RowBackedAction> addActions =
        filesAdded.stream()
            .filter(InternalDataFile.class::isInstance)
            .map(file -> (InternalDataFile) file)
            .flatMap(dFile -> createAddFileAction(dFile, tableBasePath, physicalSchema));
    int totalActions = filesAdded.size() + removeFileActions.size();
    List<RowBackedAction> allActions =
        Stream.concat(addActions, removeFileActions.stream())
            .collect(CustomCollectors.toList(totalActions));
    return JavaConverters.asScalaBuffer(allActions).toSeq();
  }

  private Stream<AddFile> createAddFileAction(
      InternalDataFile dataFile, String tableBasePath, StructType physicalSchema) {
    // Convert partition values from Map<String, String> to MapValue
    Map<String, String> partitionValuesMap =
        deltaKernelPartitionExtractor.partitionValueSerialization(dataFile);
    MapValue partitionValues = convertToMapValue(partitionValuesMap);

    Row addFileRow =
        AddFile.createAddFileRow(
            physicalSchema,
            // Delta Lake supports relative and absolute paths in theory but relative paths seem
            // more commonly supported by query engines in our testing
            PathUtils.getRelativePath(dataFile.getPhysicalPath(), tableBasePath),
            partitionValues,
            dataFile.getFileSizeBytes(),
            dataFile.getLastModified(),
            true, // dataChange
            Optional.empty(), // deletionVector
            Optional.empty(), // tags
            Optional.empty(), // baseRowId
            Optional.empty(), // defaultRowCommitVersion
            Optional.empty() // stats - TODO: convert column stats to DataFileStatistics
            );

    // Wrap the Row back into an AddFile object so we can use its methods
    return Stream.of(new AddFile(addFileRow));
  }

  private MapValue convertToMapValue(Map<String, String> map) {
    return VectorUtils.stringStringMapValue(map);
  }
}
