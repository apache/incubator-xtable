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
 
package org.apache.xtable.delta;

import static org.apache.xtable.delta.ScalaUtils.convertJavaMapToScala;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;

import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.delta.actions.AddFile;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.xtable.collectors.CustomCollectors;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.storage.DataFilesDiff;
import org.apache.xtable.model.storage.FilesDiff;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.paths.PathUtils;

@Builder
public class DeltaDataFileUpdatesExtractor {
  @Builder.Default
  private final DeltaStatsExtractor deltaStatsExtractor = DeltaStatsExtractor.getInstance();

  @Builder.Default
  private final DeltaPartitionExtractor deltaPartitionExtractor =
      DeltaPartitionExtractor.getInstance();

  @Builder.Default
  private final DeltaDataFileExtractor deltaDataFileExtractor =
      DeltaDataFileExtractor.builder().build();

  public Seq<Action> applySnapshot(
      DeltaLog deltaLog,
      List<PartitionFileGroup> partitionedDataFiles,
      InternalSchema tableSchema) {

    // all files in the current delta snapshot are potential candidates for remove actions, i.e. if
    // the file is not present in the new snapshot (addedFiles) then the file is considered removed
    Snapshot snapshot = deltaLog.snapshot();
    Map<String, Action> previousFiles =
        snapshot.allFiles().collectAsList().stream()
            .map(AddFile::remove)
            .collect(
                Collectors.toMap(
                    file -> DeltaActionsConverter.getFullPathToFile(snapshot, file.path()),
                    file -> file));

    FilesDiff<InternalDataFile, Action> diff =
        DataFilesDiff.findNewAndRemovedFiles(partitionedDataFiles, previousFiles);

    return applyDiff(
        diff.getFilesAdded(), diff.getFilesRemoved(), tableSchema, deltaLog.dataPath().toString());
  }

  public Seq<Action> applyDiff(
      DataFilesDiff dataFilesDiff, InternalSchema tableSchema, String tableBasePath) {
    List<Action> removeActions =
        dataFilesDiff.getFilesRemoved().stream()
            .flatMap(dFile -> createAddFileAction(dFile, tableSchema, tableBasePath))
            .map(AddFile::remove)
            .collect(CustomCollectors.toList(dataFilesDiff.getFilesRemoved().size()));
    return applyDiff(dataFilesDiff.getFilesAdded(), removeActions, tableSchema, tableBasePath);
  }

  private Seq<Action> applyDiff(
      Set<InternalDataFile> filesAdded,
      Collection<Action> removeFileActions,
      InternalSchema tableSchema,
      String tableBasePath) {
    Stream<Action> addActions =
        filesAdded.stream()
            .flatMap(dFile -> createAddFileAction(dFile, tableSchema, tableBasePath));
    int totalActions = filesAdded.size() + removeFileActions.size();
    List<Action> allActions =
        Stream.concat(addActions, removeFileActions.stream())
            .collect(CustomCollectors.toList(totalActions));
    return JavaConverters.asScalaBuffer(allActions).toSeq();
  }

  private Stream<AddFile> createAddFileAction(
      InternalDataFile dataFile, InternalSchema schema, String tableBasePath) {
    return Stream.of(
        new AddFile(
            // Delta Lake supports relative and absolute paths in theory but relative paths seem
            // more commonly supported by query engines in our testing
            PathUtils.getRelativePath(dataFile.getPhysicalPath(), tableBasePath),
            convertJavaMapToScala(deltaPartitionExtractor.partitionValueSerialization(dataFile)),
            dataFile.getFileSizeBytes(),
            dataFile.getLastModified(),
            true,
            getColumnStats(schema, dataFile.getRecordCount(), dataFile.getColumnStats()),
            null,
            null));
  }

  private String getColumnStats(
      InternalSchema schema, long recordCount, List<ColumnStat> columnStats) {
    try {
      return deltaStatsExtractor.convertStatsToDeltaFormat(schema, recordCount, columnStats);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Exception during delta stats generation", e);
    }
  }
}
