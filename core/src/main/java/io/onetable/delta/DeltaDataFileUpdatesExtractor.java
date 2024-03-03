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
package io.onetable.delta;

import static io.onetable.collectors.CustomCollectors.toList;
import static io.onetable.delta.ScalaUtils.convertJavaMapToScala;

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

import io.onetable.model.schema.OneSchema;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.storage.DataFilesDiff;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.model.storage.OneFileGroup;
import io.onetable.paths.PathUtils;

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
      DeltaLog deltaLog, List<OneFileGroup> partitionedDataFiles, OneSchema tableSchema) {

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

    DataFilesDiff<OneDataFile, Action> diff =
        OneDataFilesDiff.findNewAndRemovedFiles(partitionedDataFiles, previousFiles);

    return applyDiff(
        diff.getFilesAdded(), diff.getFilesRemoved(), tableSchema, deltaLog.dataPath().toString());
  }

  public Seq<Action> applyDiff(
      OneDataFilesDiff oneDataFilesDiff, OneSchema tableSchema, String tableBasePath) {
    List<Action> removeActions =
        oneDataFilesDiff.getFilesRemoved().stream()
            .flatMap(dFile -> createAddFileAction(dFile, tableSchema, tableBasePath))
            .map(AddFile::remove)
            .collect(toList(oneDataFilesDiff.getFilesRemoved().size()));
    return applyDiff(oneDataFilesDiff.getFilesAdded(), removeActions, tableSchema, tableBasePath);
  }

  private Seq<Action> applyDiff(
      Set<OneDataFile> filesAdded,
      Collection<Action> removeFileActions,
      OneSchema tableSchema,
      String tableBasePath) {
    Stream<Action> addActions =
        filesAdded.stream()
            .flatMap(dFile -> createAddFileAction(dFile, tableSchema, tableBasePath));
    int totalActions = filesAdded.size() + removeFileActions.size();
    List<Action> allActions =
        Stream.concat(addActions, removeFileActions.stream()).collect(toList(totalActions));
    return JavaConverters.asScalaBuffer(allActions).toSeq();
  }

  private Stream<AddFile> createAddFileAction(
      OneDataFile dataFile, OneSchema schema, String tableBasePath) {
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

  private String getColumnStats(OneSchema schema, long recordCount, List<ColumnStat> columnStats) {
    try {
      return deltaStatsExtractor.convertStatsToDeltaFormat(schema, recordCount, columnStats);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Exception during delta stats generation", e);
    }
  }
}
