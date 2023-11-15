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

import static io.onetable.delta.ScalaUtils.convertJavaMapToScala;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;

import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.actions.Action;
import org.apache.spark.sql.delta.actions.AddFile;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import com.fasterxml.jackson.core.JsonProcessingException;

import io.onetable.exception.OneIOException;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.model.storage.OneFileGroup;
import io.onetable.paths.PathUtils;
import io.onetable.spi.extractor.DataFileIterator;

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
    List<OneDataFile> currentDataFiles = new ArrayList<>();
    try (DataFileIterator fileIterator =
        deltaDataFileExtractor.iteratorWithoutStats(deltaLog.snapshot(), tableSchema)) {
      fileIterator.forEachRemaining(currentDataFiles::add);
      OneDataFilesDiff filesDiff =
          OneDataFilesDiff.from(
              partitionedDataFiles.stream()
                  .flatMap(group -> group.getFiles().stream())
                  .collect(Collectors.toList()),
              currentDataFiles);
      return applyDiff(filesDiff, tableSchema, deltaLog.dataPath().toString());
    } catch (Exception e) {
      throw new OneIOException("Failed to iterate through Delta data files", e);
    }
  }

  public Seq<Action> applyDiff(
      OneDataFilesDiff oneDataFilesDiff, OneSchema tableSchema, String tableBasePath) {
    List<Action> allActions = new ArrayList<>();
    allActions.addAll(
        oneDataFilesDiff.getFilesAdded().stream()
            .flatMap(dFile -> createAddFileAction(dFile, tableSchema, tableBasePath))
            .collect(Collectors.toList()));
    allActions.addAll(
        oneDataFilesDiff.getFilesRemoved().stream()
            .flatMap(dFile -> createAddFileAction(dFile, tableSchema, tableBasePath))
            .map(AddFile::remove)
            .collect(Collectors.toList()));
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
            null));
  }

  private String getColumnStats(
      OneSchema schema, long recordCount, Map<OneField, ColumnStat> columnStats) {
    try {
      return deltaStatsExtractor.convertStatsToDeltaFormat(schema, recordCount, columnStats);
    } catch (JsonProcessingException e) {
      throw new RuntimeException("Exception during delta stats generation", e);
    }
  }
}
