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

import io.onetable.model.storage.PartitionedDataFiles;
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
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.spi.extractor.PartitionedDataFileIterator;

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
      DeltaLog deltaLog, PartitionedDataFiles partitionedDataFiles, OneSchema tableSchema) {
    List<OneDataFile> currentDataFiles = new ArrayList<>();
    try (PartitionedDataFileIterator fileIterator =
        deltaDataFileExtractor.iteratorWithoutStats(deltaLog.snapshot(), tableSchema)) {
      fileIterator.forEachRemaining(currentDataFiles::add);
      OneDataFilesDiff filesDiff = OneDataFilesDiff.from(currentDataFiles, partitionedDataFiles.getAllFiles());
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
    if (dataFile instanceof OneDataFiles) {
      return ((OneDataFiles) dataFile)
          .getFiles().stream()
              .flatMap(childDataFile -> createAddFileAction(childDataFile, schema, tableBasePath));
    }
    return Stream.of(
        new AddFile(
            // Delta Lake supports relative and absolute paths in theory but relative paths seem
            // more commonly supported by query engines in our testing
            getRelativePath(dataFile.getPhysicalPath(), tableBasePath),
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

  private String getRelativePath(String fullFilePath, String tableBasePath) {
    if (fullFilePath.startsWith(tableBasePath)) {
      return fullFilePath.substring(tableBasePath.length() + 1);
    }
    return fullFilePath;
  }
}
