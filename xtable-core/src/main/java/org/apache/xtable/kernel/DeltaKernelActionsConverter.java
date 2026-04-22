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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.FileStats;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaKernelActionsConverter {
  private static final DeltaKernelActionsConverter INSTANCE = new DeltaKernelActionsConverter();

  public static DeltaKernelActionsConverter getInstance() {
    return INSTANCE;
  }

  /**
   * Converts AddFile to InternalDataFile using cached table base path (most efficient).
   *
   * @param tableBasePath cached table base path from table.getPath(engine)
   */
  public InternalDataFile convertAddActionToInternalDataFile(
      AddFile addFile,
      String tableBasePath,
      FileFormat fileFormat,
      List<InternalPartitionField> partitionFields,
      List<InternalField> fields,
      boolean includeColumnStats,
      DeltaKernelPartitionExtractor partitionExtractor,
      DeltaKernelStatsExtractor fileStatsExtractor,
      Map<String, String> partitionValues) {
    FileStats fileStats = fileStatsExtractor.getColumnStatsForFile(addFile, fields);
    List<ColumnStat> columnStats =
        includeColumnStats ? fileStats.getColumnStats() : Collections.emptyList();
    long recordCount = fileStats.getNumRecords();

    Map<String, String> scalaMap = partitionValues;

    return InternalDataFile.builder()
        .physicalPath(getFullPathToFile(addFile.getPath(), tableBasePath))
        .fileFormat(fileFormat)
        .fileSizeBytes(addFile.getSize())
        .lastModified(addFile.getModificationTime())
        .partitionValues(partitionExtractor.partitionValueExtraction(scalaMap, partitionFields))
        .columnStats(columnStats)
        .recordCount(recordCount)
        .build();
  }

  /**
   * Converts AddFile to InternalDataFile (deprecated - inefficient).
   *
   * @deprecated Use {@link #convertAddActionToInternalDataFile(AddFile, String, FileFormat, List,
   *     List, boolean, DeltaKernelPartitionExtractor, DeltaKernelStatsExtractor, Map)} with cached
   *     tableBasePath instead to avoid creating new Engine per call
   */
  @Deprecated
  public InternalDataFile convertAddActionToInternalDataFile(
      AddFile addFile,
      Table table,
      FileFormat fileFormat,
      List<InternalPartitionField> partitionFields,
      List<InternalField> fields,
      boolean includeColumnStats,
      DeltaKernelPartitionExtractor partitionExtractor,
      DeltaKernelStatsExtractor fileStatsExtractor,
      Map<String, String> partitionValues) {
    // WARNING: Creates new Configuration + Engine per call - inefficient!
    FileStats fileStats = fileStatsExtractor.getColumnStatsForFile(addFile, fields);
    List<ColumnStat> columnStats =
        includeColumnStats ? fileStats.getColumnStats() : Collections.emptyList();
    long recordCount = fileStats.getNumRecords();

    Map<String, String> scalaMap = partitionValues;

    return InternalDataFile.builder()
        .physicalPath(getFullPathToFile(addFile.getPath(), table)) // Inefficient!
        .fileFormat(fileFormat)
        .fileSizeBytes(addFile.getSize())
        .lastModified(addFile.getModificationTime())
        .partitionValues(partitionExtractor.partitionValueExtraction(scalaMap, partitionFields))
        .columnStats(columnStats)
        .recordCount(recordCount)
        .build();
  }

  /**
   * Converts RemoveFile to InternalDataFile using cached table base path (most efficient).
   *
   * @param tableBasePath cached table base path from table.getPath(engine)
   */
  public InternalDataFile convertRemoveActionToInternalDataFile(
      RemoveFile removeFile,
      String tableBasePath,
      FileFormat fileFormat,
      List<InternalPartitionField> partitionFields,
      DeltaKernelPartitionExtractor partitionExtractor,
      Map<String, String> partitionValues) {
    Map<String, String> scalaMap = partitionValues;

    return InternalDataFile.builder()
        .physicalPath(getFullPathToFile(removeFile.getPath(), tableBasePath))
        .fileFormat(fileFormat)
        .partitionValues(partitionExtractor.partitionValueExtraction(scalaMap, partitionFields))
        .build();
  }

  /**
   * Converts RemoveFile to InternalDataFile (deprecated - inefficient).
   *
   * @deprecated Use {@link #convertRemoveActionToInternalDataFile(RemoveFile, String, FileFormat,
   *     List, DeltaKernelPartitionExtractor, Map)} with cached tableBasePath instead to avoid
   *     creating new Engine per call
   */
  @Deprecated
  public InternalDataFile convertRemoveActionToInternalDataFile(
      RemoveFile removeFile,
      Table table,
      FileFormat fileFormat,
      List<InternalPartitionField> partitionFields,
      DeltaKernelPartitionExtractor partitionExtractor,
      Map<String, String> partitionValues) {
    // WARNING: Creates new Configuration + Engine per call - inefficient!
    Map<String, String> scalaMap = partitionValues;

    return InternalDataFile.builder()
        .physicalPath(getFullPathToFile(removeFile.getPath(), table)) // Inefficient!
        .fileFormat(fileFormat)
        .partitionValues(partitionExtractor.partitionValueExtraction(scalaMap, partitionFields))
        .build();
  }

  public FileFormat convertToFileFormat(String provider) {
    if (provider.equalsIgnoreCase("parquet")) {
      return FileFormat.APACHE_PARQUET;
    } else if (provider.equalsIgnoreCase("orc")) {
      return FileFormat.APACHE_ORC;
    }
    throw new NotSupportedException(
        String.format("delta file format %s is not recognized", provider));
  }

  /**
   * Constructs the full path to a file, handling both relative and absolute paths.
   *
   * <p><strong>DEPRECATED:</strong> This method creates a new Configuration and Engine on every
   * call, which is extremely inefficient. Use {@link #getFullPathToFile(String, Engine, Table)} or
   * {@link #getFullPathToFile(String, String)} instead.
   *
   * @param dataFilePath the data file path (relative or absolute)
   * @param table the Delta table
   * @return the full absolute path to the file
   * @deprecated Use {@link #getFullPathToFile(String, Engine, Table)} to avoid creating new
   *     Engine/Configuration on every call, or {@link #getFullPathToFile(String, String)} if you
   *     already have the base path
   */
  @Deprecated
  static String getFullPathToFile(String dataFilePath, Table table) {
    // WARNING: This creates new Configuration + Engine per call - severe performance issue!
    // Kept for backwards compatibility but should not be used in hot paths
    Configuration hadoopConf = new Configuration();
    Engine myEngine = DefaultEngine.create(hadoopConf);
    return getFullPathToFile(dataFilePath, myEngine, table);
  }

  /**
   * Constructs the full path to a file using a provided Engine (efficient).
   *
   * @param dataFilePath the data file path (relative or absolute)
   * @param engine the Delta Kernel engine to use for path resolution
   * @param table the Delta table
   * @return the full absolute path to the file
   */
  static String getFullPathToFile(String dataFilePath, Engine engine, Table table) {
    String tableBasePath = table.getPath(engine);
    return getFullPathToFile(dataFilePath, tableBasePath);
  }

  /**
   * Constructs the full path to a file using a provided base path (most efficient).
   *
   * @param dataFilePath the data file path (relative or absolute)
   * @param tableBasePath the table base path
   * @return the full absolute path to the file
   */
  static String getFullPathToFile(String dataFilePath, String tableBasePath) {
    if (dataFilePath.startsWith(tableBasePath)) {
      return dataFilePath;
    }
    return tableBasePath + Path.SEPARATOR + dataFilePath;
  }
}
