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

import org.apache.hadoop.fs.Path;

import io.delta.kernel.Table;
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

    return InternalDataFile.builder()
        .physicalPath(getFullPathToFile(addFile.getPath(), tableBasePath))
        .fileFormat(fileFormat)
        .fileSizeBytes(addFile.getSize())
        .lastModified(addFile.getModificationTime())
        .partitionValues(
            partitionExtractor.partitionValueExtraction(partitionValues, partitionFields))
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
    return InternalDataFile.builder()
        .physicalPath(getFullPathToFile(removeFile.getPath(), tableBasePath))
        .fileFormat(fileFormat)
        .partitionValues(
            partitionExtractor.partitionValueExtraction(partitionValues, partitionFields))
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
    // Check if the file path is already absolute and under the table base path
    // Use separator check to avoid false positives (e.g., "/foo" matching "/foobar/x.parquet")
    String basePathWithSeparator =
        tableBasePath.endsWith(Path.SEPARATOR) ? tableBasePath : tableBasePath + Path.SEPARATOR;
    if (dataFilePath.startsWith(basePathWithSeparator)) {
      return dataFilePath;
    }
    return basePathWithSeparator + dataFilePath;
  }
}
