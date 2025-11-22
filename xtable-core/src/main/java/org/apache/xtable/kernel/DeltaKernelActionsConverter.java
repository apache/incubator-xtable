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
    FileStats fileStats = fileStatsExtractor.getColumnStatsForFile(addFile, fields);
    List<ColumnStat> columnStats =
        includeColumnStats ? fileStats.getColumnStats() : Collections.emptyList();
    long recordCount = fileStats.getNumRecords();

    java.util.Map<String, String> scalaMap = partitionValues;

    return InternalDataFile.builder()
        .physicalPath(getFullPathToFile(addFile.getPath(), table))
        .fileFormat(fileFormat)
        .fileSizeBytes(addFile.getSize())
        .lastModified(addFile.getModificationTime())
        .partitionValues(partitionExtractor.partitionValueExtraction(scalaMap, partitionFields))
        .columnStats(columnStats)
        .recordCount(recordCount)
        .build();
  }

  public InternalDataFile convertRemoveActionToInternalDataFile(
      RemoveFile removeFile,
      Table table,
      FileFormat fileFormat,
      List<InternalPartitionField> partitionFields,
      DeltaKernelPartitionExtractor partitionExtractor,
      Map<String, String> partitionValues) {
    java.util.Map<String, String> scalaMap = partitionValues;

    return InternalDataFile.builder()
        .physicalPath(getFullPathToFile(removeFile.getPath(), table))
        .fileFormat(fileFormat)
        .partitionValues(partitionExtractor.partitionValueExtraction(scalaMap, partitionFields))
        .build();
  }

  public FileFormat convertToFileFormat(String provider) {
    if (provider.equals("parquet")) {
      return FileFormat.APACHE_PARQUET;
    } else if (provider.equals("orc")) {
      return FileFormat.APACHE_ORC;
    }
    throw new NotSupportedException(
        String.format("delta file format %s is not recognized", provider));
  }

  static String getFullPathToFile(String dataFilePath, Table table) {
    Configuration hadoopConf = new Configuration();
    Engine myEngine = DefaultEngine.create(hadoopConf);
    String tableBasePath = table.getPath(myEngine);
    if (dataFilePath.startsWith(tableBasePath)) {
      return dataFilePath;
    }
    return tableBasePath + Path.SEPARATOR + dataFilePath;
  }
}
