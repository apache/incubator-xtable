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

import static org.apache.xtable.delta.DeltaActionsConverter.getFullPathToFile;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import scala.collection.JavaConverters;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.DataFileStatus;
import io.delta.kernel.utils.FileStatus;

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
      FileStatus addFile,
      Snapshot deltaSnapshot,
      FileFormat fileFormat,
      List<InternalPartitionField> partitionFields,
      List<InternalField> fields,
      boolean includeColumnStats,
      DeltaKernelPartitionExtractor partitionExtractor,
      DeltaKernelStatsExtractor fileStatsExtractor,
      Map<String, String> partitionValues) {
    DataFileStatus dataFileStatus =
        new DataFileStatus(
            addFile.getPath(),
            addFile.getModificationTime(),
            addFile.getSize(),
            Optional.empty() // or Optional.empty() if not available
            );
    System.out.println("dataFileStatus:" + dataFileStatus);
    FileStats fileStats = fileStatsExtractor.getColumnStatsForFile(dataFileStatus, fields);
    System.out.println("fileStats:" + fileStats);
    List<ColumnStat> columnStats =
        includeColumnStats ? fileStats.getColumnStats() : Collections.emptyList();
    long recordCount = fileStats.getNumRecords();
    Configuration hadoopConf = new Configuration();
    Engine myEngine = DefaultEngine.create(hadoopConf);
    Table myTable = Table.forPath(myEngine, addFile.getPath());
    // The immutable map from Java to Scala is not working, need to
    scala.collection.mutable.Map<String, String> scalaMap =
        JavaConverters.mapAsScalaMap(partitionValues);

    return InternalDataFile.builder()
        .physicalPath(getFullPathToFile(deltaSnapshot, addFile.getPath(), myTable))
        .fileFormat(fileFormat)
        .fileSizeBytes(addFile.getSize())
        .lastModified(addFile.getModificationTime())
        .partitionValues(partitionExtractor.partitionValueExtraction(scalaMap, partitionFields))
        .columnStats(columnStats)
        .recordCount(recordCount)
        .build();
  }

  //
  //    public InternalDataFile convertRemoveActionToInternalDataFile(
  //            RemoveFile removeFile,
  //            Snapshot deltaSnapshot,
  //            FileFormat fileFormat,
  //            List<InternalPartitionField> partitionFields,
  //            DeltaPartitionExtractor partitionExtractor) {
  //        return InternalDataFile.builder()
  //                .physicalPath(getFullPathToFile(deltaSnapshot, removeFile.path()))
  //                .fileFormat(fileFormat)
  //                .partitionValues(
  //                        partitionExtractor.partitionValueExtraction(
  //                                removeFile.partitionValues(), partitionFields))
  //                .build();
  //    }

  public FileFormat convertToFileFormat(String provider) {
    if (provider.equals("parquet")) {
      return FileFormat.APACHE_PARQUET;
    } else if (provider.equals("orc")) {
      return FileFormat.APACHE_ORC;
    }
    throw new NotSupportedException(
        String.format("delta file format %s is not recognized", provider));
  }

  static String getFullPathToFile(Snapshot snapshot, String dataFilePath, Table myTable) {
    Configuration hadoopConf = new Configuration();
    Engine myEngine = DefaultEngine.create(hadoopConf);

    String tableBasePath = myTable.getPath(myEngine);
    //        String tableBasePath = snapshot.dataPath().toUri().toString();
    if (dataFilePath.startsWith(tableBasePath)) {
      return dataFilePath;
    }
    return tableBasePath + Path.SEPARATOR + dataFilePath;
  }

  /**
   * Extracts the representation of the deletion vector information corresponding to an AddFile
   * action. Currently, this method extracts and returns the path to the data file for which a
   * deletion vector data is present.
   *
   * @param snapshot the commit snapshot
   * @param addFile the add file action
   * @return the deletion vector representation (path of data file), or null if no deletion vector
   *     is present
   */
  //    public String extractDeletionVectorFile(Snapshot snapshot, AddFile addFile) {
  //        DeletionVectorDescriptor deletionVector = addFile.deletionVector();
  //        if (deletionVector == null) {
  //            return null;
  //        }
  //
  //        String dataFilePath = addFile.path();
  //        return getFullPathToFile(snapshot, dataFilePath);
  //    }
}
