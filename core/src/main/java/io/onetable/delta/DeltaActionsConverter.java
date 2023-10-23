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

import java.util.Collections;
import java.util.List;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.hadoop.fs.Path;

import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.AddFile;
import org.apache.spark.sql.delta.actions.RemoveFile;

import io.onetable.exception.NotSupportedException;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.storage.FileFormat;
import io.onetable.model.storage.OneDataFile;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaActionsConverter {

  private static final DeltaActionsConverter INSTANCE = new DeltaActionsConverter();

  public static DeltaActionsConverter getInstance() {
    return INSTANCE;
  }

  public OneDataFile convertAddActionToOneDataFile(
      AddFile addFile,
      Snapshot deltaSnapshot,
      FileFormat fileFormat,
      List<OnePartitionField> partitionFields,
      List<OneField> fields,
      boolean includeColumnStats,
      DeltaPartitionExtractor partitionExtractor,
      DeltaStatsExtractor fileStatsExtractor) {
    String tableBasePath = deltaSnapshot.deltaLog().dataPath().toUri().toString();
    // TODO(vamshigv): removed record count as delta api downgraded to 2.0.2
    return OneDataFile.builder()
        .physicalPath(getFullPathToFile(tableBasePath, addFile.path()))
        .fileFormat(fileFormat)
        .fileSizeBytes(addFile.size())
        .lastModified(addFile.modificationTime())
        .partitionValues(
            partitionExtractor.partitionValueExtraction(addFile.partitionValues(), partitionFields))
        .columnStats(
            includeColumnStats
                ? fileStatsExtractor.getColumnStatsForFile(addFile, fields)
                : Collections.emptyMap())
        .build();
  }

  public OneDataFile convertRemoveActionToOneDataFile(
      RemoveFile removeFile,
      Snapshot deltaSnapshot,
      FileFormat fileFormat,
      List<OnePartitionField> partitionFields,
      DeltaPartitionExtractor partitionExtractor) {
    String tableBasePath = deltaSnapshot.deltaLog().dataPath().toUri().toString();
    return OneDataFile.builder()
        .physicalPath(getFullPathToFile(tableBasePath, removeFile.path()))
        .fileFormat(fileFormat)
        .partitionValues(
            partitionExtractor.partitionValueExtraction(
                removeFile.partitionValues(), partitionFields))
        .build();
  }

  public FileFormat convertToOneTableFileFormat(String provider) {
    if (provider.equals("parquet")) {
      return FileFormat.APACHE_PARQUET;
    } else if (provider.equals("orc")) {
      return FileFormat.APACHE_ORC;
    }
    throw new NotSupportedException(
        String.format("delta file format %s is not recognized", provider));
  }

  private String getFullPathToFile(String tableBasePath, String path) {
    if (path.startsWith(tableBasePath)) {
      return path;
    }
    return tableBasePath + Path.SEPARATOR + path;
  }
}
