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
 
package io.onetable.hudi;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.function.SerializableFunction;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.exception.HoodieIOException;

import io.onetable.exception.NotSupportedException;
import io.onetable.model.OneTable;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.SchemaVersion;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.Range;
import io.onetable.model.storage.FileFormat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;

/** Extracts all the datafiles for a single partition for Hudi. */
public class HudiPartitionDataFileExtractor
    implements SerializableFunction<HudiDataFileExtractor.PartitionInfo, OneDataFile> {
  private static final Logger LOG = LogManager.getLogger(HudiPartitionDataFileExtractor.class);
  private final HoodieTableMetaClient metaClient;
  private final OneTable table;
  private final HudiPartitionValuesExtractor partitionValuesExtractor;
  private final HoodieTimeline timeline;

  public HudiPartitionDataFileExtractor(
      HoodieTableMetaClient metaClient,
      OneTable table,
      HudiPartitionValuesExtractor partitionValuesExtractor,
      HoodieTimeline timeline) {
    this.metaClient = metaClient;
    if (metaClient.getTableConfig().getBaseFileFormat() != HoodieFileFormat.PARQUET) {
      throw new NotSupportedException("File formats other than Parquet are not yet supported");
    }
    this.table = table;
    this.partitionValuesExtractor = partitionValuesExtractor;
    this.timeline = timeline;
  }

  @Override
  public OneDataFile apply(HudiDataFileExtractor.PartitionInfo partitionInfo) {
    // Extract the partition values for the current partition
    Map<OnePartitionField, Range> partitionValues =
        partitionValuesExtractor.extractPartitionValues(
            table.getPartitioningFields(), partitionInfo.getPartitionPath());

    HoodieTableFileSystemView fsView =
        new HoodieTableFileSystemView(metaClient, timeline, partitionInfo.getFileStatuses());

    SchemaVersion version = new SchemaVersion(1, null);

    // Avoid looking up file/column stats if the state is already tracked in OneTable
    Map<String, OneDataFile> pathToExistingFileDetails =
        partitionInfo.getExistingFileDetails() == null
            ? Collections.emptyMap()
            : partitionInfo.getExistingFileDetails().getFiles().stream()
                .collect(Collectors.toMap(OneDataFile::getPhysicalPath, Function.identity()));
    List<OneDataFile> partitionFiles =
        fsView
            .getLatestBaseFiles()
            .parallel()
            .map(
                hoodieBaseFile -> {
                  if (pathToExistingFileDetails.containsKey(hoodieBaseFile.getPath())) {
                    return pathToExistingFileDetails.get(hoodieBaseFile.getPath());
                  }
                  long rowCount;
                  Map<OneField, ColumnStat> columnStatMap;
                  try {
                    HudiFileStats hudiFileStats =
                        HudiFileStatsExtractor.getInstance()
                            .computeColumnStatsForFile(
                                hoodieBaseFile.getFileStatus().getPath(),
                                metaClient.getHadoopConf(),
                                table.getReadSchema());
                    rowCount = hudiFileStats.getRowCount();
                    columnStatMap = hudiFileStats.getColumnStats();
                  } catch (HoodieIOException ex) {
                    // This should only happen when referencing a commit that has files cleaned by
                    // Hudi's cleaner
                    LOG.error(
                        "Unable to get rowCount or columns stats for file: "
                            + hoodieBaseFile.getPath(),
                        ex);
                    rowCount = -1L;
                    columnStatMap = Collections.emptyMap();
                  }
                  return OneDataFile.builder()
                      .schemaVersion(version)
                      .physicalPath(hoodieBaseFile.getPath())
                      .fileFormat(getFileFormat(FSUtils.getFileExtension(hoodieBaseFile.getPath())))
                      .partitionPath(partitionInfo.getPartitionPath())
                      .partitionValues(partitionValues)
                      .fileSizeBytes(hoodieBaseFile.getFileSize())
                      .recordCount(rowCount)
                      .columnStats(columnStatMap)
                      .lastModified(hoodieBaseFile.getFileStatus().getModificationTime())
                      .build();
                })
            .collect(Collectors.toList());
    fsView.close();
    return OneDataFiles.collectionBuilder()
        .partitionPath(partitionInfo.getPartitionPath())
        .files(partitionFiles)
        .build();
  }

  private FileFormat getFileFormat(String extension) {
    if (HoodieFileFormat.PARQUET.getFileExtension().equals(extension)) {
      return FileFormat.APACHE_PARQUET;
    } else if (HoodieFileFormat.ORC.getFileExtension().equals(extension)) {
      return FileFormat.APACHE_ORC;
    } else {
      throw new UnsupportedOperationException("Unknown Hudi Fileformat " + extension);
    }
  }
}
