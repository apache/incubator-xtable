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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.Value;

import org.apache.hadoop.fs.Path;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;

import io.onetable.model.OneTable;
import io.onetable.model.schema.OneField;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.spi.DefaultSnapshotVisitor;

@AllArgsConstructor(staticName = "of")
public class BaseFileUpdatesExtractor {
  private final HoodieEngineContext engineContext;
  private final String tableBasePath;

  /**
   * Extracts the changes between the snapshot files and the base files in the Hudi table currently.
   *
   * @param snapshotFiles the snapshot files, assumes that they are grouped by partition like {@link
   *     io.onetable.spi.extractor.SourceClient#getFilesForAllPartitions(Object, OneTable)} returns.
   * @param metaClient the meta client for the Hudi table with the latest timeline state loaded
   * @param commit The current commit started by the Hudi client
   * @return The information needed to create a "replace" commit for the Hudi table
   */
  ReplaceMetadata extractSnapshotChanges(
      OneDataFiles snapshotFiles, HoodieTableMetaClient metaClient, String commit) {
    HoodieMetadataConfig metadataConfig =
        HoodieMetadataConfig.newBuilder()
            .enable(metaClient.getTableConfig().isMetadataTableAvailable())
            .build();
    HoodieTableFileSystemView fsView =
        new HoodieMetadataFileSystemView(
            engineContext, metaClient, metaClient.getActiveTimeline(), metadataConfig);
    Set<String> partitions =
        new HashSet<>(
            FSUtils.getAllPartitionPaths(
                engineContext, metadataConfig, metaClient.getBasePathV2().toString()));
    ReplaceMetadata replaceMetadata =
        snapshotFiles.getFiles().stream()
            .map(
                file -> {
                  partitions.remove(file.getPartitionPath());
                  Map<String, OneDataFile> snapshotPathsForPartition =
                      DefaultSnapshotVisitor.extractDataFilePaths(
                          OneDataFiles.collectionBuilder()
                              .files(Collections.singletonList(file))
                              .build());
                  List<String> fileIdsToRemove =
                      fsView
                          .getAllBaseFiles(file.getPartitionPath())
                          .map(
                              baseFile -> {
                                String baseFilePath = baseFile.getPath();
                                OneDataFile snapshotFile =
                                    snapshotPathsForPartition.remove(baseFilePath);
                                // if snapshotFile was not found, it means this file was deleted
                                return snapshotFile == null ? null : baseFile.getFileId();
                              })
                          .filter(Objects::nonNull)
                          .collect(Collectors.toList());
                  // for the remaining entries in the map, add the files to the commit
                  List<WriteStatus> writeStatuses =
                      snapshotPathsForPartition.values().stream()
                          .map(
                              snapshotFile ->
                                  toWriteStatus(
                                      metaClient.getBasePathV2().toString(), commit, snapshotFile))
                          .collect(Collectors.toList());
                  return ReplaceMetadata.of(
                      Collections.singletonMap(file.getPartitionPath(), fileIdsToRemove),
                      writeStatuses);
                })
            .reduce(ReplaceMetadata::combine)
            .orElseGet(ReplaceMetadata::createEmptyInstance);
    // treat any partitions not present in the snapshot as dropped
    Optional<ReplaceMetadata> droppedPartitions =
        partitions.stream()
            .map(
                partition -> {
                  List<String> fileIdsToRemove =
                      fsView
                          .getAllBaseFiles(partition)
                          .map(HoodieBaseFile::getFileId)
                          .collect(Collectors.toList());
                  return ReplaceMetadata.of(
                      Collections.singletonMap(partition, fileIdsToRemove),
                      Collections.emptyList());
                })
            .reduce(ReplaceMetadata::combine);
    return droppedPartitions.map(replaceMetadata::combine).orElse(replaceMetadata);
  }

  /**
   * Converts the provided {@link OneDataFilesDiff}.
   *
   * @param oneDataFilesDiff the diff to apply to the Hudi table
   * @param commit The current commit started by the Hudi client
   * @return The information needed to create a "replace" commit for the Hudi table
   */
  ReplaceMetadata convertDiff(OneDataFilesDiff oneDataFilesDiff, String commit) {
    // For all removed files, group by partition and extract the file id
    Map<String, List<String>> partitionToReplacedFileIds =
        oneDataFilesDiff.getFilesRemoved().stream()
            .collect(
                Collectors.groupingBy(
                    OneDataFile::getPartitionPath,
                    Collectors.mapping(this::getFileId, Collectors.toList())));
    // For all added files, group by partition and extract the file id
    List<WriteStatus> writeStatuses =
        oneDataFilesDiff.getFilesAdded().stream()
            .map(file -> toWriteStatus(tableBasePath, commit, file))
            .collect(Collectors.toList());
    return ReplaceMetadata.of(partitionToReplacedFileIds, writeStatuses);
  }

  private String getFileId(OneDataFile file) {
    return new Path(file.getPhysicalPath()).getName();
  }

  private WriteStatus toWriteStatus(String tableBasePath, String commitTime, OneDataFile file) {
    WriteStatus writeStatus = new WriteStatus();
    String fileId = getFileId(file);
    String filePath = file.getPhysicalPath().substring(tableBasePath.length());
    writeStatus.setFileId(fileId);
    writeStatus.setPartitionPath(file.getPartitionPath());
    HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
    writeStat.setFileId(fileId);
    writeStat.setPath(
        ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(filePath, commitTime));
    writeStat.setPartitionPath(file.getPartitionPath());
    writeStat.setNumWrites(file.getRecordCount());
    writeStat.setTotalWriteBytes(file.getFileSizeBytes());
    writeStat.setFileSizeInBytes(file.getFileSizeBytes());
    writeStat.putRecordsStats(convertColStats(filePath, file.getColumnStats()));
    writeStatus.setStat(writeStat);
    return writeStatus;
  }

  private Map<String, HoodieColumnRangeMetadata<Comparable>> convertColStats(
      String filePath, Map<OneField, ColumnStat> columnStatMap) {
    return columnStatMap.entrySet().stream()
        .map(
            entry -> {
              OneField field = entry.getKey();
              ColumnStat columnStat = entry.getValue();
              return HoodieColumnRangeMetadata.<Comparable>create(
                  filePath,
                  field.getName(),
                  (Comparable) columnStat.getRange().getMinValue(),
                  (Comparable) columnStat.getRange().getMaxValue(),
                  columnStat.getNumNulls(),
                  columnStat.getNumValues(),
                  columnStat.getTotalSize(),
                  -1L);
            })
        .collect(Collectors.toMap(HoodieColumnRangeMetadata::getColumnName, Function.identity()));
  }

  /** Holds the information needed to create a "replace" commit in the Hudi table. */
  @AllArgsConstructor(staticName = "of")
  @Value
  public static class ReplaceMetadata {
    Map<String, List<String>> partitionToReplacedFileIds;
    List<WriteStatus> writeStatuses;

    private ReplaceMetadata combine(ReplaceMetadata other) {
      partitionToReplacedFileIds.putAll(other.partitionToReplacedFileIds);
      writeStatuses.addAll(other.writeStatuses);
      return this;
    }

    private static ReplaceMetadata createEmptyInstance() {
      // does not use Collections.emptyMap() or Collections.emptyList() so the combine function can
      // be called on this instance without failing
      return ReplaceMetadata.of(new HashMap<>(), new ArrayList<>());
    }
  }
}
