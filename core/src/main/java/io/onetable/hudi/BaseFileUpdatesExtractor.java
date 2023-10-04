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

import static io.onetable.hudi.HudiSchemaExtractor.convertFromOneTablePath;

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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.NonNull;
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
import io.onetable.model.schema.OneType;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.spi.DefaultSnapshotVisitor;

@AllArgsConstructor(staticName = "of")
public class BaseFileUpdatesExtractor {
  private static final Pattern HUDI_BASE_FILE_PATTERN =
      Pattern.compile(
          "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-[0-9]_[0-9a-fA-F-]+_[0-9]+\\.");
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
    // Track the partitions that are not present in the snapshot, so the files for those partitions
    // can be dropped
    Set<String> partitionPathsToDrop =
        new HashSet<>(
            FSUtils.getAllPartitionPaths(
                engineContext, metadataConfig, metaClient.getBasePathV2().toString()));
    ReplaceMetadata replaceMetadata =
        snapshotFiles.getFiles().stream()
            .map(
                file -> {
                  // remove the partition from the set of partitions to drop since it is present in
                  // the snapshot
                  partitionPathsToDrop.remove(file.getPartitionPath());
                  // create a map of file path to the data file, any entries not in the hudi table
                  // will be added
                  Map<String, OneDataFile> snapshotPathsForPartition =
                      DefaultSnapshotVisitor.extractDataFilePaths(
                          OneDataFiles.collectionBuilder()
                              .files(Collections.singletonList(file))
                              .build());
                  List<String> fileIdsToRemove =
                      fsView
                          .getLatestBaseFiles(file.getPartitionPath())
                          .map(
                              baseFile -> {
                                String baseFilePath = baseFile.getPath();
                                // remove the file from the map, if it was present, so it is not
                                // added to the commit since it is already present in the Hudi table
                                OneDataFile snapshotFile =
                                    snapshotPathsForPartition.remove(baseFilePath);
                                // if snapshotFile was not found, it means this file was deleted in
                                // the source and needs to be removed from the Hudi table
                                return snapshotFile == null ? baseFile.getFileId() : null;
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
                      fileIdsToRemove.isEmpty()
                          ? Collections.emptyMap()
                          : Collections.singletonMap(file.getPartitionPath(), fileIdsToRemove),
                      writeStatuses);
                })
            .reduce(ReplaceMetadata::combine)
            .orElse(ReplaceMetadata.EMPTY);
    // treat any partitions not present in the snapshot as dropped
    Optional<ReplaceMetadata> droppedPartitions =
        partitionPathsToDrop.stream()
            .map(
                partition -> {
                  List<String> fileIdsToRemove =
                      fsView
                          .getLatestBaseFiles(partition)
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
  ReplaceMetadata convertDiff(@NonNull OneDataFilesDiff oneDataFilesDiff, @NonNull String commit) {
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
    String fileName = new Path(file.getPhysicalPath()).getName();
    // if file was created by Hudi use original fileId, otherwise use the file name as IDs
    if (isFileCreatedByHudiWriter(fileName)) {
      return FSUtils.getFileId(fileName);
    }
    return fileName;
  }

  /**
   * Checks if the file was created by Hudi. Assumes Hudi is creating files with the default fileId
   * length and format
   *
   * @param fileName the file name
   * @return true if the file was created by Hudi, false otherwise
   */
  private boolean isFileCreatedByHudiWriter(String fileName) {
    return HUDI_BASE_FILE_PATTERN.matcher(fileName).find();
  }

  private WriteStatus toWriteStatus(String tableBasePath, String commitTime, OneDataFile file) {
    WriteStatus writeStatus = new WriteStatus();
    String fileId = getFileId(file);
    String filePath = file.getPhysicalPath().substring(tableBasePath.length() + 1);
    String fileName = filePath.substring(file.getPartitionPath().length() + 1);
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
    writeStat.putRecordsStats(convertColStats(fileName, file.getColumnStats()));
    writeStatus.setStat(writeStat);
    return writeStatus;
  }

  private Map<String, HoodieColumnRangeMetadata<Comparable>> convertColStats(
      String fileName, Map<OneField, ColumnStat> columnStatMap) {
    return columnStatMap.entrySet().stream()
        .filter(
            entry -> !OneType.NON_SCALAR_TYPES.contains(entry.getKey().getSchema().getDataType()))
        .map(
            entry -> {
              OneField field = entry.getKey();
              ColumnStat columnStat = entry.getValue();
              return HoodieColumnRangeMetadata.<Comparable>create(
                  fileName,
                  convertFromOneTablePath(field.getPath()),
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
    private static final ReplaceMetadata EMPTY =
        ReplaceMetadata.of(Collections.emptyMap(), Collections.emptyList());
    Map<String, List<String>> partitionToReplacedFileIds;
    List<WriteStatus> writeStatuses;

    private ReplaceMetadata combine(ReplaceMetadata other) {
      Map<String, List<String>> partitionToReplacedFileIds =
          new HashMap<>(this.partitionToReplacedFileIds);
      partitionToReplacedFileIds.putAll(other.partitionToReplacedFileIds);
      List<WriteStatus> writeStatuses = new ArrayList<>(this.writeStatuses);
      writeStatuses.addAll(other.writeStatuses);
      return ReplaceMetadata.of(partitionToReplacedFileIds, writeStatuses);
    }
  }
}
