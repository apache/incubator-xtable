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
 
package org.apache.xtable.hudi;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS;
import static org.apache.hudi.metadata.HoodieTableMetadataUtil.existingIndexVersionOrDefault;
import static org.apache.xtable.hudi.HudiSchemaExtractor.convertFromXTablePath;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;

import org.apache.hadoop.fs.Path;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieCommonConfig;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.hadoop.fs.CachingPath;
import org.apache.hudi.metadata.HoodieIndexVersion;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.stats.HoodieColumnRangeMetadata;
import org.apache.hudi.stats.ValueMetadata;
import org.apache.hudi.stats.XTableValueMetadata;

import org.apache.xtable.collectors.CustomCollectors;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;

@AllArgsConstructor(staticName = "of")
public class BaseFileUpdatesExtractor {
  private static final Pattern HUDI_BASE_FILE_PATTERN =
      Pattern.compile(
          "^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}-[0-9]_[0-9a-fA-F-]+_[0-9]+\\.");
  private final HoodieEngineContext engineContext;
  private final Path tableBasePath;

  /**
   * Extracts the changes between the snapshot files and the base files in the Hudi table currently.
   *
   * @param partitionedDataFiles files grouped by partition to sync
   * @param metaClient the meta client for the Hudi table with the latest timeline state loaded
   * @param commit The current commit started by the Hudi client
   * @return The information needed to create a "replace" commit for the Hudi table
   */
  @SneakyThrows
  ReplaceMetadata extractSnapshotChanges(
      List<PartitionFileGroup> partitionedDataFiles,
      HoodieTableMetaClient metaClient,
      String commit) {
    HoodieMetadataConfig metadataConfig =
        HoodieMetadataConfig.newBuilder()
            .enable(metaClient.getTableConfig().isMetadataTableAvailable())
            .build();
    HoodieTableMetadata tableMetadata =
        metadataConfig.isEnabled()
            ? metaClient
                .getTableFormat()
                .getMetadataFactory()
                .create(
                    engineContext,
                    metaClient.getStorage(),
                    metadataConfig,
                    tableBasePath.toString(),
                    true)
            : null;
    FileSystemViewManager fileSystemViewManager =
        FileSystemViewManager.createViewManager(
            engineContext,
            metadataConfig,
            FileSystemViewStorageConfig.newBuilder()
                .withStorageType(FileSystemViewStorageType.MEMORY)
                .build(),
            HoodieCommonConfig.newBuilder().build(),
            meta -> tableMetadata);
    try (SyncableFileSystemView fsView = fileSystemViewManager.getFileSystemView(metaClient)) {
      return extractFromFsView(partitionedDataFiles, commit, fsView, metaClient, metadataConfig);
    } finally {
      fileSystemViewManager.close();
      if (tableMetadata != null) {
        tableMetadata.close();
      }
    }
  }

  ReplaceMetadata extractFromFsView(
      List<PartitionFileGroup> partitionedDataFiles,
      String commit,
      SyncableFileSystemView fsView,
      HoodieTableMetaClient metaClient,
      HoodieMetadataConfig metadataConfig) {
    boolean isTableInitialized = metaClient.isTimelineNonEmpty();
    // Track the partitions that are not present in the snapshot, so the files for those partitions
    // can be dropped
    HoodieIndexVersion indexVersion =
        existingIndexVersionOrDefault(PARTITION_NAME_COLUMN_STATS, metaClient);
    Set<String> partitionPathsToDrop =
        new HashSet<>(FSUtils.getAllPartitionPaths(engineContext, metaClient, metadataConfig));
    ReplaceMetadata replaceMetadata =
        partitionedDataFiles.stream()
            .map(
                partitionFileGroup -> {
                  List<InternalDataFile> dataFiles = partitionFileGroup.getDataFiles();
                  String partitionPath = getPartitionPath(tableBasePath, dataFiles);
                  // remove the partition from the set of partitions to drop since it is present in
                  // the snapshot
                  partitionPathsToDrop.remove(partitionPath);
                  // create a map of file path to the data file, any entries not in the hudi table
                  // will be added
                  Map<String, InternalDataFile> physicalPathToFile =
                      dataFiles.stream()
                          .collect(
                              Collectors.toMap(
                                  InternalDataFile::getPhysicalPath, Function.identity()));
                  List<HoodieBaseFile> baseFiles =
                      isTableInitialized
                          ? fsView.getLatestBaseFiles(partitionPath).collect(Collectors.toList())
                          : Collections.emptyList();
                  Set<String> existingPaths =
                      baseFiles.stream().map(HoodieBaseFile::getPath).collect(Collectors.toSet());
                  // Mark fileIds for removal if the file paths are no longer present in the
                  // snapshot
                  List<String> fileIdsToRemove =
                      baseFiles.stream()
                          .filter(baseFile -> !physicalPathToFile.containsKey(baseFile.getPath()))
                          .map(HoodieBaseFile::getFileId)
                          .collect(Collectors.toList());
                  // for any entries in the map that are not in the set of existing paths, create a
                  // write status to add them to the Hudi table
                  List<WriteStatus> writeStatuses =
                      physicalPathToFile.entrySet().stream()
                          .filter(entry -> !existingPaths.contains(entry.getKey()))
                          .map(Map.Entry::getValue)
                          .map(
                              snapshotFile ->
                                  toWriteStatus(
                                      tableBasePath,
                                      commit,
                                      snapshotFile,
                                      Optional.of(partitionPath),
                                      indexVersion))
                          .collect(Collectors.toList());
                  return ReplaceMetadata.of(
                      fileIdsToRemove.isEmpty()
                          ? Collections.emptyMap()
                          : Collections.singletonMap(partitionPath, fileIdsToRemove),
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
    fsView.close();
    return droppedPartitions.map(replaceMetadata::combine).orElse(replaceMetadata);
  }

  /**
   * Converts the provided {@link InternalFilesDiff}.
   *
   * @param internalFilesDiff the diff to apply to the Hudi table
   * @param commit The current commit started by the Hudi client
   * @param indexVersion the Hudi index version
   * @return The information needed to create a "replace" commit for the Hudi table
   */
  ReplaceMetadata convertDiff(
      @NonNull InternalFilesDiff internalFilesDiff,
      @NonNull String commit,
      HoodieIndexVersion indexVersion) {
    // For all removed files, group by partition and extract the file id
    Map<String, List<String>> partitionToReplacedFileIds =
        internalFilesDiff.dataFilesRemoved().stream()
            .map(file -> new CachingPath(file.getPhysicalPath()))
            .collect(
                Collectors.groupingBy(
                    path -> HudiPathUtils.getPartitionPath(tableBasePath, path),
                    Collectors.mapping(this::getFileId, Collectors.toList())));
    // For all added files, group by partition and extract the file id
    List<WriteStatus> writeStatuses =
        internalFilesDiff.dataFilesAdded().stream()
            .map(file -> toWriteStatus(tableBasePath, commit, file, Optional.empty(), indexVersion))
            .collect(CustomCollectors.toList(internalFilesDiff.dataFilesAdded().size()));
    return ReplaceMetadata.of(partitionToReplacedFileIds, writeStatuses);
  }

  private String getFileId(Path filePath) {
    String fileName = filePath.getName();
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

  private WriteStatus toWriteStatus(
      Path tableBasePath,
      String commitTime,
      InternalDataFile file,
      Optional<String> partitionPathOptional,
      HoodieIndexVersion indexVersion) {
    WriteStatus writeStatus = new WriteStatus();
    Path path = new CachingPath(file.getPhysicalPath());
    String partitionPath =
        partitionPathOptional.orElseGet(() -> HudiPathUtils.getPartitionPath(tableBasePath, path));
    String fileId = getFileId(path);
    String filePath =
        path.toUri().getPath().substring(tableBasePath.toUri().getPath().length() + 1);
    String fileName = path.getName();
    writeStatus.setFileId(fileId);
    writeStatus.setPartitionPath(partitionPath);
    HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
    writeStat.setFileId(fileId);
    writeStat.setPath(
        ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(filePath, commitTime));
    writeStat.setPartitionPath(partitionPath);
    writeStat.setNumWrites(file.getRecordCount());
    writeStat.setTotalWriteBytes(file.getFileSizeBytes());
    writeStat.setFileSizeInBytes(file.getFileSizeBytes());
    writeStat.setNumInserts(file.getRecordCount());
    // TODO: Fix this populating last instant.
    writeStat.setPrevCommit("");
    writeStat.putRecordsStats(convertColStats(fileName, file.getColumnStats(), indexVersion));
    writeStatus.setStat(writeStat);
    return writeStatus;
  }

  private Map<String, HoodieColumnRangeMetadata<Comparable>> convertColStats(
      String fileName, List<ColumnStat> columnStatMap, HoodieIndexVersion indexVersion) {
    return columnStatMap.stream()
        .filter(
            entry ->
                !InternalType.NON_SCALAR_TYPES.contains(entry.getField().getSchema().getDataType()))
        .map(
            columnStat -> {
              ValueMetadata valueMetadata =
                  XTableValueMetadata.getValueMetadata(columnStat, indexVersion);
              return HoodieColumnRangeMetadata.<Comparable>create(
                  fileName,
                  convertFromXTablePath(columnStat.getField().getPath()),
                  valueMetadata.standardizeJavaTypeAndPromote(columnStat.getRange().getMinValue()),
                  valueMetadata.standardizeJavaTypeAndPromote(columnStat.getRange().getMaxValue()),
                  columnStat.getNumNulls(),
                  columnStat.getNumValues(),
                  columnStat.getTotalSize(),
                  -1L,
                  valueMetadata);
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

  private String getPartitionPath(Path tableBasePath, List<InternalDataFile> files) {
    return HudiPathUtils.getPartitionPath(
        tableBasePath, new CachingPath(files.get(0).getPhysicalPath()));
  }
}
