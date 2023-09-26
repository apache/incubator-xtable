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

import static org.apache.hudi.common.table.timeline.HoodieTimeline.GREATER_THAN;
import static org.apache.hudi.common.table.timeline.HoodieTimeline.LESSER_THAN_OR_EQUALS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.Value;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileGroup;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineUtils;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.metadata.HoodieMetadataFileSystemView;
import org.apache.hudi.metadata.HoodieTableMetadata;

import io.onetable.exception.OneIOException;
import io.onetable.model.OneTable;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.OneDataFilesDiff;

/** Extracts all the files for Hudi table represented by {@link OneTable}. */
public class HudiDataFileExtractor implements AutoCloseable {
  private static final List<String> EMPTY_PARTITION_LIST = Collections.singletonList("");
  private static final int DEFAULT_PARALLELISM = 20;

  private final HoodieTableMetadata tableMetadata;
  private final HoodieTableMetaClient metaClient;
  private final HoodieLocalEngineContext localEngineContext;
  private final HudiPartitionValuesExtractor partitionValuesExtractor;
  private final Path basePath;

  public HudiDataFileExtractor(
      HoodieTableMetaClient metaClient, HudiPartitionValuesExtractor hudiPartitionValuesExtractor) {
    this.localEngineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
    this.basePath = metaClient.getBasePathV2();
    this.tableMetadata =
        HoodieTableMetadata.create(
            localEngineContext,
            metadataConfig,
            basePath.toString(),
            FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue(),
            true);
    this.metaClient = metaClient;
    this.partitionValuesExtractor = hudiPartitionValuesExtractor;
  }

  public List<OneDataFile> getOneDataFiles(HoodieInstant commit, OneTable table) {
    HoodieTimeline timelineForInstant =
        metaClient.getActiveTimeline().findInstantsBeforeOrEquals(commit.getTimestamp());
    List<String> allPartitionPaths;
    try {
      allPartitionPaths = tableMetadata.getAllPartitionPaths();
    } catch (IOException ex) {
      throw new OneIOException("Unable to read partition paths from Hudi Metadata", ex);
    }
    return getOneDataFilesForPartitions(allPartitionPaths, timelineForInstant, table, null);
  }

  public OneDataFilesDiff getDiffBetweenCommits(
      HoodieInstant startCommit, HoodieInstant endCommit, OneTable table) {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline timelineForInstant =
        activeTimeline.findInstantsInRange(startCommit.getTimestamp(), endCommit.getTimestamp());
    if (timelineForInstant.getWriteTimeline().countInstants() == 0) {
      return OneDataFilesDiff.builder().build();
    }
    if (timelineForInstant.getWriteTimeline().countInstants() == 0) {
      // HoodieTableFileSystemView returns a view of files using the write timeline only.
      // If there are no write commits between start and end - these are either savepoint, rollback,
      // restore and clean commits.
      // In such cases, we sync the commits from lastWriteInstantBeforeStart...start...end.
      Option<HoodieInstant> lastWriteInstantBeforeStartCommit =
          activeTimeline
              .findInstantsBeforeOrEquals(startCommit.getTimestamp())
              .getWriteTimeline()
              .lastInstant();
      if (lastWriteInstantBeforeStartCommit.isPresent()) {
        timelineForInstant =
            activeTimeline
                .findInstantsBeforeOrEquals(endCommit.getTimestamp())
                .filter(
                    instant ->
                        instant
                                .getTimestamp()
                                .compareTo(lastWriteInstantBeforeStartCommit.get().getTimestamp())
                            >= 0);
      } else {
        timelineForInstant = activeTimeline.findInstantsBeforeOrEquals(endCommit.getTimestamp());
      }
    }
    HoodieTableFileSystemView fsView =
        new HoodieMetadataFileSystemView(
            localEngineContext,
            metaClient,
            activeTimeline.findInstantsBeforeOrEquals(endCommit.getTimestamp()),
            HoodieMetadataConfig.newBuilder().enable(true).build());

    List<Pair<List<PartitionInfo>, List<PartitionInfo>>> allInfo =
        timelineForInstant.getInstants().stream()
            .map(
                instant ->
                    getAddedAndRemovedPartitionInfo(
                        activeTimeline, instant, fsView, startCommit, endCommit))
            .collect(Collectors.toList());
    List<PartitionInfo> added =
        allInfo.stream().flatMap(pair -> pair.getLeft().stream()).collect(Collectors.toList());
    List<PartitionInfo> removed =
        allInfo.stream().flatMap(pair -> pair.getRight().stream()).collect(Collectors.toList());
    int parallelism = Math.min(DEFAULT_PARALLELISM, added.size());

    HudiPartitionDataFileExtractor statsExtractor =
        new HudiPartitionDataFileExtractor(
            metaClient, table, partitionValuesExtractor, timelineForInstant);
    List<OneDataFile> filesAdded = localEngineContext.map(added, statsExtractor, parallelism);
    List<OneDataFile> extractedFilesAdded =
        filesAdded.stream()
            .flatMap(
                oneDataFile -> {
                  if (oneDataFile instanceof OneDataFiles) {
                    return ((OneDataFiles) oneDataFile).getFiles().stream();
                  } else {
                    return Stream.of(oneDataFile);
                  }
                })
            .collect(Collectors.toList());
    List<OneDataFile> filesRemoved = localEngineContext.map(removed, statsExtractor, parallelism);
    List<OneDataFile> extractedFilesRemoved =
        filesRemoved.stream()
            .flatMap(
                oneDataFile -> {
                  if (oneDataFile instanceof OneDataFiles) {
                    return ((OneDataFiles) oneDataFile).getFiles().stream();
                  } else {
                    return Stream.of(oneDataFile);
                  }
                })
            .collect(Collectors.toList());
    return OneDataFilesDiff.builder()
        .filesAdded(extractedFilesAdded)
        .filesRemoved(extractedFilesRemoved)
        .build();
  }

  private Pair<List<PartitionInfo>, List<PartitionInfo>> getAddedAndRemovedPartitionInfo(
      HoodieTimeline timeline,
      HoodieInstant instant,
      HoodieTableFileSystemView fsView,
      HoodieInstant startCommit,
      HoodieInstant endCommit) {
    try {
      List<PartitionInfo> addedFiles = new ArrayList<>();
      List<PartitionInfo> removedFiles = new ArrayList<>();
      switch (instant.getAction()) {
        case HoodieTimeline.COMMIT_ACTION:
        case HoodieTimeline.DELTA_COMMIT_ACTION:
          HoodieCommitMetadata commitMetadata =
              HoodieCommitMetadata.fromBytes(
                  timeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
          commitMetadata
              .getPartitionToWriteStats()
              .forEach(
                  (partitionPath, writeStats) -> {
                    Set<String> affectedFileGroupIds =
                        writeStats.stream()
                            .map(HoodieWriteStat::getFileId)
                            .collect(Collectors.toSet());
                    Pair<Optional<PartitionInfo>, Optional<PartitionInfo>> addedAndRemovedFiles =
                        getUpdatesToPartition(
                            fsView, startCommit, endCommit, partitionPath, affectedFileGroupIds);
                    addedAndRemovedFiles.getLeft().ifPresent(addedFiles::add);
                    addedAndRemovedFiles.getRight().ifPresent(removedFiles::add);
                  });
          break;
        case HoodieTimeline.REPLACE_COMMIT_ACTION:
          HoodieReplaceCommitMetadata replaceMetadata =
              HoodieReplaceCommitMetadata.fromBytes(
                  timeline.getInstantDetails(instant).get(), HoodieReplaceCommitMetadata.class);
          replaceMetadata
              .getPartitionToReplaceFileIds()
              .forEach(
                  (partitionPath, fileIds) -> {
                    Set<String> affectedFileGroupIds = new HashSet<>(fileIds);
                    replaceMetadata.getPartitionToWriteStats().get(partitionPath).stream()
                        .map(HoodieWriteStat::getFileId)
                        .forEach(affectedFileGroupIds::add);
                    Pair<Optional<PartitionInfo>, Optional<PartitionInfo>> addedAndRemovedFiles =
                        getUpdatesToPartition(
                            fsView, startCommit, endCommit, partitionPath, affectedFileGroupIds);
                    addedAndRemovedFiles.getLeft().ifPresent(addedFiles::add);
                    addedAndRemovedFiles.getRight().ifPresent(removedFiles::add);
                  });
          break;
        case HoodieTimeline.SAVEPOINT_ACTION:
        case HoodieTimeline.LOG_COMPACTION_ACTION:
        case HoodieTimeline.CLEAN_ACTION:
        case HoodieTimeline.ROLLBACK_ACTION:
          break;
        default:
          throw new OneIOException("Unexpected commit type " + instant.getAction());
      }
      return Pair.of(addedFiles, removedFiles);
    } catch (IOException ex) {
      throw new OneIOException("Unable to read commit metadata for commit " + instant, ex);
    }
  }

  private Pair<Optional<PartitionInfo>, Optional<PartitionInfo>> getUpdatesToPartition(
      HoodieTableFileSystemView fsView,
      HoodieInstant startCommit,
      HoodieInstant endCommit,
      String partitionPath,
      Set<String> affectedFileGroupIds) {
    List<FileStatus> filesToAdd = new ArrayList<>();
    List<FileStatus> filesToRemove = new ArrayList<>();
    Stream<HoodieFileGroup> fileGroups =
        Stream.concat(
            fsView.getAllFileGroups(partitionPath),
            fsView.getReplacedFileGroupsBeforeOrOn(endCommit.getTimestamp(), partitionPath));
    fileGroups
        .filter(fileGroup -> affectedFileGroupIds.contains(fileGroup.getFileGroupId().getFileId()))
        .forEach(
            fileGroup -> {
              List<HoodieBaseFile> baseFiles =
                  fileGroup.getAllBaseFiles().collect(Collectors.toList());
              if (HoodieTimeline.compareTimestamps(
                  baseFiles.get(0).getCommitTime(), GREATER_THAN, startCommit.getTimestamp())) {
                filesToAdd.add(baseFiles.get(0).getFileStatus());
              }
              for (HoodieBaseFile baseFile : baseFiles) {
                if (HoodieTimeline.compareTimestamps(
                    baseFile.getCommitTime(), LESSER_THAN_OR_EQUALS, startCommit.getTimestamp())) {
                  filesToRemove.add(baseFile.getFileStatus());
                  break;
                }
              }
            });
    Optional<PartitionInfo> added =
        filesToAdd.isEmpty()
            ? Optional.empty()
            : Optional.of(
                PartitionInfo.builder()
                    .partitionPath(partitionPath)
                    .fileStatuses(filesToAdd.toArray(new FileStatus[0]))
                    .build());
    Optional<PartitionInfo> removed =
        filesToRemove.isEmpty()
            ? Optional.empty()
            : Optional.of(
                PartitionInfo.builder()
                    .partitionPath(partitionPath)
                    .fileStatuses(filesToRemove.toArray(new FileStatus[0]))
                    .build());
    return Pair.of(added, removed);
  }

  public List<OneDataFile> getOneDataFilesForAffectedPartitions(
      HoodieInstant startCommit,
      HoodieInstant endCommit,
      OneTable table,
      OneDataFiles oneDataFiles) {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline timelineForInstant =
        activeTimeline.findInstantsInRange(startCommit.getTimestamp(), endCommit.getTimestamp());
    if (timelineForInstant.getWriteTimeline().countInstants() == 0) {
      // HoodieTableFileSystemView returns a view of files using the write timeline only.
      // If there are no write commits between start and end - these are either savepoint, rollback,
      // restore and clean commits.
      // In such cases, we sync the commits from lastWriteInstantBeforeStart...start...end.
      Option<HoodieInstant> lastWriteInstantBeforeStartCommit =
          activeTimeline
              .findInstantsBeforeOrEquals(startCommit.getTimestamp())
              .getWriteTimeline()
              .lastInstant();
      if (lastWriteInstantBeforeStartCommit.isPresent()) {
        timelineForInstant =
            activeTimeline
                .findInstantsBeforeOrEquals(endCommit.getTimestamp())
                .filter(
                    instant ->
                        instant
                                .getTimestamp()
                                .compareTo(lastWriteInstantBeforeStartCommit.get().getTimestamp())
                            >= 0);
      } else {
        timelineForInstant = activeTimeline.findInstantsBeforeOrEquals(endCommit.getTimestamp());
      }
    }
    List<String> affectedPartitions;
    // TimelineUtils.getAffectedPartitions does not work for unpartitioned tables, so we handle that
    // case directly
    if (table.getPartitioningFields().isEmpty()) {
      affectedPartitions = EMPTY_PARTITION_LIST;
    } else {
      affectedPartitions = TimelineUtils.getAffectedPartitions(timelineForInstant);
    }
    return getOneDataFilesForPartitions(
        affectedPartitions, timelineForInstant, table, oneDataFiles);
  }

  private List<OneDataFile> getOneDataFilesForPartitions(
      List<String> partitionPaths,
      HoodieTimeline timeline,
      OneTable table,
      OneDataFiles existingFileDetails) {
    List<PartitionInfo> partitionInfoList;
    try {
      Map<String, String> fullToPartialPartitionPath =
          partitionPaths.stream()
              .collect(
                  Collectors.toMap(
                      partitionPath ->
                          partitionPath.isEmpty()
                              ? basePath.toString()
                              : new Path(basePath, partitionPath).toString(),
                      Function.identity()));
      Map<String, FileStatus[]> partitionFileStatusMap =
          tableMetadata.getAllFilesInPartitions(
              new ArrayList<>(fullToPartialPartitionPath.keySet()));
      Map<String, OneDataFiles> existingFileDetailsPerPartition =
          getFilesByPartition(existingFileDetails);
      partitionInfoList =
          partitionFileStatusMap.entrySet().stream()
              .map(
                  partitionAndFileStatuses -> {
                    String partialPartitionPath =
                        fullToPartialPartitionPath.get(partitionAndFileStatuses.getKey());
                    return PartitionInfo.builder()
                        .partitionPath(partialPartitionPath)
                        .fileStatuses(partitionAndFileStatuses.getValue())
                        .existingFileDetails(
                            existingFileDetailsPerPartition.get(partialPartitionPath))
                        .build();
                  })
              .collect(Collectors.toList());
    } catch (IOException e) {
      throw new OneIOException("failed to get partition paths from table metadata", e);
    }
    int parallelism = Math.min(DEFAULT_PARALLELISM, partitionInfoList.size());

    HudiPartitionDataFileExtractor statsExtractor =
        new HudiPartitionDataFileExtractor(metaClient, table, partitionValuesExtractor, timeline);
    return localEngineContext.map(partitionInfoList, statsExtractor, parallelism);
  }

  private Map<String, OneDataFiles> getFilesByPartition(OneDataFiles files) {
    if (files == null) {
      return Collections.emptyMap();
    }
    return files.getFiles().stream()
        .collect(Collectors.toMap(OneDataFile::getPartitionPath, file -> (OneDataFiles) file));
  }

  @Override
  public void close() {
    try {
      this.tableMetadata.close();
    } catch (Exception e) {
      throw new OneIOException(
          "Could not close table metadata for table " + metaClient.getTableConfig().getTableName());
    }
  }

  @Builder
  @Value
  public static class PartitionInfo {
    String partitionPath;
    FileStatus[] fileStatuses;
    OneDataFiles existingFileDetails;
  }
}
