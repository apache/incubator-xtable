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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.Value;

import org.apache.hadoop.fs.Path;

import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieRollbackPartitionMetadata;
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
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
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
    return getOneDataFilesForPartitions(allPartitionPaths, timelineForInstant, table);
  }

  public OneDataFilesDiff getDiffBetweenCommits(
      HoodieInstant startCommit, HoodieInstant endCommit, OneTable table) {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline timelineForInstant =
        activeTimeline.findInstantsInRange(startCommit.getTimestamp(), endCommit.getTimestamp());
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
    fsView.close();

    List<PartitionInfo> added =
        allInfo.stream().flatMap(pair -> pair.getLeft().stream()).collect(Collectors.toList());
    List<PartitionInfo> removed =
        allInfo.stream().flatMap(pair -> pair.getRight().stream()).collect(Collectors.toList());
    int parallelism = Math.min(DEFAULT_PARALLELISM, added.size());

    HudiPartitionDataFileExtractor statsExtractor =
        new HudiPartitionDataFileExtractor(metaClient, table, partitionValuesExtractor);
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
                    replaceMetadata
                        .getPartitionToWriteStats()
                        .getOrDefault(partitionPath, Collections.emptyList())
                        .stream()
                        .map(HoodieWriteStat::getFileId)
                        .forEach(affectedFileGroupIds::add);
                    Pair<Optional<PartitionInfo>, Optional<PartitionInfo>> addedAndRemovedFiles =
                        getUpdatesToPartition(
                            fsView, startCommit, endCommit, partitionPath, affectedFileGroupIds);
                    addedAndRemovedFiles.getLeft().ifPresent(addedFiles::add);
                    addedAndRemovedFiles.getRight().ifPresent(removedFiles::add);
                  });
          break;
        case HoodieTimeline.ROLLBACK_ACTION:
          HoodieRollbackMetadata rollbackMetadata =
              TimelineMetadataUtils.deserializeAvroMetadata(
                  timeline.getInstantDetails(instant).get(), HoodieRollbackMetadata.class);
          rollbackMetadata
              .getPartitionMetadata()
              .forEach(
                  (partition, metadata) -> handleRollbackAction(removedFiles, partition, metadata));
          break;
        case HoodieTimeline.RESTORE_ACTION:
          HoodieRestoreMetadata restoreMetadata =
              TimelineMetadataUtils.deserializeAvroMetadata(
                  timeline.getInstantDetails(instant).get(), HoodieRestoreMetadata.class);
          restoreMetadata
              .getHoodieRestoreMetadata()
              .forEach(
                  (key, rollbackMetadataList) ->
                      rollbackMetadataList.forEach(
                          rollbackMeta ->
                              rollbackMeta
                                  .getPartitionMetadata()
                                  .forEach(
                                      (partition, metadata) ->
                                          handleRollbackAction(
                                              removedFiles, partition, metadata))));
          break;
        case HoodieTimeline.SAVEPOINT_ACTION:
        case HoodieTimeline.LOG_COMPACTION_ACTION:
        case HoodieTimeline.CLEAN_ACTION:
        case HoodieTimeline.INDEXING_ACTION:
        case HoodieTimeline.SCHEMA_COMMIT_ACTION:
          // these do not impact the base files
          break;
        default:
          throw new OneIOException("Unexpected commit type " + instant.getAction());
      }
      return Pair.of(addedFiles, removedFiles);
    } catch (IOException ex) {
      throw new OneIOException("Unable to read commit metadata for commit " + instant, ex);
    }
  }

  private void handleRollbackAction(
      List<PartitionInfo> removedFiles,
      String partition,
      HoodieRollbackPartitionMetadata metadata) {
    List<String> deletedPaths = metadata.getSuccessDeleteFiles();
    List<HoodieBaseFile> baseFiles =
        deletedPaths.stream()
            .map(
                path -> {
                  try {
                    URI basePathUri = basePath.toUri();
                    if (path.startsWith(basePathUri.getScheme())) {
                      return path;
                    }
                    return new URI(basePathUri.getScheme(), path, null).toString();
                  } catch (URISyntaxException e) {
                    throw new OneIOException("Unable to parse path " + path, e);
                  }
                })
            .map(HoodieBaseFile::new)
            .collect(Collectors.toList());
    removedFiles.add(
        PartitionInfo.builder()
            .partitionPath(partition)
            .baseFiles(baseFiles)
            .deletes(true)
            .build());
  }

  private Pair<Optional<PartitionInfo>, Optional<PartitionInfo>> getUpdatesToPartition(
      HoodieTableFileSystemView fsView,
      HoodieInstant startCommit,
      HoodieInstant endCommit,
      String partitionPath,
      Set<String> affectedFileGroupIds) {
    List<HoodieBaseFile> filesToAdd = new ArrayList<>();
    List<HoodieBaseFile> filesToRemove = new ArrayList<>();
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
                filesToAdd.add(baseFiles.get(0));
              }
              for (HoodieBaseFile baseFile : baseFiles) {
                if (HoodieTimeline.compareTimestamps(
                    baseFile.getCommitTime(), LESSER_THAN_OR_EQUALS, startCommit.getTimestamp())) {
                  filesToRemove.add(baseFile);
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
                    .baseFiles(filesToAdd)
                    .deletes(false)
                    .build());
    Optional<PartitionInfo> removed =
        filesToRemove.isEmpty()
            ? Optional.empty()
            : Optional.of(
                PartitionInfo.builder()
                    .partitionPath(partitionPath)
                    .baseFiles(filesToRemove)
                    .deletes(true)
                    .build());
    return Pair.of(added, removed);
  }

  private List<OneDataFile> getOneDataFilesForPartitions(
      List<String> partitionPaths, HoodieTimeline timeline, OneTable table) {

    HoodieTableFileSystemView fsView =
        new HoodieMetadataFileSystemView(
            localEngineContext,
            metaClient,
            timeline,
            HoodieMetadataConfig.newBuilder().enable(true).build());

    List<PartitionInfo> partitionInfoList =
        partitionPaths.stream()
            .map(
                partitionPath ->
                    PartitionInfo.builder()
                        .partitionPath(partitionPath)
                        .baseFiles(
                            fsView.getLatestBaseFiles(partitionPath).collect(Collectors.toList()))
                        .build())
            .collect(Collectors.toList());
    fsView.close();

    int parallelism = Math.min(DEFAULT_PARALLELISM, partitionInfoList.size());
    HudiPartitionDataFileExtractor statsExtractor =
        new HudiPartitionDataFileExtractor(metaClient, table, partitionValuesExtractor);
    return localEngineContext.map(partitionInfoList, statsExtractor, parallelism);
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
    List<HoodieBaseFile> baseFiles;
    OneDataFiles existingFileDetails;
    boolean deletes;
  }
}
