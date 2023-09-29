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
import java.util.Map;
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
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
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
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.SchemaVersion;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.Range;
import io.onetable.model.storage.FileFormat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.OneDataFilesDiff;

/** Extracts all the files for Hudi table represented by {@link OneTable}. */
public class HudiDataFileExtractor implements AutoCloseable {
  private static final SchemaVersion DEFAULT_SCHEMA_VERSION = new SchemaVersion(1, null);
  private final HoodieTableMetadata tableMetadata;
  private final HoodieTableMetaClient metaClient;
  private final HoodieEngineContext engineContext;
  private final HudiPartitionValuesExtractor partitionValuesExtractor;
  private final HudiFileStatsExtractor fileStatsExtractor;
  private final Path basePath;

  public HudiDataFileExtractor(
      HoodieTableMetaClient metaClient,
      HudiPartitionValuesExtractor hudiPartitionValuesExtractor,
      HudiFileStatsExtractor hudiFileStatsExtractor) {
    this.engineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
    this.basePath = metaClient.getBasePathV2();
    this.tableMetadata =
        HoodieTableMetadata.create(
            engineContext,
            metadataConfig,
            basePath.toString(),
            FileSystemViewStorageConfig.SPILLABLE_DIR.defaultValue(),
            true);
    this.metaClient = metaClient;
    this.partitionValuesExtractor = hudiPartitionValuesExtractor;
    this.fileStatsExtractor = hudiFileStatsExtractor;
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
            engineContext,
            metaClient,
            activeTimeline.findInstantsBeforeOrEquals(endCommit.getTimestamp()),
            HoodieMetadataConfig.newBuilder().enable(true).build());
    List<AddedAndRemovedFiles> allInfo;
    try {
      allInfo =
          timelineForInstant.getInstants().stream()
              .map(
                  instant ->
                      getAddedAndRemovedPartitionInfo(
                          activeTimeline,
                          instant,
                          fsView,
                          startCommit,
                          endCommit,
                          table.getPartitioningFields()))
              .collect(Collectors.toList());
    } finally {
      fsView.close();
    }

    Stream<OneDataFile> filesAddedWithoutStats =
        allInfo.stream().flatMap(info -> info.getAdded().stream()).parallel();
    List<OneDataFile> filesAdded =
        fileStatsExtractor
            .addStatsToFiles(filesAddedWithoutStats, table.getReadSchema())
            .collect(Collectors.toList());
    List<OneDataFile> filesRemoved =
        allInfo.stream().flatMap(info -> info.getRemoved().stream()).collect(Collectors.toList());

    return OneDataFilesDiff.builder().filesAdded(filesAdded).filesRemoved(filesRemoved).build();
  }

  private AddedAndRemovedFiles getAddedAndRemovedPartitionInfo(
      HoodieTimeline timeline,
      HoodieInstant instant,
      HoodieTableFileSystemView fsView,
      HoodieInstant startCommit,
      HoodieInstant endCommit,
      List<OnePartitionField> partitioningFields) {
    try {
      List<OneDataFile> addedFiles = new ArrayList<>();
      List<OneDataFile> removedFiles = new ArrayList<>();
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
                    Set<String> affectedFileIds =
                        writeStats.stream()
                            .map(HoodieWriteStat::getFileId)
                            .collect(Collectors.toSet());
                    Pair<List<OneDataFile>, List<OneDataFile>> addedAndRemovedFiles =
                        getUpdatesToPartition(
                            fsView,
                            startCommit,
                            endCommit,
                            partitionPath,
                            affectedFileIds,
                            partitioningFields);
                    addedFiles.addAll(addedAndRemovedFiles.getLeft());
                    removedFiles.addAll(addedAndRemovedFiles.getRight());
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
                    Set<String> affectedFileIds = new HashSet<>(fileIds);
                    replaceMetadata
                        .getPartitionToWriteStats()
                        .getOrDefault(partitionPath, Collections.emptyList())
                        .stream()
                        .map(HoodieWriteStat::getFileId)
                        .forEach(affectedFileIds::add);
                    Pair<List<OneDataFile>, List<OneDataFile>> addedAndRemovedFiles =
                        getUpdatesToPartition(
                            fsView,
                            startCommit,
                            endCommit,
                            partitionPath,
                            affectedFileIds,
                            partitioningFields);
                    addedFiles.addAll(addedAndRemovedFiles.getLeft());
                    removedFiles.addAll(addedAndRemovedFiles.getRight());
                  });
          break;
        case HoodieTimeline.ROLLBACK_ACTION:
          HoodieRollbackMetadata rollbackMetadata =
              TimelineMetadataUtils.deserializeAvroMetadata(
                  timeline.getInstantDetails(instant).get(), HoodieRollbackMetadata.class);
          rollbackMetadata
              .getPartitionMetadata()
              .forEach(
                  (partition, metadata) ->
                      removedFiles.addAll(
                          getRemovedFilesForRollback(partition, metadata, partitioningFields)));
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
                                          removedFiles.addAll(
                                              getRemovedFilesForRollback(
                                                  partition, metadata, partitioningFields)))));
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
      return AddedAndRemovedFiles.builder().added(addedFiles).removed(removedFiles).build();
    } catch (IOException ex) {
      throw new OneIOException("Unable to read commit metadata for commit " + instant, ex);
    }
  }

  private List<OneDataFile> getRemovedFilesForRollback(
      String partitionPath,
      HoodieRollbackPartitionMetadata metadata,
      List<OnePartitionField> partitioningFields) {
    Map<OnePartitionField, Range> partitionValues =
        partitionValuesExtractor.extractPartitionValues(partitioningFields, partitionPath);
    List<String> deletedPaths = metadata.getSuccessDeleteFiles();
    return deletedPaths.stream()
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
        .map(baseFile -> buildFileWithoutStats(partitionPath, partitionValues, baseFile))
        .collect(Collectors.toList());
  }

  private Pair<List<OneDataFile>, List<OneDataFile>> getUpdatesToPartition(
      HoodieTableFileSystemView fsView,
      HoodieInstant startCommit,
      HoodieInstant endCommit,
      String partitionPath,
      Set<String> affectedFileIds,
      List<OnePartitionField> partitioningFields) {
    List<OneDataFile> filesToAdd = new ArrayList<>();
    List<OneDataFile> filesToRemove = new ArrayList<>();
    Map<OnePartitionField, Range> partitionValues =
        partitionValuesExtractor.extractPartitionValues(partitioningFields, partitionPath);
    Stream<HoodieFileGroup> fileGroups =
        Stream.concat(
            fsView.getAllFileGroups(partitionPath),
            fsView.getReplacedFileGroupsBeforeOrOn(endCommit.getTimestamp(), partitionPath));
    fileGroups
        .filter(fileGroup -> affectedFileIds.contains(fileGroup.getFileGroupId().getFileId()))
        .forEach(
            fileGroup -> {
              List<HoodieBaseFile> baseFiles =
                  fileGroup.getAllBaseFiles().collect(Collectors.toList());
              if (HoodieTimeline.compareTimestamps(
                  baseFiles.get(0).getCommitTime(), GREATER_THAN, startCommit.getTimestamp())) {
                filesToAdd.add(
                    buildFileWithoutStats(partitionPath, partitionValues, baseFiles.get(0)));
              }
              for (HoodieBaseFile baseFile : baseFiles) {
                if (HoodieTimeline.compareTimestamps(
                    baseFile.getCommitTime(), LESSER_THAN_OR_EQUALS, startCommit.getTimestamp())) {
                  filesToRemove.add(
                      buildFileWithoutStats(partitionPath, partitionValues, baseFile));
                  break;
                }
              }
            });
    return Pair.of(filesToAdd, filesToRemove);
  }

  private List<OneDataFile> getOneDataFilesForPartitions(
      List<String> partitionPaths, HoodieTimeline timeline, OneTable table) {

    HoodieTableFileSystemView fsView =
        new HoodieMetadataFileSystemView(
            engineContext,
            metaClient,
            timeline,
            HoodieMetadataConfig.newBuilder().enable(true).build());

    try {
      Stream<OneDataFile> filesWithoutStats =
          partitionPaths.stream()
              .parallel()
              .flatMap(
                  partitionPath -> {
                    Map<OnePartitionField, Range> partitionValues =
                        partitionValuesExtractor.extractPartitionValues(
                            table.getPartitioningFields(), partitionPath);
                    return fsView
                        .getLatestBaseFiles(partitionPath)
                        .map(
                            baseFile ->
                                buildFileWithoutStats(partitionPath, partitionValues, baseFile));
                  });
      Stream<OneDataFile> files =
          fileStatsExtractor.addStatsToFiles(filesWithoutStats, table.getReadSchema());
      Map<String, List<OneDataFile>> collected =
          files.collect(Collectors.groupingBy(OneDataFile::getPartitionPath));
      return collected.entrySet().stream()
          .map(
              entry ->
                  OneDataFiles.collectionBuilder()
                      .partitionPath(entry.getKey())
                      .files(entry.getValue())
                      .build())
          .collect(Collectors.toList());
    } finally {
      fsView.close();
    }
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
  private static class AddedAndRemovedFiles {
    List<OneDataFile> added;
    List<OneDataFile> removed;
  }

  /**
   * Builds a {@link OneDataFile} without any statistics or rowCount value set.
   *
   * @param partitionPath partition path for the file
   * @param partitionValues values extracted from the partition path
   * @param hoodieBaseFile the base file from Hudi
   * @return {@link OneDataFile} without any statistics or rowCount value set.
   */
  private OneDataFile buildFileWithoutStats(
      String partitionPath,
      Map<OnePartitionField, Range> partitionValues,
      HoodieBaseFile hoodieBaseFile) {
    long rowCount = 0L;
    Map<OneField, ColumnStat> columnStatMap = Collections.emptyMap();
    return OneDataFile.builder()
        .schemaVersion(DEFAULT_SCHEMA_VERSION)
        .physicalPath(hoodieBaseFile.getPath())
        .fileFormat(getFileFormat(FSUtils.getFileExtension(hoodieBaseFile.getPath())))
        .partitionPath(partitionPath)
        .partitionValues(partitionValues)
        .fileSizeBytes(Math.max(0, hoodieBaseFile.getFileSize()))
        .recordCount(rowCount)
        .columnStats(columnStatMap)
        .lastModified(
            hoodieBaseFile.getFileStatus() == null
                ? 0L
                : hoodieBaseFile.getFileStatus().getModificationTime())
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
