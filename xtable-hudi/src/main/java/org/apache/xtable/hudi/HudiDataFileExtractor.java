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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.Value;

import org.apache.hadoop.fs.Path;

import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.config.HoodieCommonConfig;
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
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.FileSystemViewStorageConfig;
import org.apache.hudi.common.table.view.FileSystemViewStorageType;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.metadata.HoodieTableMetadata;

import org.apache.xtable.collectors.CustomCollectors;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.exception.ParseException;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.storage.DataFilesDiff;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.PartitionFileGroup;

/** Extracts all the files for Hudi table represented by {@link InternalTable}. */
public class HudiDataFileExtractor implements AutoCloseable {
  private final HoodieTableMetadata tableMetadata;
  private final HoodieTableMetaClient metaClient;
  private final HoodieEngineContext engineContext;
  private final HudiPartitionValuesExtractor partitionValuesExtractor;
  private final HudiFileStatsExtractor fileStatsExtractor;
  private final HoodieMetadataConfig metadataConfig;
  private final FileSystemViewManager fileSystemViewManager;
  private final Path basePath;

  public HudiDataFileExtractor(
      HoodieTableMetaClient metaClient,
      HudiPartitionValuesExtractor hudiPartitionValuesExtractor,
      HudiFileStatsExtractor hudiFileStatsExtractor) {
    this.engineContext = new HoodieLocalEngineContext(metaClient.getHadoopConf());
    metadataConfig =
        HoodieMetadataConfig.newBuilder()
            .enable(metaClient.getTableConfig().isMetadataTableAvailable())
            .build();
    this.basePath = metaClient.getBasePathV2();
    this.tableMetadata =
        metadataConfig.enabled()
            ? HoodieTableMetadata.create(engineContext, metadataConfig, basePath.toString(), true)
            : null;
    this.fileSystemViewManager =
        FileSystemViewManager.createViewManager(
            engineContext,
            metadataConfig,
            FileSystemViewStorageConfig.newBuilder()
                .withStorageType(FileSystemViewStorageType.MEMORY)
                .build(),
            HoodieCommonConfig.newBuilder().build(),
            meta -> tableMetadata);
    this.metaClient = metaClient;
    this.partitionValuesExtractor = hudiPartitionValuesExtractor;
    this.fileStatsExtractor = hudiFileStatsExtractor;
  }

  public List<PartitionFileGroup> getFilesCurrentState(InternalTable table) {
    try {
      List<String> allPartitionPaths =
          tableMetadata != null
              ? tableMetadata.getAllPartitionPaths()
              : FSUtils.getAllPartitionPaths(engineContext, metadataConfig, basePath.toString());
      return getInternalDataFilesForPartitions(allPartitionPaths, table);
    } catch (IOException ex) {
      throw new ReadException(
          "Unable to read partitions for table " + metaClient.getTableConfig().getTableName(), ex);
    }
  }

  public DataFilesDiff getDiffForCommit(
      HoodieInstant hoodieInstantForDiff,
      InternalTable table,
      HoodieInstant instant,
      HoodieTimeline visibleTimeline) {
    SyncableFileSystemView fsView = fileSystemViewManager.getFileSystemView(metaClient);
    AddedAndRemovedFiles allInfo =
        getAddedAndRemovedPartitionInfo(
            visibleTimeline, instant, fsView, hoodieInstantForDiff, table.getPartitioningFields());

    Stream<InternalDataFile> filesAddedWithoutStats = allInfo.getAdded().stream();
    List<InternalDataFile> filesAdded =
        fileStatsExtractor
            .addStatsToFiles(tableMetadata, filesAddedWithoutStats, table.getReadSchema())
            .collect(Collectors.toList());
    List<InternalDataFile> filesRemoved = allInfo.getRemoved();

    return DataFilesDiff.builder().filesAdded(filesAdded).filesRemoved(filesRemoved).build();
  }

  private AddedAndRemovedFiles getAddedAndRemovedPartitionInfo(
      HoodieTimeline timeline,
      HoodieInstant instant,
      TableFileSystemView fsView,
      HoodieInstant instantToConsider,
      List<InternalPartitionField> partitioningFields) {
    try {
      List<InternalDataFile> addedFiles = new ArrayList<>();
      List<InternalDataFile> removedFiles = new ArrayList<>();
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
                    AddedAndRemovedFiles addedAndRemovedFiles =
                        getUpdatesToPartition(
                            fsView,
                            instantToConsider,
                            partitionPath,
                            affectedFileIds,
                            partitioningFields);
                    addedFiles.addAll(addedAndRemovedFiles.getAdded());
                    removedFiles.addAll(addedAndRemovedFiles.getRemoved());
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
                    Set<String> replacedFileIdsByPartition = new HashSet<>(fileIds);
                    Set<String> newFileIds =
                        replaceMetadata
                            .getPartitionToWriteStats()
                            .getOrDefault(partitionPath, Collections.emptyList())
                            .stream()
                            .map(HoodieWriteStat::getFileId)
                            .collect(Collectors.toSet());
                    AddedAndRemovedFiles addedAndRemovedFiles =
                        getUpdatesToPartitionForReplaceCommit(
                            fsView,
                            instantToConsider,
                            partitionPath,
                            replacedFileIdsByPartition,
                            newFileIds,
                            partitioningFields);
                    addedFiles.addAll(addedAndRemovedFiles.getAdded());
                    removedFiles.addAll(addedAndRemovedFiles.getRemoved());
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
                          getRemovedFiles(
                              partition, metadata.getSuccessDeleteFiles(), partitioningFields)));
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
                                              getRemovedFiles(
                                                  partition,
                                                  metadata.getSuccessDeleteFiles(),
                                                  partitioningFields)))));
          break;
        case HoodieTimeline.CLEAN_ACTION:
        case HoodieTimeline.SAVEPOINT_ACTION:
        case HoodieTimeline.LOG_COMPACTION_ACTION:
        case HoodieTimeline.INDEXING_ACTION:
        case HoodieTimeline.SCHEMA_COMMIT_ACTION:
          // these do not impact the base files
          break;
        default:
          throw new NotSupportedException("Unexpected commit type " + instant.getAction());
      }
      return AddedAndRemovedFiles.builder().added(addedFiles).removed(removedFiles).build();
    } catch (IOException ex) {
      throw new ReadException("Unable to read commit metadata for commit " + instant, ex);
    }
  }

  private List<InternalDataFile> getRemovedFiles(
      String partitionPath,
      List<String> deletedPaths,
      List<InternalPartitionField> partitioningFields) {
    List<PartitionValue> partitionValues =
        partitionValuesExtractor.extractPartitionValues(partitioningFields, partitionPath);
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
                throw new ParseException("Unable to parse path " + path, e);
              }
            })
        .filter(uri -> !FSUtils.isLogFile(new Path(uri).getName()))
        .map(HoodieBaseFile::new)
        .map(baseFile -> buildFileWithoutStats(partitionValues, baseFile))
        .collect(CustomCollectors.toList(deletedPaths.size()));
  }

  private AddedAndRemovedFiles getUpdatesToPartition(
      TableFileSystemView fsView,
      HoodieInstant instantToConsider,
      String partitionPath,
      Set<String> affectedFileIds,
      List<InternalPartitionField> partitioningFields) {
    List<InternalDataFile> filesToAdd = new ArrayList<>(affectedFileIds.size());
    List<InternalDataFile> filesToRemove = new ArrayList<>(affectedFileIds.size());
    List<PartitionValue> partitionValues =
        partitionValuesExtractor.extractPartitionValues(partitioningFields, partitionPath);
    Stream<HoodieFileGroup> fileGroups =
        Stream.concat(
            fsView.getAllFileGroups(partitionPath), fsView.getAllReplacedFileGroups(partitionPath));
    fileGroups
        .filter(fileGroup -> affectedFileIds.contains(fileGroup.getFileGroupId().getFileId()))
        .forEach(
            fileGroup -> {
              List<HoodieBaseFile> baseFiles =
                  fileGroup.getAllBaseFiles().collect(Collectors.toList());
              boolean newBaseFileAdded = false;
              for (HoodieBaseFile baseFile : baseFiles) {
                if (baseFile.getCommitTime().equals(instantToConsider.getTimestamp())) {
                  newBaseFileAdded = true;
                  filesToAdd.add(buildFileWithoutStats(partitionValues, baseFile));
                } else if (newBaseFileAdded) {
                  // if a new base file was added, then the previous base file for the group needs
                  // to be removed
                  filesToRemove.add(buildFileWithoutStats(partitionValues, baseFile));
                  break;
                }
              }
            });
    return AddedAndRemovedFiles.builder().added(filesToAdd).removed(filesToRemove).build();
  }

  private AddedAndRemovedFiles getUpdatesToPartitionForReplaceCommit(
      TableFileSystemView fsView,
      HoodieInstant instantToConsider,
      String partitionPath,
      Set<String> replacedFileIds,
      Set<String> newFileIds,
      List<InternalPartitionField> partitioningFields) {
    List<InternalDataFile> filesToAdd = new ArrayList<>(newFileIds.size());
    List<InternalDataFile> filesToRemove = new ArrayList<>(replacedFileIds.size());
    List<PartitionValue> partitionValues =
        partitionValuesExtractor.extractPartitionValues(partitioningFields, partitionPath);
    Stream<HoodieFileGroup> fileGroups =
        Stream.concat(
            fsView.getAllFileGroups(partitionPath),
            fsView.getReplacedFileGroupsBeforeOrOn(
                instantToConsider.getTimestamp(), partitionPath));
    fileGroups.forEach(
        fileGroup -> {
          List<HoodieBaseFile> baseFiles = fileGroup.getAllBaseFiles().collect(Collectors.toList());
          String fileId = fileGroup.getFileGroupId().getFileId();
          if (newFileIds.contains(fileId)) {
            filesToAdd.add(
                buildFileWithoutStats(partitionValues, baseFiles.get(baseFiles.size() - 1)));
          } else if (replacedFileIds.contains(fileId)) {
            filesToRemove.add(buildFileWithoutStats(partitionValues, baseFiles.get(0)));
          }
        });
    return AddedAndRemovedFiles.builder().added(filesToAdd).removed(filesToRemove).build();
  }

  private List<PartitionFileGroup> getInternalDataFilesForPartitions(
      List<String> partitionPaths, InternalTable table) {

    SyncableFileSystemView fsView = fileSystemViewManager.getFileSystemView(metaClient);
    Stream<InternalDataFile> filesWithoutStats =
        partitionPaths.stream()
            .parallel()
            .flatMap(
                partitionPath -> {
                  List<PartitionValue> partitionValues =
                      partitionValuesExtractor.extractPartitionValues(
                          table.getPartitioningFields(), partitionPath);
                  return fsView
                      .getLatestBaseFiles(partitionPath)
                      .map(baseFile -> buildFileWithoutStats(partitionValues, baseFile));
                });
    Stream<InternalDataFile> files =
        fileStatsExtractor.addStatsToFiles(tableMetadata, filesWithoutStats, table.getReadSchema());
    return PartitionFileGroup.fromFiles(files);
  }

  @Override
  public void close() {
    try {
      if (tableMetadata != null) {
        tableMetadata.close();
      }
      fileSystemViewManager.close();
    } catch (Exception e) {
      throw new ReadException(
          "Could not close table metadata for table " + metaClient.getTableConfig().getTableName());
    }
  }

  @Builder
  @Value
  private static class AddedAndRemovedFiles {
    List<InternalDataFile> added;
    List<InternalDataFile> removed;
  }

  /**
   * Builds a {@link InternalDataFile} without any statistics or rowCount value set.
   *
   * @param partitionValues values extracted from the partition path
   * @param hoodieBaseFile the base file from Hudi
   * @return {@link InternalDataFile} without any statistics or rowCount value set.
   */
  private InternalDataFile buildFileWithoutStats(
      List<PartitionValue> partitionValues, HoodieBaseFile hoodieBaseFile) {
    long rowCount = 0L;
    return InternalDataFile.builder()
        .physicalPath(hoodieBaseFile.getPath())
        .fileFormat(getFileFormat(FSUtils.getFileExtension(hoodieBaseFile.getPath())))
        .partitionValues(partitionValues)
        .fileSizeBytes(Math.max(0, hoodieBaseFile.getFileSize()))
        .recordCount(rowCount)
        .columnStats(Collections.emptyList())
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
