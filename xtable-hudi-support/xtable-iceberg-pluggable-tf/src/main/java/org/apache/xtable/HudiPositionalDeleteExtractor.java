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
 
package org.apache.xtable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hudi.avro.model.HoodieCompactionOperation;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.DeleteRecord;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieDeleteBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieWriteConflictException;
import org.apache.hudi.storage.StoragePath;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.ReadException;

/**
 * Reads the row positions that the log files of a merge-on-read table delete from its base files,
 * which is what the Iceberg table records as deletion vectors. The log files are read through
 * Hudi's log reader, so inline log files and native delete files are handled alike.
 *
 * <p>Iceberg sees base files only, so a log file may hold nothing but deletes that carry the
 * position of each deleted row; anything else would be invisible to Iceberg readers and is refused.
 */
class HudiPositionalDeleteExtractor {
  static final String UPDATES_AS_DELETES_AND_INSERTS =
      "hoodie.write.updates.as.deletes.and.inserts";

  private final HoodieTableMetaClient metaClient;
  private final FileSystemViewManager viewManager;
  private SyncableFileSystemView fsView;
  private HoodieSchema tableSchema;

  HudiPositionalDeleteExtractor(
      HoodieTableMetaClient metaClient, FileSystemViewManager viewManager) {
    this.metaClient = metaClient;
    this.viewManager = viewManager;
  }

  /**
   * The positions a commit deletes, keyed by the path of the base file they are deleted from. For a
   * deltacommit that is what its log files delete from the latest base file of their file group,
   * and for a compaction what the log files it did not merge delete from the base file it wrote. On
   * a merge-on-read table only a compaction completes as a commit; the commit metadata's operation
   * type cannot tell, since a write client records the type of the last operation it ran.
   */
  Map<String, Set<Long>> positionsDeletedBy(
      HoodieCommitMetadata commitMetadata, HoodieInstant instant) {
    if (HoodieTimeline.COMMIT_ACTION.equals(instant.getAction())) {
      return positionsCarriedOverBy(commitMetadata, instant);
    }
    Map<String, Set<Long>> positionsByBaseFile = new LinkedHashMap<>();
    logFilesByFileGroup(commitMetadata, instant)
        .forEach(
            (fileGroup, logFiles) -> {
              HoodieBaseFile baseFile =
                  fsView()
                      .getLatestBaseFile(fileGroup.getPartitionPath(), fileGroup.getFileId())
                      .orElseThrow(() -> noBaseFile(fileGroup, logFiles, instant));
              positionsByBaseFile
                  .computeIfAbsent(qualifiedPath(baseFile), path -> new LinkedHashSet<>())
                  .addAll(
                      positionsDeletedFrom(
                          baseFile,
                          logFiles.stream().map(HoodieLogFile::new).collect(Collectors.toList())));
            });
    return positionsByBaseFile;
  }

  /**
   * The failure for log files of a file group without a base file to delete from. When a clustering
   * or another replace commit replaced the file group after the writer looked its rows up, the
   * deletes apply to no live base file: Hudi rejects that write under optimistic concurrency
   * control, and a single writer does not check it, so it is refused here as a write conflict.
   */
  private RuntimeException noBaseFile(
      HoodieFileGroupId fileGroup, Set<StoragePath> logFiles, HoodieInstant deltacommit) {
    boolean replaced =
        fsView()
            .getAllReplacedFileGroups(fileGroup.getPartitionPath())
            .anyMatch(replacedGroup -> replacedGroup.getFileGroupId().equals(fileGroup));
    if (replaced) {
      return new HoodieWriteConflictException(
          HoodieWriteConflictException.ConflictCategory.INGESTION_VS_TABLE_SERVICE,
          String.format(
              "Deltacommit %s of table %s deletes rows of file group %s, which a replace commit"
                  + " such as a clustering replaced after the rows were looked up, so the deletes"
                  + " apply to no live base file. Retry the write",
              deltacommit.requestedTime(), metaClient.getTableConfig().getTableName(), fileGroup));
    }
    return new NotSupportedException(
        unsupportedLogContent(
            new HoodieLogFile(logFiles.iterator().next()),
            "records of a file group without a base file"));
  }

  /**
   * A compaction merges the log files its plan lists into the base file it writes. Log files
   * committed after it was scheduled belong to the file slice of that base file, and Iceberg has
   * recorded their deletes against the base file the compaction replaces, so they are carried over.
   */
  private Map<String, Set<Long>> positionsCarriedOverBy(
      HoodieCommitMetadata commitMetadata, HoodieInstant compaction) {
    Map<HoodieFileGroupId, Set<String>> mergedLogFiles = new HashMap<>();
    for (HoodieCompactionOperation operation :
        CompactionUtils.getCompactionPlan(metaClient, compaction.requestedTime()).getOperations()) {
      mergedLogFiles.put(
          new HoodieFileGroupId(operation.getPartitionPath(), operation.getFileId()),
          operation.getDeltaFilePaths().stream()
              .map(path -> new StoragePath(path).getName())
              .collect(Collectors.toSet()));
    }
    Map<String, Set<Long>> positionsByBaseFile = new LinkedHashMap<>();
    commitMetadata
        .getPartitionToWriteStats()
        .forEach(
            (partitionPath, writeStats) -> {
              for (HoodieWriteStat writeStat : writeStats) {
                Set<String> merged =
                    mergedLogFiles.getOrDefault(
                        new HoodieFileGroupId(partitionPath, writeStat.getFileId()),
                        Collections.emptySet());
                // The view does not hold the compaction yet, so the merged slice is the replaced
                // base file with every committed log file of both slices.
                List<HoodieLogFile> carried =
                    fsView()
                        .getLatestMergedFileSliceBeforeOrOn(
                            partitionPath, compaction.requestedTime(), writeStat.getFileId())
                        .map(
                            slice ->
                                slice
                                    .getLogFiles()
                                    .filter(logFile -> !merged.contains(logFile.getFileName()))
                                    .collect(Collectors.toList()))
                        .orElse(Collections.emptyList());
                if (!carried.isEmpty()) {
                  HoodieBaseFile baseFile =
                      new HoodieBaseFile(
                          FSUtils.constructAbsolutePath(
                                  metaClient.getBasePath(), writeStat.getPath())
                              .toString());
                  positionsByBaseFile.put(
                      qualifiedPath(baseFile), positionsDeletedFrom(baseFile, carried));
                }
              }
            });
    return positionsByBaseFile;
  }

  /**
   * Everything the remaining log files delete from the latest base file of each file group a
   * rollback changed, keyed by that base file's path. Those are the file groups the rollback
   * deleted files from, and the ones whose base files Iceberg removed when it recorded the
   * rolled-back commits, which the rollback makes live again. A file group left without a base file
   * is left out; one whose log files delete nothing maps to no positions.
   *
   * @param publishedCommitTimes requested times of the commits Iceberg has recorded; files of any
   *     other commit never reached Iceberg, so deleting them changes nothing there
   * @param filesRemovedByRolledBackCommits paths of the base files Iceberg removed when it recorded
   *     the rolled-back commits
   */
  Map<String, Set<Long>> positionsDeletedAfter(
      HoodieRollbackMetadata rollbackMetadata,
      Set<String> publishedCommitTimes,
      Collection<String> filesRemovedByRolledBackCommits) {
    Set<HoodieFileGroupId> fileGroups = new LinkedHashSet<>();
    rollbackMetadata
        .getPartitionMetadata()
        .forEach(
            (partitionPath, partitionMetadata) -> {
              for (String deleted : partitionMetadata.getSuccessDeleteFiles()) {
                StoragePath path = new StoragePath(deleted);
                if (FSUtils.isLogFile(path)) {
                  HoodieLogFile logFile = new HoodieLogFile(path);
                  if (publishedCommitTimes.contains(logFile.getDeltaCommitTime())) {
                    fileGroups.add(new HoodieFileGroupId(partitionPath, logFile.getFileId()));
                  }
                } else if (FSUtils.isBaseFile(path)
                    && publishedCommitTimes.contains(FSUtils.getCommitTimeWithFullPath(deleted))) {
                  fileGroups.add(
                      new HoodieFileGroupId(partitionPath, FSUtils.getFileIdFromFilePath(path)));
                }
              }
            });
    for (String removed : filesRemovedByRolledBackCommits) {
      StoragePath path = new StoragePath(removed);
      fileGroups.add(
          new HoodieFileGroupId(
              FSUtils.getRelativePartitionPath(metaClient.getBasePath(), path.getParent()),
              FSUtils.getFileIdFromFilePath(path)));
    }
    Map<String, Set<Long>> positionsByBaseFile = new LinkedHashMap<>();
    if (fileGroups.isEmpty()) {
      return positionsByBaseFile;
    }
    Option<HoodieInstant> lastInstant = metaClient.getActiveTimeline().lastInstant();
    if (!lastInstant.isPresent()) {
      return positionsByBaseFile;
    }
    fileGroups.stream()
        .map(HoodieFileGroupId::getPartitionPath)
        .distinct()
        .forEach(
            partitionPath ->
                // The merged slice is what a snapshot reader sees: the latest base file with every
                // committed log file, including those written after a compaction was scheduled.
                fsView()
                    .getLatestMergedFileSlicesBeforeOrOn(
                        partitionPath, lastInstant.get().requestedTime())
                    .filter(slice -> fileGroups.contains(slice.getFileGroupId()))
                    .filter(slice -> slice.getBaseFile().isPresent())
                    .collect(Collectors.toList())
                    .forEach(
                        slice ->
                            positionsByBaseFile.put(
                                qualifiedPath(slice.getBaseFile().get()),
                                positionsDeletedFrom(
                                    slice.getBaseFile().get(),
                                    slice.getLogFiles().collect(Collectors.toList())))));
    return positionsByBaseFile;
  }

  /**
   * Whether no row of the base file at the given path is live: the file holds no rows, or the log
   * files of its file slice delete every one of them. A delete that leaves no row of a base file
   * removes the file from Iceberg, while Hudi keeps it until a compaction, a clustering or a clean
   * replaces it, so later instants can still name it.
   *
   * @return false also when the file is gone or is not the base file of its file group's latest
   *     merged file slice
   */
  boolean holdsNoLiveRows(String path) {
    StoragePath baseFilePath = new StoragePath(path);
    try {
      if (!metaClient.getStorage().exists(baseFilePath)) {
        return false;
      }
    } catch (IOException e) {
      throw new ReadException("Unable to look up base file " + path, e);
    }
    long rows =
        HoodieIOFactory.getIOFactory(metaClient.getStorage())
            .getFileFormatUtils(baseFilePath)
            .getRowCount(metaClient.getStorage(), baseFilePath);
    if (rows == 0) {
      return true;
    }
    if (metaClient.getTableType() != HoodieTableType.MERGE_ON_READ) {
      return false;
    }
    Option<HoodieInstant> lastInstant = metaClient.getActiveTimeline().lastInstant();
    if (!lastInstant.isPresent()) {
      return false;
    }
    Option<FileSlice> slice =
        fsView()
            .getLatestMergedFileSliceBeforeOrOn(
                FSUtils.getRelativePartitionPath(
                    metaClient.getBasePath(), baseFilePath.getParent()),
                lastInstant.get().requestedTime(),
                FSUtils.getFileIdFromFilePath(baseFilePath));
    if (!slice.isPresent()
        || !slice.get().getBaseFile().isPresent()
        || !slice.get().getBaseFile().get().getFileName().equals(baseFilePath.getName())) {
      return false;
    }
    long deleted =
        positionsDeletedFrom(
                slice.get().getBaseFile().get(),
                slice.get().getLogFiles().collect(Collectors.toList()))
            .stream()
            .filter(position -> position >= 0 && position < rows)
            .count();
    return deleted == rows;
  }

  private Map<HoodieFileGroupId, Set<StoragePath>> logFilesByFileGroup(
      HoodieCommitMetadata commitMetadata, HoodieInstant deltacommit) {
    Map<HoodieFileGroupId, Set<StoragePath>> logFiles = new LinkedHashMap<>();
    commitMetadata
        .getPartitionToWriteStats()
        .forEach(
            (partitionPath, writeStats) -> {
              for (HoodieWriteStat writeStat : writeStats) {
                Set<StoragePath> paths = new LinkedHashSet<>();
                StoragePath path =
                    FSUtils.constructAbsolutePath(metaClient.getBasePath(), writeStat.getPath());
                if (FSUtils.isLogFile(path)) {
                  paths.add(path);
                }
                if (writeStat instanceof HoodieDeltaWriteStat) {
                  // The stat also lists the log files of its handle, which under change data
                  // capture include the ones earlier commits wrote to the file slice.
                  List<String> names = ((HoodieDeltaWriteStat) writeStat).getLogFiles();
                  if (names != null) {
                    StoragePath partition =
                        FSUtils.constructAbsolutePath(metaClient.getBasePath(), partitionPath);
                    names.stream()
                        .map(name -> new StoragePath(partition, name))
                        .filter(
                            logPath ->
                                deltacommit
                                    .requestedTime()
                                    .equals(new HoodieLogFile(logPath).getDeltaCommitTime()))
                        .forEach(paths::add);
                  }
                }
                if (!paths.isEmpty()) {
                  logFiles
                      .computeIfAbsent(
                          new HoodieFileGroupId(partitionPath, writeStat.getFileId()),
                          fileGroup -> new LinkedHashSet<>())
                      .addAll(paths);
                }
              }
            });
    return logFiles;
  }

  /**
   * The positions the given log files delete from a base file. A delete block counts its positions
   * in the base file of the instant it names; a block naming another version of the file group,
   * such as one a compaction replaced after the writer looked the rows up, has its keys located in
   * the given base file instead, which is how Hudi's reader applies it.
   */
  private Set<Long> positionsDeletedFrom(
      HoodieBaseFile baseFile, Collection<HoodieLogFile> logFiles) {
    Set<Long> positions = new LinkedHashSet<>();
    Set<String> keysToLocate = new HashSet<>();
    for (HoodieLogFile logFile : logFiles) {
      try (HoodieLogFormat.Reader reader =
          HoodieLogFormat.newReader(metaClient, logFile, tableSchema())) {
        while (reader.hasNext()) {
          HoodieLogBlock block = reader.next();
          if (!(block instanceof HoodieDeleteBlock)) {
            throw new NotSupportedException(
                unsupportedLogContent(logFile, "a " + block.getBlockType() + " with records"));
          }
          HoodieDeleteBlock deleteBlock = (HoodieDeleteBlock) block;
          DeleteRecord[] deletes = deleteBlock.getRecordsToDelete();
          List<Long> blockPositions = deleteBlock.getRecordPositionList();
          String baseInstantOfPositions = deleteBlock.getBaseFileInstantTimeOfPositions();
          if (baseInstantOfPositions == null || blockPositions.size() != deletes.length) {
            throw new NotSupportedException(
                unsupportedLogContent(
                    logFile,
                    String.format(
                        "%d deletes with %d record positions",
                        deletes.length, blockPositions.size())));
          }
          if (baseInstantOfPositions.equals(baseFile.getCommitTime())) {
            positions.addAll(blockPositions);
          } else {
            for (DeleteRecord delete : deletes) {
              keysToLocate.add(delete.getRecordKey());
            }
          }
        }
      } catch (IOException e) {
        throw new ReadException("Unable to read the deletes in log file " + logFile.getPath(), e);
      }
    }
    if (!keysToLocate.isEmpty()) {
      positions.addAll(positionsOf(baseFile, keysToLocate));
    }
    return positions;
  }

  /**
   * The row positions of the given record keys in a base file, read from its record key column. A
   * key the file does not hold has no position, as deleting it by key deletes nothing.
   */
  private Set<Long> positionsOf(HoodieBaseFile baseFile, Set<String> recordKeys) {
    if (!metaClient.getTableConfig().populateMetaFields()) {
      throw new NotSupportedException(
          String.format(
              "Table %s keeps no record key column in its base files, so deletes written against"
                  + " an earlier version of %s cannot be located in it",
              metaClient.getTableConfig().getTableName(), baseFile.getPath()));
    }
    StoragePath path = baseFile.getStoragePath();
    Set<Long> positions = new LinkedHashSet<>();
    HoodieIOFactory.getIOFactory(metaClient.getStorage())
        .getFileFormatUtils(path)
        .filterRowKeys(metaClient.getStorage(), path, recordKeys)
        .forEach(keyWithPosition -> positions.add(keyWithPosition.getRight()));
    return positions;
  }

  private String unsupportedLogContent(HoodieLogFile logFile, String found) {
    return String.format(
        "Table %s is published to Iceberg as base files with deletion vectors, so its log files"
            + " may only hold deletes that carry the record position of every deleted row, but"
            + " log file %s holds %s. Write the table with %s=true and an index that supplies"
            + " record positions",
        metaClient.getTableConfig().getTableName(),
        logFile.getPath(),
        found,
        UPDATES_AS_DELETES_AND_INSERTS);
  }

  private String qualifiedPath(HoodieBaseFile baseFile) {
    return IcebergTableFormat.qualifiedPath(metaClient, baseFile);
  }

  /**
   * The view synced to the timeline as Iceberg has it, on which an instant that Iceberg has not
   * recorded is pending, so the files of the instant being published are not part of the view.
   */
  private SyncableFileSystemView fsView() {
    if (fsView == null) {
      fsView = viewManager.getFileSystemView(metaClient);
      fsView.sync();
    }
    return fsView;
  }

  private HoodieSchema tableSchema() {
    if (tableSchema == null) {
      try {
        tableSchema = new TableSchemaResolver(metaClient).getTableSchema();
      } catch (Exception e) {
        throw new ReadException(
            "Unable to resolve the schema of table " + metaClient.getTableConfig().getTableName(),
            e);
      }
    }
    return tableSchema;
  }
}
