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
 
package io.onetable.spi.extractor;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.NonNull;

import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.TableChange;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.OneDataFilesDiff;

@AllArgsConstructor(staticName = "of")
public class ExtractFromSource<COMMIT> {
  private final SourceClient<COMMIT> sourceClient;

  public OneSnapshot extractSnapshot() {
    COMMIT latestCommitInSource = sourceClient.getLatestCommit();
    OneTable table = sourceClient.getTable(latestCommitInSource);
    SchemaCatalog schemaCatalog = sourceClient.getSchemaCatalog(table, latestCommitInSource);
    OneDataFiles dataFiles = sourceClient.getFilesForAllPartitions(latestCommitInSource, table);
    return OneSnapshot.builder()
        .schemaCatalog(schemaCatalog)
        .table(table)
        .dataFiles(dataFiles)
        .build();
  }

  public List<TableChange> extractTableChanges(
      @NonNull OneDataFiles lastSyncedDataFiles, @NonNull Instant lastSyncTime) {
    COMMIT lastCommitSynced = sourceClient.getCommitAtInstant(lastSyncTime);
    // List of files in partitions which have been affected.
    List<COMMIT> commitList = sourceClient.getCommits(lastCommitSynced);
    List<TableChange> tableChangeList = new ArrayList<>();
    COMMIT previousCommit = lastCommitSynced;
    OneDataFiles latestFileState = lastSyncedDataFiles;
    for (COMMIT commit : commitList) {
      OneTable tableState = sourceClient.getTable(commit);
      OneDataFilesDiff filesDiff =
          sourceClient.getFilesDiffBetweenCommits(previousCommit, commit, tableState);
      // track updates to the state of the files in the table in memory
      latestFileState = syncChanges(latestFileState, filesDiff);
      tableChangeList.add(
          TableChange.builder()
              .filesDiff(filesDiff)
              .currentTableState(tableState)
              .dataFilesAfterDiff(latestFileState)
              .build());
      previousCommit = commit;
    }
    return tableChangeList;
  }

  public COMMIT getLastSyncCommit(Instant lastSyncTime) {
    return sourceClient.getCommitAtInstant(lastSyncTime);
  }

  private OneDataFiles syncChanges(OneDataFiles currentState, OneDataFilesDiff filesDiff) {
    Map<String, List<OneDataFile>> addedFilesByPartition =
        filesDiff.getFilesAdded().stream()
            .collect(Collectors.groupingBy(OneDataFile::getPartitionPath));
    Map<String, List<OneDataFile>> removedFilesByPartition =
        filesDiff.getFilesRemoved().stream()
            .collect(Collectors.groupingBy(OneDataFile::getPartitionPath));
    // update existing partitions
    OneDataFiles result =
        syncChanges(currentState, addedFilesByPartition, removedFilesByPartition, true);
    // add new partitions
    Set<String> existingPartitions =
        result.getFiles().stream().map(OneDataFile::getPartitionPath).collect(Collectors.toSet());
    List<OneDataFiles> newPartitions =
        addedFilesByPartition.entrySet().stream()
            .filter(entry -> !existingPartitions.contains(entry.getKey()))
            .map(
                entry ->
                    OneDataFiles.collectionBuilder()
                        .files(entry.getValue())
                        .partitionPath(entry.getKey())
                        .build())
            .collect(Collectors.toList());
    if (newPartitions.isEmpty()) {
      return result;
    } else {
      List<OneDataFile> allFiles = new ArrayList<>(result.getFiles());
      allFiles.addAll(newPartitions);
      return result.toBuilder().files(allFiles).build();
    }
  }

  private OneDataFiles syncChanges(
      OneDataFiles oneDataFiles,
      Map<String, List<OneDataFile>> addedFilesByPartition,
      Map<String, List<OneDataFile>> removedFilesByPartition,
      boolean isTopLevel) {
    if (addedFilesByPartition.isEmpty() && removedFilesByPartition.isEmpty()) {
      return oneDataFiles;
    }

    String partitionPath = oneDataFiles.getPartitionPath();
    List<OneDataFile> newFileList;
    if (addedFilesByPartition.containsKey(partitionPath)
        || removedFilesByPartition.containsKey(partitionPath)) {
      newFileList = new ArrayList<>(oneDataFiles.getFiles());
      newFileList.removeAll(
          removedFilesByPartition.getOrDefault(partitionPath, Collections.emptyList()));
      newFileList.addAll(
          addedFilesByPartition.getOrDefault(partitionPath, Collections.emptyList()));
    } else {
      newFileList =
          oneDataFiles.getFiles().stream()
              .map(
                  file -> {
                    if (file instanceof OneDataFiles) {
                      return syncChanges(
                          (OneDataFiles) file,
                          addedFilesByPartition,
                          removedFilesByPartition,
                          false);
                    } else {
                      return file;
                    }
                  })
              .filter(Objects::nonNull)
              .collect(Collectors.toList());
    }
    // Do not return null if this is the top level, at the top level an empty list represents an
    // empty table
    if (newFileList.isEmpty() && !isTopLevel) {
      return null;
    }
    return oneDataFiles.toBuilder().files(newFileList).build();
  }
}
