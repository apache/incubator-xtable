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
import java.util.List;
import java.util.Optional;

import lombok.AllArgsConstructor;

import io.onetable.model.IncrementalTableChanges;
import io.onetable.model.InstantsForIncrementalSync;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.TableChange;
import io.onetable.model.schema.SchemaCatalog;
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
    List<Instant> pendingCommits = sourceClient.getPendingCommitsBeforeCommit(latestCommitInSource);
    if (pendingCommits != null && !pendingCommits.isEmpty()) {
      table = table.toBuilder().pendingCommits(pendingCommits).build();
    }
    return OneSnapshot.builder()
        .schemaCatalog(schemaCatalog)
        .table(table)
        .dataFiles(dataFiles)
        .build();
  }

  public IncrementalTableChanges extractTableChanges(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    Optional<Instant> lastSyncInstant = instantsForIncrementalSync.getLastSyncInstant();
    List<Instant> pendingCommits = instantsForIncrementalSync.getPendingCommits();
    boolean includeStart;
    Instant instantToConsiderForStart;
    if (pendingCommits == null
        || pendingCommits.isEmpty()
        || pendingCommits.get(0).isAfter(lastSyncInstant.get())) {
      includeStart = false;
      instantToConsiderForStart = lastSyncInstant.get();
    } else {
      includeStart = true;
      instantToConsiderForStart = pendingCommits.get(0);
    }
    COMMIT lastCommitSynced = sourceClient.getCommitAtInstant(instantToConsiderForStart);
    // List of files in partitions which have been affected.
    List<COMMIT> commitList = sourceClient.getCommits(lastCommitSynced);
    List<TableChange> tableChangeList = new ArrayList<>();
    COMMIT previousCommit = lastCommitSynced;
    for (COMMIT commit : commitList) {
      if (previousCommit.equals(commit) && !includeStart) {
        previousCommit = commit;
        continue;
      }
      OneTable tableState = sourceClient.getTable(commit);
      OneDataFilesDiff filesDiff =
          sourceClient.getFilesDiffBetweenCommits(
              previousCommit, commit, tableState, previousCommit.equals(commit) && includeStart);
      tableChangeList.add(
          TableChange.builder().filesDiff(filesDiff).currentTableState(tableState).build());
      previousCommit = commit;
    }
    List<Instant> pendingCommitsForNextSync =
        sourceClient.getPendingCommitsBeforeCommit(previousCommit);
    return IncrementalTableChanges.builder()
        .tableChanges(tableChangeList)
        .pendingCommits(pendingCommitsForNextSync)
        .build();
  }

  public COMMIT getLastSyncCommit(Instant lastSyncTime) {
    return sourceClient.getCommitAtInstant(lastSyncTime);
  }
}
