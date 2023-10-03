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

import lombok.AllArgsConstructor;

import io.onetable.model.IncrementalTableChanges;
import io.onetable.model.InstantsForIncrementalSync;
import io.onetable.model.OneSnapshot;
import io.onetable.model.TableChange;

@AllArgsConstructor(staticName = "of")
public class ExtractFromSource<COMMIT extends Comparable<COMMIT>> {
  private final SourceClient<COMMIT> sourceClient;

  public OneSnapshot extractSnapshot() {
    return sourceClient.getCurrentFileState();
  }

  public IncrementalTableChanges extractTableChanges(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    Instant lastSyncInstant = instantsForIncrementalSync.getLastSyncInstant();
    List<Instant> pendingCommitInstants = instantsForIncrementalSync.getPendingCommits();
    COMMIT lastCommitSynced = sourceClient.getCommitAtInstant(lastSyncInstant);
    // List of files in partitions which have been affected.
    List<COMMIT> commitList = sourceClient.getCommits(lastCommitSynced);
    List<COMMIT> pendingCommits = sourceClient.getCommitsForInstants(pendingCommitInstants);
    // Remove pendingCommits that are already present in commitList
    List<COMMIT> updatedPendingCommits =
        removeCommitsFromPendingCommits(pendingCommits, commitList);
    // No overlap between updatedPendingCommits and commitList, process separately.
    List<TableChange> tableChangeList = new ArrayList<>();
    List<Instant> pendingCommitsForNextSync = new ArrayList<>();
    if (updatedPendingCommits != null && !updatedPendingCommits.isEmpty()) {
      for (COMMIT commit : updatedPendingCommits) {
        TableChange tableChange = sourceClient.getFilesDiffBetweenCommits(commit, commit, true);
        pendingCommitsForNextSync.addAll(tableChange.getPendingCommits());
        tableChangeList.add(tableChange);
      }
    }
    COMMIT previousCommit = lastCommitSynced;
    // Process the commits in commitList.
    for (COMMIT commit : commitList) {
      TableChange tableChange =
          sourceClient.getFilesDiffBetweenCommits(previousCommit, commit, false);
      pendingCommitsForNextSync.addAll(tableChange.getPendingCommits());
      tableChangeList.add(tableChange);
      previousCommit = commit;
    }
    return IncrementalTableChanges.builder()
        .tableChanges(tableChangeList)
        .pendingCommits(pendingCommitsForNextSync)
        .build();
  }

  private List<COMMIT> removeCommitsFromPendingCommits(
      List<COMMIT> pendingCommits, List<COMMIT> commitList) {
    // If pending commits is null or empty, or if commit list is null or empty,
    // return the same pending commits.
    if (pendingCommits == null
        || pendingCommits.isEmpty()
        || commitList == null
        || commitList.isEmpty()) {
      return pendingCommits;
    }

    // Remove until the last commit in the pending commits list that is less than or equal to
    // the first commit in the commit list.
    int lastIndexToRemove = -1;
    for (int i = pendingCommits.size() - 1; i >= 0; i--) {
      if (pendingCommits.get(i).compareTo(commitList.get(0)) > 0) {
        lastIndexToRemove = i;
      } else {
        break;
      }
    }

    // If there is no overlap between pending commits and commit list,
    // return the same pending commits.
    if (lastIndexToRemove == -1) {
      return pendingCommits;
    }
    return pendingCommits.subList(0, lastIndexToRemove);
  }

  public COMMIT getLastSyncCommit(Instant lastSyncTime) {
    return sourceClient.getCommitAtInstant(lastSyncTime);
  }
}
