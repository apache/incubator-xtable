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
public class ExtractFromSource<COMMIT> {
  private final SourceClient<COMMIT> sourceClient;

  public OneSnapshot extractSnapshot() {
    return sourceClient.getCurrentFileState();
  }

  public IncrementalTableChanges extractTableChanges(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    List<COMMIT> commitsToProcess =
        sourceClient.getNextCommitsToProcess(instantsForIncrementalSync);
    // No overlap between updatedPendingCommits and commitList, process separately.
    List<TableChange> tableChangeList = new ArrayList<>();
    List<Instant> pendingCommitsForNextSync = new ArrayList<>();
    for (COMMIT commit : commitsToProcess) {
      TableChange tableChange = sourceClient.getFilesDiffForCommit(commit);
      pendingCommitsForNextSync.addAll(tableChange.getPendingCommits());
      tableChangeList.add(tableChange);
    }
    return IncrementalTableChanges.builder()
        .tableChanges(tableChangeList)
        .pendingCommits(pendingCommitsForNextSync)
        .build();
  }
}
