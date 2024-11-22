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
 
package org.apache.xtable.spi.extractor;

import java.util.Iterator;

import lombok.AllArgsConstructor;
import lombok.Getter;

import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.IncrementalTableChanges;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.TableChange;

@AllArgsConstructor(staticName = "of")
@Getter
public class ExtractFromSource<COMMIT> {
  private final ConversionSource<COMMIT> conversionSource;

  public InternalSnapshot extractSnapshot() {
    return conversionSource.getCurrentSnapshot();
  }

  public IncrementalTableChanges extractTableChanges(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    CommitsBacklog<COMMIT> commitsBacklog =
        conversionSource.getCommitsBacklog(instantsForIncrementalSync);
    // No overlap between updatedPendingCommits and commitList, process separately.
    Iterator<TableChange> tableChangeIterator =
        commitsBacklog.getCommitsToProcess().stream()
            .map(conversionSource::getTableChangeForCommit)
            .iterator();
    return IncrementalTableChanges.builder()
        .tableChanges(tableChangeIterator)
        .pendingCommits(commitsBacklog.getInFlightInstants())
        .build();
  }
}
