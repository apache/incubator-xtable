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

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

import lombok.Value;

import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieReplaceCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import org.apache.xtable.model.IncrementalTableChanges;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.storage.InternalFilesDiff;

/**
 * Computes {@link org.apache.xtable.model.IncrementalTableChanges} between current state of the
 * table and new completed instant added to the timeline.
 */
@Value
public class HudiIncrementalTableChangeExtractor {
  HoodieTableMetaClient metaClient;
  HudiTableExtractor tableExtractor;
  HudiDataFileExtractor dataFileExtractor;

  public IncrementalTableChanges extractTableChanges(
      HoodieCommitMetadata commitMetadata, HoodieInstant completedInstant) {
    InternalTable internalTable =
        tableExtractor.table(metaClient, commitMetadata, completedInstant);
    InternalFilesDiff dataFilesDiff;
    if (commitMetadata instanceof HoodieReplaceCommitMetadata) {
      dataFilesDiff =
          dataFileExtractor.getDiffFromReplaceCommitMetadata(
              internalTable, (HoodieReplaceCommitMetadata) commitMetadata, completedInstant);
    } else {
      dataFilesDiff =
          dataFileExtractor.getDiffFromCommitMetadata(
              internalTable, commitMetadata, completedInstant);
    }
    return changesFor(internalTable, dataFilesDiff, completedInstant);
  }

  /**
   * The changes a completed rollback makes to the data files, see {@link
   * HudiDataFileExtractor#getDiffFromRollbackMetadata}.
   *
   * @param publishedCommitTimes requested times of the commits the target has recorded
   */
  public IncrementalTableChanges extractTableChanges(
      HoodieRollbackMetadata rollbackMetadata,
      HoodieInstant rollbackInstant,
      Set<String> publishedCommitTimes) {
    return extractTableChanges(
        rollbackMetadata,
        rollbackInstant,
        publishedCommitTimes,
        tableExtractor.table(metaClient, rollbackInstant));
  }

  /**
   * Same as {@link #extractTableChanges(HoodieRollbackMetadata, HoodieInstant, Set)} with the table
   * description supplied by the caller, for a rollback after which Hudi has no completed commit
   * left to derive it from.
   */
  public IncrementalTableChanges extractTableChanges(
      HoodieRollbackMetadata rollbackMetadata,
      HoodieInstant rollbackInstant,
      Set<String> publishedCommitTimes,
      InternalTable internalTable) {
    InternalFilesDiff dataFilesDiff =
        dataFileExtractor.getDiffFromRollbackMetadata(
            internalTable, rollbackMetadata, publishedCommitTimes);
    return changesFor(internalTable, dataFilesDiff, rollbackInstant);
  }

  /** An instant that changes no data files, such as a clean or a savepoint. */
  public IncrementalTableChanges extractTableChanges(HoodieInstant completedInstant) {
    InternalTable internalTable = tableExtractor.table(metaClient, completedInstant);
    return changesFor(
        internalTable,
        InternalFilesDiff.from(Collections.emptyList(), Collections.emptyList()),
        completedInstant);
  }

  private static IncrementalTableChanges changesFor(
      InternalTable internalTable, InternalFilesDiff dataFilesDiff, HoodieInstant instant) {
    Iterator<TableChange> tableChangeIterator =
        Collections.singleton(
                TableChange.builder()
                    .tableAsOfChange(internalTable)
                    .filesDiff(dataFilesDiff)
                    .sourceIdentifier(instant.getCompletionTime())
                    .build())
            .iterator();
    return IncrementalTableChanges.builder()
        .tableChanges(tableChangeIterator)
        .pendingCommits(Collections.emptyList())
        .build();
  }
}
