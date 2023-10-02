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
import java.util.List;

import io.onetable.model.OneTable;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.OneDataFilesDiff;

/**
 * A client that provides the major functionality for extracting the state at a given instant in a
 * source system. The client uses {@link Instant} to represent the point in time a commit was made
 * to be as generic as possible across source table formats.
 */
public interface SourceClient<COMMIT> {
  /**
   * Extracts the {@link OneTable} definition as of the provided commit.
   *
   * @param commit the commit to consider for reading the table state
   * @return the table definition
   */
  OneTable getTable(COMMIT commit);

  /**
   * Extracts the {@link SchemaCatalog} as of the provided instant.
   *
   * @param table the current state of the table for this commit
   * @param commit the commit to consider for reading the schema catalog
   * @return the schema catalog
   */
  SchemaCatalog getSchemaCatalog(OneTable table, COMMIT commit);

  /**
   * Extracts all of the {@link OneDataFiles} for the table, grouped by partition.
   *
   * @param commit the commit to consider for reading the files
   * @param tableDefinition the OneTable definition of the table defining the schema, partitioning
   *     fields, etc. to use when converting into the OneTable format.
   * @return a list of files grouped by partition
   */
  OneDataFiles getFilesForAllPartitions(COMMIT commit, OneTable tableDefinition);

  /**
   * Extracts a {@link OneDataFilesDiff} that contains all the {@link
   * io.onetable.model.storage.OneDataFile} added or removed by updates that happened after the
   * provided `afterCommit` up to and including the `untilCommit`.
   *
   * @param startCommit limit the changes to commits that are strictly after (and not including)
   *     this commit
   * @param endCommit limit the changes to commits up to and including this commit * @param
   * @param tableDefinition the OneTable definition of the table defining the schema, partition
   *     fields, etc. to use when converting into the OneTable format.
   * @param includeStart whether to include the start commit in the diff.
   * @return a list of files grouped by partition
   */
  OneDataFilesDiff getFilesDiffBetweenCommits(
      COMMIT startCommit, COMMIT endCommit, OneTable tableDefinition, boolean includeStart);

  /**
   * Get all the commit times that occurred on or after the provided commit from oldest to newest.
   *
   * @param onOrAfterCommit only return commits that are after (and not including) this commit
   * @return list of commit times after the provided commit time, sorted from oldest to newest
   *     commit
   */
  List<COMMIT> getCommits(COMMIT onOrAfterCommit);

  /**
   * Get the latest completed commit in the source table.
   *
   * @return the latest completed commit
   */
  COMMIT getLatestCommit();

  /**
   * Get all the pending commits that occurred before the provided commit.
   *
   * @param beforeCommit only return commits that are strictly before (and not including) that are
   *     pending.
   * @return List of pending commits before the provided commit from oldest to newest commit.
   */
  List<Instant> getPendingCommitsBeforeCommit(COMMIT beforeCommit);

  /**
   * Gets the last commit made at or before the provided instant from the source table.
   *
   * @param instant point in time
   * @return the commit at or before the provided instant
   */
  COMMIT getCommitAtInstant(Instant instant);
}
