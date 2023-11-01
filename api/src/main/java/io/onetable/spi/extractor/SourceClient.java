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

import io.onetable.model.*;
import io.onetable.model.CommitsBacklog;
import io.onetable.model.schema.SchemaCatalog;

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
   * Extracts the {@link OneSnapshot} as of latest state.
   *
   * @return {@link OneSnapshot} representing the current snapshot.
   */
  OneSnapshot getCurrentSnapshot();

  /**
   * Extracts a {@link TableChange} for the provided commit.
   *
   * @param commit commit to capture table changes for.
   * @return {@link TableChange}
   */
  TableChange getTableChangeForCommit(COMMIT commit);

  /**
   * Retrieves {@link CommitsBacklog}, i.e. commits that have not been processed yet. based on the
   * provided {@link InstantsForIncrementalSync}.
   *
   * @param instantsForIncrementalSync The input to determine the next commits to process.
   * @return {@link CommitsBacklog} to process.
   */
  CommitsBacklog<COMMIT> getCommitsBacklog(InstantsForIncrementalSync instantsForIncrementalSync);

  /**
   * Confirms the presence of a commit at or before the given instant. This check is crucial to
   * ensure that if commits have been purged from the metadata since the last sync, snapshot sync is
   * leveraged to avoid data inconsistency between source and target formats.
   *
   * @param instant the instant in time to verify against existing commits
   * @return true if a commit at or before the provided instant exists, false otherwise
   */
  boolean doesCommitExistsAsOfInstant(Instant instant);

  /**
   * Checks if the instant is impacted by any cleanup operation such as Hudi's cleaner, Delta Lake's
   * vacuum, or Iceberg's snapshot expiry in the source table format. This check is crucial to
   * ensure that we fall back to snapshot sync when the incremental sync could lead to
   * inconsistencies.
   *
   * @param instant instant to check for cleanup operations
   * @return true if the instant is impacted by cleanup operations, false otherwise
   */
  boolean isAffectedByCleanupProcess(Instant instant);
}
