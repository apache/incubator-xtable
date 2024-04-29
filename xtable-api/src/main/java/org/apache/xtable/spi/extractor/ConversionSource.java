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

import java.io.Closeable;
import java.time.Instant;

import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;

/**
 * A client that provides the major functionality for extracting the state at a given instant in a
 * source system. The client uses {@link Instant} to represent the point in time a commit was made
 * to be as generic as possible across source table formats.
 */
public interface ConversionSource<COMMIT> extends Closeable {
  /**
   * Extracts the {@link InternalTable} definition as of the provided commit.
   *
   * @param commit the commit to consider for reading the table state
   * @return the table definition
   */
  InternalTable getTable(COMMIT commit);

  /**
   * Extracts the {@link InternalSnapshot} as of latest state.
   *
   * @return {@link InternalSnapshot} representing the current snapshot.
   */
  InternalSnapshot getCurrentSnapshot();

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
   * Determines whether an incremental sync is safe from a given instant. This method checks for a
   * couple of things: the existence of a commit at or before the provided instant and whether the
   * instant has been impacted by any table cleanup operations, (Ex: Cleaner runs in Hudi, Vacuum in
   * Delta, Expiration of snapshots in Iceberg) It ensures that incremental sync is not used if
   * there is a risk of data inconsistencies due to missing commits (e.g., those purged from the
   * metadata) or due to table clean-up processes.
   *
   * @param instant the instant to check for incremental sync safety.
   * @return true if it is safe to proceed with incremental sync from the given instant or otherwise
   *     false.
   */
  boolean isIncrementalSyncSafeFrom(Instant instant);
}
