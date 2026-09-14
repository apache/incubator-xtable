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
 
package org.apache.xtable.delta;

import java.util.Properties;

import lombok.Value;

/**
 * Configuration of the Delta source format for the sync process.
 *
 * <p>Read from the source table's additional properties, mirroring {@link
 * DeltaConversionTargetConfig} on the target side.
 */
@Value
public class DeltaConversionSourceConfig {
  /**
   * When {@code true}, an incremental backlog carries the table metadata across commits instead of
   * reconstructing the Delta snapshot for every commit. Defaults to {@code false}.
   *
   * <p>Enabling it also moves {@code latestCommitTime} to the commit file's modification time
   * carried by {@link DeltaIncrementalChangesState} (the same value {@code Snapshot.timestamp()}
   * returns), at the cost of a second listing of the log per backlog. Setting it back to {@code
   * false} reverts all three.
   */
  public static final String REUSE_METADATA_ACROSS_COMMITS =
      "xtable.delta.source.reuse_metadata_across_commits";

  /**
   * Allows sync to continue after finding a Delta deletion vector even though conversion targets
   * cannot represent it. Defaults to {@code false} so unsupported deletes fail instead of silently
   * producing inconsistent target data.
   */
  public static final String ALLOW_UNSUPPORTED_DELETION_VECTORS =
      "xtable.delta.source.allow_unsupported_deletion_vectors";

  boolean reuseMetadataAcrossCommits;
  boolean allowUnsupportedDeletionVectors;

  public DeltaConversionSourceConfig(boolean reuseMetadataAcrossCommits) {
    this(reuseMetadataAcrossCommits, false);
  }

  public DeltaConversionSourceConfig(
      boolean reuseMetadataAcrossCommits, boolean allowUnsupportedDeletionVectors) {
    this.reuseMetadataAcrossCommits = reuseMetadataAcrossCommits;
    this.allowUnsupportedDeletionVectors = allowUnsupportedDeletionVectors;
  }

  public static DeltaConversionSourceConfig fromProperties(Properties properties) {
    boolean reuseMetadataAcrossCommits =
        properties != null
            && Boolean.parseBoolean(
                properties.getProperty(REUSE_METADATA_ACROSS_COMMITS, Boolean.FALSE.toString()));
    boolean allowUnsupportedDeletionVectors =
        properties != null
            && Boolean.parseBoolean(
                properties.getProperty(
                    ALLOW_UNSUPPORTED_DELETION_VECTORS, Boolean.FALSE.toString()));
    return new DeltaConversionSourceConfig(
        reuseMetadataAcrossCommits, allowUnsupportedDeletionVectors);
  }
}
