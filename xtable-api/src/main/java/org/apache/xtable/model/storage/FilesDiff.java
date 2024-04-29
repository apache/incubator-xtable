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
 
package org.apache.xtable.model.storage;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.Singular;
import lombok.experimental.SuperBuilder;

/**
 * Holds the collection of files that represent the difference between two states/commits/snapshots
 * of a table with respect to the data files. Between any two states of a table, the newer/latest
 * state may contain new files not present in the older state and may have removed files that were
 * present in the older state. In most cases the data files included in the newer state are derived
 * from a new commit in a source table format that has not been applied to a target table format
 * yet. Hence, the collection of data files in the newer state are typically {@link
 * InternalDataFile}s, whereas the files in the older state are represented using a generic type P
 * which can be a data file type in specific to the target table format.
 *
 * @param <L> the type of the files in the latest state
 * @param <P> the type of the files in the target table format
 */
@Data
@SuperBuilder
public class FilesDiff<L, P> {
  @Singular("fileAdded")
  private Set<L> filesAdded;

  @Singular("fileRemoved")
  private Set<P> filesRemoved;

  /**
   * Compares the latest files with the previous files and identifies the files that are new, i.e.
   * are present in latest files buy not present in the previously known files, and the files that
   * are removed, i.e. present in the previously known files but not present in the latest files.
   *
   * @param latestFiles a map of file path and file object representing files in the latest snapshot
   *     of a table
   * @param previousFiles a map of file path and file object representing files in a previously
   *     synced snapshot of a table.
   * @param <P> the type of the previous files
   * @return the diff of the files
   */
  public static <L, P> FilesDiff<L, P> findNewAndRemovedFiles(
      Map<String, L> latestFiles, Map<String, P> previousFiles) {
    Set<L> newFiles = new HashSet<>();
    Map<String, P> removedFiles = new HashMap<>(previousFiles);

    // if a file in latest files is also present in previous files, then it is neither new nor
    // removed.
    latestFiles.forEach(
        (key, value) -> {
          boolean notAKnownFile = removedFiles.remove(key) == null;
          if (notAKnownFile) {
            newFiles.add(value);
          }
        });
    return FilesDiff.<L, P>builder()
        .filesAdded(newFiles)
        .filesRemoved(removedFiles.values())
        .build();
  }

  /**
   * This method wraps the {@link #findNewAndRemovedFiles(Map, Map)} method, to compare the latest
   * file groups with the previous files and identifies the files that are new, i.e. are present in
   * latest files buy not present in the previously known files, and the files that are removed,
   * i.e. present in the previously known files but not present in the latest files.
   *
   * @param latestFileGroups a list of file groups representing the latest snapshot of a table
   * @param previousFiles a map of file path and file object representing files in a previously
   *     synced snapshot of a table
   * @param <P> the type of the previous files
   * @return the set of files that are added
   */
  public static <P> FilesDiff<InternalDataFile, P> findNewAndRemovedFiles(
      List<PartitionFileGroup> latestFileGroups, Map<String, P> previousFiles) {
    Map<String, InternalDataFile> latestFiles =
        latestFileGroups.stream()
            .flatMap(group -> group.getFiles().stream())
            .collect(Collectors.toMap(InternalDataFile::getPhysicalPath, Function.identity()));
    return findNewAndRemovedFiles(latestFiles, previousFiles);
  }
}
