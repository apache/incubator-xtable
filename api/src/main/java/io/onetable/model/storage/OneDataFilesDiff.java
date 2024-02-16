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
 
package io.onetable.model.storage;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;

/** Container for holding the list of files added and files removed between source and target. */
@Value
@Builder
public class OneDataFilesDiff {
  @Singular("fileAdded")
  Set<OneDataFile> filesAdded;

  @Singular("fileRemoved")
  Set<OneDataFile> filesRemoved;

  /**
   * Creates a OneDataFilesDiff from the list of files in the target table and the list of files in
   * the source table.
   *
   * @param source list of files currently in the source table
   * @param target list of files currently in the target table
   * @return files that need to be added and removed for the target table match the source table
   */
  public static OneDataFilesDiff from(List<OneDataFile> source, List<OneDataFile> target) {
    Map<String, OneDataFile> targetPaths =
        target.stream()
            .collect(Collectors.toMap(OneDataFile::getPhysicalPath, Function.identity()));
    Map<String, OneDataFile> sourcePaths =
        source.stream()
            .collect(Collectors.toMap(OneDataFile::getPhysicalPath, Function.identity()));

    Set<OneDataFile> addedFiles = findNewAndRemovedFiles(sourcePaths, targetPaths);
    Set<OneDataFile> removedFiles = new HashSet<>(targetPaths.values());
    return OneDataFilesDiff.builder().filesAdded(addedFiles).filesRemoved(removedFiles).build();
  }

  /**
   * Compares the latest files with the previous files and identifies the files that are new, i.e.
   * are present in latest files buy not present in the previously known files, and the files that
   * are removed, i.e. present in the previously known files but not present in the latest files.
   *
   * <p>Note: This method mutates the previousFiles map by removing the files that are present in
   * the latest files. After execution the previousFiles will only contain the files that are
   * removed.
   *
   * @param latestFiles a map of file path and file object representing files in the latest snapshot
   *     of a table
   * @param previousFiles a map of file path and file object representing files in a previously
   *     synced snapshot of a table. This method mutates this map by removing the files that are
   *     present in the latest files.
   * @param <L> the type of the latest files
   * @param <P> the type of the previous files
   * @return the set of files that are added
   */
  public static <L, P> Set<L> findNewAndRemovedFiles(
      Map<String, L> latestFiles, Map<String, P> previousFiles) {
    Set<L> newFiles = new HashSet<>();

    // if a file in latest files is also present in previous files, then it is neither new nor
    // removed.
    latestFiles.forEach(
        (key, value) -> {
          boolean notAKnownFile = previousFiles.remove(key) == null;
          if (notAKnownFile) {
            newFiles.add(value);
          }
        });
    return newFiles;
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
   * @param <T> the type of the previous files
   * @return the set of files that are added
   */
  public static <T> Set<OneDataFile> findNewAndRemovedFiles(
      List<OneFileGroup> latestFileGroups, Map<String, T> previousFiles) {
    Map<String, OneDataFile> latestFiles =
        latestFileGroups.stream()
            .flatMap(group -> group.getFiles().stream())
            .collect(Collectors.toMap(OneDataFile::getPhysicalPath, Function.identity()));
    return findNewAndRemovedFiles(latestFiles, previousFiles);
  }
}
