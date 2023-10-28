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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
   * @param target list of files currently in the target table
   * @param source list of files currently in the source table
   * @return the files that need to be added and removed to make the target table match the source
   *     table
   */
  public static OneDataFilesDiff from(List<OneDataFile> target, List<OneDataFile> source) {
    Map<String, OneDataFile> targetPaths =
        target.stream()
            .collect(Collectors.toMap(OneDataFile::getPhysicalPath, Function.identity()));
    Map<String, OneDataFile> sourcePaths =
        source.stream()
            .collect(Collectors.toMap(OneDataFile::getPhysicalPath, Function.identity()));

    // addedFiles
    Set<String> addedFiles = new HashSet<>(sourcePaths.keySet());
    addedFiles.removeAll(targetPaths.keySet());

    // removedFiles
    Set<String> removedFiles = new HashSet<>(targetPaths.keySet());
    removedFiles.removeAll(sourcePaths.keySet());

    Set<OneDataFile> filesAdded = new HashSet<>();
    Set<OneDataFile> filesRemoved = new HashSet<>();
    addedFiles.forEach(a -> filesAdded.add(sourcePaths.get(a)));
    removedFiles.forEach(r -> filesRemoved.add(targetPaths.get(r)));
    return OneDataFilesDiff.builder().filesAdded(filesAdded).filesRemoved(filesRemoved).build();
  }
}
