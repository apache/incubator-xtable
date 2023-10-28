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
import java.util.Objects;
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
   * @param source list of files currently in the source table
   * @param target list of files currently in the target table
   * @return files that need to be added and removed for the target table match the source table
   */
  public static OneDataFilesDiff from(List<OneDataFile> source, List<OneDataFile> target) {
    Map<String, OneDataFile> targetPaths =
        target.stream()
            .collect(Collectors.toMap(OneDataFile::getPhysicalPath, Function.identity()));
    // Any files in the source that are not in the target are added
    Set<OneDataFile> addedFiles = source.stream().map(file -> {
      OneDataFile targetFileIfPresent = targetPaths.remove(file.getPhysicalPath());
      return targetFileIfPresent == null ? file : null;
    }).filter(Objects::nonNull).collect(Collectors.toSet());
    // Any files remaining in the targetPaths map are not present in the source and should be marked for removal
    Set<OneDataFile> removedFiles = new HashSet<>(targetPaths.values());
    return OneDataFilesDiff.builder().filesAdded(addedFiles).filesRemoved(removedFiles).build();
  }
}
