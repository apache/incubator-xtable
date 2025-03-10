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

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.SuperBuilder;

/** Container for holding the list of files added and files removed between source and target. */
@Value
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
public class DataFilesDiff extends FilesDiff<InternalDataFile, InternalDataFile> {

  /**
   * Creates a DataFilesDiff from the list of files in the target table and the list of files in the
   * source table.
   *
   * @param source list of files currently in the source table
   * @param target list of files currently in the target table
   * @return files that need to be added and removed for the target table match the source table
   */
  public static DataFilesDiff from(List<InternalDataFile> source, List<InternalDataFile> target) {
    Map<String, InternalDataFile> targetPaths =
        target.stream()
            .collect(Collectors.toMap(InternalDataFile::getPhysicalPath, Function.identity()));
    Map<String, InternalDataFile> sourcePaths =
        source.stream()
            .collect(Collectors.toMap(InternalDataFile::getPhysicalPath, Function.identity()));

    FilesDiff<InternalDataFile, InternalDataFile> diff =
        findNewAndRemovedFiles(sourcePaths, targetPaths);
    return DataFilesDiff.builder()
        .filesAdded(diff.getFilesAdded())
        .filesRemoved(diff.getFilesRemoved())
        .build();
  }
}
