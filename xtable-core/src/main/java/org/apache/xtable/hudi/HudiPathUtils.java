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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;

public class HudiPathUtils {
  public static String getPartitionPath(Path tableBasePath, Path filePath) {
    String fileName = filePath.getName();
    String pathStr = filePath.toUri().getPath();
    int startIndex = tableBasePath.toUri().getPath().length() + 1;
    int endIndex = pathStr.length() - fileName.length() - 1;
    return endIndex <= startIndex ? "" : pathStr.substring(startIndex, endIndex);
  }

  /** Filters out metadata/hidden directory paths like _delta_log and .hoodie. */
  public static List<String> filterMetadataPaths(List<String> partitionPaths) {
    return partitionPaths.stream()
        .filter(
            p -> {
              if (p.isEmpty()) {
                return true;
              }
              String name = new Path(p).getName();
              return !name.startsWith("_") && !name.startsWith(".");
            })
        .collect(Collectors.toList());
  }
}
