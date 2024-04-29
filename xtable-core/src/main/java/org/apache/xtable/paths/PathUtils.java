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
 
package org.apache.xtable.paths;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PathUtils {
  /**
   * Constructs the relative path to the file from the table's base path.
   *
   * @param path the path to the file, input can be full or relative
   * @param basePath the base path of the table
   * @return the relative path to the file
   */
  public static String getRelativePath(String path, String basePath) {
    String result;
    if (path.startsWith(basePath)) {
      return path.substring(basePath.length() + 1);
    } else if (path.contains(":")) {
      // handle differences in scheme like s3 vs s3a
      int pathStartIndex = getLastLeadingSlashIndex(path);
      int basePathStartIndex = getLastLeadingSlashIndex(basePath);
      return path.substring(basePath.length() + 1 + (pathStartIndex - basePathStartIndex));
    } else {
      result = path;
    }
    // trim leading slash
    return result.startsWith("/") ? result.substring(1) : result;
  }

  /**
   * Finds the index of the last '/' character after the file scheme.
   *
   * @param path input path, assumed to contain a scheme
   * @return the index or throws exception if not found
   */
  private static int getLastLeadingSlashIndex(String path) {
    int i = path.indexOf(':') + 1;
    for (; i < path.length(); i++) {
      if (path.charAt(i) != '/') {
        return i;
      }
    }
    throw new IllegalArgumentException("Invalid path: " + path);
  }
}
