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
 
package org.apache.xtable.conversion;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DetectSourceType {
  // helper method to detect input format
  static String safeDetectFormat(String basePath, CatalogConfig catalogConfig) {
    try {
      Configuration conf = new Configuration();
      // map props to hadoop's
      if (catalogConfig != null && catalogConfig.getCatalogOptions() != null) {
        catalogConfig.getCatalogOptions().forEach(conf::set);
      }
      return DetectSourceType.detectFormat(ExternalTable.sanitizeBasePath(basePath), conf);
    } catch (IOException e) {
      return "UNKNOWN";
    }
  }

  public static String detectFormat(String pathStr, Configuration conf) throws IOException {
    Path basePath = new Path(pathStr);
    FileSystem fs = basePath.getFileSystem(conf);

    if (!fs.exists(basePath)) {
      return "UNKNOWN";
    }

    if (fs.exists(new Path(basePath, "_delta_log"))) {
      return "DELTA";
    }

    if (fs.exists(new Path(basePath, ".hoodie"))) {
      return "HUDI";
    }

    Path icebergMetaDir = new Path(basePath, "metadata");
    if (fs.exists(icebergMetaDir)) {
      return "ICEBERG";
    }

    return "UNKNOWN";
  }
}
