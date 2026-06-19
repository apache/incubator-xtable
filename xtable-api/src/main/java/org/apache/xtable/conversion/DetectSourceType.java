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

import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.Table;

public class DetectSourceType {
  // helper method to detect input format

  public static String detectFormat(String pathStr, Configuration conf) throws IOException {
    String sanitizeBasePath = ExternalTable.sanitizeBasePath(pathStr);
    Path basePath = new Path(sanitizeBasePath);
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

    // workaround for: .metadata can be set elsewhere
    try {
      HadoopTables tables = new HadoopTables(conf);
      // if the path points to a valid Iceberg table (even with a custom metadata location),
      Table table = tables.load(pathStr);
      if (table != null) {
        return "ICEBERG";
      }
    } catch (Exception e) {
    }

    return "UNKNOWN";
  }
}
