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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopTables;

import io.delta.kernel.Snapshot;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.SnapshotImpl;

import org.apache.xtable.model.storage.TableFormat;

@Log4j2
public class SourceTableFormatDetector {
  // private constructor to prevent instantiation
  private SourceTableFormatDetector() {
    throw new UnsupportedOperationException("This class cannot be instantiated");
  }
  // helper method to detect input format
  public static String detectFormat(String pathStr, Configuration conf) throws IOException {
    String sanitizeBasePath = ExternalTable.sanitizeBasePath(pathStr);
    Path basePath = new Path(sanitizeBasePath);
    FileSystem fs = basePath.getFileSystem(conf);

    List<String> matches = new ArrayList<>();

    if (fs.exists(new Path(basePath, "_delta_log"))) {
      matches.add(TableFormat.DELTA);
    }

    if (fs.exists(new Path(basePath, ".hoodie"))) {
      matches.add(TableFormat.HUDI);
    }

    try {
      HadoopTables tables = new HadoopTables(conf);
      org.apache.iceberg.Table table = tables.load(pathStr);
      if (table != null) {
        matches.add(TableFormat.ICEBERG);
      }
    } catch (NoSuchTableException e) {
      log.debug("No Iceberg table found at path: {}", pathStr);
    } catch (Exception e) {
      log.debug("Unexpected error while probing for Iceberg table at path: {}", pathStr, e);
    }

    if (matches.size() == 1) {
      return matches.get(0);
    }

    if (matches.size() > 1) {
      log.info(
          "Multiple formats detected: {}. Resolving target sync vs original source...", matches);
      return inferSourceFromSyncMetadata(basePath, fs, matches, conf);
    }
    throw new IllegalArgumentException("Unable to detect table format for path: " + pathStr);
  }

  // checks source format from a XTable target
  private static String inferSourceFromSyncMetadata(
      Path basePath, FileSystem fs, List<String> detectedFormats, Configuration conf) {
    try {
      // check Hudi metadata if present
      if (detectedFormats.contains(TableFormat.HUDI)) {
        // check XTable target for hudi property file
        Path hoodieMeta = new Path(basePath, ".hoodie");
        if (fs.exists(new Path(hoodieMeta, "hoodie.properties"))) {
          return TableFormat.HUDI;
        }
      }

      if (detectedFormats.contains(TableFormat.ICEBERG)) {
        HadoopTables tables = new HadoopTables(conf);
        org.apache.iceberg.Table table = tables.load(basePath.toString());
        // check for target property tag during a  sync run
        if (table.properties().containsKey("xtable.conversion.target")) {
          detectedFormats.remove(TableFormat.ICEBERG);
          if (detectedFormats.size() == 1) {
            return detectedFormats.get(0);
          }
        }
      }
      if (detectedFormats.contains(TableFormat.DELTA)) {
        try {

          Engine kernelEngine = DefaultEngine.create(conf);

          io.delta.kernel.Table table =
              io.delta.kernel.Table.forPath(kernelEngine, basePath.toString());
          Snapshot snapshot = table.getLatestSnapshot(kernelEngine);

          if (snapshot instanceof SnapshotImpl) {
            Map<String, String> config = ((SnapshotImpl) snapshot).getMetadata().getConfiguration();

            if (config != null && config.containsKey("xtable.conversion.target")) {
              detectedFormats.remove(TableFormat.DELTA);
              if (detectedFormats.size() == 1) {
                return detectedFormats.get(0);
              }
            }
          }
        } catch (Exception e) {
          log.debug(
              "Failed to verify Delta log metadata via internal XTable engine at path: {}",
              basePath,
              e);
        }
      }
    } catch (Exception e) {
      log.warn("Failed to parse table metadata properties during conflict resolution step", e);
    }

    return null;
  }
}
