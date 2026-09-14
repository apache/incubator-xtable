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
 
package org.apache.xtable.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

/**
 * Standalone {@code spark-submit} entry point that runs a single XTable sync using the relocated
 * {@code xtable-spark-runtime} bundle. It is the RunSync-equivalent for this bundle and is
 * primarily used to validate that the shaded jar is self-contained on a real Spark install.
 *
 * <pre>
 * $SPARK_HOME/bin/spark-submit \
 *   --class org.apache.xtable.spark.XTableSparkSync \
 *   xtable-spark-runtime_2.12-&lt;ver&gt;.jar \
 *   --basePath /warehouse/db/orders --sourceFormat HUDI --targets ICEBERG,DELTA
 * </pre>
 */
@Log4j2
public final class XTableSparkSync {

  private XTableSparkSync() {}

  public static void main(String[] args) {
    Map<String, String> opts = parseArgs(args);
    String basePath = required(opts, "basePath");
    String sourceFormat = required(opts, "sourceFormat");
    String targets = required(opts, "targets");
    String tableName = opts.getOrDefault("tableName", basePathToName(basePath));

    List<String> targetFormats =
        Arrays.stream(targets.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(s -> s.toUpperCase(java.util.Locale.ROOT))
            .collect(Collectors.toList());

    TableSyncSpec spec =
        TableSyncSpec.builder()
            .key(tableName)
            .basePath(basePath)
            .dataPath(opts.get("dataPath"))
            .namespace(opts.containsKey("namespace") ? opts.get("namespace").split("\\.") : null)
            .sourceFormat(sourceFormat.toUpperCase(java.util.Locale.ROOT))
            .targets(targetFormats)
            .build();

    SparkSession spark = SparkSession.builder().appName("xtable-spark-sync").getOrCreate();
    try {
      Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();
      log.info("Starting standalone XTable sync for {}", spec.getBasePath());
      new XTableSyncService().sync(spec, hadoopConf);
      log.info("Completed XTable sync for {}", spec.getBasePath());
    } finally {
      spark.stop();
    }
  }

  private static Map<String, String> parseArgs(String[] args) {
    Map<String, String> opts = new HashMap<>();
    List<String> tokens = new ArrayList<>(Arrays.asList(args));
    for (int i = 0; i < tokens.size(); i++) {
      String token = tokens.get(i);
      if (token.startsWith("--")) {
        String key = token.substring(2);
        if (i + 1 < tokens.size() && !tokens.get(i + 1).startsWith("--")) {
          opts.put(key, tokens.get(++i));
        } else {
          opts.put(key, "true");
        }
      }
    }
    return opts;
  }

  private static String required(Map<String, String> opts, String key) {
    String value = opts.get(key);
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("Missing required argument: --" + key);
    }
    return value.trim();
  }

  private static String basePathToName(String basePath) {
    String trimmed =
        basePath.endsWith("/") ? basePath.substring(0, basePath.length() - 1) : basePath;
    int idx = trimmed.lastIndexOf('/');
    return idx >= 0 ? trimmed.substring(idx + 1) : trimmed;
  }
}
