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
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

/**
 * Parses {@code spark.xtable.*} configuration into a list of {@link TableSyncSpec}.
 *
 * <p>Schema:
 *
 * <ul>
 *   <li>{@code spark.xtable.tables} - comma-separated per-table keys.
 *   <li>{@code spark.xtable.<key>.basePath} - source base path (path-based selection), OR
 *   <li>{@code spark.xtable.<key>.sourceTable} - {@code db.table} (name-based; resolved to a base
 *       path via the supplied {@code nameResolver}).
 *   <li>{@code spark.xtable.<key>.sourceFormat} - e.g. {@code HUDI}.
 *   <li>{@code spark.xtable.<key>.targets} - comma-separated target formats, e.g. {@code
 *       ICEBERG,DELTA}.
 *   <li>{@code spark.xtable.<key>.dataPath} (optional), {@code spark.xtable.<key>.namespace}
 *       (optional, dot-separated).
 * </ul>
 */
public final class XTableSparkConfig {
  static final String PREFIX = "spark.xtable.";
  static final String TABLES_KEY = PREFIX + "tables";

  private XTableSparkConfig() {}

  /**
   * @param conf resolves a config key to its value, or {@code null} if unset (e.g. backed by
   *     Spark's {@code RuntimeConfig}).
   * @param nameResolver resolves a {@code db.table} identifier to an absolute base path; only
   *     invoked for name-based table entries.
   */
  public static List<TableSyncSpec> parse(
      UnaryOperator<String> conf, UnaryOperator<String> nameResolver) {
    String tables = conf.apply(TABLES_KEY);
    if (isBlank(tables)) {
      return java.util.Collections.emptyList();
    }
    List<TableSyncSpec> specs = new ArrayList<>();
    for (String rawKey : tables.split(",")) {
      String key = rawKey.trim();
      if (key.isEmpty()) {
        continue;
      }
      specs.add(parseTable(key, conf, nameResolver));
    }
    return specs;
  }

  private static TableSyncSpec parseTable(
      String key, UnaryOperator<String> conf, UnaryOperator<String> nameResolver) {
    String p = PREFIX + key + ".";
    String basePath = conf.apply(p + "basePath");
    String sourceTableName = conf.apply(p + "sourceTable");
    if (isBlank(basePath) && !isBlank(sourceTableName)) {
      basePath = nameResolver.apply(sourceTableName.trim());
    }
    require(!isBlank(basePath), key, "requires '" + p + "basePath' or '" + p + "sourceTable'");

    String sourceFormat = conf.apply(p + "sourceFormat");
    require(!isBlank(sourceFormat), key, "requires '" + p + "sourceFormat'");

    String targetsStr = conf.apply(p + "targets");
    require(!isBlank(targetsStr), key, "requires '" + p + "targets'");
    List<String> targets =
        Arrays.stream(targetsStr.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(s -> s.toUpperCase(java.util.Locale.ROOT))
            .collect(Collectors.toList());
    require(!targets.isEmpty(), key, "requires at least one target in '" + p + "targets'");

    String dataPath = conf.apply(p + "dataPath");
    String namespaceStr = conf.apply(p + "namespace");
    String[] namespace = isBlank(namespaceStr) ? null : namespaceStr.trim().split("\\.");

    return TableSyncSpec.builder()
        .key(key)
        .basePath(basePath.trim())
        .dataPath(isBlank(dataPath) ? null : dataPath.trim())
        .namespace(namespace)
        .sourceFormat(sourceFormat.trim().toUpperCase(java.util.Locale.ROOT))
        .targets(targets)
        .build();
  }

  private static void require(boolean condition, String key, String message) {
    if (!condition) {
      throw new IllegalArgumentException(
          "Invalid spark.xtable config for table '" + key + "': " + message);
    }
  }

  private static boolean isBlank(String s) {
    return s == null || s.trim().isEmpty();
  }
}
