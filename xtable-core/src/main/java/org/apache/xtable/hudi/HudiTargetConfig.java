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

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

import lombok.Value;

import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.exception.HoodieException;

/** Configuration of the Hudi conversion target. */
@Value
public class HudiTargetConfig {
  /**
   * Table format version to write for the Hudi target. Supported values are {@code 6} (the legacy
   * 0.x timeline layout, column-stats index V1) and {@code 9} (the Hudi 1.x timeline layout,
   * column-stats index V2). Defaults to {@code 6}.
   */
  public static final String HUDI_TABLE_VERSION = "xtable.hudi.target.table_version";

  /**
   * Engine that writes the Hudi commit and the metadata table indexes. Supported values are {@code
   * java} (default) and {@code spark}. The Spark engine reuses the active Spark session and
   * distributes the index generation, which is recommended when the record index or a secondary
   * index is enabled because those indexes read every registered data file.
   */
  public static final String EXECUTION_ENGINE = "xtable.hudi.target.execution.engine";

  public static final String EXECUTION_ENGINE_JAVA = "java";
  public static final String EXECUTION_ENGINE_SPARK = "spark";

  /**
   * Comma separated columns to build a Hudi secondary index on, one index per column. Setting it
   * also enables the global record index, which the secondary index depends on. The indexes are
   * stored in the Hudi metadata table under the target table path and require table version 9. The
   * indexes are built when the Hudi table is created; a column added to the list of an existing
   * table is not indexed yet.
   */
  public static final String SECONDARY_INDEX_COLUMNS = "xtable.hudi.target.secondary.index.columns";

  /**
   * Lower and upper bound on the number of file groups of the record index. Both must be set
   * together; when unset the Hudi defaults apply.
   */
  public static final String RECORD_INDEX_MIN_FILEGROUP_COUNT =
      "xtable.hudi.target.metadata.record.index.min.filegroup.count";

  public static final String RECORD_INDEX_MAX_FILEGROUP_COUNT =
      "xtable.hudi.target.metadata.record.index.max.filegroup.count";

  /** Parallelism used when generating the secondary index records. */
  public static final String SECONDARY_INDEX_PARALLELISM =
      "xtable.hudi.target.metadata.index.secondary.parallelism";

  static final HoodieTableVersion DEFAULT_TABLE_VERSION = HoodieTableVersion.SIX;

  HoodieTableVersion tableVersion;
  boolean sparkEngine;
  List<String> secondaryIndexColumns;
  Optional<Integer> recordIndexMinFileGroupCount;
  Optional<Integer> recordIndexMaxFileGroupCount;
  Optional<Integer> secondaryIndexParallelism;

  public static HudiTargetConfig fromProperties(Properties properties) {
    Properties targetProperties = properties == null ? new Properties() : properties;
    HoodieTableVersion tableVersion = parseTableVersion(targetProperties);
    boolean sparkEngine = parseSparkEngine(targetProperties);
    List<String> secondaryIndexColumns =
        Arrays.stream(targetProperties.getProperty(SECONDARY_INDEX_COLUMNS, "").split(","))
            .map(String::trim)
            .filter(column -> !column.isEmpty())
            .distinct()
            .collect(Collectors.toList());
    if (!secondaryIndexColumns.isEmpty() && tableVersion != HoodieTableVersion.NINE) {
      throw new IllegalArgumentException(
          String.format(
              "%s requires Hudi target table version 9, set %s=9.",
              SECONDARY_INDEX_COLUMNS, HUDI_TABLE_VERSION));
    }
    if (secondaryIndexColumns.size() > 1 && !sparkEngine) {
      // Hudi builds the index of the first column in the commit and the others with its indexer.
      // In Hudi 1.2.1 the Java engine's JavaHoodieMetadataBulkInsertPartitioner writes an index
      // built from existing files into a single file group, so lookups miss most keys.
      throw new IllegalArgumentException(
          String.format(
              "More than one column in %s requires %s=%s.",
              SECONDARY_INDEX_COLUMNS, EXECUTION_ENGINE, EXECUTION_ENGINE_SPARK));
    }
    Optional<Integer> recordIndexMinFileGroupCount =
        parsePositiveInt(targetProperties, RECORD_INDEX_MIN_FILEGROUP_COUNT);
    Optional<Integer> recordIndexMaxFileGroupCount =
        parsePositiveInt(targetProperties, RECORD_INDEX_MAX_FILEGROUP_COUNT);
    if (recordIndexMinFileGroupCount.isPresent() != recordIndexMaxFileGroupCount.isPresent()) {
      throw new IllegalArgumentException(
          String.format(
              "%s and %s must be set together.",
              RECORD_INDEX_MIN_FILEGROUP_COUNT, RECORD_INDEX_MAX_FILEGROUP_COUNT));
    }
    return new HudiTargetConfig(
        tableVersion,
        sparkEngine,
        secondaryIndexColumns,
        recordIndexMinFileGroupCount,
        recordIndexMaxFileGroupCount,
        parsePositiveInt(targetProperties, SECONDARY_INDEX_PARALLELISM));
  }

  private static HoodieTableVersion parseTableVersion(Properties properties) {
    HoodieTableVersion tableVersion = DEFAULT_TABLE_VERSION;
    String configured = properties.getProperty(HUDI_TABLE_VERSION);
    if (configured != null && !configured.trim().isEmpty()) {
      tableVersion = parseTableVersion(configured.trim());
    }
    if (tableVersion != HoodieTableVersion.SIX && tableVersion != HoodieTableVersion.NINE) {
      throw unsupportedTableVersion(String.valueOf(tableVersion.versionCode()), null);
    }
    return tableVersion;
  }

  private static boolean parseSparkEngine(Properties properties) {
    String engine = properties.getProperty(EXECUTION_ENGINE, EXECUTION_ENGINE_JAVA).trim();
    if (EXECUTION_ENGINE_SPARK.equalsIgnoreCase(engine)) {
      return true;
    }
    if (EXECUTION_ENGINE_JAVA.equalsIgnoreCase(engine)) {
      return false;
    }
    throw new IllegalArgumentException(
        String.format(
            "Unsupported Hudi execution engine %s. Only %s and %s are supported via %s.",
            engine, EXECUTION_ENGINE_JAVA, EXECUTION_ENGINE_SPARK, EXECUTION_ENGINE));
  }

  private static Optional<Integer> parsePositiveInt(Properties properties, String key) {
    String configured = properties.getProperty(key);
    if (configured == null || configured.trim().isEmpty()) {
      return Optional.empty();
    }
    int value = Integer.parseInt(configured.trim());
    if (value <= 0) {
      throw new IllegalArgumentException(
          String.format("%s must be a positive integer but was %s.", key, configured));
    }
    return Optional.of(value);
  }

  private static HoodieTableVersion parseTableVersion(String configured) {
    try {
      return HoodieTableVersion.fromVersionCode(Integer.parseInt(configured));
    } catch (NumberFormatException | HoodieException e) {
      // A non-numeric value, or a version code that Hudi does not know.
      throw unsupportedTableVersion(configured, e);
    }
  }

  private static IllegalArgumentException unsupportedTableVersion(String value, Throwable cause) {
    return new IllegalArgumentException(
        String.format(
            "Unsupported Hudi target table version %s. Only table versions 6 and 9 are supported via %s.",
            value, HUDI_TABLE_VERSION),
        cause);
  }
}
