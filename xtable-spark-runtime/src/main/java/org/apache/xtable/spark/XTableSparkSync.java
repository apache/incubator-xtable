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

import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

import org.apache.xtable.model.storage.TableFormat;

/**
 * Standalone {@code spark-submit} entry point that runs a single XTable sync using the relocated
 * {@code xtable-spark-runtime} bundle. It is the RunSync-equivalent for this bundle and is
 * primarily used to validate that the shaded jar is self-contained on a real Spark install.
 *
 * <pre>
 * $SPARK_HOME/bin/spark-submit \
 *   --class org.apache.xtable.spark.XTableSparkSync \
 *   xtable-spark-runtime_2.12-&lt;ver&gt;.jar \
 *   --basepath /warehouse/db/orders --sourceformat HUDI --targets ICEBERG,DELTA
 * </pre>
 */
@Log4j2
public final class XTableSparkSync {

  private static final String BASE_PATH = "basepath";
  private static final String DATA_PATH = "datapath";
  private static final String SOURCE_FORMAT = "sourceformat";
  private static final String TARGETS = "targets";
  private static final String TABLE_NAME = "tablename";
  private static final String NAMESPACE = "namespace";
  private static final String PARTITION_SPEC = "partitionspec";
  private static final String HELP = "help";

  /**
   * Formats usable as --sourceformat. Must stay in sync with the source providers wired in {@link
   * XTableSyncService#sourceProviderFor}. Paimon and Parquet are read-only source formats (no
   * corresponding ConversionTarget exists).
   */
  // package-private for unit testing
  static final Set<String> SUPPORTED_SOURCE_FORMATS =
      new LinkedHashSet<>(
          Arrays.asList(
              TableFormat.HUDI,
              TableFormat.ICEBERG,
              TableFormat.DELTA,
              TableFormat.PAIMON,
              TableFormat.PARQUET));

  /**
   * Formats usable as --targets. Must stay in sync with the {@code ConversionTarget}s registered on
   * the classpath (Hudi/Iceberg/Delta); Paimon and Parquet have no target implementation. Used with
   * {@link #SUPPORTED_SOURCE_FORMATS} to fail fast on invalid formats before a SparkSession starts.
   */
  // package-private for unit testing
  static final Set<String> SUPPORTED_TARGET_FORMATS =
      new LinkedHashSet<>(Arrays.asList(TableFormat.HUDI, TableFormat.ICEBERG, TableFormat.DELTA));

  private static final Options OPTIONS =
      new Options()
          .addOption(
              Option.builder()
                  .longOpt(BASE_PATH)
                  .hasArg()
                  .required()
                  .desc("The base path of the source table")
                  .build())
          .addOption(
              Option.builder()
                  .longOpt(SOURCE_FORMAT)
                  .hasArg()
                  .required()
                  .desc("The source table format, e.g. HUDI, DELTA or ICEBERG")
                  .build())
          .addOption(
              Option.builder()
                  .longOpt(TARGETS)
                  .hasArg()
                  .required()
                  .desc("Comma-separated target formats to sync to, e.g. ICEBERG,DELTA")
                  .build())
          .addOption(
              Option.builder()
                  .longOpt(DATA_PATH)
                  .hasArg()
                  .desc("The path of the data files if different from the base path")
                  .build())
          .addOption(
              Option.builder()
                  .longOpt(TABLE_NAME)
                  .hasArg()
                  .desc("The table name; defaults to the last segment of the base path")
                  .build())
          .addOption(
              Option.builder()
                  .longOpt(NAMESPACE)
                  .hasArg()
                  .desc("The dot-separated table namespace")
                  .build())
          .addOption(
              Option.builder()
                  .longOpt(PARTITION_SPEC)
                  .hasArg()
                  .desc("The Hudi source partition field spec, e.g. level:VALUE")
                  .build())
          .addOption(Option.builder().longOpt(HELP).desc("Displays help information").build());

  private XTableSparkSync() {}

  public static void main(String[] args) {
    CommandLineParser parser = new DefaultParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(OPTIONS, args);
    } catch (ParseException e) {
      new HelpFormatter().printHelp("xtable-spark-sync", OPTIONS, true);
      throw new IllegalArgumentException("Failed to parse arguments", e);
    }
    if (cmd.hasOption(HELP)) {
      new HelpFormatter().printHelp("xtable-spark-sync", OPTIONS, true);
      return;
    }

    // Validate all arguments up front so an invalid value fails fast, before a SparkSession is
    // created (getOrCreate() below): otherwise a bad --sourceformat/--targets only surfaces deep in
    // the sync, after Spark has already been spun up.
    String basePath = cmd.getOptionValue(BASE_PATH);
    String tableName = cmd.getOptionValue(TABLE_NAME, basePathToTableName(basePath));

    String sourceFormat = cmd.getOptionValue(SOURCE_FORMAT).trim().toUpperCase(Locale.ROOT);
    validateFormat(SOURCE_FORMAT, sourceFormat, SUPPORTED_SOURCE_FORMATS);

    List<String> targetFormats =
        Arrays.stream(cmd.getOptionValue(TARGETS).split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(s -> s.toUpperCase(Locale.ROOT))
            .collect(Collectors.toList());
    if (targetFormats.isEmpty()) {
      throw new IllegalArgumentException(
          "--targets resolved to an empty list; provide at least one target format, e.g. ICEBERG,DELTA");
    }
    targetFormats.forEach(target -> validateFormat(TARGETS, target, SUPPORTED_TARGET_FORMATS));

    String namespace = cmd.getOptionValue(NAMESPACE);

    TableSyncSpec spec =
        TableSyncSpec.builder()
            .key(tableName)
            .basePath(basePath)
            .dataPath(cmd.getOptionValue(DATA_PATH))
            .namespace(namespace == null ? null : namespace.split("\\."))
            .partitionSpec(cmd.getOptionValue(PARTITION_SPEC))
            .sourceFormat(sourceFormat)
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

  // package-private for unit testing
  static void validateFormat(String option, String format, Set<String> supported) {
    if (!supported.contains(format)) {
      throw new IllegalArgumentException(
          "Unsupported --" + option + " '" + format + "'; expected one of " + supported);
    }
  }

  /**
   * Derives a table name from the last path segment of {@code basePath}, e.g. {@code
   * /warehouse/db/orders -> orders}. Trailing slashes are ignored. Throws when no usable segment
   * can be derived (e.g. {@code "/"} or an empty/blank path) so the caller is told to pass {@code
   * --tablename} explicitly instead of silently getting an empty table name.
   */
  // package-private for unit testing
  static String basePathToTableName(String basePath) {
    String trimmed = basePath == null ? "" : basePath;
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    int idx = trimmed.lastIndexOf('/');
    String name = idx >= 0 ? trimmed.substring(idx + 1) : trimmed;
    if (name.isEmpty()) {
      throw new IllegalArgumentException(
          "Cannot derive a table name from --basepath '"
              + basePath
              + "'; pass --tablename explicitly");
    }
    return name;
  }
}
