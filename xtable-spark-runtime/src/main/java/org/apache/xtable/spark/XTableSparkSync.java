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
import java.util.List;
import java.util.Locale;
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

  private static final String BASE_PATH = "basePath";
  private static final String DATA_PATH = "dataPath";
  private static final String SOURCE_FORMAT = "sourceFormat";
  private static final String TARGETS = "targets";
  private static final String TABLE_NAME = "tableName";
  private static final String NAMESPACE = "namespace";
  private static final String PARTITION_SPEC = "partitionSpec";
  private static final String HELP = "help";

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

    String basePath = cmd.getOptionValue(BASE_PATH);
    String tableName = cmd.getOptionValue(TABLE_NAME, basePathToName(basePath));
    List<String> targetFormats =
        Arrays.stream(cmd.getOptionValue(TARGETS).split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(s -> s.toUpperCase(Locale.ROOT))
            .collect(Collectors.toList());
    String namespace = cmd.getOptionValue(NAMESPACE);

    TableSyncSpec spec =
        TableSyncSpec.builder()
            .key(tableName)
            .basePath(basePath)
            .dataPath(cmd.getOptionValue(DATA_PATH))
            .namespace(namespace == null ? null : namespace.split("\\."))
            .partitionSpec(cmd.getOptionValue(PARTITION_SPEC))
            .sourceFormat(cmd.getOptionValue(SOURCE_FORMAT).toUpperCase(Locale.ROOT))
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

  private static String basePathToName(String basePath) {
    String trimmed =
        basePath.endsWith("/") ? basePath.substring(0, basePath.length() - 1) : basePath;
    int idx = trimmed.lastIndexOf('/');
    return idx >= 0 ? trimmed.substring(idx + 1) : trimmed;
  }
}
