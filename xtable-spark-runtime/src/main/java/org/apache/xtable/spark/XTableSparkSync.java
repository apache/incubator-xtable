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

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
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
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.ErrorDetails;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.model.sync.SyncStatusCode;

/**
 * {@code spark-submit} entry point for the bundle, the equivalent of {@code RunSync}.
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
  private static final String DATASET_CONFIG = "datasetconfig";
  private static final String USE_DELTA_KERNEL = "usedeltakernel";
  private static final String HELP = "help";

  /**
   * Formats usable as --sourceformat. Keep in sync with {@link
   * XTableSyncService#sourceProviderFor}.
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

  /** Formats usable as --targets. Paimon and Parquet are source-only. */
  // package-private for unit testing
  static final Set<String> SUPPORTED_TARGET_FORMATS =
      new LinkedHashSet<>(Arrays.asList(TableFormat.HUDI, TableFormat.ICEBERG, TableFormat.DELTA));

  private static final Options OPTIONS =
      new Options()
          .addOption(
              Option.builder()
                  .longOpt(BASE_PATH)
                  .hasArg()
                  .desc("The base path of the source table (single-table mode)")
                  .build())
          .addOption(
              Option.builder()
                  .longOpt(SOURCE_FORMAT)
                  .hasArg()
                  .desc("The source table format, e.g. HUDI, DELTA or ICEBERG (single-table mode)")
                  .build())
          .addOption(
              Option.builder()
                  .longOpt(TARGETS)
                  .hasArg()
                  .desc(
                      "Comma-separated target formats to sync to, e.g. ICEBERG,DELTA "
                          + "(single-table mode)")
                  .build())
          .addOption(
              Option.builder()
                  .longOpt(DATASET_CONFIG)
                  .hasArg()
                  .argName("path")
                  .desc(
                      "Path to a YAML dataset config (sourceFormat, targetFormats, datasets[]) to "
                          + "sync multiple tables in one run; may be a local or cloud (s3/gcs/abfs) "
                          + "path. Mutually exclusive with --basepath/--sourceformat/--targets.")
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
          .addOption(
              Option.builder()
                  .longOpt(USE_DELTA_KERNEL)
                  .desc(
                      "Use the Spark-free Delta Kernel implementation for the Delta source and/or "
                          + "target instead of the default delta-core path. When omitted this is "
                          + "auto-enabled on Spark 3.5+ (where the bundled delta-core does not "
                          + "run); pass it explicitly to force Kernel on any Spark version.")
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

    boolean datasetMode = cmd.hasOption(DATASET_CONFIG);
    if (datasetMode
        && (cmd.hasOption(BASE_PATH) || cmd.hasOption(SOURCE_FORMAT) || cmd.hasOption(TARGETS))) {
      throw new IllegalArgumentException(
          "--datasetconfig is mutually exclusive with --basepath/--sourceformat/--targets");
    }
    // Validate single-table flags before starting Spark. A --datasetconfig is validated after,
    // because it is read through the Spark Hadoop config.
    if (!datasetMode) {
      requireOption(cmd, BASE_PATH);
      requireOption(cmd, SOURCE_FORMAT);
      requireOption(cmd, TARGETS);
      validateFormat(
          SOURCE_FORMAT,
          cmd.getOptionValue(SOURCE_FORMAT).trim().toUpperCase(Locale.ROOT),
          SUPPORTED_SOURCE_FORMATS);
      parseTargets(cmd.getOptionValue(TARGETS)); // validates formats and non-emptiness
    }

    boolean useDeltaKernelFlag = cmd.hasOption(USE_DELTA_KERNEL);

    SparkSession spark = SparkSession.builder().appName("xtable-spark-sync").getOrCreate();
    try {
      Configuration hadoopConf = spark.sparkContext().hadoopConfiguration();

      String sourceFormat;
      List<String> targetFormats;
      List<TableSyncSpec.TableSyncSpecBuilder> builders = new ArrayList<>();
      if (datasetMode) {
        DatasetConfig config = readDatasetConfig(cmd.getOptionValue(DATASET_CONFIG), hadoopConf);
        sourceFormat = config.sourceFormat;
        targetFormats = config.targetFormats;
        for (DatasetConfig.Table table : config.datasets) {
          builders.add(
              TableSyncSpec.builder()
                  .key(
                      table.tableName != null
                          ? table.tableName
                          : basePathToTableName(table.tableBasePath))
                  .basePath(table.tableBasePath)
                  .dataPath(table.tableDataPath)
                  .namespace(table.namespace == null ? null : table.namespace.split("\\."))
                  .partitionSpec(table.partitionSpec)
                  .sourceFormat(sourceFormat)
                  .targets(targetFormats));
        }
      } else {
        sourceFormat = cmd.getOptionValue(SOURCE_FORMAT).trim().toUpperCase(Locale.ROOT);
        targetFormats = parseTargets(cmd.getOptionValue(TARGETS));
        String basePath = cmd.getOptionValue(BASE_PATH);
        String namespace = cmd.getOptionValue(NAMESPACE);
        builders.add(
            TableSyncSpec.builder()
                .key(cmd.getOptionValue(TABLE_NAME, basePathToTableName(basePath)))
                .basePath(basePath)
                .dataPath(cmd.getOptionValue(DATA_PATH))
                .namespace(namespace == null ? null : namespace.split("\\."))
                .partitionSpec(cmd.getOptionValue(PARTITION_SPEC))
                .sourceFormat(sourceFormat)
                .targets(targetFormats));
      }

      // delta-core does not run on Spark 3.5+, so use Delta Kernel there.
      boolean deltaInvolved =
          TableFormat.DELTA.equals(sourceFormat) || targetFormats.contains(TableFormat.DELTA);
      boolean useDeltaKernel = useDeltaKernelFlag;
      if (!useDeltaKernel && deltaInvolved && isSparkAtLeast35(spark.version())) {
        useDeltaKernel = true;
        log.info(
            "Spark {} detected (>= 3.5); auto-enabling the Delta Kernel implementation for Delta "
                + "source/target. Pass --usedeltakernel to force it on any Spark version.",
            spark.version());
      }

      XTableSyncService service = new XTableSyncService();
      List<TableSyncSpec> specs = new ArrayList<>();
      for (TableSyncSpec.TableSyncSpecBuilder builder : builders) {
        specs.add(builder.useDeltaKernel(useDeltaKernel).build());
      }
      int failures = syncAll(specs, spec -> service.sync(spec, hadoopConf));
      if (failures > 0) {
        throw new IllegalStateException(
            failures + " of " + specs.size() + " table sync(s) failed; see logs above");
      }
    } finally {
      spark.stop();
    }
  }

  /**
   * Syncs every spec and returns how many failed. A table fails when its sync throws or when any
   * target reports a non-SUCCESS status; one failure does not abort the rest of the batch.
   */
  // package-private for unit testing
  static int syncAll(
      List<TableSyncSpec> specs, Function<TableSyncSpec, Map<String, SyncResult>> syncer) {
    int failures = 0;
    for (TableSyncSpec spec : specs) {
      try {
        log.info("Starting XTable sync for {} ({})", spec.getKey(), spec.getBasePath());
        // TableFormatSync reports a target failure as an ERROR status rather than throwing.
        Map<String, String> failedTargets =
            syncer.apply(spec).entrySet().stream()
                .filter(
                    entry ->
                        entry.getValue().getTableFormatSyncStatus().getStatusCode()
                            != SyncStatusCode.SUCCESS)
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> describe(entry.getValue().getTableFormatSyncStatus()),
                        (a, b) -> a,
                        TreeMap::new));
        if (failedTargets.isEmpty()) {
          log.info("Completed XTable sync for {}", spec.getKey());
        } else {
          failures++;
          log.error(
              "XTable sync failed for {} ({}) on target(s) {}",
              spec.getKey(),
              spec.getBasePath(),
              failedTargets);
        }
      } catch (RuntimeException e) {
        failures++;
        log.error("XTable sync failed for {} ({})", spec.getKey(), spec.getBasePath(), e);
      }
    }
    return failures;
  }

  private static String describe(SyncResult.SyncStatus status) {
    ErrorDetails details = status.getErrorDetails();
    return details == null || details.getErrorMessage() == null
        ? String.valueOf(status.getStatusCode())
        : status.getStatusCode() + ": " + details.getErrorMessage();
  }

  private static void requireOption(CommandLine cmd, String option) {
    if (!cmd.hasOption(option)) {
      throw new IllegalArgumentException(
          "--" + option + " is required unless --" + DATASET_CONFIG + " is provided");
    }
  }

  // package-private for unit testing
  static List<String> parseTargets(String targetsValue) {
    List<String> targetFormats =
        Arrays.stream(targetsValue.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(s -> s.toUpperCase(Locale.ROOT))
            .collect(Collectors.toList());
    if (targetFormats.isEmpty()) {
      throw new IllegalArgumentException(
          "--targets resolved to an empty list; provide at least one target format, e.g. ICEBERG,DELTA");
    }
    targetFormats.forEach(target -> validateFormat(TARGETS, target, SUPPORTED_TARGET_FORMATS));
    return targetFormats;
  }

  static DatasetConfig readDatasetConfig(String path, Configuration hadoopConf) {
    Path configPath = new Path(path);
    try (InputStream in = configPath.getFileSystem(hadoopConf).open(configPath)) {
      return parseDatasetConfig(in);
    } catch (IOException e) {
      throw new IllegalArgumentException("Failed to read --datasetconfig at " + path, e);
    }
  }

  // SafeConstructor builds only maps, lists and scalars. package-private for unit testing.
  @SuppressWarnings("unchecked")
  static DatasetConfig parseDatasetConfig(InputStream in) {
    Object loaded = new Yaml(new SafeConstructor(new LoaderOptions())).load(in);
    if (!(loaded instanceof Map)) {
      throw new IllegalArgumentException("--datasetconfig is empty or not a YAML mapping");
    }
    Map<String, Object> root = (Map<String, Object>) loaded;

    DatasetConfig config = new DatasetConfig();
    config.sourceFormat = requireString(root, "sourceFormat").trim().toUpperCase(Locale.ROOT);
    validateFormat(SOURCE_FORMAT, config.sourceFormat, SUPPORTED_SOURCE_FORMATS);

    Object targetsRaw = root.get("targetFormats");
    if (!(targetsRaw instanceof List) || ((List<?>) targetsRaw).isEmpty()) {
      throw new IllegalArgumentException(
          "--datasetconfig 'targetFormats' must be a non-empty list");
    }
    config.targetFormats =
        ((List<Object>) targetsRaw)
            .stream()
                .map(String::valueOf)
                .map(s -> s.trim().toUpperCase(Locale.ROOT))
                .collect(Collectors.toList());
    config.targetFormats.forEach(t -> validateFormat(TARGETS, t, SUPPORTED_TARGET_FORMATS));

    Object datasetsRaw = root.get("datasets");
    if (!(datasetsRaw instanceof List) || ((List<?>) datasetsRaw).isEmpty()) {
      throw new IllegalArgumentException("--datasetconfig 'datasets' must be a non-empty list");
    }
    config.datasets = new ArrayList<>();
    for (Object entry : (List<Object>) datasetsRaw) {
      if (!(entry instanceof Map)) {
        throw new IllegalArgumentException("--datasetconfig 'datasets' entries must be mappings");
      }
      Map<String, Object> raw = (Map<String, Object>) entry;
      DatasetConfig.Table table = new DatasetConfig.Table();
      table.tableBasePath = requireString(raw, "tableBasePath");
      table.tableDataPath = asString(raw.get("tableDataPath"));
      table.tableName = asString(raw.get("tableName"));
      table.partitionSpec = asString(raw.get("partitionSpec"));
      table.namespace = asString(raw.get("namespace"));
      config.datasets.add(table);
    }
    return config;
  }

  private static String requireString(Map<String, Object> map, String key) {
    String value = asString(map.get(key));
    if (value == null || value.trim().isEmpty()) {
      throw new IllegalArgumentException("--datasetconfig is missing required field '" + key + "'");
    }
    return value;
  }

  private static String asString(Object value) {
    return value == null ? null : String.valueOf(value);
  }

  /** Parsed {@code --datasetconfig}; mirrors the RunSync dataset YAML so configs are shareable. */
  static final class DatasetConfig {
    String sourceFormat;
    List<String> targetFormats;
    List<Table> datasets;

    static final class Table {
      String tableBasePath;
      String tableDataPath;
      String tableName;
      String partitionSpec;
      String namespace;
    }
  }

  /**
   * True when the Spark version is 3.5 or newer; unparseable/null versions are treated as older.
   */
  // package-private for unit testing
  static boolean isSparkAtLeast35(String sparkVersion) {
    if (sparkVersion == null || sparkVersion.trim().isEmpty()) {
      return false;
    }
    String[] parts = sparkVersion.trim().split("\\.");
    try {
      int major = Integer.parseInt(parts[0]);
      int minor = parts.length > 1 ? Integer.parseInt(parts[1].replaceAll("[^0-9].*$", "")) : 0;
      return major > 3 || (major == 3 && minor >= 5);
    } catch (NumberFormatException e) {
      return false;
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
   * Returns the last path segment, e.g. {@code /warehouse/db/orders -> orders}. Throws when there
   * is none, so the user passes {@code --tablename}.
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
