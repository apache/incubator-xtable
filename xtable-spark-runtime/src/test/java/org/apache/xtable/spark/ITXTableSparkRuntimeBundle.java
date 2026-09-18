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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hudi.client.HoodieReadClient;

import org.apache.xtable.GenericTable;
import org.apache.xtable.hudi.HudiTestUtil;

/**
 * Validates that the relocated {@code xtable-spark-runtime} bundle jar is self-contained by running
 * an actual XTable sync via {@code $SPARK_HOME/bin/spark-submit} for one case per direction, and
 * asserting the target is data-equivalent to the source. Engines (and the Avro/Parquet versions
 * they require) are supplied to the submit on a flat classpath via {@code
 * spark.driver.extraClassPath} from {@code target/engine-classpath.txt}, never bundled; a flat
 * classpath keeps a single Avro on one loader, matching a real cluster. Skipped when {@code
 * SPARK_HOME} is not set.
 *
 * <p>Directions are limited to Hudi and Iceberg: the root pom pins Delta 2.4.0 (Spark 3.4), which
 * does not run on a Spark 3.5 {@code SPARK_HOME}. Delta conversion is covered in-process by {@code
 * ITConversionController} (engine) and {@code ITXTableSyncListener} (listener).
 */
class ITXTableSparkRuntimeBundle {

  @TempDir static java.nio.file.Path tempDir;

  private static SparkSession sparkSession;
  private static JavaSparkContext jsc;

  @BeforeAll
  static void setupSpark() {
    sparkSession =
        SparkSession.builder()
            .config(HoodieReadClient.addHoodieSupport(HudiTestUtil.getSparkConf(tempDir)))
            .getOrCreate();
    jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
  }

  @AfterAll
  static void stopSpark() {
    if (jsc != null) {
      jsc.close();
    }
    if (sparkSession != null) {
      sparkSession.stop();
    }
  }

  private static Stream<Arguments> directions() {
    // sourceFormat, targetFormat, hudiPartitionSpec (only used for a partitioned Hudi source)
    return Stream.of(
        Arguments.of("HUDI", "ICEBERG", "level:VALUE"), Arguments.of("ICEBERG", "HUDI", null));
  }

  @ParameterizedTest(name = "{0} -> {1}")
  @MethodSource("directions")
  void bundleRunsSyncViaSparkSubmit(String sourceFormat, String targetFormat, String partitionSpec)
      throws Exception {
    String sparkHome = System.getenv("SPARK_HOME");
    assumeTrue(sparkHome != null && !sparkHome.trim().isEmpty(), "SPARK_HOME is not set");

    String tableName = sourceFormat.toLowerCase() + "_to_" + targetFormat.toLowerCase();
    String basePath;
    String dataPath;
    List<String> columns;
    String orderByColumn;
    try (GenericTable<?, ?> table =
        GenericTable.getInstance(tableName, tempDir, sparkSession, jsc, sourceFormat, true)) {
      table.insertRows(100);
      basePath = table.getBasePath();
      // Targets write metadata alongside the data files; for Iceberg this is <basePath>/data.
      dataPath = table.getDataPath();
      columns = table.getColumnsToSelect();
      orderByColumn = table.getOrderByColumn();
    }

    File bundleJar = findBundleJar();
    File sparkSubmit = new File(sparkHome, "bin/spark-submit");
    assertTrue(sparkSubmit.canExecute(), "spark-submit not executable at " + sparkSubmit);

    // Engines are provided on a FLAT classpath (single avro on one loader), NOT via --packages,
    // which would layer them in a child classloader and break cross-boundary avro casts.
    String engineClasspath = readEngineClasspath();

    List<String> command = new ArrayList<>();
    command.add(sparkSubmit.getAbsolutePath());
    command.add("--master");
    command.add("local[2]");
    command.add("--conf");
    command.add("spark.driver.extraClassPath=" + engineClasspath);
    command.add("--conf");
    command.add("spark.executor.extraClassPath=" + engineClasspath);
    command.add("--class");
    command.add("org.apache.xtable.spark.XTableSparkSync");
    command.add(bundleJar.getAbsolutePath());
    command.add("--basePath");
    command.add(basePath);
    command.add("--dataPath");
    command.add(dataPath);
    command.add("--sourceFormat");
    command.add(sourceFormat);
    command.add("--targets");
    command.add(targetFormat);
    command.add("--tableName");
    command.add(tableName);
    if (partitionSpec != null) {
      command.add("--partitionSpec");
      command.add(partitionSpec);
    }

    ProcessBuilder pb = new ProcessBuilder(command);
    pb.environment().put("SPARK_LOCAL_IP", "127.0.0.1");
    pb.redirectErrorStream(true);
    Process process = pb.start();
    String output = readOutput(process);
    boolean finished = process.waitFor(10, TimeUnit.MINUTES);
    if (!finished) {
      process.destroyForcibly();
      throw new AssertionError("spark-submit timed out. Output:\n" + output);
    }
    assertEquals(0, process.exitValue(), "spark-submit failed. Output:\n" + output);

    assertDatasetEquivalence(
        sourceFormat, targetFormat, basePath, dataPath, columns, orderByColumn, output);
  }

  /**
   * Reads the source (at its base path) and the target (at the data path, where the target metadata
   * was written) back through Spark and asserts they are row-for-row equivalent, mirroring {@code
   * ITConversionController.checkDatasetEquivalence}: a Hudi target is read with its metadata table
   * enabled, and per-format column expressions normalize representation differences.
   */
  private void assertDatasetEquivalence(
      String sourceFormat,
      String targetFormat,
      String basePath,
      String dataPath,
      List<String> columns,
      String orderByColumn,
      String submitOutput) {
    Dataset<Row> source =
        sparkSession
            .read()
            .options(readOptions(sourceFormat))
            .format(sourceFormat.toLowerCase())
            .load(basePath)
            .orderBy(orderByColumn);
    // Compare scalar columns only. Nested/array/map read parity across writer and reader engines is
    // covered in-process by ITConversionController; here it would exercise a Hudi-Parquet list
    // encoding vs Iceberg reader quirk unrelated to whether the bundle jar runs the sync.
    List<String> scalarColumns = atomicColumns(source, columns);
    List<String> sourceRows =
        source.selectExpr(selectColumns(scalarColumns, sourceFormat)).toJSON().collectAsList();
    List<String> targetRows =
        sparkSession
            .read()
            .options(readOptions(targetFormat))
            .format(targetFormat.toLowerCase())
            .load(dataPath)
            .orderBy(orderByColumn)
            .selectExpr(selectColumns(scalarColumns, targetFormat))
            .toJSON()
            .collectAsList();
    assertEquals(
        100,
        targetRows.size(),
        "Unexpected target row count. spark-submit output:\n" + submitOutput);
    assertEquals(sourceRows, targetRows, "Target is not data-equivalent to the source");
  }

  // Keeps only the requested columns whose source type is atomic (not struct/array/map).
  private static List<String> atomicColumns(Dataset<Row> source, List<String> columns) {
    java.util.Set<String> nonAtomic = new java.util.HashSet<>();
    for (org.apache.spark.sql.types.StructField field : source.schema().fields()) {
      org.apache.spark.sql.types.DataType type = field.dataType();
      if (type instanceof org.apache.spark.sql.types.StructType
          || type instanceof org.apache.spark.sql.types.ArrayType
          || type instanceof org.apache.spark.sql.types.MapType) {
        nonAtomic.add(field.name());
      }
    }
    return columns.stream()
        .filter(c -> !nonAtomic.contains(c))
        .collect(java.util.stream.Collectors.toList());
  }

  private static java.util.Map<String, String> readOptions(String format) {
    java.util.Map<String, String> options = new java.util.HashMap<>();
    if ("HUDI".equalsIgnoreCase(format)) {
      options.put("hoodie.metadata.enable", "true");
      options.put("hoodie.datasource.read.extract.partition.values.from.path", "true");
    } else if ("ICEBERG".equalsIgnoreCase(format)) {
      // Use the row-based reader: Iceberg's vectorized Arrow reader mis-casts timestamp columns
      // written by Hudi (TimeStampMicroTZVector vs BigIntVector) for this cross-engine table.
      options.put("vectorization-enabled", "false");
    }
    return options;
  }

  // Normalizes local-timestamp columns whose Hudi/Iceberg representations differ, matching
  // ITConversionController.getSelectColumnsArr; other columns pass through unchanged.
  private static String[] selectColumns(List<String> columns, String format) {
    boolean isHudi = "HUDI".equalsIgnoreCase(format);
    boolean isIceberg = "ICEBERG".equalsIgnoreCase(format);
    return columns.stream()
        .map(
            colName -> {
              if (colName.startsWith("timestamp_local_millis")) {
                if (isHudi) {
                  return String.format(
                      "unix_millis(CAST(%s AS TIMESTAMP)) AS %s", colName, colName);
                } else if (isIceberg) {
                  return String.format("%s div 1000 AS %s", colName, colName);
                }
                return colName;
              } else if (isHudi && colName.startsWith("timestamp_local_micros")) {
                return String.format("unix_micros(CAST(%s AS TIMESTAMP)) AS %s", colName, colName);
              }
              return colName;
            })
        .toArray(String[]::new);
  }

  private String readEngineClasspath() throws Exception {
    File cpFile = new File("target/engine-classpath.txt");
    assertTrue(
        cpFile.isFile(),
        "engine classpath not built at "
            + cpFile.getAbsolutePath()
            + " (dependency:build-classpath)");
    String cp =
        new String(
                java.nio.file.Files.readAllBytes(cpFile.toPath()),
                java.nio.charset.StandardCharsets.UTF_8)
            .trim();
    assertTrue(!cp.isEmpty(), "engine classpath file is empty");
    return cp;
  }

  private File findBundleJar() {
    File targetDir = new File("target");
    File[] candidates =
        targetDir.listFiles(
            (dir, name) -> name.startsWith("xtable-spark-runtime") && name.endsWith("-bundle.jar"));
    assertTrue(
        candidates != null && candidates.length == 1,
        "Expected exactly one bundle jar in "
            + targetDir.getAbsolutePath()
            + " (run the package phase first)");
    return candidates[0];
  }

  private static String readOutput(Process process) throws Exception {
    StringBuilder sb = new StringBuilder();
    try (java.io.BufferedReader reader =
        new java.io.BufferedReader(
            new java.io.InputStreamReader(
                process.getInputStream(), java.nio.charset.StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line).append(System.lineSeparator());
      }
    }
    return sb.toString();
  }
}
