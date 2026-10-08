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
 * Runs a sync with the shaded bundle through {@code $SPARK_HOME/bin/spark-submit}, one case per
 * direction, and checks the target data equals the source. The engines come from {@code
 * target/engine-classpath.txt}. Skipped when {@code SPARK_HOME} is not set.
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
    // sourceFormat, targetFormat, hudiPartitionSpec
    return Stream.of(
        Arguments.of("HUDI", "ICEBERG", "level:VALUE"),
        Arguments.of("ICEBERG", "HUDI", null),
        Arguments.of("HUDI", "DELTA", "level:VALUE"),
        Arguments.of("DELTA", "HUDI", null));
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
      dataPath = table.getDataPath();
      columns = table.getColumnsToSelect();
      orderByColumn = table.getOrderByColumn();
    }

    File bundleJar = findBundleJar();
    File sparkSubmit = new File(sparkHome, "bin/spark-submit");
    assertTrue(sparkSubmit.canExecute(), "spark-submit not executable at " + sparkSubmit);

    // A flat classpath, not --packages: a child classloader breaks Avro casts across the boundary.
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
    command.add("--basepath");
    command.add(basePath);
    command.add("--datapath");
    command.add(dataPath);
    command.add("--sourceformat");
    command.add(sourceFormat);
    command.add("--targets");
    command.add(targetFormat);
    command.add("--tablename");
    command.add(tableName);
    if (partitionSpec != null) {
      command.add("--partitionspec");
      command.add(partitionSpec);
    }

    ProcessBuilder pb = new ProcessBuilder(command);
    pb.environment().put("SPARK_LOCAL_IP", "127.0.0.1");
    pb.redirectErrorStream(true);
    Process process = pb.start();
    // Drain on a thread so a hung spark-submit cannot block the timeout below.
    StringBuilder outputBuffer = new StringBuilder();
    Thread drainer = new Thread(() -> readOutputInto(process, outputBuffer), "spark-submit-stdout");
    drainer.setDaemon(true);
    drainer.start();
    boolean finished = process.waitFor(10, TimeUnit.MINUTES);
    if (!finished) {
      process.destroyForcibly();
      drainer.join(TimeUnit.SECONDS.toMillis(10));
      synchronized (outputBuffer) {
        throw new AssertionError("spark-submit timed out. Output:\n" + outputBuffer);
      }
    }
    drainer.join(TimeUnit.SECONDS.toMillis(10));
    String output;
    synchronized (outputBuffer) {
      output = outputBuffer.toString();
    }
    assertEquals(0, process.exitValue(), "spark-submit failed. Output:\n" + output);

    assertDatasetEquivalence(
        sourceFormat, targetFormat, basePath, dataPath, columns, orderByColumn, output);
  }

  /** Reads the source and the target back through Spark and asserts the scalar columns match. */
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
    String[] projection = comparableColumns(source, columns).toArray(new String[0]);
    List<String> sourceRows = source.selectExpr(projection).toJSON().collectAsList();
    List<String> targetRows =
        sparkSession
            .read()
            .options(readOptions(targetFormat))
            .format(targetFormat.toLowerCase())
            .load(dataPath)
            .orderBy(orderByColumn)
            .selectExpr(projection)
            .toJSON()
            .collectAsList();
    assertEquals(
        100,
        targetRows.size(),
        "Unexpected target row count. spark-submit output:\n" + submitOutput);
    assertEquals(sourceRows, targetRows, "Target is not data-equivalent to the source");
  }

  // Drops nested columns (covered by ITConversionController) and local timestamps (HUDI-7088).
  private static List<String> comparableColumns(Dataset<Row> source, List<String> columns) {
    java.util.Set<String> excluded = new java.util.HashSet<>();
    for (org.apache.spark.sql.types.StructField field : source.schema().fields()) {
      org.apache.spark.sql.types.DataType type = field.dataType();
      if (type instanceof org.apache.spark.sql.types.StructType
          || type instanceof org.apache.spark.sql.types.ArrayType
          || type instanceof org.apache.spark.sql.types.MapType) {
        excluded.add(field.name());
      }
    }
    return columns.stream()
        .filter(c -> !excluded.contains(c))
        .filter(c -> !c.startsWith("timestamp_local_"))
        .collect(java.util.stream.Collectors.toList());
  }

  private static java.util.Map<String, String> readOptions(String format) {
    java.util.Map<String, String> options = new java.util.HashMap<>();
    if ("HUDI".equalsIgnoreCase(format)) {
      options.put("hoodie.metadata.enable", "true");
      options.put("hoodie.datasource.read.extract.partition.values.from.path", "true");
    } else if ("ICEBERG".equalsIgnoreCase(format)) {
      // The vectorized reader mis-casts timestamps written by Hudi.
      options.put("vectorization-enabled", "false");
    }
    return options;
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

  // The shaded jar is the main artifact; skip original-* and the sources, javadoc and tests jars.
  private File findBundleJar() {
    File targetDir = new File("target");
    File[] candidates =
        targetDir.listFiles(
            (dir, name) ->
                name.startsWith("xtable-spark-runtime")
                    && name.endsWith(".jar")
                    && !name.startsWith("original-")
                    && !name.contains("-sources")
                    && !name.contains("-javadoc")
                    && !name.contains("-tests"));
    assertTrue(
        candidates != null && candidates.length == 1,
        "Expected exactly one bundle jar in "
            + targetDir.getAbsolutePath()
            + " (run the package phase first)");
    return candidates[0];
  }

  private static void readOutputInto(Process process, StringBuilder sb) {
    try (java.io.BufferedReader reader =
        new java.io.BufferedReader(
            new java.io.InputStreamReader(
                process.getInputStream(), java.nio.charset.StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        synchronized (sb) {
          sb.append(line).append(System.lineSeparator());
        }
      }
    } catch (java.io.IOException e) {
      // Closed on timeout; the output so far is still reported.
    }
  }
}
