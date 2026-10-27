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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarFile;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hudi.client.HoodieReadClient;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.spark.SparkSchemaUtil;

import org.apache.xtable.GenericTable;
import org.apache.xtable.catalog.TableFormatUtils;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.hudi.HudiTargetConfig;
import org.apache.xtable.hudi.HudiTestUtil;
import org.apache.xtable.hudi.index.HudiBackedSecondaryIndexReader;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.model.storage.TableFormat;

/**
 * Runs a sync with the shaded bundle through {@code $SPARK_HOME/bin/spark-submit}, one case per
 * direction, and checks the target data equals the source. The engines come from {@code
 * target/engine-classpath.txt}. Skipped when {@code SPARK_HOME} is not set.
 */
class ITXTableSparkRuntimeBundle {

  @TempDir static Path tempDir;

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
        Arguments.of("ICEBERG", "DELTA", null),
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

    List<String> syncArgs = new ArrayList<>();
    syncArgs.add("--basepath");
    syncArgs.add(basePath);
    syncArgs.add("--datapath");
    syncArgs.add(dataPath);
    syncArgs.add("--sourceformat");
    syncArgs.add(sourceFormat);
    syncArgs.add("--targets");
    syncArgs.add(targetFormat);
    syncArgs.add("--tablename");
    syncArgs.add(tableName);
    if (partitionSpec != null) {
      syncArgs.add("--partitionspec");
      syncArgs.add(partitionSpec);
    }
    String output = runBundleSync(sparkHome, syncArgs);

    assertDatasetEquivalence(
        sourceFormat, targetFormat, basePath, dataPath, columns, orderByColumn, output);
  }

  /**
   * Users name the Spark SQL extensions in {@code spark.sql.extensions}, so the bundle must ship
   * them under their own package, not relocated.
   */
  @Test
  void bundleShipsSparkSqlExtensions() throws Exception {
    try (JarFile bundle = new JarFile(findBundleJar())) {
      assertTrue(
          bundle.getEntry("org/apache/xtable/spark/extensions/XTableSparkSessionExtensions.class")
              != null,
          "XTableSparkSessionExtensions is missing from the bundle");
    }
  }

  /**
   * A sync job keeps the secondary index current: the dataset config passes the index settings to
   * the Hudi target, and the index is read back at the data path of the Iceberg table.
   */
  @Test
  void datasetConfigTargetPropertiesBuildTheSecondaryIndex() throws Exception {
    String sparkHome = System.getenv("SPARK_HOME");
    assumeTrue(sparkHome != null && !sparkHome.trim().isEmpty(), "SPARK_HOME is not set");

    String tableName = "iceberg_indexed_by_sync";
    String basePath = tempDir.resolve(tableName).toUri().toString();
    Dataset<Row> rows =
        sparkSession
            .range(100)
            .selectExpr("id", "concat('user', cast(id % 40 AS string), '@example.com') AS email");
    Schema schema = SparkSchemaUtil.convert(rows.schema());
    Table table =
        new HadoopTables(jsc.hadoopConfiguration())
            .create(schema, PartitionSpec.unpartitioned(), basePath);
    rows.write().format("iceberg").mode("append").save(basePath);
    table.refresh();
    String dataPath =
        TableFormatUtils.getTableDataLocation(
            TableFormat.ICEBERG, table.location(), table.properties());

    Path datasetConfig = tempDir.resolve(tableName + ".yaml");
    Files.write(
        datasetConfig,
        Arrays.asList(
            "sourceFormat: ICEBERG",
            "targetFormats:",
            "  - HUDI",
            "datasets:",
            "  - tableBasePath: " + basePath,
            "    tableDataPath: " + dataPath,
            "    tableName: " + tableName,
            "    targetProperties:",
            "      HUDI:",
            "        " + HudiTargetConfig.HUDI_TABLE_VERSION + ": '9'",
            "        "
                + HudiTargetConfig.EXECUTION_ENGINE
                + ": "
                + HudiTargetConfig.ExecutionEngine.SPARK.getConfigValue(),
            "        " + HudiTargetConfig.SECONDARY_INDEX_COLUMN + ": email"),
        StandardCharsets.UTF_8);
    String output =
        runBundleSync(
            sparkHome,
            Collections.singletonList(
                "spark.serializer=org.apache.spark.serializer.KryoSerializer"),
            Arrays.asList("--datasetconfig", datasetConfig.toUri().toString()));

    SourceTable sourceTable =
        SourceTable.builder()
            .name(tableName)
            .basePath(basePath)
            .dataPath(dataPath)
            .formatName(TableFormat.ICEBERG)
            .build();
    HudiBackedSecondaryIndexReader reader =
        new HudiBackedSecondaryIndexReader(
            sourceTable, new IcebergConversionSourceProvider(), sparkSession);
    assertTrue(reader.doesIndexExist("email"), "index not built. spark-submit output:\n" + output);
    assertEquals(
        String.valueOf(table.currentSnapshot().snapshotId()),
        reader.getLastSyncedSourceIdentifier().orElse(null));
    Dataset<Row> keys =
        sparkSession
            .createDataset(
                Arrays.asList("user7@example.com", "missing@example.com"), Encoders.STRING())
            .toDF("email");
    // ids 7, 47 and 87 hold user7@example.com
    assertEquals(3, reader.lookup(sourceTable, keys, "email").count());
  }

  /**
   * Iceberg keeps its metadata at the base path but writes data files to a separate {@code
   * write.data.path}. The Hudi target goes to the data path, so {@code .hoodie} lands next to the
   * Iceberg data files and nothing is written under the Iceberg base path.
   */
  @Test
  void icebergWithSeparateDataPathSyncsToHudi() throws Exception {
    String sparkHome = System.getenv("SPARK_HOME");
    assumeTrue(sparkHome != null && !sparkHome.trim().isEmpty(), "SPARK_HOME is not set");

    String tableName = "iceberg_split_paths_to_hudi";
    Path metadataDir = tempDir.resolve(tableName + "_metadata");
    Path dataDir = tempDir.resolve("separate_data").resolve(tableName);
    String basePath = metadataDir.toUri().toString();
    String dataPath = dataDir.toUri().toString();

    Dataset<Row> rows =
        sparkSession
            .range(100)
            .selectExpr(
                "concat('id-', lpad(cast(id AS string), 3, '0')) AS id",
                "element_at(array('INFO', 'WARN', 'ERROR'), cast(id % 3 + 1 AS int)) AS level",
                "cast(id AS double) * 1.5 AS amount",
                "date_add(date '2026-01-01', cast(id AS int)) AS event_date");
    Schema schema = SparkSchemaUtil.convert(rows.schema());
    new HadoopTables(jsc.hadoopConfiguration())
        .create(
            schema,
            PartitionSpec.builderFor(schema).identity("level").build(),
            Collections.singletonMap(TableProperties.WRITE_DATA_LOCATION, dataPath),
            basePath);
    rows.write().format("iceberg").mode("append").save(basePath);
    assertTrue(countParquetFiles(dataDir) > 0, "Iceberg did not write data files to " + dataDir);
    assertEquals(0, countParquetFiles(metadataDir), "Iceberg wrote data files under the base path");

    List<String> syncArgs =
        Arrays.asList(
            "--basepath",
            basePath,
            "--datapath",
            dataPath,
            "--sourceformat",
            "ICEBERG",
            "--targets",
            "HUDI",
            "--tablename",
            tableName);
    String output = runBundleSync(sparkHome, syncArgs);

    assertTrue(
        Files.isDirectory(dataDir.resolve(".hoodie")),
        "Hudi metadata is not at the data path. spark-submit output:\n" + output);
    assertTrue(
        !Files.exists(metadataDir.resolve(".hoodie")),
        "Hudi metadata was written to the base path");
    assertDatasetEquivalence(
        "ICEBERG",
        "HUDI",
        basePath,
        dataPath,
        Arrays.asList("id", "level", "amount", "event_date"),
        "id",
        output);
  }

  /** Runs {@code XTableSparkSync} from the bundle through spark-submit and returns its output. */
  private String runBundleSync(String sparkHome, List<String> syncArgs) throws Exception {
    return runBundleSync(sparkHome, Collections.emptyList(), syncArgs);
  }

  private String runBundleSync(String sparkHome, List<String> sparkConfs, List<String> syncArgs)
      throws Exception {
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
    for (String sparkConf : sparkConfs) {
      command.add("--conf");
      command.add(sparkConf);
    }
    command.add("--class");
    command.add("org.apache.xtable.spark.XTableSparkSync");
    command.add(bundleJar.getAbsolutePath());
    command.addAll(syncArgs);

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
    return output;
  }

  private static long countParquetFiles(Path dir) throws IOException {
    if (!Files.isDirectory(dir)) {
      return 0;
    }
    try (Stream<Path> files = Files.walk(dir)) {
      return files.filter(f -> f.toString().endsWith(".parquet")).count();
    }
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
    // Same target location as XTableSyncService.
    String targetPath = "HUDI".equalsIgnoreCase(targetFormat) ? dataPath : basePath;
    List<String> targetRows =
        sparkSession
            .read()
            .options(readOptions(targetFormat))
            .format(targetFormat.toLowerCase())
            .load(targetPath)
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
    Set<String> excluded = new HashSet<>();
    for (StructField field : source.schema().fields()) {
      DataType type = field.dataType();
      if (type instanceof StructType || type instanceof ArrayType || type instanceof MapType) {
        excluded.add(field.name());
      }
    }
    return columns.stream()
        .filter(c -> !excluded.contains(c))
        .filter(c -> !c.startsWith("timestamp_local_"))
        .collect(Collectors.toList());
  }

  private static Map<String, String> readOptions(String format) {
    Map<String, String> options = new HashMap<>();
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
    String cp = new String(Files.readAllBytes(cpFile.toPath()), StandardCharsets.UTF_8).trim();
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
    try (BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
      String line;
      while ((line = reader.readLine()) != null) {
        synchronized (sb) {
          sb.append(line).append(System.lineSeparator());
        }
      }
    } catch (IOException e) {
      // Closed on timeout; the output so far is still reported.
    }
  }
}
