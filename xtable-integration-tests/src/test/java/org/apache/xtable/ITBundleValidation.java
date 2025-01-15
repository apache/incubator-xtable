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
 
package org.apache.xtable;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.utilities.RunSync;

@Log4j2
class ITBundleValidation extends ConversionTestingBase {
  private static final String PROJECT_ROOT = System.getenv("ROOT_DIR");
  private static final String PROJECT_VERSION = System.getenv("PROJECT_VERSION");
  private static final String SCALA_VERSION = System.getenv("SCALA_VERSION");
  private static final String UTILITIES_JAR_PATH =
      String.format(
          "%s/xtable-utilities/target/xtable-utilities_%s-%s-bundled.jar",
          PROJECT_ROOT, SCALA_VERSION, PROJECT_VERSION);
  private static final String ICEBERG_JAR_PATH =
      String.format(
          "%s/xtable-iceberg/target/xtable-iceberg-%s-bundled.jar", PROJECT_ROOT, PROJECT_VERSION);
  private static final String HUDI_JAR_PATH =
      String.format(
          "%s/xtable-hudi/target/xtable-hudi-%s-bundled.jar", PROJECT_ROOT, PROJECT_VERSION);
  private static final String DELTA_JAR_PATH =
      String.format(
          "%s/xtable-delta/target/xtable-delta_%s-%s-bundled.jar",
          PROJECT_ROOT, SCALA_VERSION, PROJECT_VERSION);
  private static final String SPARK_BUNDLE_PATH =
      String.format(
          "%s/xtable-integration-tests/target/spark-testing-bundle_%s.jar",
          PROJECT_ROOT, SCALA_VERSION);

  private static Stream<Arguments> generateTestParametersForFormats() {
    List<Arguments> arguments = new ArrayList<>();
    List<String> formats = Arrays.asList(TableFormat.HUDI, TableFormat.DELTA, TableFormat.ICEBERG);
    for (String sourceTableFormat : formats) {
      for (String targetTableFormat : formats) {
        if (!sourceTableFormat.equals(targetTableFormat)) {
          arguments.add(Arguments.of(sourceTableFormat, targetTableFormat));
        }
      }
    }
    return arguments.stream();
  }
  /*
   * This test has the following steps at a high level.
   * 1. Insert few records.
   * 2. Upsert few records.
   * 3. Delete few records.
   * After each step the RunSync command is run.
   */
  @ParameterizedTest
  @MethodSource("generateTestParametersForFormats")
  public void testConversionWithBundles(String sourceTableFormat, String targetTableFormat) {
    String tableName = GenericTable.getTableName();
    List<String> targetTableFormats = Collections.singletonList(targetTableFormat);
    String partitionConfig = "level:VALUE";
    List<?> insertRecords;
    try (GenericTable table =
        GenericTableFactory.getInstance(
            tableName, tempDir, sparkSession, jsc, sourceTableFormat, true)) {
      String configPath =
          writeConfig(sourceTableFormat, targetTableFormats, table, tableName, partitionConfig);
      insertRecords = table.insertRows(100);

      executeRunSync(configPath, sourceTableFormat, targetTableFormat);
      checkDatasetEquivalence(sourceTableFormat, table, targetTableFormats, 100);

      // make multiple commits and then sync
      table.insertRows(100);
      table.upsertRows(insertRecords.subList(0, 20));
      executeRunSync(configPath, sourceTableFormat, targetTableFormat);
      checkDatasetEquivalence(sourceTableFormat, table, targetTableFormats, 200);

      table.deleteRows(insertRecords.subList(30, 50));
      executeRunSync(configPath, sourceTableFormat, targetTableFormat);
      checkDatasetEquivalence(sourceTableFormat, table, targetTableFormats, 180);
      checkDatasetEquivalenceWithFilter(
          sourceTableFormat, table, targetTableFormats, table.getFilterQuery());
    }
  }

  @SneakyThrows
  private String writeConfig(
      String sourceFormat,
      List<String> targetFormats,
      GenericTable table,
      String tableName,
      String partitionSpec) {
    RunSync.DatasetConfig.Table tableConfig =
        new RunSync.DatasetConfig.Table(
            table.getBasePath(), table.getDataPath(), tableName, partitionSpec, null);
    RunSync.DatasetConfig datasetConfig =
        new RunSync.DatasetConfig(
            sourceFormat, targetFormats, Collections.singletonList(tableConfig));
    Path configPath = tempDir.resolve("config_" + UUID.randomUUID());
    YAML_MAPPER.writeValue(configPath.toFile(), datasetConfig);
    return configPath.toString();
  }

  @SneakyThrows
  private void executeRunSync(
      String configPath, String sourceTableFormat, String targetTableFormat) {
    String classPath =
        String.format(
            "%s:%s:%s",
            UTILITIES_JAR_PATH,
            getJarsForFormat(sourceTableFormat),
            getJarsForFormat(targetTableFormat));
    Process process =
        new ProcessBuilder()
            .command(
                "java", "-cp", classPath, RunSync.class.getName(), "--datasetConfig", configPath)
            .redirectErrorStream(true)
            .start();
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(process.getInputStream()))) {
      String line;
      while ((line = reader.readLine()) != null) {
        log.info("System log {}", line);
      }
    }
    assertEquals(0, process.waitFor());
  }

  private String getJarsForFormat(String format) {
    switch (format) {
      case TableFormat.HUDI:
        return HUDI_JAR_PATH;
      case TableFormat.ICEBERG:
        return ICEBERG_JAR_PATH;
      case TableFormat.DELTA:
        return String.format("%s:%s", DELTA_JAR_PATH, SPARK_BUNDLE_PATH);
      default:
        throw new UnsupportedOperationException("Unsupported format: " + format);
    }
  }
}
