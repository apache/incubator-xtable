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
 
package org.apache.xtable.service;

import static org.apache.xtable.GenericTable.getTableName;
import static org.apache.xtable.model.storage.TableFormat.DELTA;
import static org.apache.xtable.model.storage.TableFormat.HUDI;
import static org.apache.xtable.model.storage.TableFormat.ICEBERG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import org.apache.xtable.GenericTable;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.delta.DeltaConversionSourceProvider;
import org.apache.xtable.hudi.HudiConversionSourceProvider;
import org.apache.xtable.hudi.HudiTestUtil;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.service.models.ConvertTableRequest;
import org.apache.xtable.service.models.ConvertTableResponse;
import org.apache.xtable.service.models.ConvertedTable;

import io.quarkus.test.junit.QuarkusTest;

@QuarkusTest
public class ITConversionService {

  private ConversionService conversionService;
  private static Path tempDir;
  protected static JavaSparkContext jsc;
  protected static SparkSession sparkSession;

  @BeforeAll
  public static void setupOnce() {
    try {
      // local fs setup
      tempDir = Files.createTempDirectory("xtable-it");
      String tableName = "xtable-service-test-" + UUID.randomUUID();
      Path basePath = tempDir.resolve(tableName);
      Files.createDirectories(basePath);

      SparkConf sparkConf = HudiTestUtil.getSparkConf(tempDir);
      sparkSession =
          SparkSession.builder().config(HoodieReadClient.addHoodieSupport(sparkConf)).getOrCreate();
      sparkSession
          .sparkContext()
          .hadoopConfiguration()
          .set("parquet.avro.write-old-list-structure", "false");
      jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeEach
  public void setUp() {
    // Create ConversionService with test's configuration
    ConversionServiceConfig serviceConfig = new ConversionServiceConfig();
    ConversionController conversionController = new ConversionController(jsc.hadoopConfiguration());
    
    Map<String, ConversionSourceProvider<?>> sourceProviders = new HashMap<>();
    ConversionSourceProvider<HoodieInstant> hudiConversionSourceProvider =
        new HudiConversionSourceProvider();
    ConversionSourceProvider<Long> deltaConversionSourceProvider =
        new DeltaConversionSourceProvider();
    ConversionSourceProvider<org.apache.iceberg.Snapshot> icebergConversionSourceProvider =
        new IcebergConversionSourceProvider();

    hudiConversionSourceProvider.init(jsc.hadoopConfiguration());
    deltaConversionSourceProvider.init(jsc.hadoopConfiguration());
    icebergConversionSourceProvider.init(jsc.hadoopConfiguration());

    sourceProviders.put(HUDI, hudiConversionSourceProvider);
    sourceProviders.put(DELTA, deltaConversionSourceProvider);
    sourceProviders.put(ICEBERG, icebergConversionSourceProvider);
    
    this.conversionService = new ConversionService(
        serviceConfig, conversionController, jsc.hadoopConfiguration(), sourceProviders);
  }

  @AfterAll
  public static void teardown() {
    if (jsc != null) {
      jsc.close();
    }
    if (sparkSession != null) {
      sparkSession.close();
    }
    try {
      Files.walk(tempDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @ParameterizedTest
  @MethodSource("generateTestParametersFormatsAndPartitioning")
  public void testVariousOperations(String sourceTableFormat, boolean isPartitioned) {
    String tableName = getTableName();
    List<String> targetTableFormats = getOtherFormats(sourceTableFormat);
    String partitionConfig = isPartitioned ? "level:VALUE" : null;

    try (GenericTable table =
        GenericTable.getInstance(
            tableName, tempDir, sparkSession, jsc, sourceTableFormat, isPartitioned)) {
      List<?> insertRecords = table.insertRows(100);

      // Create and execute conversion request
      ConvertTableRequest request =
          createConvertTableRequest(
              sourceTableFormat,
              tableName,
              table.getBasePath(),
              table.getDataPath(),
              targetTableFormats,
              partitionConfig);
      ConvertTableResponse response = conversionService.convertTable(request);
      assertConversionResponse(response, targetTableFormats);
      checkDatasetEquivalence(sourceTableFormat, table, targetTableFormats, 100);

      // Make multiple commits and then sync with service
      table.insertRows(100);
      table.upsertRows(insertRecords.subList(0, 20));
      response = conversionService.convertTable(request);
      assertConversionResponse(response, targetTableFormats);
      checkDatasetEquivalence(sourceTableFormat, table, targetTableFormats, 200);

      table.deleteRows(insertRecords.subList(30, 50));
      response = conversionService.convertTable(request);
      assertConversionResponse(response, targetTableFormats);
      checkDatasetEquivalence(sourceTableFormat, table, targetTableFormats, 180);
      checkDatasetEquivalenceWithFilter(
          sourceTableFormat, table, targetTableFormats, table.getFilterQuery());
    }

    try (GenericTable tableWithUpdatedSchema =
        GenericTable.getInstanceWithAdditionalColumns(
            tableName, tempDir, sparkSession, jsc, sourceTableFormat, isPartitioned)) {
      ConvertTableRequest request =
          createConvertTableRequest(
              sourceTableFormat,
              tableName,
              tableWithUpdatedSchema.getBasePath(),
              tableWithUpdatedSchema.getDataPath(),
              targetTableFormats,
              partitionConfig);

      List<Row> insertsAfterSchemaUpdate = tableWithUpdatedSchema.insertRows(100);
      tableWithUpdatedSchema.reload();
      ConvertTableResponse response = conversionService.convertTable(request);
      assertConversionResponse(response, targetTableFormats);
      checkDatasetEquivalence(sourceTableFormat, tableWithUpdatedSchema, targetTableFormats, 280);

      tableWithUpdatedSchema.deleteRows(insertsAfterSchemaUpdate.subList(60, 90));
      response = conversionService.convertTable(request);
      assertConversionResponse(response, targetTableFormats);
      checkDatasetEquivalence(sourceTableFormat, tableWithUpdatedSchema, targetTableFormats, 250);

      if (isPartitioned) {
        // Adds new partition.
        tableWithUpdatedSchema.insertRecordsForSpecialPartition(50);
        response = conversionService.convertTable(request);
        assertConversionResponse(response, targetTableFormats);
        checkDatasetEquivalence(sourceTableFormat, tableWithUpdatedSchema, targetTableFormats, 300);

        // Drops partition.
        tableWithUpdatedSchema.deleteSpecialPartition();
        response = conversionService.convertTable(request);
        assertConversionResponse(response, targetTableFormats);
        checkDatasetEquivalence(sourceTableFormat, tableWithUpdatedSchema, targetTableFormats, 250);

        // Insert records to the dropped partition again.
        tableWithUpdatedSchema.insertRecordsForSpecialPartition(50);
        response = conversionService.convertTable(request);
        assertConversionResponse(response, targetTableFormats);
        checkDatasetEquivalence(sourceTableFormat, tableWithUpdatedSchema, targetTableFormats, 300);
      }
    }
  }

  private static Stream<Arguments> generateTestParametersFormatsAndPartitioning() {
    List<Arguments> arguments = new ArrayList<>();
    for (String sourceTableFormat : Arrays.asList(HUDI, DELTA, ICEBERG)) {
      for (boolean isPartitioned : new boolean[] {true, false}) {
        arguments.add(Arguments.of(sourceTableFormat, isPartitioned));
      }
    }
    return arguments.stream();
  }

  protected static List<String> getOtherFormats(String sourceTableFormat) {
    return Arrays.stream(TableFormat.values())
        .filter(format -> !format.equals(sourceTableFormat))
        .collect(Collectors.toList());
  }

  protected void checkDatasetEquivalenceWithFilter(
      String sourceFormat,
      GenericTable<?, ?> sourceTable,
      List<String> targetFormats,
      String filter) {
    checkDatasetEquivalence(
        sourceFormat,
        sourceTable,
        Collections.emptyMap(),
        targetFormats,
        Collections.emptyMap(),
        null,
        filter);
  }

  protected void checkDatasetEquivalence(
      String sourceFormat,
      GenericTable<?, ?> sourceTable,
      List<String> targetFormats,
      Integer expectedCount) {
    checkDatasetEquivalence(
        sourceFormat,
        sourceTable,
        Collections.emptyMap(),
        targetFormats,
        Collections.emptyMap(),
        expectedCount,
        "1 = 1");
  }

  private void checkDatasetEquivalence(
      String sourceFormat,
      GenericTable<?, ?> sourceTable,
      Map<String, String> sourceOptions,
      List<String> targetFormats,
      Map<String, Map<String, String>> targetOptions,
      Integer expectedCount,
      String filterCondition) {
    Dataset<Row> sourceRows =
        sparkSession
            .read()
            .options(sourceOptions)
            .format(sourceFormat.toLowerCase())
            .load(sourceTable.getBasePath())
            .orderBy(sourceTable.getOrderByColumn())
            .filter(filterCondition);
    Map<String, Dataset<Row>> targetRowsByFormat =
        targetFormats.stream()
            .collect(
                Collectors.toMap(
                    Function.identity(),
                    targetFormat -> {
                      Map<String, String> finalTargetOptions =
                          targetOptions.getOrDefault(targetFormat, Collections.emptyMap());
                      if (targetFormat.equals(HUDI)) {
                        finalTargetOptions = new HashMap<>(finalTargetOptions);
                        finalTargetOptions.put(HoodieMetadataConfig.ENABLE.key(), "true");
                        finalTargetOptions.put(
                            "hoodie.datasource.read.extract.partition.values.from.path", "true");
                      }
                      return sparkSession
                          .read()
                          .options(finalTargetOptions)
                          .format(targetFormat.toLowerCase())
                          .load(sourceTable.getDataPath())
                          .orderBy(sourceTable.getOrderByColumn())
                          .filter(filterCondition);
                    }));

    String[] selectColumnsArr = sourceTable.getColumnsToSelect().toArray(new String[] {});
    List<String> dataset1Rows = sourceRows.selectExpr(selectColumnsArr).toJSON().collectAsList();
    targetRowsByFormat.forEach(
        (format, targetRows) -> {
          List<String> dataset2Rows =
              targetRows.selectExpr(selectColumnsArr).toJSON().collectAsList();
          assertEquals(
              dataset1Rows.size(),
              dataset2Rows.size(),
              String.format(
                  "Datasets have different row counts when reading from Spark. Source: %s, Target: %s",
                  sourceFormat, format));
          // sanity check the count to ensure test is set up properly
          if (expectedCount != null) {
            assertEquals(expectedCount, dataset1Rows.size());
          } else {
            // if count is not known ahead of time, ensure datasets are non-empty
            assertFalse(dataset1Rows.isEmpty());
          }
          assertEquals(
              dataset1Rows,
              dataset2Rows,
              String.format(
                  "Datasets are not equivalent when reading from Spark. Source: %s, Target: %s",
                  sourceFormat, format));
        });
  }

  private ConvertTableRequest createConvertTableRequest(
      String sourceFormat,
      String tableName,
      String tablePath,
      String dataPath,
      List<String> targetFormats,
      String partitionConfig) {
    Map<String, String> configs = new HashMap<>();
    if (partitionConfig != null) {
      configs.put("partition-spec", partitionConfig);
    }
    return ConvertTableRequest.builder()
        .sourceFormat(sourceFormat)
        .sourceTableName(tableName)
        .sourceTablePath(tablePath)
        .sourceDataPath(dataPath)
        .targetFormats(targetFormats)
        .configurations(configs)
        .build();
  }

  private void assertConversionResponse(
      ConvertTableResponse response, List<String> expectedFormats) {
    assertNotNull(response, "Response should not be null");
    assertNotNull(response.getConvertedTables(), "Converted tables should not be null");
    assertEquals(
        expectedFormats.size(),
        response.getConvertedTables().size(),
        "Should have converted tables for all target formats");

    for (ConvertedTable convertedTable : response.getConvertedTables()) {
      assertTrue(
          expectedFormats.contains(convertedTable.getTargetFormat()),
          "Unexpected target format: " + convertedTable.getTargetFormat());
      assertNotNull(convertedTable.getTargetSchema(), "Schema should not be null");
      assertNotNull(convertedTable.getTargetMetadataPath(), "Metadata path should not be null");
    }
  }
}
