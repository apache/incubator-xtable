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
 
package org.apache.xtable.parquet;

import static org.apache.xtable.GenericTable.getTableName;
import static org.apache.xtable.model.storage.TableFormat.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.Value;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.config.HoodieMetadataConfig;

import org.apache.xtable.GenericTable;
import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.hudi.HudiTestUtil;
import org.apache.xtable.model.sync.SyncMode;

public class ITParquetConversionSource {
  public static final String PARTITION_FIELD_SPEC_CONFIG =
      "xtable.parquet.source.partition_field_spec_config";
  @TempDir public static Path tempDir;
  private static JavaSparkContext jsc;
  private static SparkSession sparkSession;
  private static StructType schema;

  @BeforeAll
  public static void setupOnce() {
    SparkConf sparkConf = HudiTestUtil.getSparkConf(tempDir);

    String extraJavaOptions = "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED";
    sparkConf.set("spark.driver.extraJavaOptions", extraJavaOptions);
    sparkConf = HoodieReadClient.addHoodieSupport(sparkConf);
    sparkConf.set("parquet.avro.write-old-list-structure", "false");
    String javaOpts =
        "--add-opens=java.base/java.nio=ALL-UNNAMED "
            + "--add-opens=java.base/java.lang=ALL-UNNAMED "
            + "--add-opens=java.base/java.util=ALL-UNNAMED "
            + "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
            + "--add-opens=java.base/java.io=ALL-UNNAMED"
            + "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"
            + "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED";

    sparkConf.set("spark.driver.extraJavaOptions", javaOpts);
    sparkConf.set("spark.executor.extraJavaOptions", javaOpts);
    sparkConf.set("spark.sql.parquet.writeLegacyFormat", "false");
    sparkConf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS");

    sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
    jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
  }

  @AfterAll
  public static void teardown() {
    if (jsc != null) {
      jsc.stop();
      jsc = null;
    }
    if (sparkSession != null) {
      sparkSession.stop();
      sparkSession = null;
    }
  }
  // delimiter must be / and not - or any other one
  private static Stream<Arguments> provideArgsForFilePartitionTesting() {
    String partitionConfig = // "timestamp:YEAR:year=yyyy";
        "timestamp:MONTH:year=yyyy/month=MM"; // or "timestamp:YEAR:year=yyyy", or //
    // timestamp:DAY:year=yyyy/month=MM/day=dd
    return Stream.of(
        Arguments.of(
            buildArgsForPartition(
                PARQUET, Arrays.asList(ICEBERG, DELTA, HUDI), partitionConfig, partitionConfig)));
  }

  private static TableFormatPartitionDataHolder buildArgsForPartition(
      String sourceFormat,
      List<String> targetFormats,
      String hudiPartitionConfig,
      String xTablePartitionConfig) {
    return TableFormatPartitionDataHolder.builder()
        .sourceTableFormat(sourceFormat)
        .targetTableFormats(targetFormats)
        .hudiSourceConfig(Optional.ofNullable(hudiPartitionConfig))
        .xTablePartitionConfig(xTablePartitionConfig)
        .build();
  }

  private static ConversionConfig getTableSyncConfig(
      String sourceTableFormat,
      SyncMode syncMode,
      String tableName,
      GenericTable table,
      List<String> targetTableFormats,
      String partitionConfig,
      Duration metadataRetention) {
    Properties sourceProperties = new Properties();
    if (partitionConfig != null) {
      sourceProperties.put(PARTITION_FIELD_SPEC_CONFIG, partitionConfig);
    }
    SourceTable sourceTable =
        SourceTable.builder()
            .name(tableName)
            .formatName(sourceTableFormat)
            .basePath(table.getBasePath())
            .dataPath(table.getDataPath())
            .additionalProperties(sourceProperties)
            .build();

    List<TargetTable> targetTables =
        targetTableFormats.stream()
            .map(
                formatName ->
                    TargetTable.builder()
                        .name(tableName)
                        .formatName(formatName)
                        // set the metadata path to the data path as the default (required by Hudi)
                        .basePath(table.getDataPath())
                        .metadataRetention(metadataRetention)
                        .build())
            .collect(Collectors.toList());

    return ConversionConfig.builder()
        .sourceTable(sourceTable)
        .targetTables(targetTables)
        .syncMode(syncMode)
        .build();
  }

  private static Stream<Arguments> provideArgsForFileNonPartitionTesting() {
    String partitionConfig = null;
    return Stream.of(
        Arguments.of(
            buildArgsForPartition(
                PARQUET, Arrays.asList(ICEBERG, DELTA, HUDI), partitionConfig, partitionConfig)));
  }

  private ConversionSourceProvider<?> getConversionSourceProvider(String sourceTableFormat) {
    if (sourceTableFormat.equalsIgnoreCase(PARQUET)) {
      ConversionSourceProvider<Long> parquetConversionSourceProvider =
          new ParquetConversionSourceProvider();
      parquetConversionSourceProvider.init(jsc.hadoopConfiguration());
      return parquetConversionSourceProvider;
    } else {
      throw new IllegalArgumentException("Unsupported source format: " + sourceTableFormat);
    }
  }

  @ParameterizedTest
  @MethodSource("provideArgsForFileNonPartitionTesting")
  public void testFileNonPartitionedData(
      TableFormatPartitionDataHolder tableFormatPartitionDataHolder) throws URISyntaxException {
    String tableName = getTableName();
    String sourceTableFormat = tableFormatPartitionDataHolder.getSourceTableFormat();
    List<String> targetTableFormats = tableFormatPartitionDataHolder.getTargetTableFormats();
    String xTablePartitionConfig = tableFormatPartitionDataHolder.getXTablePartitionConfig();
    ConversionSourceProvider<?> conversionSourceProvider =
        getConversionSourceProvider(sourceTableFormat);

    List<Row> data =
        Arrays.asList(
            RowFactory.create(1, "Alice", true, 30.1, new Timestamp(System.currentTimeMillis())),
            RowFactory.create(
                2, "Bob", false, 24.6, new Timestamp(System.currentTimeMillis() + 1000)),
            RowFactory.create(
                3, "Charlie", true, 35.2, new Timestamp(System.currentTimeMillis() + 2000)),
            RowFactory.create(
                4, "David", false, 29.5, new Timestamp(System.currentTimeMillis() + 3000)),
            RowFactory.create(
                5, "Eve", true, 22.2, new Timestamp(System.currentTimeMillis() + 4000)));

    schema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("name", DataTypes.StringType, false),
              DataTypes.createStructField("hasSiblings", DataTypes.BooleanType, false),
              DataTypes.createStructField("age", DataTypes.DoubleType, false),
              DataTypes.createStructField(
                  "timestamp",
                  DataTypes.TimestampType,
                  false,
                  new MetadataBuilder().putString("precision", "millis").build())
            });
    Dataset<Row> df = sparkSession.createDataFrame(data, schema);
    String dataPath = tempDir.toAbsolutePath().toString() + "/non_partitioned_data";
    df.write().mode(SaveMode.Overwrite).parquet(dataPath);
    GenericTable table;
    table =
        GenericTable.getInstance(
            tableName, Paths.get(dataPath), sparkSession, jsc, sourceTableFormat, false);
    try (GenericTable tableToClose = table) {
      ConversionConfig conversionConfig =
          getTableSyncConfig(
              sourceTableFormat,
              SyncMode.FULL,
              tableName,
              table,
              targetTableFormats,
              xTablePartitionConfig,
              null);
      ConversionController conversionController =
          new ConversionController(jsc.hadoopConfiguration());
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalenceWithFilter(sourceTableFormat, tableToClose, targetTableFormats, false);
    }
  }

  @ParameterizedTest
  @MethodSource("provideArgsForFilePartitionTesting")
  public void testFilePartitionedData(TableFormatPartitionDataHolder tableFormatPartitionDataHolder)
      throws URISyntaxException {
    String tableName = getTableName();
    String sourceTableFormat = tableFormatPartitionDataHolder.getSourceTableFormat();
    List<String> targetTableFormats = tableFormatPartitionDataHolder.getTargetTableFormats();
    String xTablePartitionConfig = tableFormatPartitionDataHolder.getXTablePartitionConfig();
    ConversionSourceProvider<?> conversionSourceProvider =
        getConversionSourceProvider(sourceTableFormat);
    // create the data
    List<Row> data =
        Arrays.asList(
            RowFactory.create(1, "Alice", true, 30.1, new Timestamp(System.currentTimeMillis())),
            RowFactory.create(
                2, "Bob", false, 24.6, new Timestamp(System.currentTimeMillis() + 1000)),
            RowFactory.create(
                3, "Charlie", true, 35.2, new Timestamp(System.currentTimeMillis() + 2000)),
            RowFactory.create(
                4, "David", false, 29.5, new Timestamp(System.currentTimeMillis() + 3000)),
            RowFactory.create(
                5, "Eve", true, 22.2, new Timestamp(System.currentTimeMillis() + 4000)));

    schema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("name", DataTypes.StringType, false),
              DataTypes.createStructField("hasSiblings", DataTypes.BooleanType, false),
              DataTypes.createStructField("age", DataTypes.DoubleType, false),
              DataTypes.createStructField(
                  "timestamp",
                  DataTypes.TimestampType,
                  false,
                  new MetadataBuilder().putString("precision", "millis").build())
            });
    Dataset<Row> df = sparkSession.createDataFrame(data, schema);
    String dataPathPart = tempDir.toAbsolutePath().toString() + "/partitioned_data";
    df.withColumn("year", functions.year(functions.col("timestamp").cast(DataTypes.TimestampType)))
        .withColumn(
            "month",
            functions.date_format(functions.col("timestamp").cast(DataTypes.TimestampType), "MM"))
        .write()
        .mode(SaveMode.Overwrite)
        .partitionBy("year", "month")
        .parquet(dataPathPart);
    GenericTable table;
    table =
        GenericTable.getInstance(
            tableName, Paths.get(dataPathPart), sparkSession, jsc, sourceTableFormat, true);
    try (GenericTable tableToClose = table) {
      ConversionConfig conversionConfig =
          getTableSyncConfig(
              sourceTableFormat,
              SyncMode.FULL,
              tableName,
              table,
              targetTableFormats,
              xTablePartitionConfig,
              null);
      ConversionController conversionController =
          new ConversionController(jsc.hadoopConfiguration());
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalenceWithFilter(sourceTableFormat, tableToClose, targetTableFormats, true);
      // update the current parquet file data with another attribute the sync again
      List<Row> dataToAppend =
          Arrays.asList(
              RowFactory.create(
                  10,
                  "BobAppended",
                  false,
                  70.3,
                  new Timestamp(System.currentTimeMillis() + 1500)));

      Dataset<Row> dfAppend = sparkSession.createDataFrame(dataToAppend, schema);
      dfAppend
          .withColumn(
              "year", functions.year(functions.col("timestamp").cast(DataTypes.TimestampType)))
          .withColumn(
              "month",
              functions.date_format(functions.col("timestamp").cast(DataTypes.TimestampType), "MM"))
          .write()
          .mode(SaveMode.Append)
          .partitionBy("year", "month")
          .parquet(dataPathPart);
      GenericTable tableAppend;
      tableAppend =
          GenericTable.getInstance(
              tableName, Paths.get(dataPathPart), sparkSession, jsc, sourceTableFormat, true);
      try (GenericTable tableToCloseAppended = tableAppend) {
        ConversionConfig conversionConfigAppended =
            getTableSyncConfig(
                sourceTableFormat,
                SyncMode.FULL,
                tableName,
                tableAppend,
                targetTableFormats,
                xTablePartitionConfig,
                null);
        ConversionController conversionControllerAppended =
            new ConversionController(jsc.hadoopConfiguration());
        conversionControllerAppended.sync(conversionConfigAppended, conversionSourceProvider);
      }
    }
  }

  private void checkDatasetEquivalenceWithFilter(
      String sourceFormat,
      GenericTable<?, ?> sourceTable,
      List<String> targetFormats,
      boolean isPartitioned)
      throws URISyntaxException {
    checkDatasetEquivalence(
        sourceFormat,
        sourceTable,
        Collections.emptyMap(),
        targetFormats,
        Collections.emptyMap(),
        null,
        isPartitioned);
  }

  private void checkDatasetEquivalence(
      String sourceFormat,
      GenericTable<?, ?> sourceTable,
      Map<String, String> sourceOptions,
      List<String> targetFormats,
      Map<String, Map<String, String>> targetOptions,
      Integer expectedCount,
      boolean isPartitioned)
      throws URISyntaxException {
    Dataset<Row> sourceRows =
        sparkSession
            .read()
            .schema(schema)
            .options(sourceOptions)
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.parquet")
            .parquet(sourceTable.getDataPath());
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
                          .load(sourceTable.getDataPath());
                    }));

    String[] selectColumnsArr = schema.fieldNames();
    List<String> dataset1Rows = sourceRows.selectExpr(selectColumnsArr).toJSON().collectAsList();

    Set<Map.Entry<String, Dataset<Row>>> entrySet = targetRowsByFormat.entrySet();

    for (Map.Entry<String, Dataset<Row>> entry : entrySet) {

      String format = entry.getKey();

      Dataset<Row> targetRows = entry.getValue();
      targetRows.show();

      List<String> dataset2Rows = targetRows.selectExpr(selectColumnsArr).toJSON().collectAsList();

      assertEquals(
          dataset1Rows.size(),
          dataset2Rows.size(),
          String.format(
              "Datasets have different row counts when reading from Spark. Source: %s, Target: %s",
              sourceFormat, format));

      if (expectedCount != null) {
        assertEquals(expectedCount, dataset1Rows.size());
      } else {
        assertFalse(dataset1Rows.isEmpty());
      }
      if (isPartitioned) { // discard Hudi case because the partitioned col values won't be equal
        if (!format.equals("HUDI")) {
          assertEquals(
              dataset1Rows,
              dataset2Rows,
              String.format(
                  "Datasets are not equivalent when reading from Spark. Source: %s, Target: %s",
                  sourceFormat, format));
        }
      } else {
        assertEquals(
            dataset1Rows,
            dataset2Rows,
            String.format(
                "Datasets are not equivalent when reading from Spark. Source: %s, Target: %s",
                sourceFormat, format));
      }
    }
  }

  @Builder
  @Value
  private static class TableFormatPartitionDataHolder {
    String sourceTableFormat;
    List<String> targetTableFormats;
    String xTablePartitionConfig;
    Optional<String> hudiSourceConfig;
    String filter;
  }
}
