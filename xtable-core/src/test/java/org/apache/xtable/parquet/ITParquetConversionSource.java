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

import static org.apache.spark.sql.functions.expr;
import static org.apache.xtable.GenericTable.getTableName;
import static org.apache.xtable.model.storage.TableFormat.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
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
import org.apache.hudi.common.config.HoodieMetadataConfig;

import org.apache.xtable.GenericTable;
import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.hudi.HudiTestUtil;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.storage.TableFormat;
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
    sparkConf = HoodieReadClient.addHoodieSupport(sparkConf);
    sparkConf.set("parquet.avro.write-old-list-structure", "false");
    sparkConf.set("spark.sql.parquet.writeLegacyFormat", "false");
    sparkConf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS");

    sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
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

  private static TableFormatPartitionDataHolder buildArgsForPartition(
      String sourceFormat,
      List<String> targetFormats,
      String hudiPartitionConfig,
      String xTablePartitionConfig,
      SyncMode syncMode) {
    return TableFormatPartitionDataHolder.builder()
        .sourceTableFormat(sourceFormat)
        .targetTableFormats(targetFormats)
        .hudiSourceConfig(Optional.ofNullable(hudiPartitionConfig))
        .xTablePartitionConfig(xTablePartitionConfig)
        .syncMode(syncMode)
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
    String absolutePath = new java.io.File(java.net.URI.create(table.getDataPath())).getPath();
    SourceTable sourceTable =
        SourceTable.builder()
            .name(tableName)
            .formatName(sourceTableFormat)
            .basePath(absolutePath)
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
                        .basePath(absolutePath)
                        .metadataRetention(metadataRetention)
                        .build())
            .collect(Collectors.toList());

    return ConversionConfig.builder()
        .sourceTable(sourceTable)
        .targetTables(targetTables)
        .syncMode(syncMode)
        .build();
  }

  private static Stream<Arguments> provideArgsForSyncTesting() {
    List<String> partitionConfigs = Arrays.asList(null, "timestamp:MONTH:year=yyyy/month=MM");
    return partitionConfigs.stream()
        .flatMap(
            partitionConfig ->
                Arrays.stream(SyncMode.values())
                    .map(
                        syncMode ->
                            Arguments.of(
                                buildArgsForPartition(
                                    PARQUET,
                                    Arrays.asList(ICEBERG, DELTA, HUDI),
                                    partitionConfig,
                                    partitionConfig,
                                    syncMode))));
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

  private void cleanupTargetMetadata(String dataPath, List<String> formats) {
    for (String format : formats) {
      String metadataFolder = "";
      switch (format.toUpperCase()) {
        case "ICEBERG":
          metadataFolder = "metadata";
          break;
        case "DELTA":
          metadataFolder = "_delta_log";
          break;
        case "HUDI":
          metadataFolder = ".hoodie";
          break;
      }
      if (!metadataFolder.isEmpty()) {
        try {
          org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(dataPath, metadataFolder);
          org.apache.hadoop.fs.FileSystem fs = path.getFileSystem(jsc.hadoopConfiguration());
          if (fs.exists(path)) {
            fs.delete(path, true);
          }
        } catch (IOException e) {
        }
      }
    }
  }

  @ParameterizedTest
  @MethodSource("provideArgsForSyncTesting")
  void testSync(TableFormatPartitionDataHolder tableFormatPartitionDataHolder) {
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
    String dataPath =
        tempDir
            .resolve(
                (xTablePartitionConfig == null ? "non_partitioned_data_" : "partitioned_data_")
                    + tableFormatPartitionDataHolder.getSyncMode())
            .toString();

    writeData(df, dataPath, xTablePartitionConfig);
    boolean isPartitioned = xTablePartitionConfig != null;

    java.nio.file.Path pathForXTable = java.nio.file.Paths.get(dataPath);
    try (GenericTable table =
        GenericTable.getInstance(
            tableName, pathForXTable, sparkSession, jsc, sourceTableFormat, isPartitioned)) {
      ConversionConfig conversionConfig =
          getTableSyncConfig(
              sourceTableFormat,
              tableFormatPartitionDataHolder.getSyncMode(),
              tableName,
              table,
              targetTableFormats,
              xTablePartitionConfig,
              null);
      ConversionController conversionController =
          new ConversionController(jsc.hadoopConfiguration());
      conversionController.sync(conversionConfig, conversionSourceProvider);
      checkDatasetEquivalenceWithFilter(
          sourceTableFormat, table, targetTableFormats, isPartitioned);

      // update the current parquet file data with another attribute the sync again
      List<Row> dataToAppend =
          Collections.singletonList(
              RowFactory.create(
                  10,
                  "BobAppended",
                  false,
                  70.3,
                  new Timestamp(System.currentTimeMillis() + 1500)));

      Dataset<Row> dfAppend = sparkSession.createDataFrame(dataToAppend, schema);
      writeData(dfAppend, dataPath, xTablePartitionConfig);
      //cleanupTargetMetadata(dataPath, targetTableFormats);
      ConversionConfig conversionConfigAppended =
          getTableSyncConfig(
              sourceTableFormat,
              tableFormatPartitionDataHolder.getSyncMode(),
              tableName,
              table,
              targetTableFormats,
              xTablePartitionConfig,
              null);
      ConversionController conversionControllerAppended =
          new ConversionController(jsc.hadoopConfiguration());
      conversionControllerAppended.sync(conversionConfigAppended, conversionSourceProvider);
      checkDatasetEquivalenceWithFilter(
          sourceTableFormat, table, targetTableFormats, isPartitioned);
    }
  }

  private void writeData(Dataset<Row> df, String dataPath, String partitionConfig) {
    if (partitionConfig != null) {
      // extract partition columns from config
      String[] partitionCols =
          Arrays.stream(partitionConfig.split(":")[2].split("/"))
              .map(s -> s.split("=")[0])
              .toArray(String[]::new);
      // add partition columns to dataframe
      for (String partitionCol : partitionCols) {
        if (partitionCol.equals("year")) {
          df =
              df.withColumn(
                  "year", functions.year(functions.col("timestamp").cast(DataTypes.TimestampType)));
        } else if (partitionCol.equals("month")) {
          df =
              df.withColumn(
                  "month",
                  functions.date_format(
                      functions.col("timestamp").cast(DataTypes.TimestampType), "MM"));
        } else if (partitionCol.equals("day")) {
          df =
              df.withColumn(
                  "day",
                  functions.date_format(
                      functions.col("timestamp").cast(DataTypes.TimestampType), "dd"));
        }
      }
      Dataset<Row> existingData = sparkSession.read().parquet(dataPath);
      Dataset<Row> combinedData = existingData.unionByName(df, true);
      combinedData.write()
              .mode(SaveMode.Overwrite)
              .partitionBy(partitionCols)
              .parquet(dataPath);
      //df.write().mode(SaveMode.Append).partitionBy(partitionCols).parquet(dataPath);
    } else {
      Dataset<Row> existingData = sparkSession.read().parquet(dataPath);
      Dataset<Row> combinedData = existingData.unionByName(df, true);
      //df.write().mode(SaveMode.Append).parquet(dataPath);
      combinedData.write().mode(SaveMode.Overwrite).parquet(dataPath);
    }
  }

  @Test
  void testIncrementalSyncWithMultiplePartitions() throws IOException {

    Configuration conf = sparkSession.sparkContext().hadoopConfiguration();

    StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("value", DataTypes.StringType, false),
              DataTypes.createStructField("year", DataTypes.IntegerType, false),
              DataTypes.createStructField("month", DataTypes.IntegerType, false)
            });
    List<Row> data =
        Arrays.asList(
            RowFactory.create(100, "A", 2026, 12),
            RowFactory.create(101, "AA", 2026, 12),
            RowFactory.create(102, "CB", 2027, 11),
            RowFactory.create(103, "BA", 2027, 11));

    Dataset<Row> dfInit = sparkSession.createDataFrame(data, schema);
    Path fixedPath = Paths.get("target", "fixed-parquet-data", "parquet_table_test_2");
    // String outputPath = fixedPath.toString();
    Dataset<Row> df = dfInit.withColumn("full_date", expr("make_date(year, month, 1)"));
    String outputPath =
        new java.io.File("target/fixed-parquet-data/parquet_table_test_2").getAbsolutePath();
    df.coalesce(1).write().partitionBy("year", "month").mode("overwrite").parquet(outputPath);

    // test find files to sync
    FileSystem fs = FileSystem.get(fixedPath.toUri(), conf);
    // set the modification time to the table file
    // update modificationTime for file to append
    // many partitions case
    List<String> newPartitions = Arrays.asList("year=2026/month=12", "year=2027/month=11");
    long targetModificationTime = System.currentTimeMillis() - 360000;
    long newModificationTime = System.currentTimeMillis() - 50000;
    long testTime = System.currentTimeMillis() - 90000; // between two prev times
    for (String partition : newPartitions) {
      org.apache.hadoop.fs.Path partitionPath =
          new org.apache.hadoop.fs.Path(outputPath, partition);
      if (fs.exists(partitionPath)) {
        updateModificationTimeRecursive(fs, partitionPath, targetModificationTime);
      }
    }
    // create new file to append using Spark
    List<Row> futureDataToSync =
        Arrays.asList(
            RowFactory.create(101, "A", 2026, 12),
            RowFactory.create(301, "D", 2027, 11),
            RowFactory.create(302, "DA", 2027, 11));
    Dataset<Row> dfToSyncInit = sparkSession.createDataFrame(futureDataToSync, schema);
    Dataset<Row> dfToSync = dfToSyncInit.withColumn("full_date", expr("make_date(year, month, 1)"));
    dfToSync.coalesce(1).write().partitionBy("year", "month").mode("append").parquet(outputPath);

    // conversionSource operations
    Properties sourceProperties = new Properties();
    String partitionConfig = "full_date:MONTH:year=yyyy/month=MM";
    sourceProperties.put(PARTITION_FIELD_SPEC_CONFIG, partitionConfig);

    SourceTable tableConfig =
        SourceTable.builder()
            .name("parquet_table_test_2")
            .basePath(fixedPath.toAbsolutePath().toString()) // removed toUri()
            .additionalProperties(sourceProperties)
            .formatName(TableFormat.PARQUET)
            .build();

    ParquetConversionSourceProvider conversionSourceProvider =
        new ParquetConversionSourceProvider();
    conversionSourceProvider.init(conf);
    ParquetConversionSource conversionSource =
        conversionSourceProvider.getConversionSourceInstance(tableConfig);

    for (String partition : newPartitions) {
      org.apache.hadoop.fs.Path partitionPath =
          new org.apache.hadoop.fs.Path(outputPath, partition);

      RemoteIterator<LocatedFileStatus> it = fs.listFiles(partitionPath, false);
      while (it.hasNext()) {
        LocatedFileStatus fileStatus = it.next();

        if (fileStatus.getModificationTime() > newModificationTime) {
          fs.setTimes(fileStatus.getPath(), newModificationTime, -1);
        } else {
          fs.setTimes(fileStatus.getPath(), targetModificationTime, -1);
        }
      }
      fs.setTimes(partitionPath, newModificationTime, -1);
    }

    InternalTable result = conversionSource.getTable(newModificationTime);
    assertEquals(
        Instant.ofEpochMilli(newModificationTime).toString(),
        result.getLatestCommitTime().toString());
    assertEquals("parquet_table_test_2", result.getName());
    assertEquals(TableFormat.PARQUET, result.getTableFormat());
    assertNotNull(result.getReadSchema());
    InternalSnapshot snapshot = conversionSource.getCurrentSnapshot();
    assertNotNull(snapshot);
    TableChange changes = conversionSource.getTableChangeForCommit(newModificationTime);
    assertNotNull(changes);
    Instant instantBeforeFirstSnapshot =
        Instant.ofEpochMilli(snapshot.getTable().getLatestCommitTime().toEpochMilli());
    assertEquals(instantBeforeFirstSnapshot.toEpochMilli(), newModificationTime);
    assertTrue(conversionSource.isIncrementalSyncSafeFrom(Instant.ofEpochMilli(testTime)));
  }

  private void updateModificationTimeRecursive(
      FileSystem fs, org.apache.hadoop.fs.Path path, long time) throws IOException {
    RemoteIterator<LocatedFileStatus> it = fs.listFiles(path, true);
    while (it.hasNext()) {
      LocatedFileStatus status = it.next();
      if (status.getPath().getName().endsWith(".parquet")) {
        fs.setTimes(status.getPath(), time, -1);
      }
    }
  }

  private void checkDatasetEquivalenceWithFilter(
      String sourceFormat,
      GenericTable<?, ?> sourceTable,
      List<String> targetFormats,
      boolean isPartitioned) {
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
      boolean isPartitioned) {
    Dataset<Row> sourceRows =
        sparkSession
            .read()
            .schema(schema)
            .options(sourceOptions)
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.parquet")
            .parquet(sourceTable.getDataPath())
            .orderBy("id"); // order by id to ensure deterministic order for comparison
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
                          .orderBy("id");
                    }));

    String[] selectColumnsArr = schema.fieldNames();
    List<String> dataset1Rows = sourceRows.selectExpr(selectColumnsArr).toJSON().collectAsList();

    Set<Map.Entry<String, Dataset<Row>>> entrySet = targetRowsByFormat.entrySet();

    for (Map.Entry<String, Dataset<Row>> entry : entrySet) {

      String format = entry.getKey();

      Dataset<Row> targetRows = entry.getValue();

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
    SyncMode syncMode;
  }
}
