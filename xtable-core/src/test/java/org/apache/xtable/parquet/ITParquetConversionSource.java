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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.client.HoodieReadClient;

import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.hudi.HudiTestUtil;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.storage.TableFormat;

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
            .basePath(fixedPath.toAbsolutePath().toString())
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
}
