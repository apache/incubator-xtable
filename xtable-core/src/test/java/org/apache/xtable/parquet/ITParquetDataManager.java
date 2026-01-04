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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class ITParquetDataManager {
  private static SparkSession spark;

  @BeforeAll
  public static void setup() {
    spark = SparkSession.builder().appName("ParquetTest").master("local[*]").getOrCreate();
  }

  @Test
  @Disabled("This test already passed CI")
  public void testFormParquetFileSinglePartition() throws IOException {
    Configuration conf = spark.sparkContext().hadoopConfiguration();
    StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("value", DataTypes.StringType, false),
              DataTypes.createStructField("year", DataTypes.IntegerType, false),
              DataTypes.createStructField("month", DataTypes.IntegerType, false)
            });

    List<Row> data =
        Arrays.asList(RowFactory.create(101, "A", 2026, 12), RowFactory.create(102, "B", 2026, 12));
    Dataset<Row> df = spark.createDataFrame(data, schema);
    Path fixedPath = Paths.get("target", "fixed-parquet-data", "parquet-partitioned_table_test");
    String outputPath = fixedPath.toString();
    df.write().partitionBy("year", "month").mode("overwrite").parquet(outputPath);

    // test find files to sync
    long targetModifTime = System.currentTimeMillis() - 360000;
    org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(outputPath);
    FileSystem fs = FileSystem.get(hdfsPath.toUri(), conf);
    // set the modification time to the file
    updateModificationTimeRecursive(fs, hdfsPath, targetModifTime);
    // create new file to append using Spark
    List<Row> futureDataToSync =
        Arrays.asList(RowFactory.create(101, "A", 2026, 12), RowFactory.create(301, "D", 2026, 12));
    Dataset<Row> dfToSync = spark.createDataFrame(futureDataToSync, schema);
    dfToSync.write().partitionBy("year", "month").mode("append").parquet(outputPath);

    long newModifTime = System.currentTimeMillis() - 50000;

    List<org.apache.hadoop.fs.Path> resultingFiles =
        ParquetDataManager.formNewTargetFiles(conf, hdfsPath, newModifTime);
    // check if resultingFiles contains the append data only (through the partition names)
    for (org.apache.hadoop.fs.Path p : resultingFiles) {
      String pathString = p.toString();

      boolean isNewData = pathString.contains("year=2026");

      long modTime = fs.getFileStatus(p).getModificationTime();
      // test for one partition value
      assertTrue(modTime > newModifTime, "File discovered was actually old data: " + pathString);
      assertTrue(isNewData, "Path should belong to appended data: " + pathString);
    }
  }

  @Test
  @Disabled("This test already passed CI")
  public void testAppendParquetFileSinglePartition() throws IOException {
    Configuration conf = spark.sparkContext().hadoopConfiguration();
    // In testAppendParquetFileSinglePartition
    MessageType schemaParquet =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("value")
            .named("parquet_schema");
    StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("value", DataTypes.StringType, false),
              DataTypes.createStructField("year", DataTypes.IntegerType, false),
              DataTypes.createStructField("month", DataTypes.IntegerType, false)
            });
    List<Row> data =
        Arrays.asList(RowFactory.create(101, "A", 2026, 12), RowFactory.create(102, "B", 2026, 12));

    Dataset<Row> df = spark.createDataFrame(data, schema);
    Path fixedPath = Paths.get("target", "fixed-parquet-data", "parquet_table_test");
    Path appendFilePath = Paths.get("target", "fixed-parquet-data", "parquet_file_test");
    String outputPath = fixedPath.toString();
    String finalAppendFilePath = appendFilePath.toString();

    df.write().partitionBy("year", "month").mode("overwrite").parquet(outputPath);

    // test find files to sync
    long targetModifTime = System.currentTimeMillis() - 360000;
    org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(outputPath);
    FileSystem fs = FileSystem.get(hdfsPath.toUri(), conf);
    // set the modification time to the table file
    updateModificationTimeRecursive(fs, hdfsPath, targetModifTime);
    // create new file to append using Spark
    List<Row> futureDataToSync =
        Arrays.asList(RowFactory.create(101, "A", 2026, 12), RowFactory.create(301, "D", 2026, 12));
    Dataset<Row> dfToSync = spark.createDataFrame(futureDataToSync, schema);
    dfToSync
        .coalesce(1)
        .write()
        .partitionBy("year", "month")
        .mode("overwrite")
        .parquet(finalAppendFilePath);
    long newModifTime = System.currentTimeMillis() - 50000;
    // update modifTime for file to append
    updateModificationTimeRecursive(
        fs, new org.apache.hadoop.fs.Path(finalAppendFilePath), newModifTime);
    // recursively append all files under a specific partition path (e.g. "year=2026/month=12")
    // (assuming that outputPath and finalAppendFilePath are same partitions for different files)
    org.apache.hadoop.fs.Path outputFile =
        ParquetDataManager.appendPartitionedData(
            new org.apache.hadoop.fs.Path(outputPath),
            new org.apache.hadoop.fs.Path(finalAppendFilePath),
            schemaParquet);
    // TODO can create big file which can be split if reaches a certain threshold size

    // test whether the appended data can be selectively filtered for incr sync (e.g. get me the
    // data to sync only)
    List<org.apache.hadoop.fs.Path> resultingFiles =
        ParquetDataManager.formNewTargetFiles(conf, outputFile, newModifTime);
    // check if resultingFiles contains the append data only (through the partition names)
    for (org.apache.hadoop.fs.Path p : resultingFiles) {
      String pathString = p.toString();
      boolean isNewData = pathString.contains("year=2026");
      long modTime = fs.getFileStatus(p).getModificationTime();
      // test for one partition value
      assertTrue(modTime > newModifTime, "File discovered was actually old data: " + pathString);
      assertTrue(isNewData, "Path should belong to appended data: " + pathString);
    }
  }

  @Test
  public void testAppendParquetFileMultiplePartition() throws IOException {
    Configuration conf = spark.sparkContext().hadoopConfiguration();

    MessageType schemaParquet =
        Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT32)
            .named("id")
            .required(PrimitiveType.PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("value")
            .named("parquet_schema");
    StructType schema =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("id", DataTypes.IntegerType, false),
              DataTypes.createStructField("value", DataTypes.StringType, false),
              DataTypes.createStructField("year", DataTypes.IntegerType, false),
              DataTypes.createStructField("month", DataTypes.IntegerType, false)
            });
    List<Row> data =
        Arrays.asList(RowFactory.create(101, "A", 2026, 12), RowFactory.create(102, "B", 2027, 11));

    Dataset<Row> df = spark.createDataFrame(data, schema);
    Path fixedPath = Paths.get("target", "fixed-parquet-data", "parquet_table_test");
    Path appendFilePath = Paths.get("target", "fixed-parquet-data", "parquet_file_test");
    String outputPath = fixedPath.toString();
    String finalAppendFilePath = appendFilePath.toString();

    df.write().partitionBy("year", "month").mode("overwrite").parquet(outputPath);

    // test find files to sync
    long targetModifTime = System.currentTimeMillis() - 360000;
    org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(outputPath);
    FileSystem fs = FileSystem.get(hdfsPath.toUri(), conf);
    // set the modification time to the table file
    // update modifTime for file to append
    // many partitions case
    List<String> newPartitions = Arrays.asList("year=2026/month=12", "year=2027/month=11");
    for (String partition : newPartitions) {
      org.apache.hadoop.fs.Path partitionPath =
          new org.apache.hadoop.fs.Path(outputPath, partition);
      if (fs.exists(partitionPath)) {
        updateModificationTimeRecursive(fs, partitionPath, targetModifTime);
      }
    }
    // create new file to append using Spark
    List<Row> futureDataToSync =
        Arrays.asList(RowFactory.create(101, "A", 2026, 12), RowFactory.create(301, "D", 2027, 11));
    Dataset<Row> dfToSync = spark.createDataFrame(futureDataToSync, schema);
    dfToSync
        .coalesce(1)
        .write()
        .partitionBy("year", "month")
        .mode("overwrite")
        .parquet(finalAppendFilePath);
    long newModifTime = System.currentTimeMillis() - 50000;
    for (String partition : newPartitions) {
      org.apache.hadoop.fs.Path partitionPath =
          new org.apache.hadoop.fs.Path(finalAppendFilePath, partition);
      if (fs.exists(partitionPath)) {
        updateModificationTimeRecursive(fs, partitionPath, newModifTime);
      }
    }
    // recursively append all files under a specific partition path (e.g. "year=2026/month=12")
    // (assuming that outputPath and finalAppendFilePath are same partitions for different files)
    // the result contains all partition paths with the parquet files compacted
    List<org.apache.hadoop.fs.Path> outputPaths =
        ParquetDataManager.mergeDatasetsByPartition(
            new org.apache.hadoop.fs.Path(outputPath),
            new org.apache.hadoop.fs.Path(finalAppendFilePath),
            schemaParquet);
    // TODO can create big file which can be split if reaches a certain threshold size
    // implement bin-packing function for large files

    // test whether the appended data can be selectively filtered for incr sync (e.g. get me the
    // data to sync only)
    List<List<org.apache.hadoop.fs.Path>> finalPaths = new ArrayList<>();
    for (org.apache.hadoop.fs.Path resPath : outputPaths) {
      List<org.apache.hadoop.fs.Path> resultingFiles =
          ParquetDataManager.formNewTargetFiles(conf, resPath, newModifTime);
      finalPaths.add(resultingFiles);
    }
    // check if resultingFiles contains the append data only
    for (List<org.apache.hadoop.fs.Path> fPaths : finalPaths) {
      for (org.apache.hadoop.fs.Path p : fPaths) {
        String pathString = p.toString();
        //  should be TRUE
        boolean isNewData = pathString.contains("year=2026") || pathString.contains("year=2027");
        // should be FALSE
        boolean isOldData = pathString.contains("year=2024") || pathString.contains("year=2025");
        long modTime = fs.getFileStatus(p).getModificationTime();
        // test for one partition value
        assertTrue(modTime > newModifTime, "File discovered was actually old data: " + pathString);
        assertTrue(isNewData, "Path should belong to appended data: " + pathString);
        assertFalse(isOldData, "Path should NOT belong to old data: " + pathString);
      }
    }
  }

  private void updateModificationTimeRecursive(
      FileSystem fs, org.apache.hadoop.fs.Path path, long time) throws IOException {
    org.apache.hadoop.fs.RemoteIterator<org.apache.hadoop.fs.LocatedFileStatus> it =
        fs.listFiles(path, true);
    while (it.hasNext()) {
      org.apache.hadoop.fs.LocatedFileStatus status = it.next();
      if (status.getPath().getName().endsWith(".parquet")) {
        fs.setTimes(status.getPath(), time, -1);
      }
    }
  }

  @AfterAll
  public static void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }
}
