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

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.storage.TableFormat;

public class ITParquetDataManager {
  private static SparkSession spark;
  private ParquetConversionSource conversionSource;

  @BeforeAll
  public static void setup() {
    spark = SparkSession.builder().appName("ParquetTest").master("local[*]").getOrCreate();
  }

  @Test
  public void testAppendParquetFileMultiplePartition() throws IOException {

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
        Arrays.asList(
            RowFactory.create(100, "A", 2026, 12),
            RowFactory.create(101, "AA", 2026, 12),
            RowFactory.create(102, "CB", 2027, 11),
            RowFactory.create(103, "BA", 2027, 11));

    Dataset<Row> df = spark.createDataFrame(data, schema);
    Path fixedPath = Paths.get("target", "fixed-parquet-data", "parquet_table_test_2");
    Path appendFilePath = Paths.get("target", "fixed-parquet-data", "parquet_file_test_2");
    String outputPath = fixedPath.toString();
    String finalAppendFilePath = appendFilePath.toString();

    df.coalesce(1).write().partitionBy("year", "month").mode("overwrite").parquet(outputPath);

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
        Arrays.asList(
            RowFactory.create(101, "A", 2026, 12),
            RowFactory.create(301, "D", 2027, 11),
            RowFactory.create(302, "DA", 2027, 11));
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
    Dataset<Row> dfWithNewTimes = spark.read().parquet(finalAppendFilePath);
    dfWithNewTimes
        .coalesce(1)
        .write()
        .partitionBy("year", "month")
        .mode("append")
        .parquet(outputPath);
    fs.delete(new org.apache.hadoop.fs.Path(finalAppendFilePath), true);
    // conversionSource operations
    InternalTable result = conversionSource.getTable(newModifTime);
    assertEquals(newModifTime, result.getLatestCommitTime());
    assertNotNull(result);
    assertEquals("parquet_table_test_2", result.getName());
    assertEquals(TableFormat.PARQUET, result.getTableFormat());
    assertNotNull(result.getReadSchema());
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
