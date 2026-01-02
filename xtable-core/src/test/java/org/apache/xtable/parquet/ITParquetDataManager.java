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
import org.junit.jupiter.api.Test;

public class ITParquetDataManager {

  @Test
  public void testAppendParquetFile() throws IOException {
    SparkSession spark =
        SparkSession.builder().appName("TestAppendFunctionnality").master("local[*]").getOrCreate();
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
            RowFactory.create(101, "A", 2025, 12),
            RowFactory.create(102, "B", 2025, 12),
            RowFactory.create(201, "C", 2025, 11),
            RowFactory.create(301, "D", 2024, 7));

    Dataset<Row> df = spark.createDataFrame(data, schema);
    Path fixedPath = Paths.get("target", "fixed-parquet-data", "parquet-partitioned_table_test");
    String outputPath = fixedPath.toFile().getName();

    df.write().partitionBy("year", "month").mode("overwrite").parquet(outputPath);

    // test find files to sync
    long targetModifTime = System.currentTimeMillis() - 360000;
    org.apache.hadoop.fs.Path hdfsPath =
        new org.apache.hadoop.fs.Path("target/fixed-parquet-data/parquet-partitioned_table_test");
    FileSystem fs = FileSystem.get(hdfsPath.toUri(), conf);
    // set the modification time to the file
    updateModificationTimeRecursive(fs, hdfsPath, targetModifTime);
    // create new file to append using Spark
    List<Row> futureDataToSync =
        Arrays.asList(RowFactory.create(101, "A", 2026, 12), RowFactory.create(301, "D", 2027, 7));
    Dataset<Row> dfToSync = spark.createDataFrame(futureDataToSync, schema);
    dfToSync.write().partitionBy("year", "month").mode("append").parquet(outputPath);
    // TODO create the folders manually for a new partition value as appendFile works only within the same partition value

    long newModifTime = System.currentTimeMillis() - 5000;
    List<String> newPartitions = Arrays.asList("year=2026/month=12", "year=2027/month=7");

    for (String partition : newPartitions) {
      org.apache.hadoop.fs.Path partitionPath = new org.apache.hadoop.fs.Path(hdfsPath, partition);
      if (fs.exists(partitionPath)) {
        updateModificationTimeRecursive(fs, partitionPath, newModifTime);
      }
    }
    List<org.apache.hadoop.fs.Path> resultingFiles =
        ParquetDataManager.formNewTargetFiles(
            conf, new org.apache.hadoop.fs.Path(fixedPath.toUri()), newModifTime);
    // check if resultingFiles contains the append data only (through the partition names)
    for (org.apache.hadoop.fs.Path p : resultingFiles) {
      String pathString = p.toString();
      //  should be TRUE
      boolean isNewData = pathString.contains("year=2026") || pathString.contains("year=2027");

      // should be FALSE
      boolean isOldData = pathString.contains("year=2024") || pathString.contains("year=2025");

      assertTrue(isNewData, "Path should belong to appended data: " + pathString);
      assertFalse(isOldData, "Path should NOT belong to old data: " + pathString);
    }
    // TODO test appendNewParquetFile() (using a non-Spark approach to append a parquet file)

    spark.stop();
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
}
