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
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
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

import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.storage.TableFormat;

public class ITParquetDataManager {
  private static SparkSession spark;
  private static ParquetConversionSourceProvider conversionSourceProvider;
  public static final String PARTITION_FIELD_SPEC_CONFIG =
      "xtable.parquet.source.partition_field_spec_config";

  @BeforeAll
  public static void setup() {
    spark = SparkSession.builder().appName("ParquetTest").master("local[*]").getOrCreate();
    conversionSourceProvider = new ParquetConversionSourceProvider();
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "file:///");
    conversionSourceProvider.init(hadoopConf);
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

    org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(outputPath);
    FileSystem fs = FileSystem.get(hdfsPath.toUri(), conf);
    // set the modification time to the table file
    // update modifTime for file to append
    // many partitions case
    List<String> newPartitions = Arrays.asList("year=2026/month=12", "year=2027/month=11");
    long targetModifTime = System.currentTimeMillis() - 360000;
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
    /*long newModifTime = System.currentTimeMillis() - 50000;
    for (String partition : newPartitions) {
      org.apache.hadoop.fs.Path partitionPath =
          new org.apache.hadoop.fs.Path(finalAppendFilePath, partition);
      if (fs.exists(partitionPath)) {
        updateModificationTimeRecursive(fs, partitionPath, newModifTime);
      }
    }*/
    Dataset<Row> dfWithNewTimes = spark.read().parquet(finalAppendFilePath);
    dfWithNewTimes
        .coalesce(1)
        .write()
        .partitionBy("year", "month")
        .mode("append")
        .parquet(outputPath);
    fs.delete(new org.apache.hadoop.fs.Path(finalAppendFilePath), true);
    // conversionSource operations
    Properties sourceProperties = new Properties();
    String partitionConfig = "id:MONTH:year=yyyy/month=MM";
    sourceProperties.put(PARTITION_FIELD_SPEC_CONFIG, partitionConfig);
    SourceTable tableConfig =
        SourceTable.builder()
            .name("parquet_table_test_2")
            .basePath(fixedPath.toAbsolutePath().toUri().toString())
            .additionalProperties(sourceProperties)
            .formatName(TableFormat.PARQUET)
            .build();

    ParquetConversionSource conversionSource =
        conversionSourceProvider.getConversionSourceInstance(tableConfig);

    long newModifTime = System.currentTimeMillis() - 50000;

    for (String partition : newPartitions) {
      org.apache.hadoop.fs.Path partitionPath =
          new org.apache.hadoop.fs.Path(outputPath, partition);

      RemoteIterator<LocatedFileStatus> it = fs.listFiles(partitionPath, false);
      while (it.hasNext()) {
        LocatedFileStatus fileStatus = it.next();

        if (fileStatus.getModificationTime() > newModifTime) {
          fs.setTimes(fileStatus.getPath(), newModifTime, -1);
        } else {

          fs.setTimes(fileStatus.getPath(), targetModifTime, -1);
        }
      }

      fs.setTimes(partitionPath, newModifTime, -1);
    }

    InternalTable result = conversionSource.getTable(newModifTime);
    assertEquals(
        Instant.ofEpochMilli(newModifTime).toString(), result.getLatestCommitTime().toString());
    assertNotNull(result);
    assertEquals("parquet_table_test_2", result.getName());
    assertEquals(TableFormat.PARQUET, result.getTableFormat());
    assertNotNull(result.getReadSchema());
    InternalSnapshot snapshot = conversionSource.getCurrentSnapshot();
    assertNotNull(snapshot);
    TableChange changes = conversionSource.getTableChangeForCommit(newModifTime);
    assertNotNull(changes);
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
