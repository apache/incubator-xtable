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

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

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

    // TODO create an InternalDataFile and Partition Fields for testing purposes

    // TODO test appendNewParquetFile()

    // TODO validate the final table
    spark.stop();
  }
}
