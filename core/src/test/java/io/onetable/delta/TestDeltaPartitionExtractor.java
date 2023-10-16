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

package io.onetable.delta;

import io.delta.tables.DeltaTable;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import scala.collection.JavaConverters;

/**
 * Validates the partition extraction logic from Delta tables.
 */
// TODO(vamshigv): May be this test can be moved to Unit test style later.
public class TestDeltaPartitionExtractor {
  private static SparkSession sparkSession;

  @TempDir
  public static java.nio.file.Path tempDir;

  @BeforeAll
  public static void setupOnce() {
    sparkSession = buildSparkSession();
  }

  @AfterAll
  public static void teardown() {
    sparkSession.close();
  }

  @Test
  public void testUnpartitionedTable() throws Exception {
    // Define the schema for the Delta table
    StructType schema = new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("name", DataTypes.StringType, true);

    // Sample data for the Delta table
    Row[] data = new Row[]{
        RowFactory.create(1, "John"),
        RowFactory.create(2, "Alice"),
        RowFactory.create(3, "Bob")
    };

    Dataset<Row> sampleData = sparkSession.createDataFrame(Arrays.asList(data), schema);
    // Create a Delta table
    sampleData.write()
        .format("delta")
        .save("/tmp/randomDeltaTable/doesitwork");
    DeltaLog deltaLog = DeltaLog.forTable(sparkSession, "/tmp/randomDeltaTable/doesitwork");
    StructType schemaStruct = deltaLog.snapshot().metadata().schema();
    List<String> partitionColumns =
        JavaConverters.seqAsJavaList(deltaLog.metadata().partitionColumns());
    DeltaPartitionExtractor deltaPartitionExtractor = DeltaPartitionExtractor.getInstance();
    DeltaSchemaExtractor deltaSchemaExtractor = DeltaSchemaExtractor.getInstance();
    OneSchema oneSchema = deltaSchemaExtractor.toOneSchema(schemaStruct);
    List<OnePartitionField> x =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(schemaStruct, oneSchema, partitionColumns);
  }

  @Test
  public void testSimplePartitionedTable() throws Exception {
    StructType schema = new StructType()
        .add("id", DataTypes.IntegerType, false)
        .add("name", DataTypes.StringType, true)
        .add("isStudent", DataTypes.BooleanType, false)
        .add("score", DataTypes.DoubleType, true);

    // Sample data
    Row[] data = new Row[]{
        RowFactory.create(1, "John", true, 123.45),
        RowFactory.create(2, "Alice", false, 67.89),
        RowFactory.create(3, "Bob", true, 42.0)
    };

    Dataset<Row> sampleData = sparkSession.createDataFrame(Arrays.asList(data), schema);
    // Create a Delta table
    sampleData.write()
        .format("delta")
        .partitionBy("name")
        .save("/tmp/randomDeltaTable/doesitwork2");
    DeltaLog deltaLog = DeltaLog.forTable(sparkSession, "/tmp/randomDeltaTable/doesitwork2");
    StructType schemaStruct = deltaLog.snapshot().metadata().schema();
    List<String> partitionColumns =
        JavaConverters.seqAsJavaList(deltaLog.metadata().partitionColumns());
    DeltaPartitionExtractor deltaPartitionExtractor = DeltaPartitionExtractor.getInstance();
    DeltaSchemaExtractor deltaSchemaExtractor = DeltaSchemaExtractor.getInstance();
    OneSchema oneSchema = deltaSchemaExtractor.toOneSchema(schemaStruct);
    List<OnePartitionField> x =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(schemaStruct, oneSchema, partitionColumns);
  }

  private Timestamp randomTimestamp() {
    Random random = new Random();
    long currentTimeMillis = System.currentTimeMillis();
    long oneYearMillis = 365L * 24 * 60 * 60 * 1000; // One year in milliseconds

    long randomTime = currentTimeMillis - (long) (random.nextDouble() * oneYearMillis);
    return new Timestamp(randomTime);
  }

  @Test
  public void testGeneratedColumnsPartitionedTable() throws Exception {
    String tableLocation = "/tmp/randomDeltaTable/doesitwork2";

    // Sample data
    Row[] data = new Row[]{
        RowFactory.create(1, "John", true, 123.45, randomTimestamp()),
        RowFactory.create(2, "Alice", false, 67.89, randomTimestamp()),
        RowFactory.create(3, "Bob", true, 42.0, randomTimestamp())
    };

    // Define schema
    StructType schema = new StructType()
            .add("id", DataTypes.IntegerType, false)
            .add("name", DataTypes.StringType, true)
            .add("isStudent", DataTypes.BooleanType, false)
            .add("score", DataTypes.DoubleType, true)
            .add("admitted_time", DataTypes.TimestampType, true);

    Dataset<Row> sampleData = sparkSession.createDataFrame(Arrays.asList(data), schema);
    // Create a Delta table
    sampleData.write()
        .format("delta")
        .mode("append")
        .save(tableLocation);
    DeltaLog deltaLog = DeltaLog.forTable(sparkSession, "/tmp/randomDeltaTable/doesitwork2");
    StructType schemaStruct = deltaLog.snapshot().metadata().schema();
    List<String> partitionColumns =
        JavaConverters.seqAsJavaList(deltaLog.metadata().partitionColumns());
    DeltaPartitionExtractor deltaPartitionExtractor = DeltaPartitionExtractor.getInstance();
    DeltaSchemaExtractor deltaSchemaExtractor = DeltaSchemaExtractor.getInstance();
    OneSchema oneSchema = deltaSchemaExtractor.toOneSchema(schemaStruct);
    List<OnePartitionField> x =
        deltaPartitionExtractor.convertFromDeltaPartitionFormat(schemaStruct, oneSchema, partitionColumns);
  }

  private static SparkSession buildSparkSession() {
    SparkConf sparkConf =
        new SparkConf()
            .setAppName("testDeltaPartitionExtractor")
            .set("spark.serializer", KryoSerializer.class.getName())
            .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .set("spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled", "true")
            .set("spark.master", "local[2]");
    return SparkSession.builder().config(sparkConf).getOrCreate();
  }
}
