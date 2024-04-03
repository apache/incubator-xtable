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
 
package org.apache.xtable.hudi.sync;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.sync.common.HoodieSyncConfig;

public class TestXTableSyncTool {
  private static SparkSession spark;
  @TempDir static Path tempDir;

  private static Stream<Arguments> testCases() {
    return Stream.of(
        Arguments.of(""), // unpartitioned
        Arguments.of("partition_string"), // identity transform for partition
        Arguments.of("time_millis:TIMESTAMP") // timestamp transform
        );
  }

  @BeforeAll
  public static void initSpark() {
    SparkConf sparkConf =
        new SparkConf()
            .setAppName("xtable")
            .set("spark.serializer", KryoSerializer.class.getName())
            .set("spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled", "true")
            .set("spark.sql.shuffle.partitions", "1")
            .set("spark.default.parallelism", "1")
            .setMaster("local[4]");
    spark = SparkSession.builder().config(sparkConf).getOrCreate();
  }

  @ParameterizedTest
  @MethodSource(value = "testCases")
  public void testSync(String partitionPath) {
    String tableName = "table-" + UUID.randomUUID();
    String path = tempDir.toUri() + "/" + tableName;
    Map<String, String> options = new HashMap<>();
    options.put(DataSourceWriteOptions.PRECOMBINE_FIELD().key(), "key");
    options.put(DataSourceWriteOptions.RECORDKEY_FIELD().key(), "key");
    options.put(DataSourceWriteOptions.PARTITIONPATH_FIELD().key(), partitionPath);
    options.put("hoodie.table.name", tableName);
    if (partitionPath.contains("TIMESTAMP")) {
      // set custom key gen properties
      options.put("hoodie.keygen.timebased.timestamp.type", "EPOCHMILLISECONDS");
      options.put("hoodie.keygen.timebased.output.dateformat", "yyyy-MM-dd");
      options.put(
          "hoodie.datasource.write.keygenerator.class",
          "org.apache.hudi.keygen.CustomKeyGenerator");
    }
    writeBasicHudiTable(path, options);

    Properties properties = new Properties();
    properties.put(XTableSyncConfig.ONE_TABLE_FORMATS.key(), "iceberg,DELTA");
    properties.put(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME.key(), partitionPath);
    properties.put(HoodieSyncConfig.META_SYNC_BASE_PATH.key(), path);
    properties.putAll(options);

    new XTableSyncTool(properties, new Configuration()).syncHoodieTable();
    // lightweight check to make sure metadata dirs are made - assumes that InternalTable sync is
    // correct if it succeeds
    assertTrue(Files.exists(Paths.get(URI.create(path + "/_delta_log"))));
    assertTrue(Files.exists(Paths.get(URI.create(path + "/metadata"))));
  }

  protected void writeBasicHudiTable(String path, Map<String, String> options) {
    StructType schema =
        DataTypes.createStructType(
            Arrays.asList(
                DataTypes.createStructField("key", DataTypes.StringType, false),
                DataTypes.createStructField("partition_string", DataTypes.StringType, false),
                DataTypes.createStructField("time_millis", DataTypes.TimestampType, false),
                DataTypes.createStructField("value", DataTypes.StringType, true)));
    String partition = "FIRST";
    Timestamp timestamp = Timestamp.from(Instant.now());
    Row row1 = RowFactory.create("key1", partition, timestamp, "value1");
    Row row2 = RowFactory.create("key2", partition, timestamp, "value2");
    Row row3 = RowFactory.create("key3", partition, timestamp, "value3");
    spark
        .createDataset(Arrays.asList(row1, row2, row3), RowEncoder.apply(schema))
        .write()
        .format("hudi")
        .options(options)
        .save(path);
  }
}
