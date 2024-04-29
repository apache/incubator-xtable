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
 
package org.apache.xtable;

import static org.apache.xtable.model.storage.TableFormat.DELTA;
import static org.apache.xtable.model.storage.TableFormat.HUDI;
import static org.apache.xtable.model.storage.TableFormat.ICEBERG;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import org.apache.hudi.common.model.HoodieTableType;

public interface GenericTable<T, Q> extends AutoCloseable {
  // A list of values for the level field which serves as a basic field to partition on for tests
  List<String> LEVEL_VALUES = Arrays.asList("INFO", "WARN", "ERROR");
  // typical inserts or upserts do not use this partition value.
  String SPECIAL_PARTITION_VALUE = "FATAL";

  List<T> insertRows(int numRows);

  List<T> insertRecordsForSpecialPartition(int numRows);

  void upsertRows(List<T> rows);

  void deleteRows(List<T> rows);

  void deletePartition(Q partitionValue);

  void deleteSpecialPartition();

  String getBasePath();

  default String getDataPath() {
    return getBasePath();
  }

  String getOrderByColumn();

  void close();

  void reload();

  List<String> getColumnsToSelect();

  String getFilterQuery();

  static GenericTable getInstance(
      String tableName,
      Path tempDir,
      SparkSession sparkSession,
      JavaSparkContext jsc,
      String sourceFormat,
      boolean isPartitioned) {
    switch (sourceFormat) {
      case HUDI:
        return TestSparkHudiTable.forStandardSchemaAndPartitioning(
            tableName, tempDir, jsc, isPartitioned);
      case DELTA:
        return TestSparkDeltaTable.forStandardSchemaAndPartitioning(
            tableName, tempDir, sparkSession, isPartitioned ? "level" : null);
      case ICEBERG:
        return TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, isPartitioned ? "level" : null, tempDir, jsc.hadoopConfiguration());
      default:
        throw new IllegalArgumentException("Unsupported source format: " + sourceFormat);
    }
  }

  static GenericTable getInstanceWithAdditionalColumns(
      String tableName,
      Path tempDir,
      SparkSession sparkSession,
      JavaSparkContext jsc,
      String sourceFormat,
      boolean isPartitioned) {
    switch (sourceFormat) {
      case HUDI:
        return TestSparkHudiTable.forSchemaWithAdditionalColumnsAndPartitioning(
            tableName, tempDir, jsc, isPartitioned);
      case DELTA:
        return TestSparkDeltaTable.forSchemaWithAdditionalColumnsAndPartitioning(
            tableName, tempDir, sparkSession, isPartitioned ? "level" : null);
      case ICEBERG:
        return TestIcebergTable.forSchemaWithAdditionalColumnsAndPartitioning(
            tableName, isPartitioned ? "level" : null, tempDir, jsc.hadoopConfiguration());
      default:
        throw new IllegalArgumentException("Unsupported source format: " + sourceFormat);
    }
  }

  static GenericTable getInstanceWithCustomPartitionConfig(
      String tableName,
      Path tempDir,
      JavaSparkContext jsc,
      String sourceFormat,
      String partitionConfig) {
    switch (sourceFormat) {
      case HUDI:
        return TestSparkHudiTable.forStandardSchema(
            tableName, tempDir, jsc, partitionConfig, HoodieTableType.COPY_ON_WRITE);
      default:
        throw new IllegalArgumentException(
            String.format(
                "Unsupported source format: %s for custom partition config", sourceFormat));
    }
  }

  static String getTableName() {
    return "test_table_" + UUID.randomUUID().toString().replaceAll("-", "_");
  }
}
