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

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import org.apache.hudi.common.model.HoodieTableType;

import org.apache.xtable.delta.TestSparkDeltaTable;
import org.apache.xtable.hudi.TestSparkHudiTable;
import org.apache.xtable.iceberg.TestIcebergTable;

public class GenericTableFactory {
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

  static GenericTable getInstanceWithUUIDColumns(
      String tableName,
      Path tempDir,
      SparkSession sparkSession,
      JavaSparkContext jsc,
      String sourceFormat,
      boolean isPartitioned) {
    switch (sourceFormat) {
      case ICEBERG:
        return TestIcebergTable.forSchemaWithUUIDColumns(
            tableName, isPartitioned ? "level" : null, tempDir, jsc.hadoopConfiguration());
      default:
        throw new IllegalArgumentException("Unsupported source format: " + sourceFormat);
    }
  }
}
