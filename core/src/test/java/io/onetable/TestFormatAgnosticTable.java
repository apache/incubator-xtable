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
 
package io.onetable;

import java.nio.file.Path;
import java.util.Optional;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import io.onetable.model.storage.TableFormat;

public class TestFormatAgnosticTable {
  private Optional<TestSparkDeltaTable> sparkDeltaTable = Optional.empty();
  private Optional<TestSparkHudiTable> sparkHudiTable = Optional.empty();

  public TestFormatAgnosticTable(
      String tableName,
      Path tempDir,
      SparkSession sparkSession,
      JavaSparkContext javaSparkContext,
      TableFormat sourceFormat,
      boolean isPartitioned) {
    switch (sourceFormat) {
      case HUDI:
        this.sparkHudiTable =
            Optional.of(
                TestSparkHudiTable.forStandardSchemaAndPartitioning(
                    tableName, tempDir, javaSparkContext, isPartitioned));
        break;
      case DELTA:
        this.sparkDeltaTable =
            Optional.of(
                TestSparkDeltaTable.forStandardSchemaAndPartitioning(
                    tableName, tempDir, sparkSession, isPartitioned));
        break;
      default:
        throw new IllegalArgumentException("Unsupported source format: " + sourceFormat);
    }
  }
}
