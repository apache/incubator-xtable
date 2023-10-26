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
import java.text.ParseException;
import java.util.List;
import java.util.Optional;

import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import io.onetable.model.storage.TableFormat;

@Getter
public class TestFormatAgnosticTable implements AutoCloseable {
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

  public String getBasePath() {
    if (sparkHudiTable.isPresent()) {
      return sparkHudiTable.get().getBasePath();
    } else if (sparkDeltaTable.isPresent()) {
      return sparkDeltaTable.get().getBasePath();
    } else {
      throw new IllegalStateException("Neither Hoodie nor Delta table is initialized.");
    }
  }

  public InsertRecordsHolder insertRecords(int numRows) {
    if (sparkHudiTable.isPresent()) {
      List<HoodieRecord<HoodieAvroPayload>> records = sparkHudiTable.get().insertRecords(numRows,
          true);
      return InsertRecordsHolder.builder()
          .hoodieRecords(Optional.of(records))
          .build();
    } else if (sparkDeltaTable.isPresent()) {
      List<Row> rows = sparkDeltaTable.get().insertRows(numRows);
      return InsertRecordsHolder.builder()
          .deltaRows(Optional.of(rows))
          .build();
    } else {
      throw new IllegalStateException("Neither Hoodie nor Delta table is initialized.");
    }
  }

  public void upsertRecords(InsertRecordsHolder insertRecordsHolder) throws ParseException {
    if (sparkHudiTable.isPresent()) {
      sparkHudiTable.get().upsertRecords(
          insertRecordsHolder.getHoodieRecords().get().subList(0, 20), true);
    } else if (sparkDeltaTable.isPresent()) {
      sparkDeltaTable.get().upsertRows(insertRecordsHolder.getDeltaRows().get().subList(0, 20));
    } else {
      throw new IllegalStateException("Neither Hoodie nor Delta table is initialized.");
    }
  }

  public void deleteRecords(InsertRecordsHolder insertRecordsHolder) throws ParseException {
    if (sparkHudiTable.isPresent()) {
      sparkHudiTable.get().deleteRecords(
          insertRecordsHolder.getHoodieRecords().get().subList(30, 50), true);
    } else if (sparkDeltaTable.isPresent()) {
      sparkDeltaTable.get().deleteRows(insertRecordsHolder.getDeltaRows().get().subList(30, 50));
    } else {
      throw new IllegalStateException("Neither Hoodie nor Delta table is initialized.");
    }
  }

  // TODO(vamshigv): Clean up with generics later.
  @Builder
  @Value
  public static class InsertRecordsHolder {
    private final Optional<List<HoodieRecord<HoodieAvroPayload>>> hoodieRecords;
    private final Optional<List<Row>> deltaRows;
  }

  @Override
  public void close() {
    if (sparkHudiTable.isPresent()) {
      sparkHudiTable.get().close();
    }
    if (sparkDeltaTable.isPresent()) {
      sparkDeltaTable.get().close();
    }
  }
}
