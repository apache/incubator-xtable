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

import static io.onetable.delta.TestDeltaHelper.DATE_TIME_FORMATTER;
import static io.onetable.delta.TestDeltaHelper.createTestDataHelper;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.SneakyThrows;
import lombok.Value;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import org.apache.spark.sql.delta.DeltaLog;

import com.google.common.base.Preconditions;

import io.delta.tables.DeltaTable;

import io.onetable.delta.TestDeltaHelper;

@Value
public class TestSparkDeltaTable implements GenericTable<Row, Integer>, Closeable {
  // typical inserts or upserts do not use this partition value.
  private static final Integer SPECIAL_PARTITION_VALUE = 1990;
  String tableName;
  String basePath;
  SparkSession sparkSession;
  DeltaLog deltaLog;
  DeltaTable deltaTable;
  TestDeltaHelper testDeltaHelper;
  boolean tableIsPartitioned;
  boolean includeAdditionalColumns;

  public static TestSparkDeltaTable forStandardSchemaAndPartitioning(
      String tableName, Path tempDir, SparkSession sparkSession, boolean isPartitioned) {
    return new TestSparkDeltaTable(tableName, tempDir, sparkSession, isPartitioned, false);
  }

  public static TestSparkDeltaTable forSchemaWithAdditionalColumnsAndPartitioning(
      String tableName, Path tempDir, SparkSession sparkSession, boolean isPartitioned) {
    return new TestSparkDeltaTable(tableName, tempDir, sparkSession, isPartitioned, true);
  }

  public TestSparkDeltaTable(
      String name,
      Path tempDir,
      SparkSession sparkSession,
      boolean isPartitioned,
      boolean includeAdditionalColumns) {
    try {
      this.tableName = name;
      this.basePath = initBasePath(tempDir, tableName);
      this.sparkSession = sparkSession;
      this.tableIsPartitioned = isPartitioned;
      this.includeAdditionalColumns = includeAdditionalColumns;
      this.testDeltaHelper = createTestDataHelper(isPartitioned, includeAdditionalColumns);
      testDeltaHelper.createTable(sparkSession, tableName, basePath);
      this.deltaLog = DeltaLog.forTable(sparkSession, basePath);
      this.deltaTable = DeltaTable.forPath(sparkSession, basePath);
    } catch (IOException ex) {
      throw new UncheckedIOException("Unable initialize Delta spark table", ex);
    }
  }

  @Override
  public List<Row> insertRows(int numRows) {
    List<Row> rows = testDeltaHelper.generateRows(numRows);
    String insertStatement = testDeltaHelper.generateSqlForDataInsert(tableName, rows);
    sparkSession.sql(insertStatement);
    return rows;
  }

  public List<Row> insertRowsForPartition(int numRows, Integer partitionValue) {
    List<Row> rows = testDeltaHelper.generateRowsForSpecificPartition(numRows, partitionValue);
    String insertStatement = testDeltaHelper.generateSqlForDataInsert(tableName, rows);
    sparkSession.sql(insertStatement);
    return rows;
  }

  @Override
  public List<Row> insertRecordsForSpecialPartition(int numRows) {
    return insertRowsForPartition(numRows, SPECIAL_PARTITION_VALUE);
  }

  @Override
  public Integer getAnyPartitionValue(List<Row> rows) {
    Map<Integer, List<Row>> rowsByPartition = getRowsByPartition(rows);
    return rowsByPartition.keySet().stream().findFirst().get();
  }

  @Override
  public String getOrderByColumn() {
    return "id";
  }

  @SneakyThrows
  public void upsertRows(List<Row> upsertRows) {
    List<Row> upserts = testDeltaHelper.transformForUpsertsOrDeletes(upsertRows, true);
    Dataset<Row> upsertDataset =
        sparkSession.createDataFrame(upserts, testDeltaHelper.getTableStructSchema());
    deltaTable
        .alias("person")
        .merge(upsertDataset.alias("source"), "person.id = source.id")
        .whenMatched()
        .updateAll()
        .execute();
  }

  @SneakyThrows
  public void deleteRows(List<Row> deleteRows) {
    List<Row> deletes = testDeltaHelper.transformForUpsertsOrDeletes(deleteRows, false);
    Dataset<Row> deleteDataset =
        sparkSession.createDataFrame(deletes, testDeltaHelper.getTableStructSchema());
    deltaTable
        .alias("person")
        .merge(deleteDataset.alias("source"), "person.id = source.id")
        .whenMatched()
        .delete()
        .execute();
  }

  @Override
  public void deletePartition(Integer partitionValue) {
    Preconditions.checkArgument(
        tableIsPartitioned,
        "Invalid operation! Delete partition is only supported for partitioned tables.");
    Column condition = functions.col("yearOfBirth").equalTo(partitionValue);
    deltaTable.delete(condition);
  }

  @Override
  public void deleteSpecialPartition() {
    deletePartition(SPECIAL_PARTITION_VALUE);
  }

  public void runCompaction() {
    deltaTable.optimize().executeCompaction();
  }

  public void runClustering() {
    deltaTable.optimize().executeZOrderBy("gender");
  }

  public long getNumRows() {
    Dataset<Row> df = sparkSession.read().format("delta").load(basePath);
    return (int) df.count();
  }

  public Long getVersion() {
    return deltaLog.snapshot().version();
  }

  public Long getLastCommitTimestamp() {
    return deltaLog.snapshot().timestamp();
  }

  public void runVacuum() {
    deltaTable.vacuum(0.0);
  }

  private String initBasePath(Path tempDir, String tableName) throws IOException {
    Path basePath = tempDir.resolve(tableName);
    Files.createDirectories(basePath);
    return basePath.toUri().toString();
  }

  public List<String> getAllActiveFiles() {
    return deltaLog.snapshot().allFiles().collectAsList().stream()
        .map(addFile -> addSlashToBasePath(basePath) + addFile.path())
        .collect(Collectors.toList());
  }

  private String addSlashToBasePath(String basePath) {
    if (basePath.endsWith("/")) {
      return basePath;
    }
    return basePath + "/";
  }

  public Map<Integer, List<Row>> getRowsByPartition(List<Row> rows) {
    return rows.stream()
        .collect(
            Collectors.groupingBy(
                row -> {
                  try {
                    LocalDateTime parsedDateTime =
                        LocalDateTime.parse(row.getString(4), DATE_TIME_FORMATTER);
                    Timestamp timestamp = Timestamp.valueOf(parsedDateTime);
                    return timestamp.toLocalDateTime().getYear();
                  } catch (Exception e) {
                    throw new RuntimeException(e);
                  }
                }));
  }

  @Override
  public void close() {
    // no-op as spark session lifecycle is managed by the caller
  }
}
