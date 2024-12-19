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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.SneakyThrows;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import org.apache.spark.sql.delta.DeltaLog;

import com.google.common.base.Preconditions;

import io.delta.tables.DeltaTable;

import org.apache.xtable.delta.TestDeltaHelper;

@Getter
public class TestSparkDeltaTable implements GenericTable<Row, Object>, Closeable {
  // typical inserts or upserts do not use this partition value.
  private static final Integer SPECIAL_DATE_PARTITION_VALUE = 1990;
  private final String tableName;
  private final String basePath;
  private final SparkSession sparkSession;
  private final TestDeltaHelper testDeltaHelper;
  private final String partitionField;
  private final boolean includeAdditionalColumns;
  DeltaLog deltaLog;
  DeltaTable deltaTable;

  public static TestSparkDeltaTable forStandardSchemaAndPartitioning(
      String tableName, Path tempDir, SparkSession sparkSession, String partitionField) {
    return new TestSparkDeltaTable(tableName, tempDir, sparkSession, partitionField, false);
  }

  public static TestSparkDeltaTable forSchemaWithAdditionalColumnsAndPartitioning(
      String tableName, Path tempDir, SparkSession sparkSession, String partitionField) {
    return new TestSparkDeltaTable(tableName, tempDir, sparkSession, partitionField, true);
  }

  public TestSparkDeltaTable(
      String name,
      Path tempDir,
      SparkSession sparkSession,
      String partitionField,
      boolean includeAdditionalColumns) {
    try {
      this.tableName = name;
      this.basePath = initBasePath(tempDir, tableName);
      this.sparkSession = sparkSession;
      this.partitionField = partitionField;
      this.includeAdditionalColumns = includeAdditionalColumns;
      this.testDeltaHelper =
          TestDeltaHelper.createTestDataHelper(partitionField, includeAdditionalColumns);
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
    Dataset<Row> df = sparkSession.createDataFrame(rows, testDeltaHelper.getTableStructSchema());
    df.write().format("delta").mode("append").save(basePath);
    return rows;
  }

  public List<Row> insertRowsForPartition(int numRows, Integer partitionValue) {
    return insertRowsForPartition(numRows, partitionValue, SPECIAL_PARTITION_VALUE);
  }

  public List<Row> insertRowsForPartition(int numRows, Integer year, String level) {
    List<Row> rows = testDeltaHelper.generateRowsForSpecificPartition(numRows, year, level);
    Dataset<Row> df = sparkSession.createDataFrame(rows, testDeltaHelper.getTableStructSchema());
    df.write().format("delta").mode("append").save(basePath);
    return rows;
  }

  @Override
  public List<Row> insertRecordsForSpecialPartition(int numRows) {
    return insertRowsForPartition(numRows, SPECIAL_DATE_PARTITION_VALUE, SPECIAL_PARTITION_VALUE);
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
  public void deletePartition(Object partitionValue) {
    Preconditions.checkArgument(
        partitionField != null,
        "Invalid operation! Delete partition is only supported for partitioned tables.");
    Column condition = functions.col(partitionField).equalTo(partitionValue);
    deltaTable.delete(condition);
  }

  @Override
  public void deleteSpecialPartition() {
    if (partitionField.equals("level")) {
      deletePartition(SPECIAL_PARTITION_VALUE);
    } else if (partitionField.equals("yearOfBirth")) {
      deletePartition(SPECIAL_DATE_PARTITION_VALUE);
    } else {
      throw new IllegalArgumentException(
          "Delete special partition is only supported for tables partitioned on level or yearOfBirth");
    }
  }

  @Override
  public String getFilterQuery() {
    return "id % 2 = 0";
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
                row -> row.getTimestamp(4).toInstant().atZone(ZoneId.of("UTC")).getYear()));
  }

  @Override
  public String getBasePath() {
    return basePath;
  }

  @Override
  public void close() {
    // no-op as spark session lifecycle is managed by the caller
  }

  @Override
  public void reload() {
    // Handle to reload the table on demand.
    this.deltaLog = DeltaLog.forTable(sparkSession, basePath);
    this.deltaTable = DeltaTable.forPath(sparkSession, basePath);
  }

  @Override
  public List<String> getColumnsToSelect() {
    // Exclude generated columns.
    return Arrays.asList(testDeltaHelper.getTableStructSchema().fieldNames()).stream()
        .filter(columnName -> !columnName.equals("yearOfBirth"))
        .collect(Collectors.toList());
  }
}
