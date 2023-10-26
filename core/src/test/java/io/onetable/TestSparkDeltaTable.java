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
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
public class TestSparkDeltaTable implements Closeable {
  String tableName;
  String basePath;
  SparkSession sparkSession;
  DeltaLog deltaLog;
  DeltaTable deltaTable;
  TestDeltaHelper testDeltaHelper;
  boolean tableIsPartitioned;

  public static TestSparkDeltaTable forStandardSchemaAndPartitioning(
      String tableName, Path tempDir, SparkSession sparkSession, boolean isPartitioned) {
    return new TestSparkDeltaTable(tableName, tempDir, sparkSession, isPartitioned);
  }

  public TestSparkDeltaTable(
      String name, Path tempDir, SparkSession sparkSession, boolean isPartitioned) {
    try {
      this.tableName = generateTableName(name);
      this.basePath = initBasePath(tempDir, tableName);
      this.sparkSession = sparkSession;
      this.tableIsPartitioned = isPartitioned;
      this.testDeltaHelper = createTestDataHelper(isPartitioned);
      createTable(testDeltaHelper.getCreateTableSqlStr(), tableName, basePath);
      this.deltaLog = DeltaLog.forTable(sparkSession, basePath);
      this.deltaTable = DeltaTable.forPath(sparkSession, basePath);
    } catch (IOException ex) {
      throw new UncheckedIOException("Unable initialize Delta spark table", ex);
    }
  }

  private void createTable(String sqlFormatStr, String tableName, String basePath) {
    sparkSession.sql(String.format(sqlFormatStr, tableName, basePath));
  }

  public List<Row> insertRows(int numRows) {
    List<Row> rows = testDeltaHelper.generateRows(numRows);
    String insertStatement = testDeltaHelper.generateSqlForDataInsert(tableName, rows);
    sparkSession.sql(insertStatement);
    return rows;
  }

  public List<Row> insertRows(int numRows, int partitionValue) {
    List<Row> rows = testDeltaHelper.generateRowsForSpecificPartition(numRows, partitionValue);
    String insertStatement = testDeltaHelper.generateSqlForDataInsert(tableName, rows);
    sparkSession.sql(insertStatement);
    return rows;
  }

  public List<Row> insertRowsWithAdditionalColumns(int numRows) {
    List<Row> rows = testDeltaHelper.generateRowsWithAdditionalColumn(numRows);
    String insertStatement = testDeltaHelper.generateInsertSqlForAdditionalColumn(tableName, rows);
    sparkSession.sql(insertStatement);
    return rows;
  }

  public void upsertRows(List<Row> upsertRows) throws ParseException {
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

  public void deleteRows(List<Row> deleteRows) throws ParseException {
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

  public void deletePartition(int partitionValue) {
    Preconditions.checkArgument(
        tableIsPartitioned,
        "Invalid operation! Delete partition is only supported for partitioned tables.");
    Column condition = functions.col("yearOfBirth").equalTo(partitionValue);
    deltaTable.delete(condition);
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

  private String generateTableName(String tableName) {
    return tableName + "_" + System.currentTimeMillis();
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
    if (sparkSession != null) {
      sparkSession.close();
    }
  }
}
