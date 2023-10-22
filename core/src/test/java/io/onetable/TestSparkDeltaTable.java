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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import lombok.Value;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.sql.delta.DeltaLog;

import io.delta.tables.DeltaTable;

@Value
public class TestSparkDeltaTable {

  private static final StructType PERSON_SCHEMA =
      new StructType(
          new StructField[] {
            new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField("firstName", DataTypes.StringType, true, Metadata.empty()),
            new StructField("lastName", DataTypes.StringType, true, Metadata.empty()),
            new StructField("gender", DataTypes.StringType, true, Metadata.empty()),
            new StructField("birthDate", DataTypes.TimestampType, true, Metadata.empty()),
            new StructField("yearOfBirth", DataTypes.IntegerType, true, Metadata.empty())
          });
  // Until Delta 2.4 even generated columns should be provided values.
  private static final String SQL_SELECT_TEMPLATE =
      "SELECT %d AS id, "
          + "'%s' AS firstName, "
          + "'%s' AS lastName, "
          + "'%s' AS gender, "
          + "timestamp('%s') AS birthDate, "
          + "year(timestamp('%s')) AS yearOfBirth";
  private static final Random RANDOM = new Random();
  private static final String[] GENDERS = {"Male", "Female"};
  private static final SimpleDateFormat TIMESTAMP_FORMAT =
      new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  private final String tableName;
  private final String basePath;
  private final SparkSession sparkSession;
  private final DeltaLog deltaLog;
  private final DeltaTable deltaTable;

  public TestSparkDeltaTable(String name, Path tempDir, SparkSession sparkSession) {
    try {
      this.tableName = generateTableName(name);
      this.basePath = initBasePath(tempDir, tableName);
      this.sparkSession = sparkSession;
      createTable();
      this.deltaLog = DeltaLog.forTable(sparkSession, basePath);
      this.deltaTable = DeltaTable.forPath(sparkSession, basePath);
    } catch (IOException ex) {
      throw new UncheckedIOException("Unable initialize Delta spark table", ex);
    }
  }

  private void createTable() {
    sparkSession.sql(
        "CREATE TABLE `"
            + tableName
            + "` ("
            + "    id INT, "
            + "    firstName STRING, "
            + "    lastName STRING, "
            + "    gender STRING, "
            + "    birthDate TIMESTAMP, "
            + "    yearOfBirth INT "
            + ") USING DELTA "
            + "PARTITIONED BY (yearOfBirth) "
            + "LOCATION '"
            + basePath
            + "'");
  }

  public List<Row> insertRows(int numRows) throws ParseException {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < numRows; i++) {
      rows.add(generateRandomRow());
    }
    List<String> selectsForInsert =
        rows.stream().map(this::generateSelectForRow).collect(Collectors.toList());
    String insertStatement =
        String.format(
            "INSERT INTO `%s` %s", tableName, String.join(" UNION ALL ", selectsForInsert));
    sparkSession.sql(insertStatement);
    return rows;
  }

  public void upsertRows(List<Row> upsertRows) throws ParseException {
    List<Row> upserts = transformForUpsertsOrDeletes(upsertRows, true);
    Dataset<Row> upsertDataset = sparkSession.createDataFrame(upserts, PERSON_SCHEMA);
    deltaTable
        .alias("person")
        .merge(upsertDataset.alias("source"), "person.id = source.id")
        .whenMatched()
        .updateAll()
        .execute();
  }

  public void deleteRows(List<Row> deleteRows) throws ParseException {
    List<Row> deletes = transformForUpsertsOrDeletes(deleteRows, false);
    Dataset<Row> deleteDataset = sparkSession.createDataFrame(deletes, PERSON_SCHEMA);
    deltaTable
        .alias("person")
        .merge(deleteDataset.alias("source"), "person.id = source.id")
        .whenMatched()
        .delete()
        .execute();
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

  private List<Row> transformForUpsertsOrDeletes(List<Row> rows, boolean isUpsert)
      throws ParseException {
    // Generate random values for few columns for upserts.
    // For deletes, retain the same values as the original row.
    List<Row> upserts = new ArrayList<>();
    for (Row row : rows) {
      java.util.Date parsedDate = TIMESTAMP_FORMAT.parse(row.getString(4));
      Timestamp timestamp = new java.sql.Timestamp(parsedDate.getTime());
      Row upsert =
          RowFactory.create(
              row.getInt(0),
              isUpsert ? generateRandomName() : row.getString(1),
              isUpsert ? generateRandomName() : row.getString(2),
              row.getString(3),
              timestamp,
              timestamp.toLocalDateTime().getYear());
      upserts.add(upsert);
    }
    return upserts;
  }

  private String generateSelectForRow(Row row) {
    return String.format(
        SQL_SELECT_TEMPLATE,
        row.getInt(0),
        row.getString(1),
        row.getString(2),
        row.getString(3),
        row.getString(4),
        row.getString(4));
  }

  private Row generateRandomRow() throws ParseException {
    int id = RANDOM.nextInt(1000000) + 1;
    String firstName = generateRandomName();
    String lastName = generateRandomName();
    String gender = GENDERS[RANDOM.nextInt(GENDERS.length)];

    long offset = TIMESTAMP_FORMAT.parse("2013-01-01 00:00:00").getTime();
    long end = TIMESTAMP_FORMAT.parse("2023-01-01 00:00:00").getTime();
    long diff = end - offset + 1;
    Date randomDate = new Date(offset + (long) (RANDOM.nextDouble() * diff));
    String birthDate = TIMESTAMP_FORMAT.format(randomDate);

    return RowFactory.create(id, firstName, lastName, gender, birthDate);
  }

  private String generateRandomName() {
    StringBuilder name = new StringBuilder(5);
    for (int i = 0; i < 5; i++) {
      char randomChar = (char) (RANDOM.nextInt(26) + 'A');
      name.append(randomChar);
    }
    return name.toString();
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
}
