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

import java.sql.Timestamp;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

@Builder
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TestDeltaHelper {
  private static final StructField[] COMMON_FIELDS =
      new StructField[] {
        new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("firstName", DataTypes.StringType, true, Metadata.empty()),
        new StructField("lastName", DataTypes.StringType, true, Metadata.empty()),
        new StructField("gender", DataTypes.StringType, true, Metadata.empty()),
        new StructField("birthDate", DataTypes.TimestampType, true, Metadata.empty())
      };
  private static final StructType PERSON_SCHEMA_PARTITIONED =
      new StructType(
          appendFields(
              COMMON_FIELDS,
              new StructField("yearOfBirth", DataTypes.IntegerType, true, Metadata.empty())));
  private static final StructType PERSON_SCHEMA_NON_PARTITIONED = new StructType(COMMON_FIELDS);

  // Until Delta 2.4 even generated columns should be provided values.
  private static final String COMMON_SQL_SELECT_FIELDS =
      "SELECT %d AS id, "
          + "'%s' AS firstName, "
          + "'%s' AS lastName, "
          + "'%s' AS gender, "
          + "timestamp('%s') AS birthDate";
  private static final String SQL_SELECT_TEMPLATE_PARTITIONED =
      COMMON_SQL_SELECT_FIELDS + ", year(timestamp('%s')) AS yearOfBirth";
  private static final String SQL_SELECT_TEMPLATE_NON_PARTITIONED = COMMON_SQL_SELECT_FIELDS;

  private static final String SQL_CREATE_TABLE_TEMPLATE_PARTITIONED =
      "CREATE TABLE `%s` ("
          + "    id INT, "
          + "    firstName STRING, "
          + "    lastName STRING, "
          + "    gender STRING, "
          + "    birthDate TIMESTAMP, "
          + "    yearOfBirth INT GENERATED ALWAYS AS (YEAR(birthDate))"
          + ") USING DELTA "
          + "PARTITIONED BY (yearOfBirth) "
          + "LOCATION '%s'";
  private static final String SQL_CREATE_TABLE_TEMPLATE_NON_PARTITIONED =
      "CREATE TABLE `%s` ("
          + "    id INT, "
          + "    firstName STRING, "
          + "    lastName STRING, "
          + "    gender STRING, "
          + "    birthDate TIMESTAMP "
          + ") USING DELTA "
          + "LOCATION '%s'";

  private static final String SQL_INSERT_TEMPLATE_PARTITIONED =
      "INSERT INTO `%s` (id, firstName, lastName, gender, birthDate, yearOfBirth) %s";
  private static final String SQL_INSERT_TEMPLATE_NON_PARTITIONED =
      "INSERT INTO `%s` (id, firstName, lastName, gender, birthDate) %s";

  private static final Random RANDOM = new Random();
  private static final String[] GENDERS = {"Male", "Female"};

  public static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  private static StructField[] appendFields(
      StructField[] originalFields, StructField... newFields) {
    return Stream.concat(Stream.of(originalFields), Stream.of(newFields))
        .toArray(StructField[]::new);
  }

  private final StructType tableStructSchema;
  private final String selectTemplateForInserts;
  private final String createTableSqlStr;
  private final String insertIntoTemplateSqlStr;
  private final boolean tableIsPartitioned;

  public static TestDeltaHelper createTestDataHelper(boolean isPartitioned) {
    TestDeltaHelperBuilder builder = TestDeltaHelper.builder();
    if (isPartitioned) {
      builder
          .createTableSqlStr(SQL_CREATE_TABLE_TEMPLATE_PARTITIONED)
          .selectTemplateForInserts(SQL_SELECT_TEMPLATE_PARTITIONED)
          .tableStructSchema(PERSON_SCHEMA_PARTITIONED)
          .insertIntoTemplateSqlStr(SQL_INSERT_TEMPLATE_PARTITIONED);
    } else {
      builder
          .createTableSqlStr(SQL_CREATE_TABLE_TEMPLATE_NON_PARTITIONED)
          .selectTemplateForInserts(SQL_SELECT_TEMPLATE_NON_PARTITIONED)
          .tableStructSchema(PERSON_SCHEMA_NON_PARTITIONED)
          .insertIntoTemplateSqlStr(SQL_INSERT_TEMPLATE_NON_PARTITIONED);
    }
    return builder.tableIsPartitioned(isPartitioned).build();
  }

  public String generateSelectTemplateForInsertsWithAdditionalColumn() {
    return this.selectTemplateForInserts + ", '%s' AS street";
  }

  public Row generateRandomRow() {
    int year = 2013 + RANDOM.nextInt(11);
    return generateRandomRowForGivenYear(year);
  }

  public Row generateRandomRowWithAdditionalColumns() {
    int year = 2013 + RANDOM.nextInt(11);
    Object[] rowValues =
        generateRandomValuesForGivenYear(year, Collections.singletonList("street"));
    return RowFactory.create(rowValues);
  }

  public Row generateRandomRowForGivenYear(int year) {
    return RowFactory.create(generateRandomValuesForGivenYear(year, Collections.emptyList()));
  }

  /*
   * Generates a random row for the given schema and additional columns. Additional columns
   * are appended to the end. String values are generated for additional columns.
   */
  private Object[] generateRandomValuesForGivenYear(int yearValue, List<String> additionalColumns) {
    int id = RANDOM.nextInt(1000000) + 1;
    String firstName = generateRandomName();
    String lastName = generateRandomName();
    String gender = GENDERS[RANDOM.nextInt(GENDERS.length)];

    LocalDateTime localDateTime =
        LocalDateTime.of(yearValue, RANDOM.nextInt(12) + 1, 1, 0, 0)
            .plusDays(RANDOM.nextInt(365))
            .plusHours(RANDOM.nextInt(24))
            .plusMinutes(RANDOM.nextInt(60))
            .plusSeconds(RANDOM.nextInt(60));
    String birthDate = DATE_TIME_FORMATTER.format(localDateTime);

    Object[] row = new Object[5 + additionalColumns.size()];
    row[0] = id;
    row[1] = firstName;
    row[2] = lastName;
    row[3] = gender;
    row[4] = birthDate;
    IntStream.range(0, additionalColumns.size()).forEach(i -> row[5 + i] = generateRandomName());
    return row;
  }

  public static String generateRandomName() {
    StringBuilder name = new StringBuilder(5);
    for (int i = 0; i < 5; i++) {
      char randomChar = (char) (RANDOM.nextInt(26) + 'A');
      name.append(randomChar);
    }
    return name.toString();
  }

  public String generateSqlForDataInsert(String tableName, List<Row> rows) {
    List<String> selectsForInsert =
        rows.stream().map(this::generateSelectForRow).collect(Collectors.toList());
    String insertStatement =
        String.format(
            insertIntoTemplateSqlStr, tableName, String.join(" UNION ALL ", selectsForInsert));
    return insertStatement;
  }

  public String generateSelectForRow(Row row) {
    if (tableIsPartitioned) {
      return String.format(
          selectTemplateForInserts,
          row.getInt(0),
          row.getString(1),
          row.getString(2),
          row.getString(3),
          row.getString(4),
          row.getString(4));
    } else {
      return String.format(
          selectTemplateForInserts,
          row.getInt(0),
          row.getString(1),
          row.getString(2),
          row.getString(3),
          row.getString(4));
    }
  }

  public List<Row> transformForUpsertsOrDeletes(List<Row> rows, boolean isUpsert)
      throws ParseException {
    // Generate random values for few columns for upserts.
    // For deletes, retain the same values as the original row.
    List<Row> upserts = new ArrayList<>();
    for (Row row : rows) {
      LocalDateTime parsedDateTime = LocalDateTime.parse(row.getString(4), DATE_TIME_FORMATTER);
      Timestamp timestamp = Timestamp.valueOf(parsedDateTime);
      Row upsert;
      if (tableIsPartitioned) {
        upsert =
            RowFactory.create(
                row.getInt(0),
                isUpsert ? generateRandomName() : row.getString(1),
                isUpsert ? generateRandomName() : row.getString(2),
                row.getString(3),
                timestamp,
                timestamp.toLocalDateTime().getYear());
      } else {
        upsert =
            RowFactory.create(
                row.getInt(0),
                isUpsert ? generateRandomName() : row.getString(1),
                isUpsert ? generateRandomName() : row.getString(2),
                row.getString(3),
                timestamp);
      }
      upserts.add(upsert);
    }
    return upserts;
  }

  private String generateSelectForRowWithAdditionalColumn(Row row) {
    return String.format(
        generateSelectTemplateForInsertsWithAdditionalColumn(),
        row.getInt(0),
        row.getString(1),
        row.getString(2),
        row.getString(3),
        row.getString(4),
        row.getString(4),
        row.getString(5));
  }

  public String generateInsertSqlForAdditionalColumn(String tableName, List<Row> rows) {
    List<String> selectsForInsert =
        rows.stream()
            .map(this::generateSelectForRowWithAdditionalColumn)
            .collect(Collectors.toList());
    String insertStatement =
        String.format(
            "INSERT INTO `%s` %s", tableName, String.join(" UNION ALL ", selectsForInsert));
    return insertStatement;
  }

  public List<Row> generateRows(int numRows) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < numRows; i++) {
      rows.add(generateRandomRow());
    }
    return rows;
  }

  public List<Row> generateRowsForSpecificPartition(int numRows, int partitionValue) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < numRows; i++) {
      rows.add(generateRandomRowForGivenYear(partitionValue));
    }
    return rows;
  }

  public List<Row> generateRowsWithAdditionalColumn(int numRows) {
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < numRows; i++) {
      rows.add(generateRandomRowWithAdditionalColumns());
    }
    return rows;
  }
}
