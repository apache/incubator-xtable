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

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import io.delta.tables.DeltaTable;
import io.delta.tables.DeltaTableBuilder;

@Builder
@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TestDeltaHelper {
  private static final StructField[] COMMON_FIELDS =
      new StructField[] {
        new StructField("id", IntegerType, false, Metadata.empty()),
        new StructField("firstName", StringType, true, Metadata.empty()),
        new StructField("lastName", StringType, true, Metadata.empty()),
        new StructField("gender", StringType, true, Metadata.empty())
      };
  private static final StructField[] COMMON_DATE_FIELDS =
      new StructField[] {new StructField("birthDate", TimestampType, true, Metadata.empty())};
  private static final StructField[] ADDITIONAL_FIELDS =
      new StructField[] {new StructField("street", StringType, true, Metadata.empty())};
  private static final StructField[] PARTITIONED_FIELDS =
      new StructField[] {new StructField("yearOfBirth", IntegerType, true, Metadata.empty())};

  private static final Random RANDOM = new Random();
  private static final String[] GENDERS = {"Male", "Female"};

  public static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  StructType tableStructSchema;
  String selectTemplateForInserts;
  String insertIntoTemplateSqlStr;
  boolean tableIsPartitioned;
  boolean includeAdditionalColumns;

  public static TestDeltaHelper createTestDataHelper(
      boolean isPartitioned, boolean includeAdditionalColumns) {
    StructType tableSchema = generateDynamicSchema(isPartitioned, includeAdditionalColumns);
    String selectTemplateForInserts = generateSqlSelectForInsert(tableSchema);
    String insertIntoTemplateSqlStr = generateSqlInsertTemplate(tableSchema);
    return TestDeltaHelper.builder()
        .tableStructSchema(tableSchema)
        .selectTemplateForInserts(selectTemplateForInserts)
        .insertIntoTemplateSqlStr(insertIntoTemplateSqlStr)
        .tableIsPartitioned(isPartitioned)
        .includeAdditionalColumns(includeAdditionalColumns)
        .build();
  }

  private static StructType generateDynamicSchema(
      boolean isPartitioned, boolean includeAdditionalColumns) {
    List<StructField> fields = new ArrayList<>(Arrays.asList(COMMON_FIELDS));
    if (includeAdditionalColumns) {
      fields.addAll(Arrays.asList(ADDITIONAL_FIELDS));
    }
    fields.addAll(Arrays.asList(COMMON_DATE_FIELDS));
    if (isPartitioned) {
      fields.addAll(Arrays.asList(PARTITIONED_FIELDS));
    }
    return new StructType(fields.toArray(new StructField[0]));
  }

  public void createTable(SparkSession sparkSession, String tableName, String basePath) {
    DeltaTableBuilder tableBuilder =
        DeltaTable.create(sparkSession)
            .tableName(tableName)
            .location(basePath)
            .addColumn("id", IntegerType)
            .addColumn("firstName", StringType)
            .addColumn("lastName", StringType)
            .addColumn("gender", StringType);
    if (includeAdditionalColumns) {
      tableBuilder.addColumn("street", StringType);
    }
    tableBuilder.addColumn("birthDate", TimestampType);
    if (tableIsPartitioned) {
      tableBuilder
          .addColumn(
              DeltaTable.columnBuilder("yearOfBirth")
                  .dataType(IntegerType)
                  .generatedAlwaysAs("YEAR(birthDate)")
                  .build())
          .partitionedBy("yearOfBirth");
    }
    tableBuilder.execute();
  }

  public Row generateRandomRow() {
    int year = 2013 + RANDOM.nextInt(11);
    return generateRandomRowForGivenYear(year);
  }

  public Row generateRandomRowForGivenYear(int year) {
    return RowFactory.create(generateRandomValuesForGivenYear(year));
  }

  /*
   * Generates a random row for the given schema and additional columns. Additional columns
   * are appended to the end. String values are generated for additional columns.
   */
  private Object[] generateRandomValuesForGivenYear(int yearValue) {
    Object[] row = new Object[tableStructSchema.size()];
    for (int i = 0; i < tableStructSchema.size(); i++) {
      row[i] = generateValueForField(tableStructSchema.fields()[i], yearValue);
    }
    return row;
  }

  private Object generateValueForField(StructField field, int yearValue) {
    switch (field.name()) {
      case "id":
        return RANDOM.nextInt(1000000) + 1;
      case "firstName":
      case "lastName":
      case "street":
        return generateRandomString();
      case "gender":
        return GENDERS[RANDOM.nextInt(GENDERS.length)];
      case "birthDate":
        return generateRandomTimeGivenYear(yearValue);
      case "yearOfBirth":
        return yearValue;
      default:
        throw new IllegalArgumentException("Please add code to handle field: " + field.name());
    }
  }

  private String generateRandomTimeGivenYear(int yearValue) {
    int month = RANDOM.nextInt(12) + 1;
    int daysInMonth = YearMonth.of(yearValue, month).lengthOfMonth();
    int day = RANDOM.nextInt(daysInMonth) + 1;

    LocalDateTime localDateTime =
        LocalDateTime.of(
            yearValue, month, day, RANDOM.nextInt(24), RANDOM.nextInt(60), RANDOM.nextInt(60));
    return DATE_TIME_FORMATTER.format(localDateTime);
  }

  public static String generateRandomString() {
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
    return String.format(
        insertIntoTemplateSqlStr, tableName, String.join(" UNION ALL ", selectsForInsert));
  }

  public String generateSelectForRow(Row row) {
    List<Object> values = new ArrayList<>();
    for (int i = 0; i < row.size(); i++) {
      values.add(row.get(i));
    }
    if (tableIsPartitioned) {
      values.add(values.get(values.size() - 1));
    }
    return String.format(selectTemplateForInserts, values.toArray());
  }

  public List<Row> transformForUpsertsOrDeletes(List<Row> rows, boolean isUpsert) {
    // Generate random values for few columns for upserts.
    // For deletes, retain the same values as the original row.
    return rows.stream()
        .map(
            row -> {
              Object[] newRowData = new Object[row.size()];
              int limit = tableIsPartitioned ? row.size() - 2 : row.size() - 1;
              for (int i = 0; i < limit; i++) {
                if (i == 1 || i == 2) {
                  newRowData[i] = isUpsert ? generateRandomString() : row.get(i);
                } else {
                  newRowData[i] = row.get(i);
                }
              }
              if (tableIsPartitioned) {
                LocalDateTime parsedDateTime =
                    LocalDateTime.parse(row.getString(row.size() - 2), DATE_TIME_FORMATTER);
                newRowData[row.size() - 2] = Timestamp.valueOf(parsedDateTime);
                newRowData[row.size() - 1] = parsedDateTime.getYear();
              } else {
                LocalDateTime parsedDateTime =
                    LocalDateTime.parse(row.getString(row.size() - 1), DATE_TIME_FORMATTER);
                newRowData[row.size() - 1] = Timestamp.valueOf(parsedDateTime);
              }
              return RowFactory.create(newRowData);
            })
        .collect(Collectors.toList());
  }

  public List<Row> generateRows(int numRows) {
    return IntStream.range(0, numRows)
        .mapToObj(i -> generateRandomRow())
        .collect(Collectors.toList());
  }

  public List<Row> generateRowsForSpecificPartition(int numRows, int partitionValue) {
    return IntStream.range(0, numRows)
        .mapToObj(i -> generateRandomRowForGivenYear(partitionValue))
        .collect(Collectors.toList());
  }

  private static String generateSqlInsertTemplate(StructType schema) {
    String fieldList =
        Arrays.stream(schema.fields()).map(StructField::name).collect(Collectors.joining(", "));
    return String.format("INSERT INTO `%%s` (%s) %%s", fieldList);
  }

  private static String generateSqlSelectForInsert(StructType schema) {
    StructField[] fields = schema.fields();
    StringBuilder sqlBuilder = new StringBuilder("SELECT ");
    for (int i = 0; i < fields.length; i++) {
      StructField field = fields[i];
      sqlBuilder.append(getFormattedField(field));
      if (i < fields.length - 1) {
        sqlBuilder.append(", ");
      }
    }
    return sqlBuilder.toString();
  }

  private static String getFormattedField(StructField field) {
    String fieldName = field.name();
    DataType fieldType = field.dataType();
    if (fieldName.equals("yearOfBirth")) {
      return "year(timestamp('%s')) AS yearOfBirth";
    } else if (fieldType == IntegerType) {
      return String.format("%%d AS %s", fieldName);
    } else if (fieldType == StringType) {
      return String.format("'%%s' AS %s", fieldName);
    } else if (fieldType == TimestampType) {
      return String.format("timestamp('%%s') AS %s", fieldName);
    }
    throw new IllegalArgumentException("Unsupported field type: " + fieldType);
  }
}
