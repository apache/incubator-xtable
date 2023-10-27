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
        new StructField("gender", StringType, true, Metadata.empty()),
        new StructField("birthDate", TimestampType, true, Metadata.empty())
      };
  private static final StructField[] ADDITIONAL_FIELDS =
      new StructField[] {new StructField("street", StringType, true, Metadata.empty())};
  private static final StructField[] PARTITIONED_FIELDS =
      new StructField[] {new StructField("yearOfBirth", IntegerType, true, Metadata.empty())};

  private static final Random RANDOM = new Random();
  private static final String[] GENDERS = {"Male", "Female"};

  StructType tableStructSchema;
  boolean tableIsPartitioned;
  boolean includeAdditionalColumns;

  public static TestDeltaHelper createTestDataHelper(
      boolean isPartitioned, boolean includeAdditionalColumns) {
    StructType tableSchema = generateDynamicSchema(isPartitioned, includeAdditionalColumns);
    return TestDeltaHelper.builder()
        .tableStructSchema(tableSchema)
        .tableIsPartitioned(isPartitioned)
        .includeAdditionalColumns(includeAdditionalColumns)
        .build();
  }

  private static StructType generateDynamicSchema(
      boolean isPartitioned, boolean includeAdditionalColumns) {
    List<StructField> fields = new ArrayList<>(Arrays.asList(COMMON_FIELDS));
    if (isPartitioned) {
      fields.addAll(Arrays.asList(PARTITIONED_FIELDS));
    }
    if (includeAdditionalColumns) {
      fields.addAll(Arrays.asList(ADDITIONAL_FIELDS));
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
            .addColumn("gender", StringType)
            .addColumn("birthDate", TimestampType);
    if (tableIsPartitioned) {
      tableBuilder
          .addColumn(
              DeltaTable.columnBuilder("yearOfBirth")
                  .dataType(IntegerType)
                  .generatedAlwaysAs("YEAR(birthDate)")
                  .build())
          .partitionedBy("yearOfBirth");
    }
    if (includeAdditionalColumns) {
      tableBuilder.addColumn("street", StringType);
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

  private Timestamp generateRandomTimeGivenYear(int yearValue) {
    int month = RANDOM.nextInt(12) + 1;
    int daysInMonth = YearMonth.of(yearValue, month).lengthOfMonth();
    int day = RANDOM.nextInt(daysInMonth) + 1;

    LocalDateTime localDateTime =
        LocalDateTime.of(
            yearValue, month, day, RANDOM.nextInt(24), RANDOM.nextInt(60), RANDOM.nextInt(60));
    return Timestamp.valueOf(localDateTime);
  }

  public static String generateRandomString() {
    StringBuilder name = new StringBuilder(5);
    for (int i = 0; i < 5; i++) {
      char randomChar = (char) (RANDOM.nextInt(26) + 'A');
      name.append(randomChar);
    }
    return name.toString();
  }

  public List<Row> transformForUpsertsOrDeletes(List<Row> rows, boolean isUpsert) {
    // Generate random values for few columns for upserts.
    // For deletes, retain the same values as the original row.
    return rows.stream()
        .map(
            row -> {
              Object[] newRowData = new Object[row.size()];
              for (int i = 0; i < row.size(); i++) {
                if (i == 1 || i == 2) {
                  newRowData[i] = isUpsert ? generateRandomString() : row.get(i);
                } else {
                  newRowData[i] = row.get(i);
                }
              }
              if (tableIsPartitioned) {
                Timestamp timestampValue = row.getTimestamp(row.size() - 2);
                newRowData[row.size() - 2] = timestampValue;
                newRowData[row.size() - 1] = timestampValue.toLocalDateTime().getYear();
              } else {
                Timestamp timestampValue = row.getTimestamp(row.size() - 1);
                newRowData[row.size() - 1] = timestampValue;
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
}
