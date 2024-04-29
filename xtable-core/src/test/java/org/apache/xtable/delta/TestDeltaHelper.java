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
 
package org.apache.xtable.delta;

import static org.apache.spark.sql.types.DataTypes.BinaryType;
import static org.apache.spark.sql.types.DataTypes.BooleanType;
import static org.apache.spark.sql.types.DataTypes.DateType;
import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.FloatType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.TimestampType;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import io.delta.tables.DeltaTable;
import io.delta.tables.DeltaTableBuilder;

import org.apache.xtable.GenericTable;

@Builder
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TestDeltaHelper {
  private static final AtomicInteger ID_GENERATOR = new AtomicInteger(0);
  private static final StructType STRUCT_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {new StructField("nested_int", IntegerType, true, Metadata.empty())});

  private static final StructField[] COMMON_FIELDS =
      new StructField[] {
        new StructField("id", IntegerType, false, Metadata.empty()),
        new StructField("firstName", StringType, true, Metadata.empty()),
        new StructField("lastName", StringType, true, Metadata.empty()),
        new StructField("gender", StringType, true, Metadata.empty()),
        new StructField("birthDate", TimestampType, false, Metadata.empty()),
        new StructField("level", StringType, false, Metadata.empty()),
        new StructField("boolean_field", BooleanType, true, Metadata.empty()),
        new StructField("date_field", DateType, true, Metadata.empty()),
        new StructField("timestamp_field", TimestampType, true, Metadata.empty()),
        new StructField("double_field", DoubleType, true, Metadata.empty()),
        new StructField("float_field", FloatType, true, Metadata.empty()),
        new StructField("long_field", LongType, true, Metadata.empty()),
        new StructField("binary_field", BinaryType, true, Metadata.empty()),
        new StructField(
            "primitive_map",
            DataTypes.createMapType(StringType, IntegerType),
            true,
            Metadata.empty()),
        new StructField(
            "record_map",
            DataTypes.createMapType(StringType, STRUCT_SCHEMA),
            true,
            Metadata.empty()),
        new StructField(
            "primitive_list", DataTypes.createArrayType(IntegerType), true, Metadata.empty()),
        new StructField(
            "record_list", DataTypes.createArrayType(STRUCT_SCHEMA), true, Metadata.empty()),
        new StructField("record_field", STRUCT_SCHEMA, true, Metadata.empty()),
      };
  private static final StructField[] ADDITIONAL_FIELDS =
      new StructField[] {new StructField("street", StringType, true, Metadata.empty())};
  private static final StructField[] DATE_PARTITIONED_FIELDS =
      new StructField[] {new StructField("yearOfBirth", IntegerType, true, Metadata.empty())};

  private static final Random RANDOM = new Random();
  private static final String[] GENDERS = {"Male", "Female"};

  StructType tableStructSchema;
  String partitionField;
  boolean includeAdditionalColumns;

  public static TestDeltaHelper createTestDataHelper(
      String partitionField, boolean includeAdditionalColumns) {
    StructType tableSchema = generateDynamicSchema(partitionField, includeAdditionalColumns);
    return TestDeltaHelper.builder()
        .tableStructSchema(tableSchema)
        .partitionField(partitionField)
        .includeAdditionalColumns(includeAdditionalColumns)
        .build();
  }

  private static StructType generateDynamicSchema(
      String partitionField, boolean includeAdditionalColumns) {
    List<StructField> fields = new ArrayList<>(Arrays.asList(COMMON_FIELDS));
    if ("yearOfBirth".equals(partitionField)) {
      fields.addAll(Arrays.asList(DATE_PARTITIONED_FIELDS));
    }
    if (includeAdditionalColumns) {
      fields.addAll(Arrays.asList(ADDITIONAL_FIELDS));
    }
    return new StructType(fields.toArray(new StructField[0]));
  }

  public void createTable(SparkSession sparkSession, String tableName, String basePath) {
    DeltaTableBuilder tableBuilder =
        DeltaTable.createIfNotExists(sparkSession).tableName(tableName).location(basePath);
    Arrays.stream(COMMON_FIELDS).forEach(tableBuilder::addColumn);
    if ("yearOfBirth".equals(partitionField)) {
      tableBuilder
          .addColumn(
              DeltaTable.columnBuilder("yearOfBirth")
                  .dataType(IntegerType)
                  .generatedAlwaysAs("YEAR(birthDate)")
                  .build())
          .partitionedBy("yearOfBirth");
    } else if ("level".equals(partitionField)) {
      tableBuilder.partitionedBy(partitionField);
    } else if (partitionField != null) {
      throw new IllegalArgumentException("Unexpected partition field: " + partitionField);
    }
    if (includeAdditionalColumns) {
      tableBuilder.addColumn("street", StringType);
    }
    tableBuilder.execute();
  }

  public Row generateRandomRow() {
    int year = 2013 + RANDOM.nextInt(11);
    String levelValue =
        GenericTable.LEVEL_VALUES.get(RANDOM.nextInt(GenericTable.LEVEL_VALUES.size()));
    return generateRandomRowForGivenYearAndLevel(year, levelValue);
  }

  public Row generateRandomRowForGivenYearAndLevel(int year, String level) {
    return RowFactory.create(generateRandomValuesForGivenYearAndLevel(year, level));
  }

  /*
   * Generates a random row for the given schema and additional columns. Additional columns
   * are appended to the end. Random values are generated for additional columns.
   */
  private Object[] generateRandomValuesForGivenYearAndLevel(int yearValue, String levelValue) {
    Object[] row = new Object[tableStructSchema.size()];
    for (int i = 0; i < tableStructSchema.size(); i++) {
      row[i] = generateValueForField(tableStructSchema.fields()[i], yearValue, levelValue);
    }
    return row;
  }

  private Object generateValueForField(StructField field, int yearValue, String levelValue) {
    switch (field.name()) {
      case "id":
        return ID_GENERATOR.incrementAndGet();
      case "gender":
        return GENDERS[RANDOM.nextInt(GENDERS.length)];
      case "birthDate":
        return generateRandomTimeGivenYear(yearValue);
      case "yearOfBirth":
        return yearValue;
      case "level":
        return levelValue;
      default:
        return getRandomValueForField(field.dataType(), field.nullable());
    }
  }

  private static Object getRandomValueForField(DataType type, boolean isNullable) {
    if (isNullable && RANDOM.nextBoolean()) {
      return null;
    }
    switch (type.typeName()) {
      case "integer":
        return RANDOM.nextInt();
      case "string":
        return generateRandomString();
      case "boolean":
        return RANDOM.nextBoolean();
      case "float":
        return RANDOM.nextFloat();
      case "double":
        return RANDOM.nextDouble();
      case "binary":
        return generateRandomString().getBytes();
      case "long":
        return RANDOM.nextLong();
      case "date":
        return new Date(System.currentTimeMillis());
      case "timestamp":
        return Timestamp.from(Instant.now());
      case "array":
        ArrayType arrayType = (ArrayType) type;
        return IntStream.range(0, RANDOM.nextInt(5))
            .mapToObj(i -> getRandomValueForField(arrayType.elementType(), false))
            .collect(Collectors.toList());
      case "map":
        MapType mapType = (MapType) type;
        return IntStream.range(0, RANDOM.nextInt(5))
            .mapToObj(i -> getRandomValueForField(mapType.valueType(), false))
            .collect(Collectors.toMap(unused -> generateRandomString(), Function.identity()));
      case "struct":
        StructType structType = (StructType) type;
        return RowFactory.create(
            Arrays.stream(structType.fields())
                .map(field -> getRandomValueForField(field.dataType(), false))
                .toArray());
      default:
        throw new IllegalArgumentException("Please add code to handle field type: " + type);
    }
  }

  private int generateRandomInRange(int minVal, int maxVal) {
    if (minVal > maxVal) {
      throw new IllegalArgumentException(
          "Max value must be greater than min value for random number generation.");
    }
    return RANDOM.nextInt((maxVal - minVal) + 1) + minVal;
  }

  private Timestamp generateRandomTimeGivenYear(int yearValue) {
    int month = RANDOM.nextInt(12) + 1;
    // Adjust days to avoid timezone issues with Delta generated columns check constraint for tests.
    int daysToConsider = YearMonth.of(yearValue, month).lengthOfMonth() - 5;
    // Generate a random day between 5th and last day of the month avoiding wrap around issues.
    int day = generateRandomInRange(5, daysToConsider);

    LocalDateTime localDateTime =
        LocalDateTime.of(
            yearValue, month, day, RANDOM.nextInt(24), RANDOM.nextInt(60), RANDOM.nextInt(60));
    ZonedDateTime zonedDateTimeInUTC = localDateTime.atZone(ZoneId.of("UTC"));
    return Timestamp.from(zonedDateTimeInUTC.toInstant());
  }

  public static String generateRandomString() {
    return RandomStringUtils.randomAlphanumeric(5);
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
              return RowFactory.create(newRowData);
            })
        .collect(Collectors.toList());
  }

  public List<Row> generateRows(int numRows) {
    return IntStream.range(0, numRows)
        .mapToObj(i -> generateRandomRow())
        .collect(Collectors.toList());
  }

  public List<Row> generateRowsForSpecificPartition(int numRows, int partitionValue, String level) {
    return IntStream.range(0, numRows)
        .mapToObj(i -> generateRandomRowForGivenYearAndLevel(partitionValue, level))
        .collect(Collectors.toList());
  }
}
