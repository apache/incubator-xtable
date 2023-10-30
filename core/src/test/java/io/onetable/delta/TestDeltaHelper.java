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
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;

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

@Builder
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class TestDeltaHelper {
  private static final StructType STRUCT_SCHEMA =
      DataTypes.createStructType(
          new StructField[] {new StructField("nested_int", IntegerType, true, Metadata.empty())});

  private static final StructField[] COMMON_FIELDS =
      new StructField[] {
        new StructField("id", IntegerType, false, Metadata.empty()),
        new StructField("firstName", StringType, true, Metadata.empty()),
        new StructField("lastName", StringType, true, Metadata.empty()),
        new StructField("gender", StringType, true, Metadata.empty()),
        new StructField("birthDate", TimestampType, true, Metadata.empty()),
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
        DeltaTable.createIfNotExists(sparkSession).tableName(tableName).location(basePath);
    Arrays.stream(COMMON_FIELDS).forEach(tableBuilder::addColumn);
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
      case "gender":
        return GENDERS[RANDOM.nextInt(GENDERS.length)];
      case "birthDate":
        return generateRandomTimeGivenYear(yearValue);
      case "yearOfBirth":
        return yearValue;
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

  private Timestamp generateRandomTimeGivenYear(int yearValue) {
    int month = RANDOM.nextInt(11) + 1;
    int daysInMonth = YearMonth.of(yearValue, month).lengthOfMonth();
    int day = RANDOM.nextInt(daysInMonth) + 1;

    LocalDateTime localDateTime =
        LocalDateTime.of(
            yearValue, month, day, RANDOM.nextInt(24), RANDOM.nextInt(60), RANDOM.nextInt(60));
    ZonedDateTime zonedDateTimeInUTC = localDateTime.atZone(ZoneId.of("UTC"));
    return Timestamp.from(zonedDateTimeInUTC.toInstant());
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
