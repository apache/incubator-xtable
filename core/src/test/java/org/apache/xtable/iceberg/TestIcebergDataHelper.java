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
 
package org.apache.xtable.iceberg;

import static org.apache.xtable.GenericTable.LEVEL_VALUES;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.Value;

import org.apache.commons.lang3.RandomStringUtils;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.NestedField;

@Builder
@Value
public class TestIcebergDataHelper {
  private static final Random RANDOM = new Random();
  private static final List<Types.NestedField> COMMON_FIELDS =
      Arrays.asList(
          NestedField.optional(1, "id", Types.StringType.get()),
          NestedField.optional(2, "ts", Types.LongType.get()),
          NestedField.optional(3, "level", Types.StringType.get()),
          NestedField.optional(4, "severity", Types.IntegerType.get()),
          NestedField.optional(5, "double_field", Types.DoubleType.get()),
          NestedField.optional(6, "float_field", Types.FloatType.get()),
          NestedField.optional(7, "int_field", Types.IntegerType.get()),
          NestedField.optional(8, "long_field", Types.LongType.get()),
          NestedField.optional(9, "boolean_field", Types.BooleanType.get()),
          NestedField.optional(10, "string_field", Types.StringType.get()),
          NestedField.optional(11, "bytes_field", Types.BinaryType.get()),
          NestedField.optional(12, "decimal_field", Types.DecimalType.of(20, 2)),
          NestedField.optional(
              13,
              "nested_record",
              Types.StructType.of(
                  NestedField.optional(14, "nested_int", Types.IntegerType.get()),
                  NestedField.optional(15, "level", Types.StringType.get()))),
          NestedField.optional(
              16,
              "nullable_map_field",
              Types.MapType.ofOptional(
                  17,
                  18,
                  Types.StringType.get(),
                  Types.StructType.of(
                      NestedField.optional(19, "nested_int", Types.IntegerType.get()),
                      NestedField.optional(20, "level", Types.StringType.get())))),
          NestedField.optional(
              21,
              "array_field",
              Types.ListType.ofOptional(
                  22,
                  Types.StructType.of(
                      NestedField.optional(23, "nested_int", Types.IntegerType.get()),
                      NestedField.optional(24, "level", Types.StringType.get())))),
          NestedField.optional(25, "enum_field", Types.StringType.get()),
          NestedField.optional(26, "date_nullable_field", Types.DateType.get()),
          NestedField.optional(
              28, "timestamp_micros_nullable_field", Types.TimestampType.withZone()),
          NestedField.optional(
              30, "timestamp_local_micros_nullable_field", Types.TimestampType.withoutZone()));
  private static final List<Types.NestedField> ADDITIONAL_FIELDS =
      Arrays.asList(
          NestedField.optional(31, "additional_column1", Types.StringType.get()),
          NestedField.optional(32, "additional_column2", Types.LongType.get()));
  private static final Schema BASE_SCHEMA = new Schema(COMMON_FIELDS);
  private static final Schema SCHEMA_WITH_ADDITIONAL_COLUMNS =
      new Schema(
          Stream.concat(COMMON_FIELDS.stream(), ADDITIONAL_FIELDS.stream())
              .collect(Collectors.toList()));

  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final LocalDate EPOCH_DAY = EPOCH.toLocalDate();

  Schema tableSchema;
  String recordKeyField;
  List<String> partitionFieldNames;

  public static TestIcebergDataHelper createIcebergDataHelper(
      String recordKeyField, List<String> partitionFields, boolean includeAdditionalColumns) {
    Schema tableSchema = getSchema(includeAdditionalColumns);
    return TestIcebergDataHelper.builder()
        .tableSchema(tableSchema)
        .recordKeyField(recordKeyField)
        .partitionFieldNames(partitionFields)
        .build();
  }

  private static Schema getSchema(boolean includeAdditionalColumns) {
    return includeAdditionalColumns ? SCHEMA_WITH_ADDITIONAL_COLUMNS : BASE_SCHEMA;
  }

  public List<Record> generateInsertRecords(int numRecords) {
    List<Instant> startTimeWindows = getStartTimeWindows();
    List<Instant> endTimeWindows = getEndTimeWindows();
    return IntStream.range(0, numRecords)
        .mapToObj(
            index ->
                generateInsertRecord(
                    startTimeWindows.get(index % 3),
                    endTimeWindows.get(index % 3),
                    tableSchema.asStruct()))
        .collect(Collectors.toList());
  }

  public List<Record> generateInsertRecordForPartition(int numRecords, Object partitionValue) {
    List<Instant> startTimeWindows = getStartTimeWindows();
    List<Instant> endTimeWindows = getEndTimeWindows();
    return IntStream.range(0, numRecords)
        .mapToObj(
            index ->
                generateInsertRecordForPartition(
                    startTimeWindows.get(index % 3),
                    endTimeWindows.get(index % 3),
                    tableSchema.asStruct(),
                    partitionValue))
        .collect(Collectors.toList());
  }

  public List<Record> generateUpsertRecords(List<Record> inputRecords) {
    List<Instant> startTimeWindows = getStartTimeWindows();
    List<Instant> endTimeWindows = getEndTimeWindows();
    return IntStream.range(0, inputRecords.size())
        .mapToObj(
            index ->
                generateUpsertRecord(
                    inputRecords.get(index),
                    startTimeWindows.get(index % 3),
                    endTimeWindows.get(index % 3)))
        .collect(Collectors.toList());
  }

  public PartitionSpec getPartitionSpec() {
    if (partitionFieldNames.isEmpty()) {
      return PartitionSpec.unpartitioned();
    }
    if (partitionFieldNames.size() > 1) {
      throw new IllegalArgumentException(
          "Please modify the code to support multiple partition columns");
    }
    if (!partitionFieldNames.get(0).equals("level")) {
      throw new IllegalArgumentException(
          "Please modify the test to support partitioning on " + partitionFieldNames.get(0));
    }
    return PartitionSpec.builderFor(tableSchema).identity("level").build();
  }

  private Record generateUpsertRecord(
      Record existingRecord, Instant startTimeWindow, Instant endTimeWindow) {
    Record record = GenericRecord.create(tableSchema);
    for (Types.NestedField field : tableSchema.columns()) {
      String fieldName = field.name();
      Object value;

      if (fieldName.equals(recordKeyField)
          || (partitionFieldNames != null && partitionFieldNames.contains(fieldName))) {
        value = existingRecord.getField(fieldName);
      } else {
        // key and partition values aren't expected to be updated.
        value = generateValueForField(field, null, startTimeWindow, endTimeWindow, null);
      }

      record.setField(fieldName, value);
    }
    return record;
  }

  private Record generateInsertRecord(
      Instant startTimeWindow, Instant endTimeWindow, Types.StructType structType) {
    return generateInsertRecordForPartition(startTimeWindow, endTimeWindow, structType, null);
  }

  private Record generateInsertRecordForPartition(
      Instant startTimeWindow,
      Instant endTimeWindow,
      Types.StructType structType,
      Object partitionValue) {
    Record record = GenericRecord.create(structType);
    for (Types.NestedField field : structType.fields()) {
      String fieldName = field.name();
      String keyValue = UUID.randomUUID().toString();
      Object value =
          generateValueForField(field, keyValue, startTimeWindow, endTimeWindow, partitionValue);
      record.setField(fieldName, value);
    }
    return record;
  }

  private Object generateValueForField(
      Types.NestedField field,
      String keyValue,
      Instant timeLowerBound,
      Instant timeUpperBound,
      Object partitionValue) {
    String fieldName = field.name();
    Type fieldType = field.type();
    if (partitionValue != null && partitionFieldNames.contains(fieldName)) {
      return partitionValue;
    } else if (fieldName.equals(recordKeyField)) {
      return keyValue;
    } else if (fieldName.equals("ts")) {
      return System.currentTimeMillis();
    } else if (fieldName.equals("level")) {
      return LEVEL_VALUES.get(RANDOM.nextInt(LEVEL_VALUES.size()));
    } else if (fieldName.equals("severity")) {
      return RANDOM.nextBoolean() ? null : RANDOM.nextInt(3);
    } else {
      return generateRandomValueForType(fieldType, timeLowerBound, timeUpperBound);
    }
  }

  private Object generateRandomValueForType(
      Type fieldType, Instant timeLowerBound, Instant timeUpperBound) {
    switch (fieldType.typeId()) {
      case FLOAT:
        return RANDOM.nextFloat();
      case DOUBLE:
        return RANDOM.nextDouble();
      case LONG:
        return RANDOM.nextLong();
      case INTEGER:
        return RANDOM.nextInt();
      case BOOLEAN:
        return RANDOM.nextBoolean();
      case STRING:
        return RandomStringUtils.randomAlphabetic(10);
      case BINARY:
        byte[] bytes = new byte[10];
        RANDOM.nextBytes(bytes);
        return ByteBuffer.wrap(bytes);
      case FIXED:
        int size = ((Types.FixedType) fieldType).length();
        byte[] fixedBytes = new byte[size];
        RANDOM.nextBytes(fixedBytes);
        return ByteBuffer.wrap(fixedBytes);
      case DATE:
        long randomDay = timeLowerBound.until(timeUpperBound, ChronoUnit.DAYS);
        return LocalDate.ofEpochDay(randomDay);
      case TIME:
        long totalMicrosInDay = ChronoUnit.DAYS.getDuration().toMillis() * 1000;
        return (long) (RANDOM.nextDouble() * (totalMicrosInDay));
      case DECIMAL:
        Types.DecimalType decimalType = (Types.DecimalType) fieldType;
        return new BigDecimal(RANDOM.nextDouble() * Math.pow(10, decimalType.scale()))
            .setScale(decimalType.scale(), RoundingMode.HALF_UP);
      case TIMESTAMP:
        Types.TimestampType timestampType = (Types.TimestampType) fieldType;

        long lowerBoundMillis = timeLowerBound.toEpochMilli();
        long upperBoundMillis = timeUpperBound.toEpochMilli();
        long randomMillisInRange =
            lowerBoundMillis + (long) (RANDOM.nextDouble() * (upperBoundMillis - lowerBoundMillis));
        if (timestampType.shouldAdjustToUTC()) {
          return EPOCH.plus(randomMillisInRange, ChronoUnit.MILLIS);
        } else {
          return EPOCH.plus(randomMillisInRange, ChronoUnit.MILLIS).toLocalDateTime();
        }
      case STRUCT:
        return generateInsertRecord(timeLowerBound, timeUpperBound, fieldType.asStructType());
      case UUID:
        return UUID.randomUUID().toString();
      case LIST:
        Types.ListType listType = (Types.ListType) fieldType;
        int listSize = RANDOM.nextInt(5) + 1;
        List<Object> resultList = new ArrayList<>(listSize);
        IntStream.range(0, listSize)
            .forEach(
                index ->
                    resultList.add(
                        generateRandomValueForType(
                            listType.elementType(), timeLowerBound, timeUpperBound)));
        return resultList;
      case MAP:
        Types.MapType mapType = (Types.MapType) fieldType;
        int mapSize = RANDOM.nextInt(5) + 1;
        Map<Object, Object> resultMap = new HashMap<>();
        for (int i = 0; i < mapSize; i++) {
          Object key =
              generateRandomValueForType(mapType.keyType(), timeLowerBound, timeUpperBound);
          Object value =
              generateRandomValueForType(mapType.valueType(), timeLowerBound, timeUpperBound);
          resultMap.put(key, value);
        }
        return resultMap;
      default:
        throw new UnsupportedOperationException("Unhandled field type: " + fieldType.typeId());
    }
  }

  private List<Instant> getStartTimeWindows() {
    Instant currentTime = Instant.now().truncatedTo(ChronoUnit.DAYS);
    return Arrays.asList(
        currentTime.minus(2, ChronoUnit.DAYS),
        currentTime.minus(3, ChronoUnit.DAYS),
        currentTime.minus(4, ChronoUnit.DAYS));
  }

  private List<Instant> getEndTimeWindows() {
    Instant currentTime = Instant.now().truncatedTo(ChronoUnit.DAYS);
    return Arrays.asList(
        currentTime.minus(1, ChronoUnit.DAYS),
        currentTime.minus(2, ChronoUnit.DAYS),
        currentTime.minus(3, ChronoUnit.DAYS));
  }
}
