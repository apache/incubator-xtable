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
 
package io.onetable.avro;

import static io.onetable.GenericTable.LEVEL_VALUES;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import lombok.Builder;
import lombok.Value;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang3.RandomStringUtils;

@Builder
@Value
public class TestAvroDataHelper {
  private static final Random RANDOM = new Random();

  Schema tableSchema;
  String recordKeyField;
  List<String> partitionFieldNames;

  public List<GenericRecord> generateInsertRecords(int numRecords) {
    Instant currentTime = Instant.now().truncatedTo(ChronoUnit.DAYS);
    List<Instant> startTimeWindows =
        Arrays.asList(
            currentTime.minus(2, ChronoUnit.DAYS),
            currentTime.minus(3, ChronoUnit.DAYS),
            currentTime.minus(4, ChronoUnit.DAYS));
    List<Instant> endTimeWindows =
        Arrays.asList(
            currentTime.minus(1, ChronoUnit.DAYS),
            currentTime.minus(2, ChronoUnit.DAYS),
            currentTime.minus(3, ChronoUnit.DAYS));
    return IntStream.range(0, numRecords)
        .mapToObj(
            index ->
                generateInsertRecord(
                    startTimeWindows.get(index % 3), endTimeWindows.get(index % 3), tableSchema))
        .collect(Collectors.toList());
  }

  public List<GenericRecord> generateUpsertRecords(List<GenericRecord> inputRecords) {
    List<GenericRecord> upsertRecords = new ArrayList<>();
    for (GenericRecord existingRecord : inputRecords) {
      GenericRecord upsertedRecord = generateUpsertRecord(existingRecord);
      upsertRecords.add(upsertedRecord);
    }
    return upsertRecords;
  }

  private GenericRecord generateUpsertRecord(GenericRecord existingRecord) {
    // TODO(vamshigv): deduplicate this code later.
    Instant currentTime = Instant.now().truncatedTo(ChronoUnit.DAYS);
    List<Instant> startTimeWindows =
        Arrays.asList(
            currentTime.minus(2, ChronoUnit.DAYS),
            currentTime.minus(3, ChronoUnit.DAYS),
            currentTime.minus(4, ChronoUnit.DAYS));
    List<Instant> endTimeWindows =
        Arrays.asList(
            currentTime.minus(1, ChronoUnit.DAYS),
            currentTime.minus(2, ChronoUnit.DAYS),
            currentTime.minus(3, ChronoUnit.DAYS));
    GenericRecord record = new GenericData.Record(tableSchema);
    for (Schema.Field field : tableSchema.getFields()) {
      String fieldName = field.name();
      Object value;

      if (fieldName.equals(recordKeyField)) {
        value = existingRecord.get(fieldName);
      } else if (partitionFieldNames != null && partitionFieldNames.contains(fieldName)) {
        value = existingRecord.get(fieldName);
      } else {
        value =
            generateValueForField(
                field, null, startTimeWindows.get(0), endTimeWindows.get(0), null);
      }
      record.put(fieldName, value);
    }
    return record;
  }

  private GenericRecord generateInsertRecord(
      Instant startTimeWindow, Instant endTimeWindow, Schema schema) {
    return generateInsertRecordForPartition(startTimeWindow, endTimeWindow, schema, null);
  }

  private GenericRecord generateInsertRecordForPartition(
      Instant startTimeWindow, Instant endTimeWindow, Schema schema, Object partitionValue) {
    GenericRecord record = new GenericData.Record(schema);
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.name();
      String keyValue = UUID.randomUUID().toString();
      Object value =
          generateValueForField(field, keyValue, startTimeWindow, endTimeWindow, partitionValue);
      record.put(fieldName, value);
    }
    return record;
  }

  private Object generateValueForField(
      Schema.Field field,
      String keyValue,
      Instant timeLowerBound,
      Instant timeUpperBound,
      Object partitionValue) {
    // TODO(vamshigv): use early return here later.
    Object value;
    String fieldName = field.name();
    Schema fieldSchema = getFieldSchema(field);
    if (partitionValue != null && partitionFieldNames.contains(fieldName)) {
      value = partitionValue;
    } else if (fieldName.equals(recordKeyField)) {
      // set key to the provided value
      value = keyValue;
    } else if (fieldName.equals("ts")) {
      // always set ts to current time for update ordering
      value = System.currentTimeMillis();
    } else if (fieldName.equals("level")) {
      // a simple string field to be used for basic partitioning if required
      value = LEVEL_VALUES.get(RANDOM.nextInt(LEVEL_VALUES.size()));
    } else if (fieldName.equals("severity")) {
      // a bounded integer field to be used for partition testing
      value = RANDOM.nextBoolean() ? null : RANDOM.nextInt(3);
    } else if (fieldName.startsWith("time")) {
      // limit time fields to particular windows for the sake of testing time based partitions
      long timeWindow = timeUpperBound.toEpochMilli() - timeLowerBound.toEpochMilli();
      LogicalType logicalType = fieldSchema.getLogicalType();
      if (logicalType instanceof LogicalTypes.TimestampMillis
          || logicalType instanceof LogicalTypes.LocalTimestampMillis) {
        value = timeLowerBound.plusMillis(RANDOM.nextInt((int) timeWindow)).toEpochMilli();
      } else if (logicalType instanceof LogicalTypes.TimestampMicros
          || logicalType instanceof LogicalTypes.LocalTimestampMicros) {
        value = timeLowerBound.plusMillis(RANDOM.nextInt((int) timeWindow)).toEpochMilli() * 1000;
      } else {
        throw new IllegalArgumentException(
            "Unhandled timestamp type: " + fieldSchema.getLogicalType());
      }
    } else if (fieldName.startsWith("date")) {
      value = (int) timeLowerBound.atZone(ZoneId.of("UTC")).toLocalDate().toEpochDay();
    } else if (fieldSchema.isNullable() && RANDOM.nextBoolean()) {
      // set the value to null to help generate interesting col stats and test null handling
      value = null;
    } else {
      Schema.Type fieldType = fieldSchema.getType();
      switch (fieldType) {
        case FLOAT:
          value = RANDOM.nextFloat();
          break;
        case DOUBLE:
          value = RANDOM.nextDouble();
          break;
        case LONG:
          value = RANDOM.nextLong();
          break;
        case INT:
          value = RANDOM.nextInt();
          break;
        case BOOLEAN:
          value = RANDOM.nextBoolean();
          break;
        case STRING:
          value = RandomStringUtils.randomAlphabetic(10);
          break;
        case BYTES:
          value =
              ByteBuffer.wrap(
                  RandomStringUtils.randomAlphabetic(10).getBytes(StandardCharsets.UTF_8));
          break;
        case FIXED:
          if (fieldSchema.getLogicalType() != null
              && fieldSchema.getLogicalType() instanceof LogicalTypes.Decimal) {
            value = BigDecimal.valueOf(RANDOM.nextInt(1000000), 2);
          } else {
            value =
                new GenericData.Fixed(
                    fieldSchema,
                    RandomStringUtils.randomAlphabetic(10).getBytes(StandardCharsets.UTF_8));
          }
          break;
        case ENUM:
          value =
              new GenericData.EnumSymbol(
                  fieldSchema,
                  fieldSchema
                      .getEnumSymbols()
                      .get(RANDOM.nextInt(fieldSchema.getEnumSymbols().size())));
          break;
        case RECORD:
          value =
              generateInsertRecordForPartition(
                  timeLowerBound, timeUpperBound, fieldSchema, partitionValue);
          break;
        case ARRAY:
          value =
              IntStream.range(0, RANDOM.nextInt(2) + 1)
                  .mapToObj(
                      unused ->
                          generateInsertRecordForPartition(
                              timeLowerBound,
                              timeUpperBound,
                              fieldSchema.getElementType(),
                              partitionValue))
                  .collect(Collectors.toList());
          break;
        case MAP:
          value =
              IntStream.range(0, RANDOM.nextInt(2) + 1)
                  .mapToObj(
                      unused ->
                          generateInsertRecordForPartition(
                              timeLowerBound,
                              timeUpperBound,
                              fieldSchema.getValueType(),
                              partitionValue))
                  .collect(
                      Collectors.toMap(
                          unused -> RandomStringUtils.randomAlphabetic(5), Function.identity()));
          break;
        default:
          throw new UnsupportedOperationException(
              "Field type not properly handle in data generation: " + fieldType);
      }
    }
    return value;
  }

  private Schema getFieldSchema(Schema.Field field) {
    return field.schema().getType() == Schema.Type.UNION
        ? field.schema().getTypes().get(1)
        : field.schema();
  }
}
