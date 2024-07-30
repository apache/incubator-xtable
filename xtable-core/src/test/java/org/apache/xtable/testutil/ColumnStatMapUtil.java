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
 
package org.apache.xtable.testutil;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ColumnStatMapUtil {
  private static final InternalField LONG_FIELD =
      InternalField.builder()
          .name("long_field")
          .schema(InternalSchema.builder().name("long").dataType(InternalType.LONG).build())
          .build();

  private static final InternalField STRING_FIELD =
      InternalField.builder()
          .name("string_field")
          .schema(InternalSchema.builder().name("string").dataType(InternalType.STRING).build())
          .build();
  private static final InternalField NULL_STRING_FIELD =
      InternalField.builder()
          .name("null_string_field")
          .schema(InternalSchema.builder().name("string").dataType(InternalType.STRING).build())
          .build();
  private static final InternalField TIMESTAMP_FIELD =
      InternalField.builder()
          .name("timestamp_field")
          .schema(
              InternalSchema.builder()
                  .name("long")
                  .dataType(InternalType.TIMESTAMP)
                  .metadata(
                      Collections.singletonMap(
                          InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                          InternalSchema.MetadataValue.MILLIS))
                  .build())
          .build();
  private static final InternalField TIMESTAMP_MICROS_FIELD =
      InternalField.builder()
          .name("timestamp_micros_field")
          .schema(
              InternalSchema.builder()
                  .name("long")
                  .dataType(InternalType.TIMESTAMP)
                  .metadata(
                      Collections.singletonMap(
                          InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                          InternalSchema.MetadataValue.MICROS))
                  .build())
          .build();
  private static final InternalField LOCAL_TIMESTAMP_FIELD =
      InternalField.builder()
          .name("local_timestamp_field")
          .schema(
              InternalSchema.builder()
                  .name("long")
                  .dataType(InternalType.TIMESTAMP_NTZ)
                  .metadata(
                      Collections.singletonMap(
                          InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                          InternalSchema.MetadataValue.MILLIS))
                  .build())
          .build();
  private static final InternalField DATE_FIELD =
      InternalField.builder()
          .name("date_field")
          .schema(InternalSchema.builder().name("int").dataType(InternalType.DATE).build())
          .build();

  private static final InternalField ARRAY_LONG_FIELD_ELEMENT =
      InternalField.builder()
          .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
          .parentPath("array_long_field")
          .schema(InternalSchema.builder().name("long").dataType(InternalType.LONG).build())
          .build();
  private static final InternalField ARRAY_LONG_FIELD =
      InternalField.builder()
          .name("array_long_field")
          .schema(
              InternalSchema.builder()
                  .name("array")
                  .dataType(InternalType.LIST)
                  .fields(Collections.singletonList(ARRAY_LONG_FIELD_ELEMENT))
                  .build())
          .build();

  private static final InternalField MAP_KEY_STRING_FIELD =
      InternalField.builder()
          .name(InternalField.Constants.MAP_KEY_FIELD_NAME)
          .parentPath("map_string_long_field")
          .schema(InternalSchema.builder().name("map_key").dataType(InternalType.STRING).build())
          .build();
  private static final InternalField MAP_VALUE_LONG_FIELD =
      InternalField.builder()
          .name(InternalField.Constants.MAP_VALUE_FIELD_NAME)
          .parentPath("map_string_long_field")
          .schema(InternalSchema.builder().name("long").dataType(InternalType.LONG).build())
          .build();
  private static final InternalField MAP_STRING_LONG_FIELD =
      InternalField.builder()
          .name("map_string_long_field")
          .schema(
              InternalSchema.builder()
                  .name("map")
                  .dataType(InternalType.MAP)
                  .fields(Arrays.asList(MAP_KEY_STRING_FIELD, MAP_VALUE_LONG_FIELD))
                  .build())
          .build();

  private static final InternalField NESTED_ARRAY_STRING_FIELD_ELEMENT =
      InternalField.builder()
          .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
          .parentPath("nested_struct_field.array_string_field")
          .schema(InternalSchema.builder().name("string").dataType(InternalType.STRING).build())
          .build();
  private static final InternalField NESTED_ARRAY_STRING_FIELD =
      InternalField.builder()
          .name("array_string_field")
          .parentPath("nested_struct_field")
          .schema(
              InternalSchema.builder()
                  .name("array")
                  .dataType(InternalType.LIST)
                  .fields(Collections.singletonList(NESTED_ARRAY_STRING_FIELD_ELEMENT))
                  .build())
          .build();

  private static final InternalField NESTED_LONG_FIELD =
      InternalField.builder()
          .name("nested_long_field")
          .parentPath("nested_struct_field")
          .schema(InternalSchema.builder().name("long").dataType(InternalType.LONG).build())
          .build();

  private static final InternalField NESTED_STRUCT_FIELD =
      InternalField.builder()
          .name("nested_struct_field")
          .schema(
              InternalSchema.builder()
                  .name("nested_struct_field")
                  .dataType(InternalType.RECORD)
                  .fields(Arrays.asList(NESTED_ARRAY_STRING_FIELD, NESTED_LONG_FIELD))
                  .build())
          .build();

  private static final InternalField DECIMAL_FIELD =
      InternalField.builder()
          .name("decimal_field")
          .schema(InternalSchema.builder().name("decimal").dataType(InternalType.DECIMAL).build())
          .build();

  private static final InternalField FLOAT_FIELD =
      InternalField.builder()
          .name("float_field")
          .schema(InternalSchema.builder().name("float").dataType(InternalType.FLOAT).build())
          .build();

  private static final InternalField DOUBLE_FIELD =
      InternalField.builder()
          .name("double_field")
          .schema(InternalSchema.builder().name("double").dataType(InternalType.DOUBLE).build())
          .build();

  public static InternalSchema getSchema() {
    return InternalSchema.builder()
        .name("record")
        .dataType(InternalType.RECORD)
        .fields(
            Arrays.asList(
                LONG_FIELD,
                STRING_FIELD,
                NULL_STRING_FIELD,
                TIMESTAMP_FIELD,
                TIMESTAMP_MICROS_FIELD,
                LOCAL_TIMESTAMP_FIELD,
                DATE_FIELD,
                ARRAY_LONG_FIELD,
                MAP_STRING_LONG_FIELD,
                NESTED_STRUCT_FIELD,
                DECIMAL_FIELD,
                FLOAT_FIELD,
                DOUBLE_FIELD))
        .build();
  }

  public static List<ColumnStat> getColumnStats() {
    ColumnStat longColumnStats =
        ColumnStat.builder()
            .field(LONG_FIELD)
            .numNulls(4)
            .range(Range.vector(10L, 20L))
            .numValues(50)
            .totalSize(123)
            .build();
    ColumnStat stringColumnStats =
        ColumnStat.builder()
            .field(STRING_FIELD)
            .numNulls(1)
            .range(Range.vector("a", "c"))
            .numValues(50)
            .totalSize(500)
            .build();
    ColumnStat nullStringColumnStats =
        ColumnStat.builder()
            .field(NULL_STRING_FIELD)
            .numNulls(3)
            .range(Range.vector(null, null))
            .numValues(50)
            .totalSize(0)
            .build();
    ColumnStat timeStampColumnStats =
        ColumnStat.builder()
            .field(TIMESTAMP_FIELD)
            .numNulls(105)
            .range(Range.vector(1665263297000L, 1665436097000L))
            .numValues(50)
            .totalSize(999)
            .build();
    ColumnStat timeStampMicrosColumnStats =
        ColumnStat.builder()
            .field(TIMESTAMP_MICROS_FIELD)
            .numNulls(1)
            .range(Range.vector(1665263297000000L, 1665436097000000L))
            .numValues(50)
            .totalSize(400)
            .build();
    ColumnStat localTimeStampColumnStats =
        ColumnStat.builder()
            .field(LOCAL_TIMESTAMP_FIELD)
            .numNulls(1)
            .range(Range.vector(1665263297000L, 1665436097000L))
            .numValues(50)
            .totalSize(400)
            .build();
    ColumnStat dateColumnStats =
        ColumnStat.builder()
            .field(DATE_FIELD)
            .numNulls(250)
            .range(Range.vector(18181, 18547))
            .numValues(50)
            .totalSize(12345)
            .build();
    ColumnStat arrayLongElementColumnStats =
        ColumnStat.builder()
            .field(ARRAY_LONG_FIELD)
            .numNulls(2)
            .range(Range.vector(50L, 100L))
            .numValues(50)
            .totalSize(1234)
            .build();
    ColumnStat mapKeyStringColumnStats =
        ColumnStat.builder()
            .field(MAP_KEY_STRING_FIELD)
            .numNulls(3)
            .range(Range.vector("key1", "key2"))
            .numValues(50)
            .totalSize(1234)
            .build();
    ColumnStat mapValueLongColumnStats =
        ColumnStat.builder()
            .field(MAP_VALUE_LONG_FIELD)
            .numNulls(3)
            .range(Range.vector(200L, 300L))
            .numValues(50)
            .totalSize(1234)
            .build();
    ColumnStat nestedArrayStringElementColumnStats =
        ColumnStat.builder()
            .field(NESTED_ARRAY_STRING_FIELD_ELEMENT)
            .numNulls(7)
            .range(Range.vector("nested1", "nested2"))
            .numValues(50)
            .totalSize(1234)
            .build();
    ColumnStat nestedLongColumnStats =
        ColumnStat.builder()
            .field(NESTED_LONG_FIELD)
            .numNulls(4)
            .range(Range.vector(500L, 600L))
            .numValues(50)
            .totalSize(1234)
            .build();
    ColumnStat decimalColumnStats =
        ColumnStat.builder()
            .field(DECIMAL_FIELD)
            .numNulls(1)
            .range(Range.vector(new BigDecimal("1.0"), new BigDecimal("2.0")))
            .numValues(50)
            .totalSize(123)
            .build();
    ColumnStat floatColumnStats =
        ColumnStat.builder()
            .field(FLOAT_FIELD)
            .numNulls(2)
            .range(Range.vector(1.23f, 6.54321f))
            .numValues(50)
            .totalSize(123)
            .build();
    ColumnStat doubleColumnStats =
        ColumnStat.builder()
            .field(DOUBLE_FIELD)
            .numNulls(3)
            .range(Range.vector(1.23, 6.54321))
            .numValues(50)
            .totalSize(123)
            .build();

    ColumnStat ignoredColumnStatsArrayLongField =
        ColumnStat.builder()
            .field(ARRAY_LONG_FIELD)
            .numNulls(0)
            .range(Range.scalar("IGNORED"))
            .build();
    ColumnStat ignoredColumnStatsMapStringField =
        ColumnStat.builder()
            .field(MAP_STRING_LONG_FIELD)
            .numNulls(0)
            .range(Range.scalar("IGNORED"))
            .build();
    ColumnStat ignoredColumnStatsNestedStructField =
        ColumnStat.builder()
            .field(NESTED_STRUCT_FIELD)
            .numNulls(0)
            .range(Range.scalar("IGNORED"))
            .build();
    ColumnStat ignoredColumnStatsNestedArrayStringField =
        ColumnStat.builder()
            .field(NESTED_ARRAY_STRING_FIELD)
            .numNulls(0)
            .range(Range.scalar("IGNORED"))
            .build();

    return Arrays.asList(
        longColumnStats,
        stringColumnStats,
        nullStringColumnStats,
        timeStampColumnStats,
        timeStampMicrosColumnStats,
        localTimeStampColumnStats,
        dateColumnStats,
        arrayLongElementColumnStats,
        mapKeyStringColumnStats,
        mapValueLongColumnStats,
        nestedArrayStringElementColumnStats,
        nestedLongColumnStats,
        decimalColumnStats,
        floatColumnStats,
        doubleColumnStats,
        ignoredColumnStatsArrayLongField,
        ignoredColumnStatsMapStringField,
        ignoredColumnStatsNestedStructField,
        ignoredColumnStatsNestedArrayStringField);
  }
}
