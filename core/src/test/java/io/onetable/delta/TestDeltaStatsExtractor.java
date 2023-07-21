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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.Range;

public class TestDeltaStatsExtractor {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testDeltaStats() throws JsonProcessingException {
    StructType structSchema = getStructSchema();

    OneField longField =
        OneField.builder()
            .name("long_field")
            .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
            .build();
    OneField stringField =
        OneField.builder()
            .name("string_field")
            .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
            .build();
    OneField nullStringField =
        OneField.builder()
            .name("null_string_field")
            .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
            .build();
    OneField timestampField =
        OneField.builder()
            .name("timestamp_field")
            .schema(
                OneSchema.builder()
                    .name("long")
                    .dataType(OneType.TIMESTAMP)
                    .metadata(
                        Collections.singletonMap(
                            OneSchema.MetadataKey.TIMESTAMP_PRECISION,
                            OneSchema.MetadataValue.MILLIS))
                    .build())
            .build();
    OneField timestampMicrosField =
        OneField.builder()
            .name("timestamp_micros_field")
            .schema(
                OneSchema.builder()
                    .name("long")
                    .dataType(OneType.TIMESTAMP)
                    .metadata(
                        Collections.singletonMap(
                            OneSchema.MetadataKey.TIMESTAMP_PRECISION,
                            OneSchema.MetadataValue.MICROS))
                    .build())
            .build();
    OneField localTimestampField =
        OneField.builder()
            .name("local_timestamp_field")
            .schema(
                OneSchema.builder()
                    .name("long")
                    .dataType(OneType.TIMESTAMP_NTZ)
                    .metadata(
                        Collections.singletonMap(
                            OneSchema.MetadataKey.TIMESTAMP_PRECISION,
                            OneSchema.MetadataValue.MILLIS))
                    .build())
            .build();
    OneField dateField =
        OneField.builder()
            .name("date_field")
            .schema(OneSchema.builder().name("int").dataType(OneType.DATE).build())
            .build();

    OneField arrayLongFieldElement =
        OneField.builder()
            .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
            .parentPath("array_long_field")
            .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
            .build();
    OneField arrayLongField =
        OneField.builder()
            .name("array_long_field")
            .schema(
                OneSchema.builder()
                    .name("array")
                    .dataType(OneType.ARRAY)
                    .fields(Collections.singletonList(arrayLongFieldElement))
                    .build())
            .build();

    OneField mapKeyStringField =
        OneField.builder()
            .name(OneField.Constants.MAP_KEY_FIELD_NAME)
            .parentPath("map_string_long_field")
            .schema(OneSchema.builder().name("map_key").dataType(OneType.STRING).build())
            .build();
    OneField mapValueLongField =
        OneField.builder()
            .name(OneField.Constants.MAP_VALUE_FIELD_NAME)
            .parentPath("map_string_long_field")
            .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
            .build();
    OneField mapStringLongField =
        OneField.builder()
            .name("map_string_long_field")
            .schema(
                OneSchema.builder()
                    .name("map")
                    .dataType(OneType.MAP)
                    .fields(Arrays.asList(mapKeyStringField, mapValueLongField))
                    .build())
            .build();

    OneField nestedArrayStringFieldElement =
        OneField.builder()
            .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
            .parentPath("nested_struct_field.array_string_field")
            .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
            .build();
    OneField nestedArrayStringField =
        OneField.builder()
            .name("array_string_field")
            .parentPath("nested_struct_field")
            .schema(
                OneSchema.builder()
                    .name("array")
                    .dataType(OneType.ARRAY)
                    .fields(Collections.singletonList(nestedArrayStringFieldElement))
                    .build())
            .build();

    OneField nestedLongField =
        OneField.builder()
            .name("nested_long_field")
            .parentPath("nested_struct_field")
            .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
            .build();

    OneField nestedStructField =
        OneField.builder()
            .name("nested_struct_field")
            .schema(
                OneSchema.builder()
                    .name("nested_struct_field")
                    .dataType(OneType.RECORD)
                    .fields(Arrays.asList(nestedArrayStringField, nestedLongField))
                    .build())
            .build();

    ColumnStat longColumnStats =
        ColumnStat.builder().numNulls(4).range(Range.vector(10L, 20L)).build();
    ColumnStat stringColumnStats =
        ColumnStat.builder().numNulls(1).range(Range.vector("a", "c")).build();
    ColumnStat nullStringColumnStats =
        ColumnStat.builder().numNulls(3).range(Range.vector(null, null)).build();
    ColumnStat timeStampColumnStats =
        ColumnStat.builder()
            .numNulls(105)
            .range(Range.vector(1665263297000L, 1665436097000L))
            .build();
    ColumnStat timeStampMicrosColumnStats =
        ColumnStat.builder()
            .numNulls(1)
            .range(Range.vector(1665263297000000L, 1665436097000000L))
            .build();
    ColumnStat localTimeStampColumnStats =
        ColumnStat.builder()
            .numNulls(1)
            .range(Range.vector(1665263297000L, 1665436097000L))
            .build();
    ColumnStat dateColumnStats =
        ColumnStat.builder().numNulls(250).range(Range.vector(18181, 18547)).build();
    ColumnStat ignoredColumnStats =
        ColumnStat.builder().numNulls(0).range(Range.scalar("IGNORED")).build();
    ColumnStat arrayLongElementColumnStats =
        ColumnStat.builder().numNulls(2).range(Range.vector(50L, 100L)).build();
    ColumnStat mapKeyStringColumnStats =
        ColumnStat.builder().numNulls(3).range(Range.vector("key1", "key2")).build();
    ColumnStat mapValueLongColumnStats =
        ColumnStat.builder().numNulls(3).range(Range.vector(200L, 300L)).build();
    ColumnStat nestedArrayStringElementColumnStats =
        ColumnStat.builder().numNulls(7).range(Range.vector("nested1", "nested2")).build();
    ColumnStat nestedLongColumnStats =
        ColumnStat.builder().numNulls(4).range(Range.vector(500L, 600L)).build();

    Map<OneField, ColumnStat> columnStatMap = new HashMap<>();
    columnStatMap.put(longField, longColumnStats);
    columnStatMap.put(stringField, stringColumnStats);
    columnStatMap.put(nullStringField, nullStringColumnStats);
    columnStatMap.put(timestampField, timeStampColumnStats);
    columnStatMap.put(timestampMicrosField, timeStampMicrosColumnStats);
    columnStatMap.put(localTimestampField, localTimeStampColumnStats);
    columnStatMap.put(dateField, dateColumnStats);
    columnStatMap.put(arrayLongField, ignoredColumnStats);
    columnStatMap.put(arrayLongFieldElement, arrayLongElementColumnStats);
    columnStatMap.put(mapStringLongField, ignoredColumnStats);
    columnStatMap.put(mapKeyStringField, mapKeyStringColumnStats);
    columnStatMap.put(mapValueLongField, mapValueLongColumnStats);
    columnStatMap.put(nestedStructField, ignoredColumnStats);
    columnStatMap.put(nestedArrayStringField, ignoredColumnStats);
    columnStatMap.put(nestedArrayStringFieldElement, nestedArrayStringElementColumnStats);
    columnStatMap.put(nestedLongField, nestedLongColumnStats);

    String actualStats =
        DeltaStatsExtractor.getInstance()
            .convertStatsToDeltaFormat(structSchema, 50L, columnStatMap);
    Map<String, Object> actualStatsMap = MAPPER.readValue(actualStats, HashMap.class);
    assertEquals(50, actualStatsMap.get("numRecords"));

    Map<String, Object> minValueStatsMap =
        (HashMap<String, Object>) actualStatsMap.get("minValues");
    assertEquals(10, minValueStatsMap.get("long_field"));
    assertEquals("a", minValueStatsMap.get("string_field"));
    assertEquals(null, minValueStatsMap.get("null_string_field"));
    assertEquals("2022-10-08 21:08:17", minValueStatsMap.get("timestamp_field"));
    assertEquals("2022-10-08 21:08:17", minValueStatsMap.get("timestamp_micros_field"));
    // TOD0: Local timestamp depends on env where it is run, it is non determinstic and this has to
    // be computed dynamically.
    // assertEquals("2022-10-08 14:08:17", minValueStatsMap.get("local_timestamp_field"));
    assertEquals("2019-10-12", minValueStatsMap.get("date_field"));
    Map<String, Object> nestedMapInMinValueStatsMap =
        (HashMap<String, Object>) minValueStatsMap.get("nested_struct_field");
    assertEquals(500, nestedMapInMinValueStatsMap.get("nested_long_field"));

    Map<String, Object> maxValueStatsMap =
        (HashMap<String, Object>) actualStatsMap.get("maxValues");
    assertEquals(20, maxValueStatsMap.get("long_field"));
    assertEquals("c", maxValueStatsMap.get("string_field"));
    assertEquals(null, maxValueStatsMap.get("null_string_field"));
    assertEquals("2022-10-10 21:08:17", maxValueStatsMap.get("timestamp_field"));
    assertEquals("2022-10-10 21:08:17", maxValueStatsMap.get("timestamp_micros_field"));
    // TOD0: Local timestamp depends on env where it is run, it is non determinstic and this has to
    // be computed dynamically.
    // assertEquals("2022-10-10 14:08:17", maxValueStatsMap.get("local_timestamp_field"));
    assertEquals("2020-10-12", maxValueStatsMap.get("date_field"));
    Map<String, Object> nestedMapInMaxValueStatsMap =
        (HashMap<String, Object>) maxValueStatsMap.get("nested_struct_field");
    assertEquals(600, nestedMapInMaxValueStatsMap.get("nested_long_field"));

    Map<String, Object> nullValueStatsMap =
        (HashMap<String, Object>) actualStatsMap.get("nullCount");
    assertEquals(4, nullValueStatsMap.get("long_field"));
    assertEquals(1, nullValueStatsMap.get("string_field"));

    assertEquals(3, nullValueStatsMap.get("null_string_field"));
    assertEquals(105, nullValueStatsMap.get("timestamp_field"));
    assertEquals(1, nullValueStatsMap.get("timestamp_micros_field"));
    assertEquals(1, nullValueStatsMap.get("local_timestamp_field"));
    assertEquals(250, nullValueStatsMap.get("date_field"));
    Map<String, Object> nestedMapInNullCountMap =
        (HashMap<String, Object>) nullValueStatsMap.get("nested_struct_field");
    assertEquals(4, nestedMapInNullCountMap.get("nested_long_field"));
  }

  private StructType getStructSchema() {
    StructType nestedStructSchema = new StructType();
    nestedStructSchema =
        nestedStructSchema.add(
            new StructField(
                "array_string_field",
                DataTypes.createArrayType(DataTypes.StringType),
                false,
                Metadata.empty()));
    nestedStructSchema =
        nestedStructSchema.add(
            new StructField("nested_long_field", DataTypes.LongType, false, Metadata.empty()));

    return new StructType(
        new StructField[] {
          new StructField("long_field", DataTypes.LongType, false, Metadata.empty()),
          new StructField("string_field", DataTypes.StringType, false, Metadata.empty()),
          new StructField("null_string_field", DataTypes.StringType, true, Metadata.empty()),
          new StructField("timestamp_field", DataTypes.TimestampType, false, Metadata.empty()),
          new StructField(
              "timestamp_micros_field", DataTypes.TimestampType, false, Metadata.empty()),
          new StructField(
              "local_timestamp_field", DataTypes.TimestampType, false, Metadata.empty()),
          new StructField("date_field", DataTypes.DateType, false, Metadata.empty()),
          new StructField(
              "array_long_field",
              DataTypes.createArrayType(DataTypes.LongType, false),
              false,
              Metadata.empty()),
          new StructField(
              "map_string_long_field",
              DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType, false),
              false,
              Metadata.empty()),
          new StructField("nested_struct_field", nestedStructSchema, false, Metadata.empty())
        });
  }
}
