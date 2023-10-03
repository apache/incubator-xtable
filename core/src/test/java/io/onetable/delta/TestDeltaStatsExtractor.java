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

import static io.onetable.testutil.ColumnStatMapUtil.getColumnStatMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.jetbrains.annotations.NotNull;
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

    Map<OneField, ColumnStat> columnStatMap = getColumnStatMap();

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
