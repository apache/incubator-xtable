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

import static org.apache.xtable.testutil.ColumnStatMapUtil.getColumnStats;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

import org.apache.spark.sql.delta.actions.AddFile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.testutil.ColumnStatMapUtil;

public class TestDeltaStatsExtractor {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testDeltaStats() throws JsonProcessingException {
    InternalSchema schema = ColumnStatMapUtil.getSchema();

    List<ColumnStat> columnStats = getColumnStats();

    String actualStats =
        DeltaStatsExtractor.getInstance().convertStatsToDeltaFormat(schema, 50L, columnStats);
    Map<String, Object> actualStatsMap = MAPPER.readValue(actualStats, HashMap.class);
    assertEquals(50, actualStatsMap.get("numRecords"));

    Map<String, Object> minValueStatsMap =
        (HashMap<String, Object>) actualStatsMap.get("minValues");
    assertEquals(10, minValueStatsMap.get("long_field"));
    assertEquals("a", minValueStatsMap.get("string_field"));
    assertEquals(null, minValueStatsMap.get("null_string_field"));
    assertEquals("2022-10-08 21:08:17", minValueStatsMap.get("timestamp_field"));
    assertEquals("2022-10-08 21:08:17", minValueStatsMap.get("timestamp_micros_field"));
    assertEquals(1.23, minValueStatsMap.get("float_field"));
    assertEquals(1.23, minValueStatsMap.get("double_field"));
    assertEquals(1.0, minValueStatsMap.get("decimal_field"));
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
    assertEquals(6.54321, maxValueStatsMap.get("float_field"));
    assertEquals(6.54321, maxValueStatsMap.get("double_field"));
    assertEquals(2.0, maxValueStatsMap.get("decimal_field"));
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
    assertEquals(2, nullValueStatsMap.get("float_field"));
    assertEquals(3, nullValueStatsMap.get("double_field"));
    assertEquals(1, nullValueStatsMap.get("decimal_field"));
    Map<String, Object> nestedMapInNullCountMap =
        (HashMap<String, Object>) nullValueStatsMap.get("nested_struct_field");
    assertEquals(4, nestedMapInNullCountMap.get("nested_long_field"));
  }

  @Test
  void roundTripStatsConversion() throws IOException {
    InternalSchema schema = ColumnStatMapUtil.getSchema();
    List<InternalField> fields = schema.getAllFields();
    List<ColumnStat> columnStats = getColumnStats();

    String stats =
        DeltaStatsExtractor.getInstance().convertStatsToDeltaFormat(schema, 50L, columnStats);
    AddFile addFile = new AddFile("file://path/to/file", null, 0, 0, true, stats, null, null);
    DeltaStatsExtractor extractor = DeltaStatsExtractor.getInstance();
    List<ColumnStat> actual = extractor.getColumnStatsForFile(addFile, fields);

    Set<ColumnStat> expected = new HashSet<>();
    columnStats.forEach(
        stat -> {
          InternalType dataType = stat.getField().getSchema().getDataType();
          if (dataType != InternalType.RECORD
              && dataType != InternalType.LIST
              && dataType != InternalType.MAP) {
            ColumnStat columnStatWithoutSize = stat.toBuilder().totalSize(0).build();
            expected.add(columnStatWithoutSize);
          }
        });
    assertEquals(expected, new HashSet<>(actual));
  }

  @Test
  void convertStatsToInternalRepresentation() throws IOException {
    List<InternalField> fields = getSchemaFields();
    Map<String, Object> minValues = generateMap("a", 1, 1.0, 10);
    Map<String, Object> maxValues = generateMap("b", 2, 2.0, 20);
    Map<String, Object> nullValues = generateMap(1L, 2L, 3L, 4L);
    Map<String, Object> deltaStats = new HashMap<>();
    deltaStats.put("minValues", minValues);
    deltaStats.put("maxValues", maxValues);
    deltaStats.put("nullCount", nullValues);
    deltaStats.put("numRecords", 100);
    String stats = MAPPER.writeValueAsString(deltaStats);
    AddFile addFile = new AddFile("file://path/to/file", null, 0, 0, true, stats, null, null);
    DeltaStatsExtractor extractor = DeltaStatsExtractor.getInstance();
    List<ColumnStat> actual = extractor.getColumnStatsForFile(addFile, fields);

    List<ColumnStat> expected =
        Arrays.asList(
            ColumnStat.builder()
                .field(fields.get(0))
                .numValues(100)
                .numNulls(1)
                .range(Range.vector("a", "b"))
                .build(),
            ColumnStat.builder()
                .field(fields.get(2))
                .numValues(100)
                .numNulls(2)
                .range(Range.vector(1, 2))
                .build(),
            ColumnStat.builder()
                .field(fields.get(4))
                .numValues(100)
                .numNulls(3)
                .range(Range.vector(1.0, 2.0))
                .build(),
            ColumnStat.builder()
                .field(fields.get(5))
                .numValues(100)
                .numNulls(4)
                .range(Range.vector(10, 20))
                .build());
    assertEquals(expected, actual);
  }

  private List<InternalField> getSchemaFields() {
    return Arrays.asList(
        InternalField.builder()
            .name("top_level_string")
            .schema(InternalSchema.builder().dataType(InternalType.STRING).build())
            .build(),
        InternalField.builder()
            .name("nested")
            .schema(InternalSchema.builder().dataType(InternalType.RECORD).build())
            .build(),
        InternalField.builder()
            .name("int_field")
            .parentPath("nested")
            .schema(InternalSchema.builder().dataType(InternalType.INT).build())
            .build(),
        InternalField.builder()
            .name("double_nesting")
            .parentPath("nested")
            .schema(InternalSchema.builder().dataType(InternalType.RECORD).build())
            .build(),
        InternalField.builder()
            .name("double_field")
            .parentPath("nested.double_nesting")
            .schema(InternalSchema.builder().dataType(InternalType.DOUBLE).build())
            .build(),
        InternalField.builder()
            .name("top_level_int")
            .schema(InternalSchema.builder().dataType(InternalType.INT).build())
            .build());
  }

  private Map<String, Object> generateMap(
      Object topLevelStringValue,
      Object nestedIntValue,
      Object doubleNestedValue,
      Object topLevelIntValue) {
    Map<String, Object> doubleNestedValues = new HashMap<>();
    doubleNestedValues.put("double_field", doubleNestedValue);
    Map<String, Object> nestedValues = new HashMap<>();
    nestedValues.put("int_field", nestedIntValue);
    nestedValues.put("double_nesting", doubleNestedValues);
    Map<String, Object> values = new HashMap<>();
    values.put("top_level_string", topLevelStringValue);
    values.put("nested", nestedValues);
    values.put("top_level_int", topLevelIntValue);
    return values;
  }
}
