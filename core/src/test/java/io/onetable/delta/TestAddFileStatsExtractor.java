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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.spark.sql.delta.actions.AddFile;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.Range;

public class TestAddFileStatsExtractor {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final List<OneField> fields =
      Arrays.asList(
          OneField.builder()
              .name("top_level_string")
              .schema(OneSchema.builder().dataType(OneType.STRING).build())
              .build(),
          OneField.builder()
              .name("nested")
              .schema(OneSchema.builder().dataType(OneType.RECORD).build())
              .build(),
          OneField.builder()
              .name("int_field")
              .parentPath("nested")
              .schema(OneSchema.builder().dataType(OneType.INT).build())
              .build(),
          OneField.builder()
              .name("double_nesting")
              .parentPath("nested")
              .schema(OneSchema.builder().dataType(OneType.RECORD).build())
              .build(),
          OneField.builder()
              .name("double_field")
              .parentPath("nested.double_nesting")
              .schema(OneSchema.builder().dataType(OneType.DOUBLE).build())
              .build(),
          OneField.builder()
              .name("top_level_int")
              .schema(OneSchema.builder().dataType(OneType.INT).build())
              .build());

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

  @Test
  void convertStatsToInternalRepresentation() throws IOException {
    Map<String, Object> minValues = generateMap("a", 1, 1.0, 10);
    Map<String, Object> maxValues = generateMap("b", 2, 2.0, 20);
    Map<String, Object> nullValues = generateMap(1L, 2L, 3L, 4L);
    DeltaStats deltaStats =
        DeltaStats.builder()
            .minValues(minValues)
            .maxValues(maxValues)
            .nullCount(nullValues)
            .numRecords(100)
            .build();
    String stats = MAPPER.writeValueAsString(deltaStats);
    AddFile addFile = new AddFile("file://path/to/file", null, 0, 0, true, stats, null);

    AddFileStatsExtractor extractor = AddFileStatsExtractor.of(fields);
    Map<OneField, ColumnStat> actual = extractor.getColumnStatsForFile(addFile);

    Map<OneField, ColumnStat> expected =
        ImmutableMap.<OneField, ColumnStat>builder()
            .put(
                fields.get(0),
                ColumnStat.builder()
                    .numValues(100)
                    .numNulls(1)
                    .range(Range.vector("a", "b"))
                    .build())
            .put(
                fields.get(2),
                ColumnStat.builder().numValues(100).numNulls(2).range(Range.vector(1, 2)).build())
            .put(
                fields.get(4),
                ColumnStat.builder()
                    .numValues(100)
                    .numNulls(3)
                    .range(Range.vector(1.0, 2.0))
                    .build())
            .put(
                fields.get(5),
                ColumnStat.builder().numValues(100).numNulls(4).range(Range.vector(10, 20)).build())
            .build();
    assertEquals(expected, actual);
  }
}
