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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;

import org.apache.spark.sql.delta.actions.AddFile;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.onetable.exception.OneIOException;
import io.onetable.model.schema.OneField;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.Range;

@AllArgsConstructor(staticName = "of")
public class AddFileStatsExtractor {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  // list of all fields in the schema
  @NonNull private final List<OneField> fields;

  public Map<OneField, ColumnStat> getColumnStatsForFile(AddFile addFile) {
    // TODO: Additional work needed to track maps & arrays.
    try {
      DeltaStats deltaStats = MAPPER.readValue(addFile.stats(), DeltaStats.class);
      Map<String, Object> fieldPathToMaxValue = flattenStatMap(deltaStats.getMaxValues());
      Map<String, Object> fieldPathToMinValue = flattenStatMap(deltaStats.getMinValues());
      Map<String, Object> fieldPathToNullCount = flattenStatMap(deltaStats.getNullCount());
      return fields.stream()
          .filter(field -> fieldPathToMaxValue.containsKey(field.getPath()))
          .collect(
              Collectors.toMap(
                  Function.identity(),
                  field -> {
                    String fieldPath = field.getPath();
                    Object minValue = fieldPathToMinValue.get(fieldPath);
                    Object maxValue = fieldPathToMaxValue.get(fieldPath);
                    Number nullCount = (Number) fieldPathToNullCount.get(fieldPath);
                    Range range = Range.vector(minValue, maxValue);
                    return ColumnStat.builder()
                        .numValues(deltaStats.getNumRecords())
                        .numNulls(nullCount.longValue())
                        .range(range)
                        .build();
                  }));
    } catch (IOException ex) {
      throw new OneIOException("Unable to parse stats json", ex);
    }
  }

  /**
   * Takes the input map which represents a json object and flattens it.
   *
   * @param statMap input json map
   * @return map with keys representing the dot-path for the field
   */
  private Map<String, Object> flattenStatMap(Map<String, Object> statMap) {
    Map<String, Object> result = new HashMap<>();
    Queue<StatField> statFieldQueue = new ArrayDeque<>();
    statFieldQueue.add(StatField.of("", statMap));
    while (!statFieldQueue.isEmpty()) {
      StatField statField = statFieldQueue.poll();
      String prefix = statField.getParentPath().isEmpty() ? "" : statField.getParentPath() + ".";
      statField
          .getValues()
          .forEach(
              (fieldName, value) -> {
                String fullName = prefix + fieldName;
                if (value instanceof Map) {
                  statFieldQueue.add(StatField.of(fullName, (Map<String, Object>) value));
                } else {
                  result.put(fullName, value);
                }
              });
    }
    return result;
  }

  @Value
  @AllArgsConstructor(staticName = "of")
  private static class StatField {
    String parentPath;
    Map<String, Object> values;
  }
}
