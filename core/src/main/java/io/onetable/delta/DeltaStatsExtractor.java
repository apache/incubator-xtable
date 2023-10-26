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

import static io.onetable.delta.DeltaValueConverter.convertToDeltaColumnStatValue;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;

import org.apache.spark.sql.delta.actions.AddFile;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.onetable.exception.OneIOException;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.Range;

/**
 * DeltaStatsExtractor extracts column stats and also responsible for their serialization leveraging
 * {@link DeltaValueConverter}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaStatsExtractor {
  private static final Set<OneType> FIELD_TYPES_WITH_STATS_SUPPORT =
      new HashSet<>(
          Arrays.asList(
              OneType.BOOLEAN,
              OneType.DATE,
              OneType.DECIMAL,
              OneType.DOUBLE,
              OneType.INT,
              OneType.LONG,
              OneType.FLOAT,
              OneType.STRING,
              OneType.TIMESTAMP,
              OneType.TIMESTAMP_NTZ));

  private static final DeltaStatsExtractor INSTANCE = new DeltaStatsExtractor();

  private static final String PATH_DELIMITER = "\\.";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static DeltaStatsExtractor getInstance() {
    return INSTANCE;
  }

  public String convertStatsToDeltaFormat(
      OneSchema schema, long numRecords, Map<OneField, ColumnStat> columnStats)
      throws JsonProcessingException {
    DeltaStats.DeltaStatsBuilder deltaStatsBuilder = DeltaStats.builder();
    deltaStatsBuilder.numRecords(numRecords);
    if (columnStats == null) {
      return MAPPER.writeValueAsString(deltaStatsBuilder.build());
    }
    Map<String, ColumnStat> columnStatsMapKeyedByPath =
        getColumnStatKeyedByFullyQualifiedPath(columnStats);
    Map<String, OneField> pathFieldMap = getPathToFieldMap(columnStats);
    Set<String> validPaths = getPathsFromStructSchemaForMinAndMaxStats(schema);
    DeltaStats deltaStats =
        deltaStatsBuilder
            .minValues(getMinValues(pathFieldMap, columnStatsMapKeyedByPath, validPaths))
            .maxValues(getMaxValues(pathFieldMap, columnStatsMapKeyedByPath, validPaths))
            .nullCount(getNullCount(columnStatsMapKeyedByPath, validPaths))
            .build();
    return MAPPER.writeValueAsString(deltaStats);
  }

  private Set<String> getPathsFromStructSchemaForMinAndMaxStats(OneSchema schema) {
    return schema.getAllFields().stream()
        .filter(
            field -> {
              OneType type = field.getSchema().getDataType();
              return FIELD_TYPES_WITH_STATS_SUPPORT.contains(type);
            })
        .map(OneField::getPath)
        .collect(Collectors.toSet());
  }

  private String combinePath(String parentPath, String fieldName) {
    if (parentPath == null || parentPath.isEmpty()) {
      return fieldName;
    }
    return parentPath + "." + fieldName;
  }

  private Map<String, Object> getMinValues(
      Map<String, OneField> pathFieldMap,
      Map<String, ColumnStat> columnStatsMapKeyedByPath,
      Set<String> validPaths) {
    return getValues(
        pathFieldMap,
        columnStatsMapKeyedByPath,
        validPaths,
        columnStat -> columnStat.getRange().getMinValue());
  }

  private Map<String, Object> getMaxValues(
      Map<String, OneField> pathFieldMap,
      Map<String, ColumnStat> columnStatsMapKeyedByPath,
      Set<String> validPaths) {
    return getValues(
        pathFieldMap,
        columnStatsMapKeyedByPath,
        validPaths,
        columnStat -> columnStat.getRange().getMaxValue());
  }

  private Map<String, Object> getValues(
      Map<String, OneField> pathFieldMap,
      Map<String, ColumnStat> columnStatsMapKeyedByPath,
      Set<String> validPaths,
      Function<ColumnStat, Object> valueExtractor) {
    Map<String, Object> jsonObject = new HashMap<>();
    columnStatsMapKeyedByPath.forEach(
        (path, columnStats) -> {
          if (validPaths.contains(path)) {
            OneSchema fieldSchema = pathFieldMap.get(path).getSchema();
            String[] pathParts = path.split(PATH_DELIMITER);
            insertValueAtPath(
                jsonObject,
                pathParts,
                convertToDeltaColumnStatValue(valueExtractor.apply(columnStats), fieldSchema));
          }
        });
    return jsonObject;
  }

  private Map<String, Object> getNullCount(
      Map<String, ColumnStat> columnStatsMapKeyedByPath, Set<String> validPaths) {
    // TODO: Additional work needed to track nulls maps & arrays.
    Map<String, Object> jsonObject = new HashMap<>();
    for (Map.Entry<String, ColumnStat> e : columnStatsMapKeyedByPath.entrySet()) {
      if (validPaths.contains(e.getKey())) {
        String[] pathParts = e.getKey().split(PATH_DELIMITER);
        insertValueAtPath(jsonObject, pathParts, e.getValue().getNumNulls());
      }
    }
    return jsonObject;
  }

  private void insertValueAtPath(Map<String, Object> jsonObject, String[] pathParts, Object value) {
    if (pathParts == null || pathParts.length == 0) {
      return;
    }
    Map<String, Object> currObject = jsonObject;
    for (int i = 0; i < pathParts.length; i++) {
      String part = pathParts[i];
      if (i == pathParts.length - 1) {
        currObject.put(part, value);
      } else {
        if (!currObject.containsKey(part)) {
          currObject.put(part, new HashMap<String, Object>());
        }
        try {
          currObject = (HashMap<String, Object>) currObject.get(part);
        } catch (ClassCastException e) {
          throw new RuntimeException(
              String.format(
                  "Cannot cast to hashmap while inserting stats at path %s",
                  String.join("->", pathParts)),
              e);
        }
      }
    }
  }

  private Map<String, ColumnStat> getColumnStatKeyedByFullyQualifiedPath(
      Map<OneField, ColumnStat> columnStats) {
    return columnStats.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().getPath(), Map.Entry::getValue));
  }

  private Map<String, OneField> getPathToFieldMap(Map<OneField, ColumnStat> columnStats) {
    return columnStats.entrySet().stream()
        .collect(Collectors.toMap(e -> e.getKey().getPath(), Map.Entry::getKey));
  }

  public Map<OneField, ColumnStat> getColumnStatsForFile(AddFile addFile, List<OneField> fields) {
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
                    Object minValue =
                        DeltaValueConverter.convertFromDeltaColumnStatValue(
                            fieldPathToMinValue.get(fieldPath), field.getSchema());
                    Object maxValue =
                        DeltaValueConverter.convertFromDeltaColumnStatValue(
                            fieldPathToMaxValue.get(fieldPath), field.getSchema());
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

  @Builder
  @Value
  private static class DeltaStats {
    long numRecords;
    Map<String, Object> minValues;
    Map<String, Object> maxValues;
    Map<String, Object> nullCount;
  }

  @Value
  @AllArgsConstructor(staticName = "of")
  private static class StatField {
    String parentPath;
    Map<String, Object> values;
  }
}
