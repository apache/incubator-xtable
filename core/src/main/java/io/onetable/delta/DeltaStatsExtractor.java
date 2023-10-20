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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;

import lombok.Value;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.NumericType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampType;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.stat.ColumnStat;

/**
 * DeltaStatsExtractor extracts column stats and also responsible for their serialization leveraging
 * {@link DeltaValueConverter}.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaStatsExtractor {
  private static final DeltaStatsExtractor INSTANCE = new DeltaStatsExtractor();

  private static final String PATH_DELIMITER = "\\.";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String NUM_RECORDS = "numRecords";
  private static final String MIN_VALUES = "minValues";
  private static final String MAX_VALUES = "maxValues";
  private static final String NULL_COUNT = "nullCount";

  public static DeltaStatsExtractor getInstance() {
    return INSTANCE;
  }

  public String convertStatsToDeltaFormat(
      StructType deltaTableSchema, long numRecords, Map<OneField, ColumnStat> columnStats)
      throws JsonProcessingException {
    DeltaStats.DeltaStatsBuilder deltaStatsBuilder = DeltaStats.builder();
    deltaStatsBuilder.numRecords(numRecords);
    if (columnStats == null) {
      return MAPPER.writeValueAsString(deltaStatsBuilder.build());
    }
    Map<String, ColumnStat> columnStatsMapKeyedByPath =
        getColumnStatKeyedByFullyQualifiedPath(columnStats);
    Map<String, OneField> pathFieldMap = getPathToFieldMap(columnStats);
    Set<String> validPaths = getPathsFromStructSchemaForMinAndMaxStats(deltaTableSchema, "");
    DeltaStats deltaStats = deltaStatsBuilder.minValues(getMinValues(pathFieldMap, columnStatsMapKeyedByPath, validPaths))
            .maxValues(getMaxValues(pathFieldMap, columnStatsMapKeyedByPath, validPaths))
                .nullCount(getNullCount(columnStatsMapKeyedByPath, validPaths))
        .build();
    return MAPPER.writeValueAsString(deltaStats);
  }

  private Set<String> getPathsFromStructSchemaForMinAndMaxStats(
      StructType schema, String parentPath) {
    Set<String> allPaths = new HashSet<>();
    for (StructField sf : schema.fields()) {
      // Delta only supports min/max stats for these fields.
      if (sf.dataType() instanceof DateType
          || sf.dataType() instanceof NumericType
          || sf.dataType() instanceof TimestampType
          || sf.dataType() instanceof StringType) {
        allPaths.add(combinePath(parentPath, sf.name()));
      } else if (sf.dataType() instanceof StructType) {
        allPaths.addAll(
            getPathsFromStructSchemaForMinAndMaxStats(
                (StructType) sf.dataType(), combinePath(parentPath, sf.name())));
      }
    }
    return allPaths;
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

  @Builder
  @Value
  private static class DeltaStats {
    long numRecords;
    Map<String, Object> minValues;
    Map<String, Object> maxValues;
    Map<String, Object> nullCount;
  }
}
