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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
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
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.StringUtils;

import org.apache.spark.sql.delta.actions.AddFile;

import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.collectors.CustomCollectors;
import org.apache.xtable.model.exception.ParseException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.FileStats;
import org.apache.xtable.model.stat.Range;

/**
 * DeltaStatsExtractor extracts column stats and also responsible for their serialization leveraging
 * {@link DeltaValueConverter}.
 */
@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaStatsExtractor {
  private static final Set<InternalType> FIELD_TYPES_WITH_STATS_SUPPORT =
      new HashSet<>(
          Arrays.asList(
              InternalType.BOOLEAN,
              InternalType.DATE,
              InternalType.DECIMAL,
              InternalType.DOUBLE,
              InternalType.INT,
              InternalType.LONG,
              InternalType.FLOAT,
              InternalType.STRING,
              InternalType.TIMESTAMP,
              InternalType.TIMESTAMP_NTZ));

  private static final DeltaStatsExtractor INSTANCE = new DeltaStatsExtractor();

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /* this data structure collects type names of all unrecognized Delta Lake stats. For instance
  data file stats in presence of delete vectors would contain 'tightBounds' stat which is
  currently not handled by XTable */
  private final Set<String> unsupportedStats = new HashSet<>();

  public static DeltaStatsExtractor getInstance() {
    return INSTANCE;
  }

  public String convertStatsToDeltaFormat(
      InternalSchema schema, long numRecords, List<ColumnStat> columnStats)
      throws JsonProcessingException {
    DeltaStats.DeltaStatsBuilder deltaStatsBuilder = DeltaStats.builder();
    deltaStatsBuilder.numRecords(numRecords);
    if (columnStats == null) {
      return MAPPER.writeValueAsString(deltaStatsBuilder.build());
    }
    Set<String> validPaths = getPathsFromStructSchemaForMinAndMaxStats(schema);
    List<ColumnStat> validColumnStats =
        columnStats.stream()
            .filter(stat -> validPaths.contains(stat.getField().getPath()))
            .collect(Collectors.toList());
    DeltaStats deltaStats =
        deltaStatsBuilder
            .minValues(getMinValues(validColumnStats))
            .maxValues(getMaxValues(validColumnStats))
            .nullCount(getNullCount(validColumnStats))
            .build();
    return MAPPER.writeValueAsString(deltaStats); // TODO check error here (serialization of stats)
  }

  private Set<String> getPathsFromStructSchemaForMinAndMaxStats(InternalSchema schema) {
    return schema.getAllFields().stream()
        .filter(
            field -> {
              InternalType type = field.getSchema().getDataType();
              return FIELD_TYPES_WITH_STATS_SUPPORT.contains(type);
            })
        .map(InternalField::getPath)
        .collect(Collectors.toSet());
  }

  private Map<String, Object> getMinValues(List<ColumnStat> validColumnStats) {
    return getValues(validColumnStats, columnStat -> columnStat.getRange().getMinValue());
  }

  private Map<String, Object> getMaxValues(List<ColumnStat> validColumnStats) {
    return getValues(validColumnStats, columnStat -> columnStat.getRange().getMaxValue());
  }

  private Map<String, Object> getValues(
      List<ColumnStat> validColumnStats, Function<ColumnStat, Object> valueExtractor) {
    Map<String, Object> jsonObject = new HashMap<>();
    validColumnStats.forEach(
        columnStat -> {
          InternalField field = columnStat.getField();
          String[] pathParts = field.getPathParts();
          insertValueAtPath(
              jsonObject,
              pathParts,
              DeltaValueConverter.convertToDeltaColumnStatValue(
                  valueExtractor.apply(columnStat), field.getSchema()));
        });
    return jsonObject;
  }

  private Map<String, Object> getNullCount(List<ColumnStat> validColumnStats) {
    // TODO: Additional work needed to track nulls maps & arrays.
    Map<String, Object> jsonObject = new HashMap<>();
    validColumnStats.forEach(
        columnStat -> {
          String[] pathParts = columnStat.getField().getPathParts();
          insertValueAtPath(jsonObject, pathParts, columnStat.getNumNulls());
        });
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

  public FileStats getColumnStatsForFile(AddFile addFile, List<InternalField> fields) {
    if (StringUtils.isEmpty(addFile.stats())) {
      return FileStats.builder().columnStats(Collections.emptyList()).numRecords(0).build();
    }
    // TODO: Additional work needed to track maps & arrays.
    try {
      DeltaStats deltaStats = MAPPER.readValue(addFile.stats(), DeltaStats.class);
      collectUnsupportedStats(deltaStats.getAdditionalStats());

      Map<String, Object> fieldPathToMaxValue = flattenStatMap(deltaStats.getMaxValues());
      Map<String, Object> fieldPathToMinValue = flattenStatMap(deltaStats.getMinValues());
      Map<String, Object> fieldPathToNullCount = flattenStatMap(deltaStats.getNullCount());
      List<ColumnStat> columnStats =
          fields.stream()
              .filter(field -> fieldPathToMaxValue.containsKey(field.getPath()))
              .map(
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
                        .field(field)
                        .numValues(deltaStats.getNumRecords())
                        .numNulls(nullCount.longValue())
                        .range(range)
                        .build();
                  })
              .collect(CustomCollectors.toList(fields.size()));
      return FileStats.builder()
          .columnStats(columnStats)
          .numRecords(deltaStats.getNumRecords())
          .build();
    } catch (IOException ex) {
      throw new ParseException("Unable to parse stats json", ex);
    }
  }

  private void collectUnsupportedStats(Map<String, Object> additionalStats) {
    if (additionalStats == null || additionalStats.isEmpty()) {
      return;
    }

    additionalStats.keySet().stream()
        .filter(key -> !unsupportedStats.contains(key))
        .forEach(
            key -> {
              log.info("Unrecognized/unsupported Delta data file stat: {}", key);
              unsupportedStats.add(key);
            });
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

  /**
   * Returns the names of all unsupported stats that have been discovered during the parsing of
   * Delta Lake stats.
   *
   * @return set of unsupported stats
   */
  @VisibleForTesting
  Set<String> getUnsupportedStats() {
    return Collections.unmodifiableSet(unsupportedStats);
  }

  @Builder
  @Value
  private static class DeltaStats {
    long numRecords;
    Map<String, Object> minValues;
    Map<String, Object> maxValues;
    Map<String, Object> nullCount;

    /* this is a catch-all for any additional stats that are not explicitly handled */
    @JsonIgnore
    @Getter(lazy = true)
    Map<String, Object> additionalStats = new HashMap<>();

    @JsonAnySetter
    public void setAdditionalStat(String key, Object value) {
      getAdditionalStats().put(key, value);
    }
  }

  @Value
  @AllArgsConstructor(staticName = "of")
  private static class StatField {
    String parentPath;
    Map<String, Object> values;
  }
}
