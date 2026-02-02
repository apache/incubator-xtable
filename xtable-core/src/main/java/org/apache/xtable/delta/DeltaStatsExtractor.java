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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import org.apache.spark.sql.delta.Snapshot;
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
import org.apache.xtable.parquet.ParquetMetadataExtractor;
import org.apache.xtable.parquet.ParquetStatsExtractor;

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
    return MAPPER.writeValueAsString(deltaStats);
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

  /**
   * Extracts column statistics for a Delta Lake data file. This method first attempts to read
   * statistics from the Delta checkpoint (fast path). If checkpoint statistics are NULL or empty,
   * it falls back to reading statistics directly from the Parquet file footer (slow path).
   *
   * <p>Delta Lake can store statistics in two locations:
   *
   * <ul>
   *   <li><b>Checkpoint files (JSON format):</b> Preferred and faster, but may be NULL if
   *       'delta.checkpoint.writeStatsAsJson' is false or stats collection was disabled
   *   <li><b>Parquet file footers:</b> Fallback option that requires opening each data file
   *       individually, which is more expensive but ensures statistics are always available
   * </ul>
   *
   * <p>Performance Considerations: When checkpoint statistics are NULL for many files, the fallback
   * to Parquet footers can significantly slow down conversion. For large tables with thousands of
   * files, consider enabling Delta checkpoint statistics via: {@code ALTER TABLE table_name SET
   * TBLPROPERTIES ('delta.checkpoint.writeStatsAsJson' = 'true')}
   *
   * @param addFile the Delta AddFile action containing file metadata
   * @param snapshot the Delta snapshot providing table context and base path
   * @param fields the schema fields for which to extract statistics
   * @return FileStats containing column statistics and record count
   */
  public FileStats getColumnStatsForFile(
      AddFile addFile, Snapshot snapshot, List<InternalField> fields) {
    // Attempt to read statistics from Delta checkpoint (fast path)
    String statsString = addFile.stats();

    if (StringUtils.isNotEmpty(statsString)) {
      log.debug("Reading stats from checkpoint for file: {}", addFile.path());
      return parseStatsFromJson(statsString, fields);
    }

    // Checkpoint statistics are NULL or empty - fall back to Parquet footer (slow path)
    log.debug(
        "Stats not found in Delta checkpoint for file: {}, falling back to Parquet footer read",
        addFile.path());
    return readStatsFromParquetFooter(addFile, snapshot, fields);
  }

  /**
   * Legacy method for backward compatibility. Use getColumnStatsForFile(AddFile, Snapshot, List)
   * instead.
   */
  public FileStats getColumnStatsForFile(AddFile addFile, List<InternalField> fields) {
    String statsString = addFile.stats();
    if (StringUtils.isEmpty(statsString)) {
      return FileStats.builder().columnStats(Collections.emptyList()).numRecords(0).build();
    }
    return parseStatsFromJson(statsString, fields);
  }

  /** Parses stats from JSON string and converts to FileStats. */
  private FileStats parseStatsFromJson(String statsString, List<InternalField> fields) {
    // TODO: Additional work needed to track maps & arrays.
    try {
      DeltaStats deltaStats = MAPPER.readValue(statsString, DeltaStats.class);
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

  /**
   * Reads column statistics directly from a Parquet file footer. This method is used as a fallback
   * when Delta checkpoint statistics are NULL or unavailable.
   *
   * <p>This operation is expensive as it requires:
   *
   * <ul>
   *   <li>Opening each Parquet file individually (I/O overhead)
   *   <li>Reading the file footer metadata
   *   <li>Parsing column chunk metadata for all columns
   *   <li>Converting Parquet statistics to internal format
   * </ul>
   *
   * <p>For cloud storage (S3, GCS, ADLS), this can add significant latency due to network overhead.
   * The method performs several safety checks to prevent errors:
   *
   * <ul>
   *   <li>Filters out statistics with NULL min/max ranges (prevents NullPointerException)
   *   <li>Skips DECIMAL and complex types (prevents ClassCastException)
   *   <li>Validates Binary-to-primitive type conversions
   * </ul>
   *
   * <p>Record Count: The record count is read from Parquet row group metadata, which is always
   * reliable regardless of column statistics availability.
   *
   * @param addFile the Delta AddFile action containing the file path
   * @param snapshot the Delta snapshot providing table base path
   * @param fields the schema fields for which to extract statistics
   * @return FileStats with extracted statistics, or empty stats if reading fails
   */
  private FileStats readStatsFromParquetFooter(
      AddFile addFile, Snapshot snapshot, List<InternalField> fields) {
    try {
      // Construct absolute path to the Parquet data file
      // Handle both absolute paths and relative paths from table base
      String tableBasePath = snapshot.deltaLog().dataPath().toString();
      String filePath = addFile.path();
      String fullPath =
          filePath.startsWith(tableBasePath) ? filePath : tableBasePath + "/" + filePath;

      // Read Parquet file footer metadata using Hadoop FileSystem API
      Configuration conf = new Configuration();
      Path parquetPath = new Path(fullPath);

      ParquetMetadata footer = ParquetMetadataExtractor.readParquetMetadata(conf, parquetPath);
      List<ColumnStat> parquetStats = ParquetStatsExtractor.getColumnStatsForaFile(footer);

      // Extract record count from Parquet row groups metadata
      // This is always reliable and doesn't depend on column statistics
      long numRecords = footer.getBlocks().stream().mapToLong(block -> block.getRowCount()).sum();

      // Build lookup map for efficient field matching by path
      Map<String, ColumnStat> pathToStat =
          parquetStats.stream()
              .collect(
                  Collectors.toMap(
                      stat -> stat.getField().getPath(),
                      Function.identity(),
                      (stat1, stat2) -> stat1)); // Keep first occurrence on collision

      // Map Parquet stats to requested Delta schema fields
      // Filter out statistics with NULL ranges to prevent downstream NullPointerException
      List<ColumnStat> mappedStats =
          fields.stream()
              .filter(field -> pathToStat.containsKey(field.getPath()))
              .map(
                  field -> {
                    ColumnStat parquetStat = pathToStat.get(field.getPath());
                    // Rebuild ColumnStat with correct Delta field reference
                    // while preserving Parquet statistics values
                    return ColumnStat.builder()
                        .field(field)
                        .numValues(parquetStat.getNumValues())
                        .numNulls(parquetStat.getNumNulls())
                        .totalSize(parquetStat.getTotalSize())
                        .range(parquetStat.getRange())
                        .build();
                  })
              .filter(
                  stat ->
                      stat.getRange() != null
                          && stat.getRange().getMinValue() != null
                          && stat.getRange().getMaxValue() != null)
              .collect(Collectors.toList());

      log.debug(
          "Successfully extracted {} column stats from Parquet footer for file: {}",
          mappedStats.size(),
          addFile.path());

      return FileStats.builder().columnStats(mappedStats).numRecords(numRecords).build();

    } catch (Exception e) {
      // Log warning but continue conversion - the file will be added without statistics
      // This is preferable to failing the entire conversion
      log.warn(
          "Failed to read stats from Parquet footer for file {}: {}. "
              + "File will be included without column statistics.",
          addFile.path(),
          e.getMessage());

      // Return empty statistics but note that record count is also 0
      // Delta AddFile doesn't contain record count, so we cannot preserve it here
      // The file will still be added to target table with 0 record count in metadata
      return FileStats.builder().columnStats(Collections.emptyList()).numRecords(0).build();
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
    // Return empty map if input is null
    if (statMap == null) {
      return result;
    }
    Queue<StatField> statFieldQueue = new ArrayDeque<>();
    statFieldQueue.add(StatField.of("", statMap));
    while (!statFieldQueue.isEmpty()) {
      StatField statField = statFieldQueue.poll();
      // Skip if values map is null (can happen with malformed or partial stats)
      if (statField.getValues() == null) {
        continue;
      }
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
