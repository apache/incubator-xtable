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
 
package org.apache.xtable.hudi;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.AllArgsConstructor;
import lombok.NonNull;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.api.Binary;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hadoop.CachingPath;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.MetadataPartitionType;

import org.apache.xtable.collectors.CustomCollectors;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.InternalDataFile;

/** Responsible for Column stats extraction for Hudi. */
@AllArgsConstructor
public class HudiFileStatsExtractor {
  /*
   * The column stats for a decimal field are read back as bytes, requiring conversion here.
   * This is a workaround for a known issue tracked at https://issues.apache.org/jira/browse/HUDI-7037.
   *
   * Due to the use of Avro 1.11 in this project and to avoid packaging issues, the scale value is hardcoded
   * instead of being dynamically retrieved. Otherwise, scale can be obtained using:
   *
   *   ((LogicalTypes.Decimal) DecimalWrapper.SCHEMA$.getField("value").schema().getLogicalType()).getScale();
   */
  private static final int DECIMAL_WRAPPER_SCALE = 15;

  private static final ParquetUtils UTILS = new ParquetUtils();
  private static final String ARRAY_DOT_FIELD = ".array.";
  private static final String PARQUET_ELMENT_DOT_FIELD = ".list.element.";

  @NonNull private final HoodieTableMetaClient metaClient;

  /**
   * Adds column stats and row count information to the provided stream of files.
   *
   * @param metadataTable the metadata table for the hudi table if it exists, otherwise null
   * @param files a stream of files that require column stats and row count information
   * @param schema the schema of the files (assumed to be the same for all files in stream)
   * @return a stream of files with column stats and row count information
   */
  public Stream<InternalDataFile> addStatsToFiles(
      HoodieTableMetadata metadataTable, Stream<InternalDataFile> files, InternalSchema schema) {
    boolean useMetadataTableColStats =
        metadataTable != null
            && metaClient
                .getTableConfig()
                .isMetadataPartitionAvailable(MetadataPartitionType.COLUMN_STATS);
    final Map<String, InternalField> nameFieldMap =
        schema.getAllFields().stream()
            .collect(
                Collectors.toMap(
                    field -> getFieldNameForStats(field, useMetadataTableColStats),
                    Function.identity()));
    return useMetadataTableColStats
        ? computeColumnStatsFromMetadataTable(metadataTable, files, nameFieldMap)
        : computeColumnStatsFromParquetFooters(files, nameFieldMap);
  }

  private Stream<InternalDataFile> computeColumnStatsFromParquetFooters(
      Stream<InternalDataFile> files, Map<String, InternalField> nameFieldMap) {
    return files.map(
        file -> {
          HudiFileStats fileStats =
              computeColumnStatsForFile(new Path(file.getPhysicalPath()), nameFieldMap);
          return file.toBuilder()
              .columnStats(fileStats.getColumnStats())
              .recordCount(fileStats.getRowCount())
              .build();
        });
  }

  private Pair<String, String> getPartitionAndFileName(String path) {
    Path filePath = new CachingPath(path);
    String partitionPath = HudiPathUtils.getPartitionPath(metaClient.getBasePathV2(), filePath);
    return Pair.of(partitionPath, filePath.getName());
  }

  private Stream<InternalDataFile> computeColumnStatsFromMetadataTable(
      HoodieTableMetadata metadataTable,
      Stream<InternalDataFile> files,
      Map<String, InternalField> nameFieldMap) {
    Map<Pair<String, String>, InternalDataFile> filePathsToDataFile =
        files.collect(
            Collectors.toMap(
                file -> getPartitionAndFileName(file.getPhysicalPath()), Function.identity()));
    if (filePathsToDataFile.isEmpty()) {
      return Stream.empty();
    }
    List<Pair<String, String>> filePaths = new ArrayList<>(filePathsToDataFile.keySet());
    Map<Pair<String, String>, List<Pair<InternalField, HoodieMetadataColumnStats>>> stats =
        nameFieldMap.entrySet().parallelStream()
            .flatMap(
                fieldNameToField -> {
                  String fieldName = fieldNameToField.getKey();
                  InternalField field = fieldNameToField.getValue();
                  return metadataTable.getColumnStats(filePaths, fieldName).entrySet().stream()
                      .map(
                          filePairToStats ->
                              Pair.of(
                                  filePairToStats.getKey(),
                                  Pair.of(field, filePairToStats.getValue())));
                })
            .collect(
                Collectors.groupingBy(
                    Map.Entry::getKey,
                    Collectors.mapping(
                        Map.Entry::getValue, CustomCollectors.toList(nameFieldMap.size()))));
    return filePathsToDataFile.entrySet().stream()
        .map(
            pathToDataFile -> {
              Pair<String, String> filePath = pathToDataFile.getKey();
              InternalDataFile file = pathToDataFile.getValue();
              List<Pair<InternalField, HoodieMetadataColumnStats>> fileStats =
                  stats.getOrDefault(filePath, Collections.emptyList());
              List<ColumnStat> columnStats =
                  fileStats.stream()
                      .map(pair -> getColumnStatFromHudiStat(pair.getLeft(), pair.getRight()))
                      .collect(CustomCollectors.toList(fileStats.size()));
              long recordCount = getMaxFromColumnStats(columnStats).orElse(0L);
              return file.toBuilder().columnStats(columnStats).recordCount(recordCount).build();
            });
  }

  private Optional<Long> getMaxFromColumnStats(List<ColumnStat> columnStats) {
    return columnStats.stream()
        .filter(entry -> entry.getField().getParentPath() == null)
        .map(ColumnStat::getNumValues)
        .filter(numValues -> numValues > 0)
        .max(Long::compareTo);
  }

  private HudiFileStats computeColumnStatsForFile(
      Path filePath, Map<String, InternalField> nameFieldMap) {
    List<HoodieColumnRangeMetadata<Comparable>> columnRanges =
        UTILS.readRangeFromParquetMetadata(
            metaClient.getHadoopConf(), filePath, new ArrayList<>(nameFieldMap.keySet()));
    List<ColumnStat> columnStats =
        columnRanges.stream()
            .map(
                colRange ->
                    getColumnStatFromColRange(nameFieldMap.get(colRange.getColumnName()), colRange))
            .collect(CustomCollectors.toList(columnRanges.size()));
    Long rowCount = getMaxFromColumnStats(columnStats).orElse(null);
    if (rowCount == null) {
      rowCount = UTILS.getRowCount(metaClient.getHadoopConf(), filePath);
    }
    return new HudiFileStats(columnStats, rowCount);
  }

  private static ColumnStat getColumnStatFromHudiStat(
      InternalField field, HoodieMetadataColumnStats columnStats) {
    if (columnStats == null) {
      return ColumnStat.builder().build();
    }
    Comparable<?> minValue = HoodieAvroUtils.unwrapAvroValueWrapper(columnStats.getMinValue());
    Comparable<?> maxValue = HoodieAvroUtils.unwrapAvroValueWrapper(columnStats.getMaxValue());
    if (field.getSchema().getDataType() == InternalType.DECIMAL) {
      minValue =
          minValue instanceof ByteBuffer
              ? convertBytesToBigDecimal((ByteBuffer) minValue, DECIMAL_WRAPPER_SCALE)
              : minValue;
      maxValue =
          maxValue instanceof ByteBuffer
              ? convertBytesToBigDecimal((ByteBuffer) maxValue, DECIMAL_WRAPPER_SCALE)
              : maxValue;
    }
    return getColumnStatFromValues(
        minValue,
        maxValue,
        field,
        columnStats.getNullCount(),
        columnStats.getValueCount(),
        columnStats.getTotalSize());
  }

  private static BigDecimal convertBytesToBigDecimal(ByteBuffer value, int scale) {
    byte[] bytes = new byte[value.remaining()];
    value.duplicate().get(bytes);
    return new BigDecimal(new BigInteger(bytes), scale);
  }

  private static ColumnStat getColumnStatFromColRange(
      InternalField field, HoodieColumnRangeMetadata<Comparable> colRange) {
    if (colRange == null) {
      return ColumnStat.builder().build();
    }
    return getColumnStatFromValues(
        colRange.getMinValue(),
        colRange.getMaxValue(),
        field,
        colRange.getNullCount(),
        colRange.getValueCount(),
        colRange.getTotalSize());
  }

  private static ColumnStat getColumnStatFromValues(
      Comparable minValue,
      Comparable maxValue,
      InternalField field,
      long nullCount,
      long valueCount,
      long totalSize) {
    // Special type handling
    if (minValue instanceof Date || maxValue instanceof Date) {
      minValue = minValue == null ? null : dateToDaysSinceEpoch(minValue);
      maxValue = maxValue == null ? null : dateToDaysSinceEpoch(maxValue);
    } else if (field.getSchema().getDataType() == InternalType.ENUM
        && (minValue instanceof ByteBuffer || maxValue instanceof ByteBuffer)) {
      minValue = minValue == null ? null : new String(((ByteBuffer) minValue).array());
      maxValue = maxValue == null ? null : new String(((ByteBuffer) maxValue).array());
    } else if (field.getSchema().getDataType() == InternalType.FIXED
        && (minValue instanceof Binary || maxValue instanceof Binary)) {
      minValue = minValue == null ? null : ByteBuffer.wrap(((Binary) minValue).getBytes());
      maxValue = maxValue == null ? null : ByteBuffer.wrap(((Binary) maxValue).getBytes());
    }
    boolean isScalar = minValue == null || minValue.compareTo(maxValue) == 0;
    Range range = isScalar ? Range.scalar(minValue) : Range.vector(minValue, maxValue);
    return ColumnStat.builder()
        .field(field)
        .range(range)
        .numNulls(nullCount)
        .numValues(valueCount)
        .totalSize(totalSize)
        .build();
  }

  private static int dateToDaysSinceEpoch(Object date) {
    return (int) ((Date) date).toLocalDate().toEpochDay();
  }

  private String getFieldNameForStats(InternalField field, boolean isReadFromMetadataTable) {
    String convertedDotPath = HudiSchemaExtractor.convertFromXTablePath(field.getPath());
    // the array field naming is different for metadata table
    if (isReadFromMetadataTable) {
      return convertedDotPath.replace(ARRAY_DOT_FIELD, PARQUET_ELMENT_DOT_FIELD);
    }
    return convertedDotPath;
  }
}
