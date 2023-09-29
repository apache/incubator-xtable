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
 
package io.onetable.hudi;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.AllArgsConstructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.api.Binary;

import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.util.ParquetUtils;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.Range;
import io.onetable.model.storage.OneDataFile;

/** Responsible for Column stats extraction for Hudi. */
@AllArgsConstructor
public class HudiFileStatsExtractor {

  private static final ParquetUtils UTILS = new ParquetUtils();

  private final Configuration conf;

  /**
   * Adds column stats and row count information to the provided stream of files.
   *
   * @param files a stream of files that require column stats and row count information
   * @param schema the schema of the files (assumed to be the same for all files in stream)
   * @return a stream of files with column stats and row count information
   */
  public Stream<OneDataFile> addStatsToFiles(Stream<OneDataFile> files, OneSchema schema) {
    final Map<String, OneField> nameFieldMap =
        getAllFields(schema).stream()
            .collect(
                Collectors.toMap(
                    field -> HudiSchemaExtractor.convertFromOneTablePath(field.getPath()),
                    Function.identity()));
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

  private HudiFileStats computeColumnStatsForFile(
      Path filePath, Map<String, OneField> nameFieldMap) {
    List<HoodieColumnRangeMetadata<Comparable>> columnRanges =
        UTILS.readRangeFromParquetMetadata(conf, filePath, new ArrayList<>(nameFieldMap.keySet()));
    Map<OneField, ColumnStat> columnStatMap =
        columnRanges.stream()
            .collect(
                Collectors.toMap(
                    colRange -> nameFieldMap.get(colRange.getColumnName()),
                    colRange ->
                        getColumnStatFromColRange(
                            nameFieldMap.get(colRange.getColumnName()), colRange)));
    Long rowCount = null;
    for (Map.Entry<OneField, ColumnStat> entry : columnStatMap.entrySet()) {
      if (entry.getKey().getParentPath() == null) {
        rowCount = entry.getValue().getNumValues();
      }
    }
    if (rowCount == null) {
      rowCount = UTILS.getRowCount(conf, filePath);
    }
    return new HudiFileStats(columnStatMap, rowCount);
  }

  private static ColumnStat getColumnStatFromColRange(
      OneField field, HoodieColumnRangeMetadata<Comparable> colRange) {
    if (colRange == null) {
      return ColumnStat.builder().build();
    }
    Comparable minValue = colRange.getMinValue();
    Comparable maxValue = colRange.getMaxValue();
    // Special type handling
    if (minValue instanceof Date || maxValue instanceof Date) {
      minValue = minValue == null ? null : dateToDaysSinceEpoch(minValue);
      maxValue = maxValue == null ? null : dateToDaysSinceEpoch(maxValue);
    } else if (field.getSchema().getDataType() == OneType.ENUM
        && (minValue instanceof ByteBuffer || maxValue instanceof ByteBuffer)) {
      minValue = minValue == null ? null : new String(((ByteBuffer) minValue).array());
      maxValue = maxValue == null ? null : new String(((ByteBuffer) maxValue).array());
    } else if (field.getSchema().getDataType() == OneType.FIXED
        && (minValue instanceof Binary || maxValue instanceof Binary)) {
      minValue = minValue == null ? null : ByteBuffer.wrap(((Binary) minValue).getBytes());
      maxValue = maxValue == null ? null : ByteBuffer.wrap(((Binary) maxValue).getBytes());
    }
    boolean isScalar = minValue == null || minValue.compareTo(maxValue) == 0;
    Range range = isScalar ? Range.scalar(minValue) : Range.vector(minValue, maxValue);
    return ColumnStat.builder()
        .range(range)
        .numNulls(colRange.getNullCount())
        .numValues(colRange.getValueCount())
        .totalSize(colRange.getTotalSize())
        .build();
  }

  private static List<OneField> getAllFields(OneSchema schema) {
    List<OneField> output = new ArrayList<>();
    Queue<OneField> fieldQueue = new ArrayDeque<>(schema.getFields());
    while (!fieldQueue.isEmpty()) {
      OneField currentField = fieldQueue.poll();
      if (currentField.getSchema().getFields() != null) {
        fieldQueue.addAll(currentField.getSchema().getFields());
      }
      output.add(currentField);
    }
    return output;
  }

  private static int dateToDaysSinceEpoch(Object date) {
    return (int) ((Date) date).toLocalDate().toEpochDay();
  }
}
