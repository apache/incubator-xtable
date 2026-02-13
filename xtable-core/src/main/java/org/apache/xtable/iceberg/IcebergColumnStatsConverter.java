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
 
package org.apache.xtable.iceberg;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import org.apache.xtable.collectors.CustomCollectors;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;

/** Column stats extractor for iceberg table format. */
@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IcebergColumnStatsConverter {
  private static final IcebergSchemaExtractor SCHEMA_EXTRACTOR =
      IcebergSchemaExtractor.getInstance();
  private static final IcebergColumnStatsConverter INSTANCE = new IcebergColumnStatsConverter();

  public static IcebergColumnStatsConverter getInstance() {
    return INSTANCE;
  }

  public Metrics toIceberg(Schema schema, long totalRowCount, List<ColumnStat> fieldColumnStats) {
    Map<Integer, Long> columnSizes = new HashMap<>();
    Map<Integer, Long> valueCounts = new HashMap<>();
    Map<Integer, Long> nullValueCounts = new HashMap<>();
    Map<Integer, Long> nanValueCounts = null; // InternalTable currently doesn't track this
    Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
    Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
    fieldColumnStats.forEach(
        columnStats -> {
          InternalField field = columnStats.getField();
          Types.NestedField icebergField =
              schema.findField(IcebergSchemaExtractor.convertFromXTablePath(field.getPath()));
          int fieldId = icebergField.fieldId();
          columnSizes.put(fieldId, columnStats.getTotalSize());
          valueCounts.put(fieldId, columnStats.getNumValues());
          nullValueCounts.put(fieldId, columnStats.getNumNulls());
          Type fieldType = icebergField.type();
          Object minValue = coerceStatValue(fieldType, columnStats.getRange().getMinValue());
          ByteBuffer minBuffer = safeToByteBuffer(fieldType, minValue, field.getPath(), "min");
          if (minBuffer != null) {
            lowerBounds.put(fieldId, minBuffer);
          }
          Object maxValue = coerceStatValue(fieldType, columnStats.getRange().getMaxValue());
          ByteBuffer maxBuffer = safeToByteBuffer(fieldType, maxValue, field.getPath(), "max");
          if (maxBuffer != null) {
            upperBounds.put(fieldId, maxBuffer);
          }
        });
    return new Metrics(
        totalRowCount,
        columnSizes,
        valueCounts,
        nullValueCounts,
        nanValueCounts,
        lowerBounds,
        upperBounds);
  }

  public List<ColumnStat> fromIceberg(
      List<InternalField> fields,
      Map<Integer, Long> valueCounts,
      Map<Integer, Long> nullCounts,
      Map<Integer, Long> size,
      Map<Integer, ByteBuffer> minValues,
      Map<Integer, ByteBuffer> maxValues) {
    if (valueCounts == null || valueCounts.isEmpty()) {
      return Collections.emptyList();
    }
    return fields.stream()
        .filter(field -> valueCounts.containsKey(field.getFieldId()))
        .map(
            field -> {
              Integer fieldId = field.getFieldId();
              long numValues = valueCounts.get(fieldId);
              long numNulls = nullCounts.get(fieldId);
              long totalSize = size.get(fieldId);
              Type fieldType = SCHEMA_EXTRACTOR.toIcebergType(field, new AtomicInteger(1));
              Object minValue = convertFromIcebergValue(fieldType, minValues.get(fieldId));
              Object maxValue = convertFromIcebergValue(fieldType, maxValues.get(fieldId));
              Range range = Range.vector(minValue, maxValue);
              return ColumnStat.builder()
                  .field(field)
                  .numValues(numValues)
                  .numNulls(numNulls)
                  .totalSize(totalSize)
                  .range(range)
                  .build();
            })
        .collect(CustomCollectors.toList(fields.size()));
  }

  private Object convertFromIcebergValue(Type fieldType, ByteBuffer value) {
    if (value == null) {
      return null;
    }
    Object convertedValue = Conversions.fromByteBuffer(fieldType, value);
    if (fieldType.typeId() == Type.TypeID.STRING) {
      // occasionally the string is returned as HeapCharBuffer so just convert to string
      return convertedValue.toString();
    }
    return convertedValue;
  }

  private ByteBuffer safeToByteBuffer(Type fieldType, Object value, String fieldPath, String side) {
    if (value == null) {
      return null;
    }
    try {
      return Conversions.toByteBuffer(fieldType, value);
    } catch (RuntimeException e) {
      log.warn(
          "Skipping {} bound for field {} due to uncoercible value {} for type {}",
          side,
          fieldPath,
          value,
          fieldType,
          e);
      return null;
    }
  }

  private Object coerceStatValue(Type fieldType, Object value) {
    if (value == null) {
      return null;
    }
    switch (fieldType.typeId()) {
      case STRING:
        return value.toString();
      case FLOAT:
        return value instanceof Number ? ((Number) value).floatValue() : null;
      case DOUBLE:
        return value instanceof Number ? ((Number) value).doubleValue() : null;
      case INTEGER:
        return value instanceof Number ? ((Number) value).intValue() : null;
      case LONG:
        return value instanceof Number ? ((Number) value).longValue() : null;
      case DATE:
        return value instanceof Number ? ((Number) value).intValue() : null;
      case TIME:
      case TIMESTAMP:
        return value instanceof Number ? ((Number) value).longValue() : null;
      case BOOLEAN:
        if (value instanceof Boolean) {
          return value;
        }
        if (value instanceof Number) {
          return ((Number) value).intValue() != 0;
        }
        return null;
      default:
        return value;
    }
  }
}
