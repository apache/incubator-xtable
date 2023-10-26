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
 
package io.onetable.iceberg;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import io.onetable.model.schema.OneField;
import io.onetable.model.stat.ColumnStat;

/** Column stats extractor for iceberg table format. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IcebergColumnStatsConverter {
  private static final IcebergColumnStatsConverter INSTANCE = new IcebergColumnStatsConverter();

  public static IcebergColumnStatsConverter getInstance() {
    return INSTANCE;
  }

  public Metrics convert(
      Schema schema, long totalRowCount, Map<OneField, ColumnStat> fieldColumnStatMap) {
    Map<Integer, Long> columnSizes = new HashMap<>();
    Map<Integer, Long> valueCounts = new HashMap<>();
    Map<Integer, Long> nullValueCounts = new HashMap<>();
    Map<Integer, Long> nanValueCounts = null; // OneTable currently doesn't track this
    Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
    Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
    fieldColumnStatMap.forEach(
        (field, columnStats) -> {
          Types.NestedField icebergField =
              schema.findField(IcebergSchemaExtractor.convertFromOneTablePath(field.getPath()));
          int fieldId = icebergField.fieldId();
          columnSizes.put(fieldId, columnStats.getTotalSize());
          valueCounts.put(fieldId, columnStats.getNumValues());
          nullValueCounts.put(fieldId, columnStats.getNumNulls());
          Type fieldType = icebergField.type();
          if (columnStats.getRange().getMinValue() != null) {
            // TODO(vamshigv): for timestamp type string to long is failing while iceberg sync.
            lowerBounds.put(
                fieldId, Conversions.toByteBuffer(fieldType, columnStats.getRange().getMinValue()));
          }
          if (columnStats.getRange().getMaxValue() != null) {
            upperBounds.put(
                fieldId, Conversions.toByteBuffer(fieldType, columnStats.getRange().getMaxValue()));
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
}
