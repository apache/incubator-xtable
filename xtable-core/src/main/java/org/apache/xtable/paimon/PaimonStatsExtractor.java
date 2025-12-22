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
 
package org.apache.xtable.paimon;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.TimestampType;

import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;

@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PaimonStatsExtractor {
  private static final PaimonStatsExtractor INSTANCE = new PaimonStatsExtractor();

  public static PaimonStatsExtractor getInstance() {
    return INSTANCE;
  }

  public List<ColumnStat> extractColumnStats(DataFileMeta file, InternalSchema internalSchema) {
    List<ColumnStat> columnStats = new ArrayList<>();
    Map<String, InternalField> fieldMap =
        internalSchema.getAllFields().stream()
            .collect(Collectors.toMap(InternalField::getPath, f -> f));

    // stats for all columns are present in valueStats, we can safely ignore file.keyStats() - TODO:
    // validate this assumption
    SimpleStats valueStats = file.valueStats();
    if (valueStats != null) {
      List<String> colNames = file.valueStatsCols();
      if (colNames == null || colNames.isEmpty()) {
        // if column names are not present, we assume all columns in the schema are present in the
        // same order as the schema - TODO: validate this assumption
        colNames =
            internalSchema.getAllFields().stream()
                .map(InternalField::getPath)
                .collect(Collectors.toList());
      }

      if (colNames.size() != valueStats.minValues().getFieldCount()) {
        // paranoia check - this should never happen, but if the code reaches here, then there is a
        // bug! Please file a bug report
        throw new ReadException(
            String.format(
                "Mismatch between column stats names and values arity: names=%d, values=%d",
                colNames.size(), valueStats.minValues().getFieldCount()));
      }

      collectColumnStats(columnStats, valueStats, colNames, fieldMap, file.rowCount());
    }

    return columnStats;
  }

  private void collectColumnStats(
      List<ColumnStat> columnStats,
      SimpleStats stats,
      List<String> colNames,
      Map<String, InternalField> fieldMap,
      long rowCount) {

    BinaryRow minValues = stats.minValues();
    BinaryRow maxValues = stats.maxValues();
    BinaryArray nullCounts = stats.nullCounts();

    for (int i = 0; i < colNames.size(); i++) {
      String colName = colNames.get(i);
      InternalField field = fieldMap.get(colName);
      if (field == null) {
        continue;
      }

      // Check if we already have stats for this field
      boolean alreadyExists =
          columnStats.stream().anyMatch(cs -> cs.getField().getPath().equals(colName));
      if (alreadyExists) {
        continue;
      }

      InternalType type = field.getSchema().getDataType();
      Object min = getValue(minValues, i, type, field.getSchema());
      Object max = getValue(maxValues, i, type, field.getSchema());
      Long nullCount = (nullCounts != null && i < nullCounts.size()) ? nullCounts.getLong(i) : 0L;

      columnStats.add(
          ColumnStat.builder()
              .field(field)
              .range(Range.vector(min, max))
              .numNulls(nullCount)
              .numValues(rowCount)
              .build());
    }
  }

  private Object getValue(BinaryRow row, int index, InternalType type, InternalSchema fieldSchema) {
    if (row.isNullAt(index)) {
      return null;
    }
    switch (type) {
      case BOOLEAN:
        return row.getBoolean(index);
      case INT:
      case DATE:
        return row.getInt(index);
      case LONG:
        return row.getLong(index);
      case TIMESTAMP:
      case TIMESTAMP_NTZ:
        int tsPrecision;
        InternalSchema.MetadataValue tsPrecisionEnum =
            (InternalSchema.MetadataValue)
                fieldSchema.getMetadata().get(InternalSchema.MetadataKey.TIMESTAMP_PRECISION);
        if (tsPrecisionEnum == InternalSchema.MetadataValue.MILLIS) {
          tsPrecision = 3;
        } else if (tsPrecisionEnum == InternalSchema.MetadataValue.MICROS) {
          tsPrecision = 6;
        } else if (tsPrecisionEnum == InternalSchema.MetadataValue.NANOS) {
          tsPrecision = 9;
        } else {
          log.warn(
              "Field idx={}, name={} does not have MetadataKey.TIMESTAMP_PRECISION set, defaulting to default precision",
              index,
              fieldSchema.getName());
          tsPrecision = TimestampType.DEFAULT_PRECISION;
        }
        Timestamp ts = row.getTimestamp(index, tsPrecision);

        // according to docs for org.apache.xtable.model.stat.Range, timestamp is stored as millis
        // or micros - even if precision is higher than micros, return micros
        if (tsPrecisionEnum == InternalSchema.MetadataValue.MILLIS) {
          return ts.getMillisecond();
        } else {
          return ts.toMicros();
        }
      case FLOAT:
        return row.getFloat(index);
      case DOUBLE:
        return row.getDouble(index);
      case STRING:
      case ENUM:
        return row.getString(index).toString();
      case DECIMAL:
        int precision =
            (int) fieldSchema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_PRECISION);
        int scale = (int) fieldSchema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_SCALE);
        return row.getDecimal(index, precision, scale).toBigDecimal();
      default:
        log.warn(
            "Handling of {}-type stats for column idx={}, name={} is not yet implemented, skipping stats for this column",
            type,
            index,
            fieldSchema.getName());
        return null;
    }
  }
}
