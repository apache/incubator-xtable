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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

  /**
   * Extracts the column stats of a data file.
   *
   * @param file the data file
   * @param fileSchema the schema the data file was written with, which its stats are laid out by
   * @param internalSchema the schema to report the stats in
   * @return the column stats
   */
  public List<ColumnStat> extractColumnStats(
      DataFileMeta file, InternalSchema fileSchema, InternalSchema internalSchema) {
    List<ColumnStat> columnStats = new ArrayList<>();
    Map<String, InternalField> fieldMap = getFieldsByFileColumnName(fileSchema, internalSchema);

    // stats for all columns are present in valueStats, we can safely ignore file.keyStats()
    SimpleStats valueStats = file.valueStats();
    if (valueStats != null) {
      List<String> colNames = file.valueStatsCols();
      if (colNames == null) {
        // if column names are not present, then stats are being collected for all columns of the
        // schema the file was written with
        colNames =
            fileSchema.getFields().stream()
                .map(InternalField::getName)
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

  /**
   * Maps the column names of the file's schema to the fields of the schema the stats are reported
   * in. Columns are matched by field id, since a column can be renamed, dropped or added after the
   * file was written. A column whose type changed is left out, since the file stores its stats in
   * the old type.
   */
  private static Map<String, InternalField> getFieldsByFileColumnName(
      InternalSchema fileSchema, InternalSchema internalSchema) {
    Map<Integer, InternalField> fieldsById =
        internalSchema.getFields().stream()
            .collect(Collectors.toMap(InternalField::getFieldId, f -> f));
    Map<String, InternalField> fieldsByFileColumnName = new HashMap<>();
    for (InternalField fileField : fileSchema.getFields()) {
      InternalField field = fieldsById.get(fileField.getFieldId());
      if (field != null && hasSameType(fileField.getSchema(), field.getSchema())) {
        fieldsByFileColumnName.put(fileField.getName(), field);
      }
    }
    return fieldsByFileColumnName;
  }

  private static boolean hasSameType(InternalSchema fileSchema, InternalSchema schema) {
    return fileSchema.getDataType() == schema.getDataType()
        && Objects.equals(fileSchema.getMetadata(), schema.getMetadata());
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
          columnStats.stream().anyMatch(cs -> cs.getField().getPath().equals(field.getPath()));
      if (alreadyExists) {
        continue;
      }

      InternalType type = field.getSchema().getDataType();
      Object min = getValue(minValues, i, type, field.getSchema());
      Object max = getValue(maxValues, i, type, field.getSchema());
      long nullCount = (nullCounts != null && i < nullCounts.size()) ? nullCounts.getLong(i) : 0L;

      columnStats.add(
          ColumnStat.builder()
              .field(field)
              .range(min != null && max != null ? Range.vector(min, max) : null)
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
