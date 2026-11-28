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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.Value;
import lombok.extern.log4j.Log4j2;

import org.apache.paimon.data.BinaryArray;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.utils.InternalRowUtils;

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

  private static final List<InternalSchema.MetadataValue> TIMESTAMP_PRECISIONS =
      Arrays.asList(
          InternalSchema.MetadataValue.MILLIS,
          InternalSchema.MetadataValue.MICROS,
          InternalSchema.MetadataValue.NANOS);

  private final PaimonSchemaExtractor schemaExtractor = PaimonSchemaExtractor.getInstance();

  public static PaimonStatsExtractor getInstance() {
    return INSTANCE;
  }

  /**
   * Maps the columns of the schema data files were written with to the fields of the schema the
   * stats are reported in. Columns are matched by field id, since a column can be renamed, dropped
   * or added after a file was written. Build it once for each schema id of the files.
   *
   * @param fileSchema the schema the data files were written with, which their stats are stored in
   * @param internalSchema the schema to report the stats in
   * @return the stats layout of the files written with {@code fileSchema}
   */
  public StatsLayout getStatsLayout(TableSchema fileSchema, InternalSchema internalSchema) {
    Map<Integer, InternalField> fieldsById = getFieldsById(internalSchema);
    Map<Integer, InternalField> fileFieldsById =
        getFieldsById(schemaExtractor.toInternalSchema(fileSchema));
    List<String> columnNames = new ArrayList<>(fileSchema.fields().size());
    Map<String, StatsColumn> columnsByName = new HashMap<>();
    for (DataField dataField : fileSchema.fields()) {
      columnNames.add(dataField.name());
      InternalField field = fieldsById.get(dataField.id());
      InternalSchema fileFieldSchema = fileFieldsById.get(dataField.id()).getSchema();
      if (field != null && canReportStats(fileFieldSchema, field.getSchema())) {
        columnsByName.put(dataField.name(), new StatsColumn(dataField.type(), field));
      }
    }
    return new StatsLayout(columnNames, columnsByName);
  }

  /**
   * Extracts the column stats of a data file.
   *
   * @param file the data file
   * @param statsLayout the stats layout of the schema the data file was written with
   * @return the column stats
   */
  public List<ColumnStat> extractColumnStats(DataFileMeta file, StatsLayout statsLayout) {
    List<ColumnStat> columnStats = new ArrayList<>();

    // stats for all columns are present in valueStats, we can safely ignore file.keyStats()
    SimpleStats valueStats = file.valueStats();
    if (valueStats != null) {
      List<String> colNames = file.valueStatsCols();
      if (colNames == null) {
        // if column names are not present, then stats are being collected for all columns of the
        // schema the file was written with
        colNames = statsLayout.getColumnNames();
      }

      if (colNames.size() != valueStats.minValues().getFieldCount()) {
        // paranoia check - this should never happen, but if the code reaches here, then there is a
        // bug! Please file a bug report
        throw new ReadException(
            String.format(
                "Mismatch between column stats names and values arity: names=%d, values=%d",
                colNames.size(), valueStats.minValues().getFieldCount()));
      }

      collectColumnStats(
          columnStats, valueStats, colNames, statsLayout.getColumnsByName(), file.rowCount());
    }

    return columnStats;
  }

  private static Map<Integer, InternalField> getFieldsById(InternalSchema schema) {
    return schema.getFields().stream()
        .collect(Collectors.toMap(InternalField::getFieldId, Function.identity()));
  }

  /**
   * Whether the stats a file stores for a column, in the column's type when the file was written,
   * can be reported for the field. They can when the field has the same type, or a wider one that
   * holds every value of the file's type: INT to BIGINT, FLOAT to DOUBLE, a decimal with at least
   * as many digits before and after the point, or a timestamp with a finer precision. TINYINT,
   * SMALLINT and INT are all INT here, and their stats are read with the file's Paimon type.
   */
  private static boolean canReportStats(InternalSchema fileSchema, InternalSchema schema) {
    InternalType fileType = fileSchema.getDataType();
    switch (schema.getDataType()) {
      case LONG:
        return fileType == InternalType.INT || fileType == InternalType.LONG;
      case DOUBLE:
        return fileType == InternalType.FLOAT || fileType == InternalType.DOUBLE;
      case DECIMAL:
        return fileType == InternalType.DECIMAL
            && getDecimalScale(fileSchema) <= getDecimalScale(schema)
            && getDecimalPrecision(fileSchema) - getDecimalScale(fileSchema)
                <= getDecimalPrecision(schema) - getDecimalScale(schema);
      case TIMESTAMP:
      case TIMESTAMP_NTZ:
        return fileType == schema.getDataType()
            && TIMESTAMP_PRECISIONS.indexOf(getTimestampPrecision(fileSchema))
                <= TIMESTAMP_PRECISIONS.indexOf(getTimestampPrecision(schema));
      default:
        return fileType == schema.getDataType()
            && Objects.equals(fileSchema.getMetadata(), schema.getMetadata());
    }
  }

  private static int getDecimalPrecision(InternalSchema schema) {
    return (int) schema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_PRECISION);
  }

  private static int getDecimalScale(InternalSchema schema) {
    return (int) schema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_SCALE);
  }

  private static InternalSchema.MetadataValue getTimestampPrecision(InternalSchema schema) {
    return schema.getMetadata() == null
        ? null
        : (InternalSchema.MetadataValue)
            schema.getMetadata().get(InternalSchema.MetadataKey.TIMESTAMP_PRECISION);
  }

  private void collectColumnStats(
      List<ColumnStat> columnStats,
      SimpleStats stats,
      List<String> colNames,
      Map<String, StatsColumn> columnsByName,
      long rowCount) {

    BinaryRow minValues = stats.minValues();
    BinaryRow maxValues = stats.maxValues();
    BinaryArray nullCounts = stats.nullCounts();

    for (int i = 0; i < colNames.size(); i++) {
      StatsColumn column = columnsByName.get(colNames.get(i));
      if (column == null) {
        continue;
      }
      InternalField field = column.getField();

      // Check if we already have stats for this field
      boolean alreadyExists =
          columnStats.stream().anyMatch(cs -> cs.getField().getPath().equals(field.getPath()));
      if (alreadyExists) {
        continue;
      }

      Object min = getValue(minValues, i, column.getFileType(), field.getSchema());
      Object max = getValue(maxValues, i, column.getFileType(), field.getSchema());
      Long nullCount =
          nullCounts != null && i < nullCounts.size() && !nullCounts.isNullAt(i)
              ? nullCounts.getLong(i)
              : null;

      columnStats.add(
          ColumnStat.builder()
              .field(field)
              .range(min != null && max != null ? Range.vector(min, max) : null)
              .numNulls(nullCount)
              .numValues(rowCount)
              .build());
    }
  }

  /** Reads a stats value with the type the file stores it in and returns it in the field's type. */
  private Object getValue(BinaryRow row, int index, DataType fileType, InternalSchema fieldSchema) {
    if (row.isNullAt(index)) {
      return null;
    }
    InternalType type = fieldSchema.getDataType();
    switch (type) {
      case BOOLEAN:
      case FLOAT:
        return InternalRowUtils.get(row, index, fileType);
      case INT:
      case DATE:
        // a TINYINT or SMALLINT column is stored as a byte or a short
        return ((Number) InternalRowUtils.get(row, index, fileType)).intValue();
      case LONG:
        return ((Number) InternalRowUtils.get(row, index, fileType)).longValue();
      case DOUBLE:
        return ((Number) InternalRowUtils.get(row, index, fileType)).doubleValue();
      case TIMESTAMP:
      case TIMESTAMP_NTZ:
        Timestamp ts = (Timestamp) InternalRowUtils.get(row, index, fileType);
        // according to docs for org.apache.xtable.model.stat.Range, timestamp is stored as millis
        // or micros - even if precision is higher than micros, return micros
        if (getTimestampPrecision(fieldSchema) == InternalSchema.MetadataValue.MILLIS) {
          return ts.getMillisecond();
        } else {
          return ts.toMicros();
        }
      case STRING:
      case ENUM:
        return InternalRowUtils.get(row, index, fileType).toString();
      case DECIMAL:
        // the file can store the column with a smaller scale, which widening keeps exact
        return ((Decimal) InternalRowUtils.get(row, index, fileType))
            .toBigDecimal()
            .setScale(getDecimalScale(fieldSchema));
      default:
        log.warn(
            "Handling of {}-type stats for column idx={}, name={} is not yet implemented, skipping stats for this column",
            type,
            index,
            fieldSchema.getName());
        return null;
    }
  }

  /** The stats layout of the data files written with one schema. */
  @Value
  public static class StatsLayout {
    /**
     * The column names of the schema in order, which the stats follow when the file does not list
     * the columns it has stats for.
     */
    List<String> columnNames;

    /** The columns whose stats can be reported, by column name in the schema. */
    Map<String, StatsColumn> columnsByName;
  }

  /** A column whose stats can be reported, with the type the stats are stored in. */
  @Value
  static class StatsColumn {
    DataType fileType;
    InternalField field;
  }
}
