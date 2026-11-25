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
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;

import org.apache.hudi.common.util.PartitionPathEncodeUtils;

import org.apache.xtable.exception.PartitionValuesExtractorException;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;

/** Extracts Partition Values for Hudi/Parquet from Partition Path. */
@AllArgsConstructor
public class PathBasedPartitionValuesExtractor {
  private static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";
  private static final Pattern INTEGRAL_VALUE = Pattern.compile("-?\\d+");
  private static final DateTimeFormatter TIMESTAMP_FORMATTER =
      new DateTimeFormatterBuilder()
          .append(DateTimeFormatter.ISO_LOCAL_DATE)
          .appendPattern("[' ']['T']")
          .append(DateTimeFormatter.ISO_LOCAL_TIME)
          .optionalStart()
          .appendOffsetId()
          .optionalEnd()
          .toFormatter();
  @NonNull protected final Map<String, String> pathToPartitionFieldFormat;

  public List<PartitionValue> extractPartitionValues(
      List<InternalPartitionField> partitionColumns, String partitionPath) {
    if (partitionColumns == null) {
      return Collections.emptyList();
    }
    int totalNumberOfPartitions = partitionColumns.size();
    List<PartitionValue> result = new ArrayList<>(totalNumberOfPartitions);
    String remainingPartitionPath = partitionPath;
    for (InternalPartitionField partitionField : partitionColumns) {
      String sourceFieldName = partitionField.getSourceField().getPath();
      if (remainingPartitionPath.startsWith(sourceFieldName + "=")) {
        // Strip off hive style partitioning
        remainingPartitionPath = remainingPartitionPath.substring(sourceFieldName.length() + 1);
      }
      // handle hive default partition case
      PartialResult valueAndRemainingPath;
      if (remainingPartitionPath.startsWith(HIVE_DEFAULT_PARTITION)) {
        String remaining =
            remainingPartitionPath.length() > HIVE_DEFAULT_PARTITION.length()
                ? remainingPartitionPath.substring(HIVE_DEFAULT_PARTITION.length() + 1)
                : "";
        valueAndRemainingPath = new PartialResult(null, remaining);
      } else {
        valueAndRemainingPath =
            parsePartitionPath(partitionField, remainingPartitionPath, totalNumberOfPartitions);
      }
      result.add(
          PartitionValue.builder()
              .partitionField(partitionField)
              .range(Range.scalar(valueAndRemainingPath.getValue()))
              .build());
      remainingPartitionPath = valueAndRemainingPath.getRemainingPath();
    }
    return result;
  }

  private PartialResult parsePartitionPath(
      InternalPartitionField field, String remainingPath, int totalNumberOfPartitions) {
    switch (field.getTransformType()) {
      case YEAR:
      case MONTH:
      case DAY:
      case HOUR:
        return parseDate(
            remainingPath, pathToPartitionFieldFormat.get(field.getSourceField().getPath()));
      case VALUE:
        // if there is only one partition field, then assume full partition path is used even if
        // value contains slashes
        // this case is possible if user is directly relying on directly _hoodie_partition_path due
        // to custom partitioning logic
        boolean isSlashDelimited = totalNumberOfPartitions > 1;
        return parseValue(remainingPath, field.getSourceField().getSchema(), isSlashDelimited);
      default:
        throw new IllegalArgumentException(
            "Unexpected partition type: " + field.getTransformType());
    }
  }

  protected static PartialResult parseDate(String remainingPath, String format) {
    try {
      String dateString = remainingPath.substring(0, format.length());
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat(format);
      simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
      long dateAsEpochMillis = simpleDateFormat.parse(dateString).toInstant().toEpochMilli();
      return new PartialResult(
          dateAsEpochMillis,
          remainingPath.substring(Math.min(remainingPath.length(), format.length() + 1)));
    } catch (ParseException ex) {
      throw new PartitionValuesExtractorException(
          "Unable to parse date from path: " + remainingPath, ex);
    }
  }

  protected static PartialResult parseValue(
      String remainingPath, InternalSchema sourceFieldSchema, boolean isSlashDelimited) {
    if (remainingPath.isEmpty()) {
      throw new PartitionValuesExtractorException("Missing partition value");
    }
    int endCharIndex;
    if (isSlashDelimited) {
      int slashIndex = remainingPath.indexOf("/");
      endCharIndex = slashIndex == -1 ? remainingPath.length() : slashIndex;
    } else {
      endCharIndex = remainingPath.length();
    }
    String valueAsString = remainingPath.substring(0, endCharIndex);
    String unParsedPath =
        remainingPath.substring(Math.min(endCharIndex + 1, remainingPath.length()));
    Object parsedValue;
    InternalType sourceFieldType = sourceFieldSchema.getDataType();
    switch (sourceFieldType) {
      case STRING:
      case ENUM:
        parsedValue = valueAsString;
        break;
      case INT:
        parsedValue = Integer.parseInt(valueAsString);
        break;
      case DATE:
        parsedValue = parseDateValue(valueAsString);
        break;
      case LONG:
        parsedValue = Long.parseLong(valueAsString);
        break;
      case TIMESTAMP:
      case TIMESTAMP_NTZ:
        parsedValue = parseTimestampValue(valueAsString, sourceFieldSchema);
        break;
      case DOUBLE:
        parsedValue = Double.parseDouble(valueAsString);
        break;
      case FLOAT:
        parsedValue = Float.parseFloat(valueAsString);
        break;
      case DECIMAL:
        parsedValue = new BigDecimal(valueAsString);
        break;
      case FIXED:
      case BYTES:
      case UUID:
        parsedValue = valueAsString.getBytes(StandardCharsets.UTF_8);
        break;
      case BOOLEAN:
        parsedValue = Boolean.parseBoolean(valueAsString);
        break;
      default:
        throw new PartitionValuesExtractorException(
            "Unexpected source field type in partition parser: " + sourceFieldType);
    }
    return new PartialResult(parsedValue, unParsedPath);
  }

  /** Days since epoch, from either a number of days or an ISO date such as 2024-02-29. */
  private static int parseDateValue(String value) {
    if (INTEGRAL_VALUE.matcher(value).matches()) {
      return Integer.parseInt(value);
    }
    try {
      return (int) LocalDate.parse(value).toEpochDay();
    } catch (DateTimeParseException ex) {
      throw new PartitionValuesExtractorException("Unable to parse date from path: " + value, ex);
    }
  }

  /**
   * Time since epoch in the field's timestamp precision, from either a number in that precision or
   * a date and time such as 2024-02-29 10:30:00, which may be path escaped and carry an offset.
   */
  private static long parseTimestampValue(String value, InternalSchema fieldSchema) {
    if (INTEGRAL_VALUE.matcher(value).matches()) {
      return Long.parseLong(value);
    }
    String unescaped = PartitionPathEncodeUtils.unescapePathName(value);
    Instant instant;
    try {
      TemporalAccessor parsed =
          TIMESTAMP_FORMATTER.parseBest(unescaped, OffsetDateTime::from, LocalDateTime::from);
      if (parsed instanceof OffsetDateTime) {
        instant = ((OffsetDateTime) parsed).toInstant();
      } else {
        // Hudi's SQL key generator writes a timestamp as wall-clock time in the writer JVM's
        // default time zone, while a timestamp without time zone has no zone to apply.
        ZoneId zone =
            fieldSchema.getDataType() == InternalType.TIMESTAMP_NTZ
                ? ZoneOffset.UTC
                : ZoneId.systemDefault();
        instant = ((LocalDateTime) parsed).atZone(zone).toInstant();
      }
    } catch (DateTimeParseException ex) {
      throw new PartitionValuesExtractorException(
          "Unable to parse timestamp from path: " + value, ex);
    }
    Object precision =
        fieldSchema.getMetadata() == null
            ? null
            : fieldSchema.getMetadata().get(InternalSchema.MetadataKey.TIMESTAMP_PRECISION);
    if (precision == InternalSchema.MetadataValue.MILLIS) {
      return instant.toEpochMilli();
    }
    if (precision == InternalSchema.MetadataValue.NANOS) {
      return TimeUnit.SECONDS.toNanos(instant.getEpochSecond()) + instant.getNano();
    }
    return TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
        + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
  }

  @Value
  protected static class PartialResult {
    Object value;
    String remainingPath;
  }
}
