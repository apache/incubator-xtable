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
 
package org.apache.xtable.parquet;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import lombok.Value;

import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import org.apache.xtable.exception.PartitionValuesExtractorException;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;

/** Partition value extractor for Parquet. */
// Extracts the partitionFields and values and create InputParitionFields object (fields and types)
// then convert those to InternalPartitionField for the ConversionSource
// @NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ParquetPartitionValueExtractor {
  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final ParquetPartitionValueExtractor INSTANCE =
      new ParquetPartitionValueExtractor();
  private static final ParquetSchemaExtractor schemaExtractor =
      ParquetSchemaExtractor.getInstance();
  private static final ParquetMetadataExtractor parquetMetadataExtractor =
      ParquetMetadataExtractor.getInstance();

  private static final ParquetPartitionSpecExtractor partitionsSpecExtractor =
      ParquetPartitionSpecExtractor.getInstance();

  public static ParquetPartitionValueExtractor getInstance() {
    return INSTANCE;
  }

  private static final String HIVE_DEFAULT_PARTITION = "__HIVE_DEFAULT_PARTITION__";
  private final Map<String, String> pathToPartitionFieldFormat = null;

  public List<InternalPartitionField> extractParquertPartitions(
      ParquetMetadata footer, String path) {
    MessageType parquetSchema = parquetMetadataExtractor.getSchema(footer);
    InternalSchema internalSchema = schemaExtractor.toInternalSchema(parquetSchema, path);
    List<InternalPartitionField> partitions = partitionsSpecExtractor.spec(internalSchema);
    return partitions;
  }

  public List<PartitionValue> extractPartitionValues(
      List<InternalPartitionField> partitionColumns, String partitionPath) {
    if (partitionColumns == null) {
      return Collections.emptyList();
    }
    int totalNumberOfPartitions = partitionColumns.size();
    List<PartitionValue> result = new ArrayList<>(totalNumberOfPartitions);
    String remainingPartitionPath = partitionPath;
    for (InternalPartitionField partitionField : partitionColumns) {
      String sourceFieldName = partitionField.getSourceField().getName();
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
        boolean isSlashDelimited = totalNumberOfPartitions > 1;
        return parseValue(
            remainingPath, field.getSourceField().getSchema().getDataType(), isSlashDelimited);
      default:
        throw new IllegalArgumentException(
            "Unexpected partition type: " + field.getTransformType());
    }
  }

  private static PartialResult parseDate(String remainingPath, String format) {
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

  private static PartialResult parseValue(
      String remainingPath, InternalType sourceFieldType, boolean isSlashDelimited) {
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
    switch (sourceFieldType) {
      case STRING:
      case ENUM:
        parsedValue = valueAsString;
        break;
      case INT:
      case DATE:
        parsedValue = Integer.parseInt(valueAsString);
        break;
      case LONG:
      case TIMESTAMP:
      case TIMESTAMP_NTZ:
        parsedValue = Long.parseLong(valueAsString);
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

  @Value
  private static class PartialResult {
    Object value;
    String remainingPath;
  }
}
