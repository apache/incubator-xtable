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

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.avro.generic.IndexedRecord;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;

import io.onetable.avro.AvroSchemaConverter;
import io.onetable.exception.NotSupportedException;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.stat.Range;

/** Partition value extractor for Iceberg. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IcebergPartitionValueConverter {
  private static final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);
  private static final IcebergPartitionValueConverter INSTANCE =
      new IcebergPartitionValueConverter();
  private static final AvroSchemaConverter SCHEMA_CONVERTER = AvroSchemaConverter.getInstance();
  private static final String DOT = ".";
  private static final String DOT_REPLACEMENT = "_x2E";
  private static final String YEAR = "year";
  private static final String MONTH = "month";
  private static final String DAY = "day";
  private static final String HOUR = "hour";
  private static final String IDENTITY = "identity";

  public static IcebergPartitionValueConverter getInstance() {
    return INSTANCE;
  }

  public Map<OnePartitionField, Range> toOneTable(
      StructLike structLike, PartitionSpec partitionSpec) {
    if (!partitionSpec.isPartitioned()) {
      return Collections.emptyMap();
    }
    Map<OnePartitionField, Range> partitionValues = new HashMap<>();
    IndexedRecord partitionData = ((IndexedRecord) structLike);
    for (PartitionField partitionField : partitionSpec.fields()) {
      Object value;
      PartitionTransformType transformType;
      int fieldPosition =
          partitionData.getSchema().getField(escapeFieldName(partitionField.name())).pos();
      // Convert date based partitions into millis since epoch
      switch (partitionField.transform().toString()) {
        case YEAR:
          value =
              EPOCH
                  .plusYears(structLike.get(fieldPosition, Integer.class))
                  .toInstant()
                  .toEpochMilli();
          transformType = PartitionTransformType.YEAR;
          break;
        case MONTH:
          value =
              EPOCH
                  .plusMonths(structLike.get(fieldPosition, Integer.class))
                  .toInstant()
                  .toEpochMilli();
          transformType = PartitionTransformType.MONTH;
          break;
        case DAY:
          value =
              EPOCH
                  .plusDays(structLike.get(fieldPosition, Integer.class))
                  .toInstant()
                  .toEpochMilli();
          transformType = PartitionTransformType.DAY;
          break;
        case HOUR:
          value =
              EPOCH
                  .plusHours(structLike.get(fieldPosition, Integer.class))
                  .toInstant()
                  .toEpochMilli();
          transformType = PartitionTransformType.HOUR;
          break;
        case IDENTITY:
          value = structLike.get(fieldPosition, Object.class);
          transformType = PartitionTransformType.VALUE;
          break;
        default:
          throw new NotSupportedException(
              "Partition transform not supported: " + partitionField.transform().toString());
      }
      OneSchema fieldSchema =
          SCHEMA_CONVERTER.toOneSchema(
              partitionData.getSchema().getFields().get(fieldPosition).schema());
      int splitPoint = partitionField.name().lastIndexOf(DOT);
      String fieldName = partitionField.name().substring(splitPoint + 1);
      String path = splitPoint == -1 ? null : partitionField.name().substring(0, splitPoint);
      OneField sourceField =
          OneField.builder().name(fieldName).parentPath(path).schema(fieldSchema).build();
      OnePartitionField onePartitionField =
          OnePartitionField.builder().sourceField(sourceField).transformType(transformType).build();
      partitionValues.put(onePartitionField, Range.scalar(value));
    }
    return partitionValues;
  }

  private static String escapeFieldName(String fieldName) {
    return fieldName.replace(DOT, DOT_REPLACEMENT);
  }

  public PartitionKey toIceberg(
      PartitionSpec partitionSpec, Schema schema, Map<OnePartitionField, Range> partitionValues) {
    if (partitionValues == null || partitionValues.isEmpty()) {
      return null;
    }
    Map<String, Map.Entry<OnePartitionField, Range>> nameToPartitionInfo =
        partitionValues.entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> entry.getKey().getSourceField().getName(), Function.identity()));
    PartitionKey partitionKey = new PartitionKey(partitionSpec, schema);
    for (int i = 0; i < partitionSpec.fields().size(); i++) {
      PartitionField icebergPartitionField = partitionSpec.fields().get(i);
      String sourceFieldName = schema.findField(icebergPartitionField.sourceId()).name();
      Map.Entry<OnePartitionField, Range> partitionInfo = nameToPartitionInfo.get(sourceFieldName);
      switch (partitionInfo.getKey().getTransformType()) {
        case YEAR:
          partitionKey.set(
              i,
              Transforms.year(Types.TimestampType.withoutZone())
                  .apply(millisToMicros((Long) partitionInfo.getValue().getMaxValue())));
          break;
        case MONTH:
          partitionKey.set(
              i,
              Transforms.month(Types.TimestampType.withoutZone())
                  .apply(millisToMicros((Long) partitionInfo.getValue().getMaxValue())));
          break;
        case DAY:
          partitionKey.set(
              i,
              Transforms.day(Types.TimestampType.withoutZone())
                  .apply(millisToMicros((Long) partitionInfo.getValue().getMaxValue())));
          break;
        case HOUR:
          partitionKey.set(
              i,
              Transforms.hour(Types.TimestampType.withoutZone())
                  .apply(millisToMicros((Long) partitionInfo.getValue().getMaxValue())));
          break;
        case VALUE:
          partitionKey.set(
              i,
              Transforms.identity(Types.StringType.get())
                  .apply(partitionInfo.getValue().getMaxValue()));
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported type: " + partitionInfo.getKey().getTransformType());
      }
    }
    return partitionKey;
  }

  private static Long millisToMicros(Long millis) {
    if (millis == null) {
      return null;
    }
    return TimeUnit.MILLISECONDS.toMicros(millis);
  }
}
