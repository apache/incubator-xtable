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
 
package io.onetable.delta;

import java.sql.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;

/**
 * DeltaValueSerializer is specialized in serializing column stats and partition values depending on
 * the data type and partition transform type.
 */
public class DeltaValueSerializer {
  private static final String DATE_FORMAT_STR = "yyyy-MM-dd HH:mm:ss";

  public static Object getFormattedValueForColumnStats(Object value, OneSchema fieldSchema) {
    if (value == null) {
      return null;
    }
    // Needs special handling for date and time.
    OneType fieldType = fieldSchema.getDataType();
    if (fieldType != OneType.DATE
        && fieldType != OneType.TIMESTAMP
        && fieldType != OneType.TIMESTAMP_NTZ) {
      return value;
    }
    if (fieldType == OneType.DATE) {
      return LocalDate.ofEpochDay((int) value).toString();
    }
    OneSchema.MetadataValue timestampPrecision =
        (OneSchema.MetadataValue)
            fieldSchema
                .getMetadata()
                .getOrDefault(
                    OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MILLIS);
    long millis =
        timestampPrecision == OneSchema.MetadataValue.MICROS
            ? TimeUnit.MILLISECONDS.convert((Long) value, TimeUnit.MICROSECONDS)
            : (long) value;
    DateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STR);
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    return dateFormat.format(Date.from(Instant.ofEpochMilli(millis)));
  }

  public static String getFormattedValueForPartition(
      Object value,
      OneType fieldType,
      PartitionTransformType partitionTransformType,
      String dateFormat) {
    if (value == null) {
      return null;
    }
    if (partitionTransformType == PartitionTransformType.VALUE) {
      if (fieldType == OneType.DATE) {
        return LocalDate.ofEpochDay((int) value).toString();
      } else {
        return value.toString();
      }
    } else {
      // use appropriate date formatter for value serialization.
      DateFormat formatter = new SimpleDateFormat(dateFormat);
      formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
      return formatter.format(Date.from(Instant.ofEpochMilli((long) value)));
    }
  }
}
