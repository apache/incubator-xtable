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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import io.onetable.exception.NotSupportedException;
import io.onetable.exception.OneIOException;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;

/**
 * DeltaValueConverter is specialized in (de)serializing column stats and partition values depending
 * on the data type and partition transform type.
 */
public class DeltaValueConverter {
  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final DateTimeFormatter DATE_TIME_FORMATTER =
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(UTC);
  private static final TimeZone TIME_ZONE = TimeZone.getTimeZone("UTC");

  public static Object convertFromDeltaColumnStatValue(Object value, OneSchema fieldSchema) {
    if (value == null) {
      return null;
    }
    if (noConversionForSchema(fieldSchema)) {
      return castObjectToInternalType(value, fieldSchema.getDataType());
    }
    // Needs special handling for date and time.
    OneType fieldType = fieldSchema.getDataType();
    if (fieldType == OneType.DATE) {
      return (int) LocalDate.parse(value.toString()).toEpochDay();
    }

    Instant instant;
    try {
      instant = OffsetDateTime.parse(value.toString()).toInstant();
    } catch (DateTimeParseException parseException) {
      // fall back to parsing without offset
      try {
        instant =
            LocalDateTime.parse(value.toString(), DATE_TIME_FORMATTER).atZone(UTC).toInstant();
      } catch (DateTimeParseException ex) {
        throw new OneIOException("Unable to parse time from column stats", ex);
      }
    }
    OneSchema.MetadataValue timestampPrecision =
        (OneSchema.MetadataValue)
            fieldSchema
                .getMetadata()
                .getOrDefault(
                    OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MICROS);
    if (timestampPrecision == OneSchema.MetadataValue.MILLIS) {
      return instant.toEpochMilli();
    }
    return TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
        + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
  }

  public static Object convertToDeltaColumnStatValue(Object value, OneSchema fieldSchema) {
    if (value == null) {
      return null;
    }
    if (noConversionForSchema(fieldSchema)) {
      return value;
    }
    // Needs special handling for date and time.
    OneType fieldType = fieldSchema.getDataType();
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
    return DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(millis));
  }

  private static boolean noConversionForSchema(OneSchema fieldSchema) {
    OneType fieldType = fieldSchema.getDataType();
    return fieldType != OneType.DATE
        && fieldType != OneType.TIMESTAMP
        && fieldType != OneType.TIMESTAMP_NTZ;
  }

  public static String convertToDeltaPartitionValue(
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
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateFormat).withZone(UTC);
      return formatter.format(Instant.ofEpochMilli((long) value));
    }
  }

  public static Object convertFromDeltaPartitionValue(
      String value,
      OneType fieldType,
      PartitionTransformType partitionTransformType,
      String dateFormat) {
    if (value == null) {
      return null;
    }
    if (partitionTransformType == PartitionTransformType.VALUE) {
      switch (fieldType) {
        case DATE:
          return (int) LocalDate.parse(value).toEpochDay();
        case INT:
          return Integer.parseInt(value);
        case LONG:
          return Long.parseLong(value);
        case DOUBLE:
          return Double.parseDouble(value);
        case FLOAT:
          return Float.parseFloat(value);
        case STRING:
        case ENUM:
          return value;
        case DECIMAL:
          return new BigDecimal(value);
        case BYTES:
        case FIXED:
          return value.getBytes(StandardCharsets.UTF_8);
        case BOOLEAN:
          return Boolean.parseBoolean(value);
        default:
          throw new NotSupportedException("Unsupported partition value type: " + fieldType);
      }
    } else {
      // use appropriate date formatter for value serialization.
      try {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateFormat);
        LocalDateTime localDateTime = LocalDateTime.parse(value, formatter);
        return localDateTime.atZone(UTC).toInstant().toEpochMilli();
      } catch (DateTimeParseException ex) {
        throw new OneIOException("Unable to parse partition value", ex);
      }
    }
  }

  private static Object castObjectToInternalType(Object value, OneType valueType) {
    switch (valueType) {
      case FLOAT:
        if (value instanceof Double) {
          return ((Double) value).floatValue();
        }
        break;
      case DECIMAL:
        return numberTypeToBigDecimal(value);
      case LONG:
        if (value instanceof Integer) {
          return ((Integer) value).longValue();
        }
        break;
    }
    return value;
  }

  private static BigDecimal numberTypeToBigDecimal(Object value) {
    // BigDecimal is parsed as Integer, Long, BigInteger and double if none of the above.
    if (value instanceof Integer) {
      return BigDecimal.valueOf((Integer) value);
    } else if (value instanceof Long) {
      return BigDecimal.valueOf((Long) value);
    } else if (value instanceof BigInteger) {
      return new BigDecimal((BigInteger) value);
    } else if (value instanceof Double) {
      return BigDecimal.valueOf((Double) value);
    } else {
      return (BigDecimal) value;
    }
  }
}
