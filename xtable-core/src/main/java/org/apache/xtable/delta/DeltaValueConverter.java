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
 
package org.apache.xtable.delta;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;

/**
 * DeltaValueConverter is specialized in (de)serializing column stats and partition values depending
 * on the data type and partition transform type.
 */
public class DeltaValueConverter {
  private static final String DATE_FORMAT_STR = "yyyy-MM-dd HH:mm:ss";
  private static final TimeZone TIME_ZONE = TimeZone.getTimeZone("UTC");

  static DateFormat getDateFormat(String dataFormatString) {
    DateFormat dateFormat = new SimpleDateFormat(dataFormatString);
    dateFormat.setLenient(false);
    dateFormat.setTimeZone(TIME_ZONE);
    return dateFormat;
  }

  public static Object convertFromDeltaColumnStatValue(Object value, InternalSchema fieldSchema) {
    if (value == null) {
      return null;
    }
    if (noConversionForSchema(fieldSchema)) {
      return castObjectToInternalType(value, fieldSchema.getDataType());
    }
    // Needs special handling for date and time.
    InternalType fieldType = fieldSchema.getDataType();
    if (fieldType == InternalType.DATE) {
      return (int) LocalDate.parse(value.toString()).toEpochDay();
    }

    Instant instant;
    try {
      instant = OffsetDateTime.parse(value.toString()).toInstant();
    } catch (DateTimeParseException parseException) {
      // fall back to parsing without offset
      DateFormat dateFormat = getDateFormat(DATE_FORMAT_STR);
      try {
        instant = dateFormat.parse(value.toString()).toInstant();
      } catch (ParseException ex) {
        throw new org.apache.xtable.model.exception.ParseException(
            "Unable to parse time from column stats", ex);
      }
    }
    InternalSchema.MetadataValue timestampPrecision =
        (InternalSchema.MetadataValue)
            fieldSchema
                .getMetadata()
                .getOrDefault(
                    InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                    InternalSchema.MetadataValue.MICROS);
    if (timestampPrecision == InternalSchema.MetadataValue.MILLIS) {
      return instant.toEpochMilli();
    }
    return TimeUnit.SECONDS.toMicros(instant.getEpochSecond())
        + TimeUnit.NANOSECONDS.toMicros(instant.getNano());
  }

  public static Object convertToDeltaColumnStatValue(Object value, InternalSchema fieldSchema) {
    if (value == null) {
      return null;
    }
    if (noConversionForSchema(fieldSchema)) {
      return value;
    }
    // Needs special handling for date and time.
    InternalType fieldType = fieldSchema.getDataType();
    if (fieldType == InternalType.DATE) {
      return LocalDate.ofEpochDay((int) value).toString();
    }
    InternalSchema.MetadataValue timestampPrecision =
        (InternalSchema.MetadataValue)
            fieldSchema
                .getMetadata()
                .getOrDefault(
                    InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                    InternalSchema.MetadataValue.MILLIS);
    long millis =
        timestampPrecision == InternalSchema.MetadataValue.MICROS
            ? TimeUnit.MILLISECONDS.convert((Long) value, TimeUnit.MICROSECONDS)
            : (long) value;
    DateFormat dateFormat = getDateFormat(DATE_FORMAT_STR);
    return dateFormat.format(Date.from(Instant.ofEpochMilli(millis)));
  }

  private static boolean noConversionForSchema(InternalSchema fieldSchema) {
    InternalType fieldType = fieldSchema.getDataType();
    return fieldType != InternalType.DATE
        && fieldType != InternalType.TIMESTAMP
        && fieldType != InternalType.TIMESTAMP_NTZ;
  }

  public static String convertToDeltaPartitionValue(
      Object value,
      InternalType fieldType,
      PartitionTransformType partitionTransformType,
      String dateFormat) {
    if (value == null) {
      return null;
    }
    if (partitionTransformType == PartitionTransformType.VALUE) {
      if (fieldType == InternalType.DATE) {
        return LocalDate.ofEpochDay((int) value).toString();
      } else {
        return value.toString();
      }
    } else {
      // use appropriate date formatter for value serialization.
      DateFormat formatter = getDateFormat(dateFormat);
      return formatter.format(Date.from(Instant.ofEpochMilli((long) value)));
    }
  }

  public static Object convertFromDeltaPartitionValue(
      String value,
      InternalType fieldType,
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
        DateFormat formatter = getDateFormat(dateFormat);
        return formatter.parse(value).toInstant().toEpochMilli();
      } catch (ParseException ex) {
        throw new org.apache.xtable.model.exception.ParseException(
            "Unable to parse partition value", ex);
      }
    }
  }

  private static Object castObjectToInternalType(Object value, InternalType valueType) {
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
