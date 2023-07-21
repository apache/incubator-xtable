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
 
package io.onetable.model.stat;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.onetable.model.exception.OneParseException;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.storage.OneDataFile;

/**
 * Represents a range of values in the specified data type. Can represent a scalar value when the
 * {@link #minValue} and {@link #maxValue} are the same.
 *
 * <p>For the ranges stored in {@link OneDataFile#getPartitionValues()}, the values will be based
 * off the {@link PartitionTransformType}. {@link PartitionTransformType#HOUR}, {@link
 * PartitionTransformType#DAY}, {@link PartitionTransformType#MONTH}, {@link
 * PartitionTransformType#YEAR} are all stored as a long representing a point in time as
 * milliseconds since epoch. {@link PartitionTransformType#VALUE} will match the rules below.
 *
 * <p>The minValue and maxValue object type will match the underlying OneType (INT is integer,
 * DOUBLE is double, etc.) except for these cases:
 *
 * <ul>
 *   <li>{@link OneType#TIMESTAMP} will be stored as a long represent millis or micros since epoch
 *       depending on the {@link OneSchema.MetadataKey#TIMESTAMP_PRECISION}'s value ({@link
 *       OneSchema.MetadataValue#MICROS} or {@link OneSchema.MetadataValue#MILLIS})
 *   <li>{@link OneType#TIMESTAMP_NTZ} will be stored as a long represent millis or micros since
 *       epoch depending on the {@link OneSchema.MetadataKey#TIMESTAMP_PRECISION}'s value ({@link
 *       OneSchema.MetadataValue#MICROS} or {@link OneSchema.MetadataValue#MILLIS})
 *   <li>{@link OneType#DATE} will be stored as an integer representing days since epoch
 *   <li>{@link OneType#ENUM} will be stored as a string
 *   <li>{@link OneType#FIXED} will be stored as a {@link java.nio.ByteBuffer}
 * </ul>
 *
 * @since 0.1
 */
@Value
public class Range {
  RangeType rangeType;

  @Getter(AccessLevel.PRIVATE)
  @JsonProperty
  ValueType valueType;

  Object minValue;
  Object maxValue;

  private Range(RangeType rangeType, ValueType valueType, Object minValue, Object maxValue) {
    this.rangeType = rangeType;
    this.valueType = valueType;
    this.minValue = minValue;
    this.maxValue = maxValue;
  }

  public static Range scalar(Object value) {
    return new Range(RangeType.SCALAR, getValueTypeForObject(value), value, value);
  }

  public static Range vector(Object minValue, Object maxValue) {
    return new Range(
        RangeType.VECTOR, getValueTypeForObject(minValue, maxValue), minValue, maxValue);
  }

  private enum RangeType {
    SCALAR,
    VECTOR
  }

  @AllArgsConstructor
  private enum ValueType {
    STRING(String.class),
    INTEGER(Integer.class),
    LONG(Long.class),
    DOUBLE(Double.class),
    FLOAT(Float.class),
    BIG_DECIMAL(BigDecimal.class),
    BYTE_BUFFER(ByteBuffer.class),
    BOOLEAN(Boolean.class);

    @Getter private final Class<?> typeClass;
  }

  private static final Map<Class<?>, ValueType> VALUE_TYPE_MAP =
      Arrays.stream(ValueType.values())
          .collect(Collectors.toMap(ValueType::getTypeClass, Function.identity()));

  private static ValueType getValueTypeForObject(Object... objects) {
    for (Object object : objects) {
      if (object != null) {
        Class<?> clazz = object.getClass();
        if (VALUE_TYPE_MAP.containsKey(clazz)) {
          return VALUE_TYPE_MAP.get(clazz);
        } else {
          for (ValueType valueType : ValueType.values()) {
            if (valueType.getTypeClass().isAssignableFrom(clazz)) {
              return valueType;
            }
          }
          throw new OneParseException("Unhandled ValueType for class " + clazz);
        }
      }
    }
    return null;
  }

  /**
   * A method for parsing from json while preserving the original minValue and maxValue java types.
   * Since we use Object to represent the minValue and maxValue, Jackson will not necessarily parse
   * the objects into their desired types, so it needs to be handled manually here.
   *
   * @param rangeType the type of range (scalar or vector)
   * @param valueType the type for the minValue and maxValue
   * @param minValue the minValue as an object parsed by Jackson
   * @param maxValue the maxValue as an object parsed by Jackson
   * @return the Range with the minValue and maxValue object types matching the valueType
   */
  @SuppressWarnings("unused")
  @JsonCreator
  static Range fromJson(
      @JsonProperty("rangeType") RangeType rangeType,
      @JsonProperty("valueType") ValueType valueType,
      @JsonProperty("minValue") Object minValue,
      @JsonProperty("maxValue") Object maxValue) {
    Object typedMinValue = minValue;
    Object typedMaxValue = maxValue;
    if (valueType != null) {
      switch (valueType) {
        case FLOAT:
          // float is parsed as double
          typedMinValue = ((Double) minValue).floatValue();
          typedMaxValue = ((Double) maxValue).floatValue();
          break;
        case BIG_DECIMAL:
          // BigDecimal is parsed as double or long
          if (minValue instanceof Long) {
            typedMinValue = BigDecimal.valueOf((Long) minValue);
          } else {
            typedMinValue = BigDecimal.valueOf((Double) minValue);
          }
          if (maxValue instanceof Long) {
            typedMaxValue = BigDecimal.valueOf((Long) maxValue);
          } else {
            typedMaxValue = BigDecimal.valueOf((Double) maxValue);
          }
          break;
        case BYTE_BUFFER:
          typedMinValue = ByteBuffer.wrap(Base64.getDecoder().decode(minValue.toString()));
          typedMaxValue = ByteBuffer.wrap(Base64.getDecoder().decode(maxValue.toString()));
          break;
        case LONG:
          if (minValue instanceof Integer) {
            typedMinValue = ((Integer) minValue).longValue();
          }
          if (maxValue instanceof Integer) {
            typedMaxValue = ((Integer) maxValue).longValue();
          }
      }
    }
    return new Range(rangeType, valueType, typedMinValue, typedMaxValue);
  }
}
