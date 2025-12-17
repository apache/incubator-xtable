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
 
package org.apache.hudi.stats;

import static org.apache.xtable.model.schema.InternalSchema.MetadataKey.TIMESTAMP_PRECISION;
import static org.apache.xtable.model.schema.InternalSchema.MetadataValue.MICROS;

import java.lang.reflect.Constructor;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;

import org.apache.hudi.metadata.HoodieIndexVersion;

import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.stat.ColumnStat;

public class XTableValueMetadata {

  public static ValueMetadata getValueMetadata(
      ColumnStat columnStat, HoodieIndexVersion indexVersion) {
    if (indexVersion.lowerThan(HoodieIndexVersion.V2)) {
      return ValueMetadata.V1EmptyMetadata.get();
    }
    if (columnStat == null) {
      throw new IllegalArgumentException("ColumnStat cannot be null");
    }
    InternalSchema internalSchema = columnStat.getField().getSchema();
    ValueType valueType = fromInternalSchema(internalSchema);
    if (valueType == ValueType.V1) {
      throw new IllegalStateException(
          "InternalType V1 should not be returned from fromInternalSchema");
    } else if (valueType == ValueType.DECIMAL) {
      if (internalSchema.getMetadata() == null) {
        throw new IllegalArgumentException("Decimal metadata is null");
      } else if (!internalSchema
          .getMetadata()
          .containsKey(InternalSchema.MetadataKey.DECIMAL_SCALE)) {
        throw new IllegalArgumentException("Decimal scale is null");
      } else if (!internalSchema
          .getMetadata()
          .containsKey(InternalSchema.MetadataKey.DECIMAL_PRECISION)) {
        throw new IllegalArgumentException("Decimal precision is null");
      }
      int scale = (int) internalSchema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_SCALE);
      int precision =
          (int) internalSchema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_PRECISION);
      return ValueMetadata.DecimalMetadata.create(precision, scale);
    } else {
      return createValueMetadata(valueType);
    }
  }

  static ValueType fromInternalSchema(InternalSchema internalSchema) {
    switch (internalSchema.getDataType()) {
      case NULL:
        return ValueType.NULL;
      case BOOLEAN:
        return ValueType.BOOLEAN;
      case INT:
        return ValueType.INT;
      case LONG:
        return ValueType.LONG;
      case FLOAT:
        return ValueType.FLOAT;
      case DOUBLE:
        return ValueType.DOUBLE;
      case STRING:
        return ValueType.STRING;
      case BYTES:
        return ValueType.BYTES;
      case FIXED:
        return ValueType.FIXED;
      case DECIMAL:
        return ValueType.DECIMAL;
      case UUID:
        return ValueType.UUID;
      case DATE:
        return ValueType.DATE;
      case TIMESTAMP:
        if (internalSchema.getMetadata() != null
            && MICROS == internalSchema.getMetadata().get(TIMESTAMP_PRECISION)) {
          return ValueType.TIMESTAMP_MICROS;
        } else {
          return ValueType.TIMESTAMP_MILLIS;
        }
      case TIMESTAMP_NTZ:
        if (internalSchema.getMetadata() != null
            && MICROS == internalSchema.getMetadata().get(TIMESTAMP_PRECISION)) {
          return ValueType.LOCAL_TIMESTAMP_MICROS;
        } else {
          return ValueType.LOCAL_TIMESTAMP_MILLIS;
        }
      default:
        throw new UnsupportedOperationException(
            "InternalType " + internalSchema.getDataType() + " is not supported");
    }
  }

  // only for testing
  public static ValueMetadata getValueMetadata(
      ValueType valueType, HoodieIndexVersion indexVersion) {
    if (indexVersion.lowerThan(HoodieIndexVersion.V2)) {
      return ValueMetadata.V1EmptyMetadata.get();
    }
    return createValueMetadata(valueType);
  }

  /**
   * Creates a ValueMetadata instance using reflection to access the protected constructor. This is
   * necessary because XTable classes may be loaded by a different classloader than Hudi classes in
   * Spark environments, making direct constructor access illegal.
   */
  private static ValueMetadata createValueMetadata(ValueType valueType) {
    try {
      Constructor<ValueMetadata> constructor =
          ValueMetadata.class.getDeclaredConstructor(ValueType.class);
      constructor.setAccessible(true);
      return constructor.newInstance(valueType);
    } catch (Exception e) {
      throw new RuntimeException(
          "Failed to create ValueMetadata instance for type: " + valueType, e);
    }
  }

  public static Comparable<?> convertHoodieTypeToRangeType(
      Comparable<?> val, ValueMetadata valueMetadata) {
    if (val instanceof Instant) {
      if (valueMetadata.getValueType().equals(ValueType.TIMESTAMP_MILLIS)) {
        return ValueType.fromTimestampMillis(val, valueMetadata);
      } else if (valueMetadata.getValueType().equals(ValueType.TIMESTAMP_MICROS)) {
        return ValueType.fromTimestampMicros(val, valueMetadata);
      } else {
        throw new IllegalArgumentException(
            "Unsupported value type: " + valueMetadata.getValueType());
      }
    } else if (val instanceof LocalDateTime) {
      if (valueMetadata.getValueType().equals(ValueType.LOCAL_TIMESTAMP_MILLIS)) {
        return ValueType.fromLocalTimestampMillis(val, valueMetadata);
      } else if (valueMetadata.getValueType().equals(ValueType.LOCAL_TIMESTAMP_MICROS)) {
        return ValueType.fromLocalTimestampMicros(val, valueMetadata);
      } else {
        throw new IllegalArgumentException(
            "Unsupported value type: " + valueMetadata.getValueType());
      }
    } else if (val instanceof LocalDate) {
      if (valueMetadata.getValueType().equals(ValueType.DATE)) {
        return ValueType.fromDate(val, valueMetadata);
      } else {
        throw new IllegalArgumentException(
            "Unsupported value type: " + valueMetadata.getValueType());
      }
    } else {
      return val;
    }
  }
}
