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
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

/**
 * Utility class for converting Parquet statistics from binary format to logical types. This class
 * provides safe type conversions that avoid ClassCastException by skipping complex or incompatible
 * type conversions.
 */
public class ParquetStatsConverterUtil {

  /**
   * Converts Parquet statistics from binary representation to their logical type representation.
   * This method performs type-safe conversions and returns null for types that cannot be safely
   * converted, preventing ClassCastException errors during stats extraction.
   *
   * <p>Supported conversions:
   *
   * <ul>
   *   <li>STRING (BINARY with STRING logical type) - converted to Java String
   *   <li>Primitive types (INT32, INT64, FLOAT, DOUBLE, BOOLEAN) - returned as-is if already
   *       primitives
   *   <li>DECIMAL - converted to BigDecimal using precision and scale from metadata
   * </ul>
   *
   * <p>Unsupported types that return null:
   *
   * <ul>
   *   <li>Binary-encoded primitives - indicates encoding mismatch
   *   <li>Complex BINARY types (JSON, BSON, UUID, etc.)
   *   <li>INT96 - complex temporal types (legacy timestamp format)
   * </ul>
   *
   * @param columnMetaData the Parquet column metadata containing statistics
   * @param isMin true to extract minimum value, false for maximum value
   * @return the converted logical value, or null if conversion is not supported or value is null
   */
  public static Object convertStatBinaryTypeToLogicalType(
      ColumnChunkMetaData columnMetaData, boolean isMin) {
    PrimitiveType primitiveType = columnMetaData.getPrimitiveType();
    LogicalTypeAnnotation logicalType = primitiveType.getLogicalTypeAnnotation();

    // Handle DECIMAL types with proper precision/scale conversion
    if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
      return convertDecimalStat(columnMetaData, isMin, logicalType);
    }

    // Extract the raw statistic value from Parquet metadata
    Object rawValue =
        isMin
            ? columnMetaData.getStatistics().genericGetMin()
            : columnMetaData.getStatistics().genericGetMax();

    if (rawValue == null) {
      return null;
    }

    // Perform type-specific conversion based on Parquet primitive type
    // Only safe conversions are performed to prevent runtime ClassCastException
    switch (primitiveType.getPrimitiveTypeName()) {
      case BINARY:
        // Handle BINARY type with STRING logical annotation
        if (logicalType != null && logicalType.toString().equals("STRING")) {
          if (rawValue instanceof Binary) {
            return new String(((Binary) rawValue).getBytes(), StandardCharsets.UTF_8);
          }
        }
        // Skip other BINARY logical types (JSON, BSON, UUID, etc.) as they require
        // specialized conversion logic
        return null;

      case INT32:
        // Regular INT32 - return as-is if not Binary
        // Note: DECIMAL stored as INT32 is already handled at the top level
        if (!(rawValue instanceof Binary)) {
          return rawValue;
        }
        return null;

      case INT64:
        // Regular INT64 - return as-is if not Binary
        // Note: DECIMAL stored as INT64 is already handled at the top level
        if (!(rawValue instanceof Binary)) {
          return rawValue;
        }
        return null;

      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        // These types should be returned as Java primitives by Parquet
        // If we receive a Binary object instead, it indicates an encoding mismatch
        // and we skip the conversion to prevent ClassCastException
        if (!(rawValue instanceof Binary)) {
          return rawValue;
        }
        return null;

      case FIXED_LEN_BYTE_ARRAY:
        // Note: DECIMAL stored as FIXED_LEN_BYTE_ARRAY is already handled at the top level
        // Other FIXED_LEN_BYTE_ARRAY types - skip to avoid conversion issues
        return null;

      case INT96:
        // INT96 is legacy timestamp format - skip to avoid conversion issues
        return null;

      default:
        // Unknown or unsupported Parquet type - skip for safety
        return null;
    }
  }

  /**
   * Converts DECIMAL statistics from Parquet format to BigDecimal. Handles DECIMAL values stored as
   * INT32, INT64, or FIXED_LEN_BYTE_ARRAY in Parquet.
   *
   * <p>Parquet DECIMAL storage formats:
   * <ul>
   *   <li>INT32: For precision <= 9, stored as signed integer
   *   <li>INT64: For precision <= 18, stored as signed long
   *   <li>FIXED_LEN_BYTE_ARRAY: For precision > 18, stored as binary (big-endian)
   * </ul>
   *
   * @param columnMetaData the Parquet column metadata containing statistics
   * @param isMin true to extract minimum value, false for maximum value
   * @param decimalType the DECIMAL logical type annotation with precision and scale
   * @return BigDecimal value, or null if conversion fails
   */
  private static Object convertDecimalStat(
      ColumnChunkMetaData columnMetaData,
      boolean isMin,
      LogicalTypeAnnotation decimalType) {
    try {
      LogicalTypeAnnotation.DecimalLogicalTypeAnnotation decimalAnnotation =
          (LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) decimalType;
      int scale = decimalAnnotation.getScale();

      // Extract the raw statistic value
      Object rawValue =
          isMin
              ? columnMetaData.getStatistics().genericGetMin()
              : columnMetaData.getStatistics().genericGetMax();

      if (rawValue == null) {
        return null;
      }

      PrimitiveType primitiveType = columnMetaData.getPrimitiveType();
      PrimitiveType.PrimitiveTypeName typeName = primitiveType.getPrimitiveTypeName();

      // Convert based on Parquet storage format
      switch (typeName) {
        case INT32:
          // DECIMAL stored as INT32 (precision <= 9)
          if (rawValue instanceof Integer) {
            return BigDecimal.valueOf((Integer) rawValue, scale);
          }
          return null;

        case INT64:
          // DECIMAL stored as INT64 (precision <= 18)
          if (rawValue instanceof Long) {
            return BigDecimal.valueOf((Long) rawValue, scale);
          }
          return null;

        case FIXED_LEN_BYTE_ARRAY:
        case BINARY:
          // DECIMAL stored as FIXED_LEN_BYTE_ARRAY or BINARY (precision > 18)
          // Values are stored as big-endian binary
          if (rawValue instanceof Binary) {
            Binary binaryValue = (Binary) rawValue;
            byte[] bytes = binaryValue.getBytes();
            // Parquet DECIMAL is stored as big-endian signed integer
            BigInteger unscaledValue = new BigInteger(bytes);
            return new BigDecimal(unscaledValue, scale);
          }
          return null;

        default:
          // Unsupported DECIMAL storage format
          return null;
      }
    } catch (Exception e) {
      // If conversion fails, return null to skip this statistic
      // This prevents ClassCastException and allows other stats to be extracted
      return null;
    }
  }
}
