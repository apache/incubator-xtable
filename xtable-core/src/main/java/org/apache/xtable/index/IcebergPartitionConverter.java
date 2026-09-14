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
 
package org.apache.xtable.index;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;

import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;

/**
 * Converts Iceberg partition values to the Spark {@link InternalRow} representation Iceberg's own
 * Spark reader exposes through the {@code _partition} metadata column.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class IcebergPartitionConverter {

  /**
   * Parses a partition path (for example {@code year=2024/month=01}) with the given spec and
   * converts the values to an {@link InternalRow}.
   *
   * @return the partition row, or null when the table is unpartitioned
   */
  public static InternalRow convertPartitionToInternalRow(
      String partitionPath, Types.StructType partitionType, PartitionSpec spec) {
    if (partitionPath == null || partitionPath.isEmpty() || partitionType.fields().isEmpty()) {
      return null;
    }
    return convertStructLikeToInternalRow(DataFiles.data(spec, partitionPath), partitionType);
  }

  /** Converts an Iceberg struct to a Spark row using Iceberg's Spark type mapping. */
  public static InternalRow convertStructLikeToInternalRow(
      StructLike struct, Types.StructType structType) {
    List<Types.NestedField> fields = structType.fields();
    Object[] values = new Object[fields.size()];
    for (int index = 0; index < fields.size(); index++) {
      Type fieldType = fields.get(index).type();
      values[index] = convertValue(fieldType, struct.get(index, fieldType.typeId().javaClass()));
    }
    return new GenericInternalRow(values);
  }

  private static Object convertValue(Type type, Object value) {
    if (value == null) {
      return null;
    }
    switch (type.typeId()) {
      case DECIMAL:
        return Decimal.apply((BigDecimal) value);
      case STRING:
        if (value instanceof Utf8) {
          Utf8 utf8 = (Utf8) value;
          return UTF8String.fromBytes(utf8.getBytes(), 0, utf8.getByteLength());
        }
        return UTF8String.fromString(value.toString());
      case FIXED:
        if (value instanceof byte[]) {
          return value;
        }
        if (value instanceof GenericData.Fixed) {
          return ((GenericData.Fixed) value).bytes();
        }
        return ByteBuffers.toByteArray((ByteBuffer) value);
      case BINARY:
        return ByteBuffers.toByteArray((ByteBuffer) value);
      case STRUCT:
        return convertStructLikeToInternalRow((StructLike) value, (Types.StructType) type);
      default:
        return value;
    }
  }
}
