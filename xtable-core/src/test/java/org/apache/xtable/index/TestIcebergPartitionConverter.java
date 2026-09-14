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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.unsafe.types.UTF8String;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

public class TestIcebergPartitionConverter {
  private static final byte[] BYTES = new byte[] {1, 2, 3};

  private static Stream<Arguments> singleFieldConversions() {
    return Stream.of(
        Arguments.of(Types.StringType.get(), "hello", UTF8String.fromString("hello")),
        Arguments.of(Types.StringType.get(), new Utf8("hello"), UTF8String.fromString("hello")),
        Arguments.of(
            Types.DecimalType.of(10, 2),
            new BigDecimal("123.45"),
            Decimal.apply(new BigDecimal("123.45"))),
        Arguments.of(Types.IntegerType.get(), 42, 42),
        Arguments.of(Types.LongType.get(), 123L, 123L),
        Arguments.of(Types.BooleanType.get(), true, true),
        Arguments.of(Types.DoubleType.get(), 3.14, 3.14),
        Arguments.of(Types.FloatType.get(), 2.5f, 2.5f),
        Arguments.of(Types.StringType.get(), null, null));
  }

  @ParameterizedTest
  @MethodSource("singleFieldConversions")
  void convertsSingleField(Type fieldType, Object value, Object expected) {
    Types.StructType structType =
        Types.StructType.of(Types.NestedField.optional(1, "field", fieldType));
    InternalRow row =
        IcebergPartitionConverter.convertStructLikeToInternalRow(
            new TestStructLike(value), structType);
    assertEquals(1, row.numFields());
    if (expected == null) {
      assertTrue(row.isNullAt(0));
    } else {
      assertEquals(expected, row.get(0, null));
    }
  }

  private static Stream<Arguments> binaryConversions() {
    return Stream.of(
        Arguments.of(Types.BinaryType.get(), ByteBuffer.wrap(BYTES)),
        Arguments.of(Types.FixedType.ofLength(3), ByteBuffer.wrap(BYTES)),
        Arguments.of(Types.FixedType.ofLength(3), BYTES),
        Arguments.of(
            Types.FixedType.ofLength(3),
            new GenericData.Fixed(Schema.createFixed("fixed", null, null, 3), BYTES)));
  }

  @ParameterizedTest
  @MethodSource("binaryConversions")
  void convertsBinaryField(Type fieldType, Object value) {
    Types.StructType structType =
        Types.StructType.of(Types.NestedField.required(1, "data", fieldType));
    InternalRow row =
        IcebergPartitionConverter.convertStructLikeToInternalRow(
            new TestStructLike(value), structType);
    assertArrayEquals(BYTES, row.getBinary(0));
  }

  @Test
  void convertsNestedStruct() {
    Types.StructType innerStructType =
        Types.StructType.of(Types.NestedField.required(1, "inner_value", Types.StringType.get()));
    Types.StructType outerStructType =
        Types.StructType.of(
            Types.NestedField.required(2, "outer_value", Types.IntegerType.get()),
            Types.NestedField.required(3, "nested", innerStructType));
    InternalRow row =
        IcebergPartitionConverter.convertStructLikeToInternalRow(
            new TestStructLike(100, new TestStructLike("inner")), outerStructType);
    assertEquals(2, row.numFields());
    assertEquals(100, row.getInt(0));
    assertEquals(UTF8String.fromString("inner"), row.getStruct(1, 1).getUTF8String(0));
  }

  @Test
  void convertsEmptyStruct() {
    InternalRow row =
        IcebergPartitionConverter.convertStructLikeToInternalRow(
            new TestStructLike(), Types.StructType.of());
    assertEquals(0, row.numFields());
  }

  @Test
  void convertsPartitionPath() {
    org.apache.iceberg.Schema schema =
        new org.apache.iceberg.Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.required(2, "year", Types.IntegerType.get()),
            Types.NestedField.required(3, "category", Types.StringType.get()));
    PartitionSpec spec =
        PartitionSpec.builderFor(schema).identity("year").identity("category").build();
    InternalRow row =
        IcebergPartitionConverter.convertPartitionToInternalRow(
            "year=2024/category=books", spec.partitionType(), spec);
    assertEquals(2, row.numFields());
    assertEquals(2024, row.getInt(0));
    assertEquals(UTF8String.fromString("books"), row.getUTF8String(1));
  }

  @Test
  void unpartitionedTableHasNoPartitionRow() {
    PartitionSpec spec = PartitionSpec.unpartitioned();
    assertNull(
        IcebergPartitionConverter.convertPartitionToInternalRow("", spec.partitionType(), spec));
    assertNull(
        IcebergPartitionConverter.convertPartitionToInternalRow(null, spec.partitionType(), spec));
  }

  private static class TestStructLike implements StructLike {
    private final Object[] values;

    TestStructLike(Object... values) {
      this.values = values;
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      return (T) values[pos];
    }

    @Override
    public <T> void set(int pos, T value) {
      values[pos] = value;
    }
  }
}
