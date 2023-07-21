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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;

public class TestDeltaValueSerializer {

  @ParameterizedTest
  @MethodSource("valuesWithSchemaProviderForColStats")
  public void formattedValueDifferentTypesForColStats(
      Object fieldValue, OneSchema fieldSchema, Object expectedValue) {
    assertEquals(
        expectedValue,
        DeltaValueSerializer.getFormattedValueForColumnStats(fieldValue, fieldSchema));
  }

  @ParameterizedTest
  @MethodSource("valuesWithSchemaProviderForPartitions")
  public void formattedValueDifferentTypesForPartition(
      Object fieldValue,
      OneType oneType,
      PartitionTransformType transformType,
      String dateFormat,
      String expectedValue) {
    assertEquals(
        expectedValue,
        DeltaValueSerializer.getFormattedValueForPartition(
            fieldValue, oneType, transformType, dateFormat));
  }

  private static Stream<Arguments> valuesWithSchemaProviderForColStats() {
    return Stream.of(
        Arguments.of(
            null, OneSchema.builder().name("string").dataType(OneType.STRING).build(), null),
        Arguments.of(
            "some value",
            OneSchema.builder().name("string").dataType(OneType.STRING).build(),
            "some value"),
        Arguments.of(23L, OneSchema.builder().name("long").dataType(OneType.LONG).build(), 23L),
        Arguments.of(
            25.5, OneSchema.builder().name("double").dataType(OneType.DOUBLE).build(), 25.5),
        Arguments.of(
            18181, OneSchema.builder().name("int").dataType(OneType.DATE).build(), "2019-10-12"),
        Arguments.of(
            true, OneSchema.builder().name("boolean").dataType(OneType.BOOLEAN).build(), true),
        Arguments.of(
            1665263297000L,
            OneSchema.builder()
                .name("long")
                .dataType(OneType.TIMESTAMP)
                .metadata(
                    Collections.singletonMap(
                        OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MILLIS))
                .build(),
            "2022-10-08 21:08:17"),
        Arguments.of(
            1665263297000000L,
            OneSchema.builder()
                .name("long")
                .dataType(OneType.TIMESTAMP)
                .metadata(
                    Collections.singletonMap(
                        OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MICROS))
                .build(),
            "2022-10-08 21:08:17"),
        Arguments.of(
            1665263297000L,
            OneSchema.builder()
                .name("long")
                .dataType(OneType.TIMESTAMP_NTZ)
                .metadata(
                    Collections.singletonMap(
                        OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MILLIS))
                .build(),
            "2022-10-08 21:08:17"),
        Arguments.of(
            1665263297000000L,
            OneSchema.builder()
                .name("long")
                .dataType(OneType.TIMESTAMP_NTZ)
                .metadata(
                    Collections.singletonMap(
                        OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MICROS))
                .build(),
            "2022-10-08 21:08:17"));
  }

  private static Stream<Arguments> valuesWithSchemaProviderForPartitions() {
    return Stream.of(
        Arguments.of(null, OneType.STRING, PartitionTransformType.VALUE, "", null),
        Arguments.of("some value", OneType.STRING, PartitionTransformType.VALUE, "", "some value"),
        Arguments.of(23L, OneType.LONG, PartitionTransformType.VALUE, "", "23"),
        Arguments.of(25.5, OneType.FLOAT, PartitionTransformType.VALUE, "", "25.5"),
        Arguments.of(18181, OneType.DATE, PartitionTransformType.VALUE, "YYYY-MM-DD", "2019-10-12"),
        Arguments.of(true, OneType.BOOLEAN, PartitionTransformType.VALUE, "", "true"),
        Arguments.of(
            1665263297000L,
            OneType.TIMESTAMP,
            PartitionTransformType.HOUR,
            "yyyy-MM-dd HH",
            "2022-10-08 21"),
        Arguments.of(
            1665263297000L,
            OneType.TIMESTAMP_NTZ,
            PartitionTransformType.DAY,
            "yyyy-MM-dd",
            "2022-10-08"),
        Arguments.of(
            1665263297000L, OneType.TIMESTAMP, PartitionTransformType.MONTH, "yyyy-MM", "2022-10"),
        Arguments.of(
            1665263297000L, OneType.TIMESTAMP_NTZ, PartitionTransformType.YEAR, "yyyy", "2022"));
  }
}
