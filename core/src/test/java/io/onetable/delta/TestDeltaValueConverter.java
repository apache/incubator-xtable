/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.onetable.delta;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.TimeZone;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;

public class TestDeltaValueConverter {

  @ParameterizedTest
  @MethodSource("valuesWithSchemaProviderForColStats")
  public void formattedValueDifferentTypesForColStats(
      Object fieldValue, OneSchema fieldSchema, Object expectedDeltaValue) {
    Object deltaRepresentation =
        DeltaValueConverter.convertToDeltaColumnStatValue(fieldValue, fieldSchema);
    Object internalRepresentation =
        DeltaValueConverter.convertFromDeltaColumnStatValue(deltaRepresentation, fieldSchema);
    assertEquals(expectedDeltaValue, deltaRepresentation);
    assertEquals(fieldValue, internalRepresentation);
  }

  @ParameterizedTest
  @MethodSource("valuesWithSchemaProviderForPartitions")
  public void formattedValueDifferentTypesForPartition(
      Object fieldValue,
      OneType oneType,
      PartitionTransformType transformType,
      String dateFormat,
      String expectedValue) {
    String deltaRepresentation =
        DeltaValueConverter.convertToDeltaPartitionValue(
            fieldValue, oneType, transformType, dateFormat);
    Object internalRepresentation =
        DeltaValueConverter.convertFromDeltaPartitionValue(
            deltaRepresentation, oneType, transformType, dateFormat);
    assertEquals(expectedValue, deltaRepresentation);
    assertEquals(fieldValue, internalRepresentation);
  }

  @Test
  void parseWrongDateTime() throws ParseException {
    String dateFormatString = "yyyy-MM-dd HH:mm:ss";
    String wrongDateTime = "2020-02-30 12:00:00";

    DateFormat lenientDateFormat = new SimpleDateFormat(dateFormatString);
    lenientDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    lenientDateFormat.parse(wrongDateTime);

    DateFormat strictDateFormat = DeltaValueConverter.getDateFormat(dateFormatString);
    assertThrows(ParseException.class, () -> strictDateFormat.parse(wrongDateTime));
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
        Arguments.of(25.5f, OneType.FLOAT, PartitionTransformType.VALUE, "", "25.5"),
        Arguments.of(18181, OneType.DATE, PartitionTransformType.VALUE, "YYYY-MM-DD", "2019-10-12"),
        Arguments.of(true, OneType.BOOLEAN, PartitionTransformType.VALUE, "", "true"),
        Arguments.of(
            1665262800000L,
            OneType.TIMESTAMP,
            PartitionTransformType.HOUR,
            "yyyy-MM-dd HH",
            "2022-10-08 21"),
        Arguments.of(
            1665187200000L,
            OneType.TIMESTAMP_NTZ,
            PartitionTransformType.DAY,
            "yyyy-MM-dd",
            "2022-10-08"),
        Arguments.of(
            1664582400000L, OneType.TIMESTAMP, PartitionTransformType.MONTH, "yyyy-MM", "2022-10"),
        Arguments.of(
            1640995200000L, OneType.TIMESTAMP_NTZ, PartitionTransformType.YEAR, "yyyy", "2022"));
  }
}
