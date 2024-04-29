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

import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;

public class TestDeltaValueConverter {

  @ParameterizedTest
  @MethodSource("valuesWithSchemaProviderForColStats")
  public void formattedValueDifferentTypesForColStats(
      Object fieldValue, InternalSchema fieldSchema, Object expectedDeltaValue) {
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
      InternalType internalType,
      PartitionTransformType transformType,
      String dateFormat,
      String expectedValue) {
    String deltaRepresentation =
        DeltaValueConverter.convertToDeltaPartitionValue(
            fieldValue, internalType, transformType, dateFormat);
    Object internalRepresentation =
        DeltaValueConverter.convertFromDeltaPartitionValue(
            deltaRepresentation, internalType, transformType, dateFormat);
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
            null,
            InternalSchema.builder().name("string").dataType(InternalType.STRING).build(),
            null),
        Arguments.of(
            "some value",
            InternalSchema.builder().name("string").dataType(InternalType.STRING).build(),
            "some value"),
        Arguments.of(
            23L, InternalSchema.builder().name("long").dataType(InternalType.LONG).build(), 23L),
        Arguments.of(
            25.5,
            InternalSchema.builder().name("double").dataType(InternalType.DOUBLE).build(),
            25.5),
        Arguments.of(
            18181,
            InternalSchema.builder().name("int").dataType(InternalType.DATE).build(),
            "2019-10-12"),
        Arguments.of(
            true,
            InternalSchema.builder().name("boolean").dataType(InternalType.BOOLEAN).build(),
            true),
        Arguments.of(
            1665263297000L,
            InternalSchema.builder()
                .name("long")
                .dataType(InternalType.TIMESTAMP)
                .metadata(
                    Collections.singletonMap(
                        InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                        InternalSchema.MetadataValue.MILLIS))
                .build(),
            "2022-10-08 21:08:17"),
        Arguments.of(
            1665263297000000L,
            InternalSchema.builder()
                .name("long")
                .dataType(InternalType.TIMESTAMP)
                .metadata(
                    Collections.singletonMap(
                        InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                        InternalSchema.MetadataValue.MICROS))
                .build(),
            "2022-10-08 21:08:17"),
        Arguments.of(
            1665263297000L,
            InternalSchema.builder()
                .name("long")
                .dataType(InternalType.TIMESTAMP_NTZ)
                .metadata(
                    Collections.singletonMap(
                        InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                        InternalSchema.MetadataValue.MILLIS))
                .build(),
            "2022-10-08 21:08:17"),
        Arguments.of(
            1665263297000000L,
            InternalSchema.builder()
                .name("long")
                .dataType(InternalType.TIMESTAMP_NTZ)
                .metadata(
                    Collections.singletonMap(
                        InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                        InternalSchema.MetadataValue.MICROS))
                .build(),
            "2022-10-08 21:08:17"));
  }

  private static Stream<Arguments> valuesWithSchemaProviderForPartitions() {
    return Stream.of(
        Arguments.of(null, InternalType.STRING, PartitionTransformType.VALUE, "", null),
        Arguments.of(
            "some value", InternalType.STRING, PartitionTransformType.VALUE, "", "some value"),
        Arguments.of(23L, InternalType.LONG, PartitionTransformType.VALUE, "", "23"),
        Arguments.of(25.5f, InternalType.FLOAT, PartitionTransformType.VALUE, "", "25.5"),
        Arguments.of(
            18181, InternalType.DATE, PartitionTransformType.VALUE, "YYYY-MM-DD", "2019-10-12"),
        Arguments.of(true, InternalType.BOOLEAN, PartitionTransformType.VALUE, "", "true"),
        Arguments.of(
            1665262800000L,
            InternalType.TIMESTAMP,
            PartitionTransformType.HOUR,
            "yyyy-MM-dd HH",
            "2022-10-08 21"),
        Arguments.of(
            1665187200000L,
            InternalType.TIMESTAMP_NTZ,
            PartitionTransformType.DAY,
            "yyyy-MM-dd",
            "2022-10-08"),
        Arguments.of(
            1664582400000L,
            InternalType.TIMESTAMP,
            PartitionTransformType.MONTH,
            "yyyy-MM",
            "2022-10"),
        Arguments.of(
            1640995200000L,
            InternalType.TIMESTAMP_NTZ,
            PartitionTransformType.YEAR,
            "yyyy",
            "2022"));
  }
}
