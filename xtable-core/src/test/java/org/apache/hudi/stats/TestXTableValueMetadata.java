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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import org.apache.hudi.metadata.HoodieIndexVersion;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.ColumnStat;

public class TestXTableValueMetadata {

  @Test
  void testGetValueMetadataReturnsV1EmptyMetadataForV1Index() {
    ColumnStat columnStat = mock(ColumnStat.class);
    ValueMetadata result = XTableValueMetadata.getValueMetadata(columnStat, HoodieIndexVersion.V1);

    assertInstanceOf(ValueMetadata.V1EmptyMetadata.class, result);
  }

  @Test
  void testGetValueMetadataThrowsForNullColumnStatWithV2Index() {
    assertThrows(
        IllegalArgumentException.class,
        () -> XTableValueMetadata.getValueMetadata((ColumnStat) null, HoodieIndexVersion.V2));
  }

  @Test
  void testGetValueMetadataForDecimalType() {
    Map<InternalSchema.MetadataKey, Object> metadata = new HashMap<>();
    metadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, 10);
    metadata.put(InternalSchema.MetadataKey.DECIMAL_SCALE, 2);

    InternalSchema schema =
        InternalSchema.builder()
            .name("decimal_field")
            .dataType(InternalType.DECIMAL)
            .metadata(metadata)
            .build();

    InternalField field = InternalField.builder().name("decimal_field").schema(schema).build();

    ColumnStat columnStat = mock(ColumnStat.class);
    when(columnStat.getField()).thenReturn(field);

    ValueMetadata result = XTableValueMetadata.getValueMetadata(columnStat, HoodieIndexVersion.V2);

    assertInstanceOf(ValueMetadata.DecimalMetadata.class, result);
    ValueMetadata.DecimalMetadata decimalMetadata = (ValueMetadata.DecimalMetadata) result;
    assertEquals(10, decimalMetadata.getPrecision());
    assertEquals(2, decimalMetadata.getScale());
  }

  @Test
  void testGetValueMetadataForDecimalMissingScale() {
    Map<InternalSchema.MetadataKey, Object> metadata = new HashMap<>();
    metadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, 10);

    InternalSchema schema =
        InternalSchema.builder()
            .name("decimal_field")
            .dataType(InternalType.DECIMAL)
            .metadata(metadata)
            .build();

    InternalField field = InternalField.builder().name("decimal_field").schema(schema).build();

    ColumnStat columnStat = mock(ColumnStat.class);
    when(columnStat.getField()).thenReturn(field);

    assertThrows(
        IllegalArgumentException.class,
        () -> XTableValueMetadata.getValueMetadata(columnStat, HoodieIndexVersion.V2));
  }

  @Test
  void testGetValueMetadataForDecimalMissingPrecision() {
    Map<InternalSchema.MetadataKey, Object> metadata = new HashMap<>();
    metadata.put(InternalSchema.MetadataKey.DECIMAL_SCALE, 2);

    InternalSchema schema =
        InternalSchema.builder()
            .name("decimal_field")
            .dataType(InternalType.DECIMAL)
            .metadata(metadata)
            .build();

    InternalField field = InternalField.builder().name("decimal_field").schema(schema).build();

    ColumnStat columnStat = mock(ColumnStat.class);
    when(columnStat.getField()).thenReturn(field);

    assertThrows(
        IllegalArgumentException.class,
        () -> XTableValueMetadata.getValueMetadata(columnStat, HoodieIndexVersion.V2));
  }

  @Test
  void testGetValueMetadataForDecimalNullMetadata() {
    InternalSchema schema =
        InternalSchema.builder()
            .name("decimal_field")
            .dataType(InternalType.DECIMAL)
            .metadata(null)
            .build();

    InternalField field = InternalField.builder().name("decimal_field").schema(schema).build();

    ColumnStat columnStat = mock(ColumnStat.class);
    when(columnStat.getField()).thenReturn(field);

    assertThrows(
        IllegalArgumentException.class,
        () -> XTableValueMetadata.getValueMetadata(columnStat, HoodieIndexVersion.V2));
  }

  @ParameterizedTest
  @EnumSource(
      value = InternalType.class,
      names = {
        "NULL", "BOOLEAN", "INT", "LONG", "FLOAT", "DOUBLE", "STRING", "BYTES", "FIXED", "UUID",
        "DATE"
      })
  void testFromInternalSchemaBasicTypes(InternalType dataType) {
    InternalSchema schema = InternalSchema.builder().name("field").dataType(dataType).build();

    ValueType result = XTableValueMetadata.fromInternalSchema(schema);

    assertEquals(dataType.name(), result.name());
  }

  @Test
  void testFromInternalSchemaTimestampMillis() {
    InternalSchema schema =
        InternalSchema.builder()
            .name("timestamp_field")
            .dataType(InternalType.TIMESTAMP)
            .metadata(
                Collections.singletonMap(
                    InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                    InternalSchema.MetadataValue.MILLIS))
            .build();

    ValueType result = XTableValueMetadata.fromInternalSchema(schema);

    assertEquals(ValueType.TIMESTAMP_MILLIS, result);
  }

  @Test
  void testFromInternalSchemaTimestampMicros() {
    InternalSchema schema =
        InternalSchema.builder()
            .name("timestamp_field")
            .dataType(InternalType.TIMESTAMP)
            .metadata(
                Collections.singletonMap(
                    InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                    InternalSchema.MetadataValue.MICROS))
            .build();

    ValueType result = XTableValueMetadata.fromInternalSchema(schema);

    assertEquals(ValueType.TIMESTAMP_MICROS, result);
  }

  @Test
  void testFromInternalSchemaTimestampNtzMillis() {
    InternalSchema schema =
        InternalSchema.builder()
            .name("timestamp_ntz_field")
            .dataType(InternalType.TIMESTAMP_NTZ)
            .metadata(
                Collections.singletonMap(
                    InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                    InternalSchema.MetadataValue.MILLIS))
            .build();

    ValueType result = XTableValueMetadata.fromInternalSchema(schema);

    assertEquals(ValueType.LOCAL_TIMESTAMP_MILLIS, result);
  }

  @Test
  void testFromInternalSchemaTimestampNtzMicros() {
    InternalSchema schema =
        InternalSchema.builder()
            .name("timestamp_ntz_field")
            .dataType(InternalType.TIMESTAMP_NTZ)
            .metadata(
                Collections.singletonMap(
                    InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                    InternalSchema.MetadataValue.MICROS))
            .build();

    ValueType result = XTableValueMetadata.fromInternalSchema(schema);

    assertEquals(ValueType.LOCAL_TIMESTAMP_MICROS, result);
  }

  @Test
  void testFromInternalSchemaUnsupportedType() {
    InternalSchema schema =
        InternalSchema.builder().name("record_field").dataType(InternalType.RECORD).build();

    assertThrows(
        UnsupportedOperationException.class, () -> XTableValueMetadata.fromInternalSchema(schema));
  }

  @Test
  void testGetValueMetadataWithValueTypeForV1Index() {
    ValueMetadata result =
        XTableValueMetadata.getValueMetadata(ValueType.INT, HoodieIndexVersion.V1);

    assertInstanceOf(ValueMetadata.V1EmptyMetadata.class, result);
  }

  @Test
  void testGetValueMetadataWithValueTypeForV2Index() {
    ValueMetadata result =
        XTableValueMetadata.getValueMetadata(ValueType.STRING, HoodieIndexVersion.V2);

    assertEquals(ValueType.STRING, result.getValueType());
  }

  @Test
  void testConvertHoodieTypeToRangeTypeForInstantMillis() {
    Instant instant = Instant.now();
    ValueMetadata valueMetadata =
        XTableValueMetadata.getValueMetadata(ValueType.TIMESTAMP_MILLIS, HoodieIndexVersion.V2);

    Comparable<?> result = XTableValueMetadata.convertHoodieTypeToRangeType(instant, valueMetadata);

    assertEquals(ValueType.fromTimestampMillis(instant, valueMetadata), result);
  }

  @Test
  void testConvertHoodieTypeToRangeTypeForInstantMicros() {
    Instant instant = Instant.now();
    ValueMetadata valueMetadata =
        XTableValueMetadata.getValueMetadata(ValueType.TIMESTAMP_MICROS, HoodieIndexVersion.V2);

    Comparable<?> result = XTableValueMetadata.convertHoodieTypeToRangeType(instant, valueMetadata);

    assertEquals(ValueType.fromTimestampMicros(instant, valueMetadata), result);
  }

  @Test
  void testConvertHoodieTypeToRangeTypeForInstantWithInvalidType() {
    Instant instant = Instant.now();
    ValueMetadata valueMetadata =
        XTableValueMetadata.getValueMetadata(ValueType.STRING, HoodieIndexVersion.V2);

    assertThrows(
        IllegalArgumentException.class,
        () -> XTableValueMetadata.convertHoodieTypeToRangeType(instant, valueMetadata));
  }

  @Test
  void testConvertHoodieTypeToRangeTypeForLocalDateTimeMillis() {
    LocalDateTime localDateTime = LocalDateTime.now();
    ValueMetadata valueMetadata =
        XTableValueMetadata.getValueMetadata(
            ValueType.LOCAL_TIMESTAMP_MILLIS, HoodieIndexVersion.V2);

    Comparable<?> result =
        XTableValueMetadata.convertHoodieTypeToRangeType(localDateTime, valueMetadata);

    assertEquals(ValueType.fromLocalTimestampMillis(localDateTime, valueMetadata), result);
  }

  @Test
  void testConvertHoodieTypeToRangeTypeForLocalDateTimeMicros() {
    LocalDateTime localDateTime = LocalDateTime.now();
    ValueMetadata valueMetadata =
        XTableValueMetadata.getValueMetadata(
            ValueType.LOCAL_TIMESTAMP_MICROS, HoodieIndexVersion.V2);

    Comparable<?> result =
        XTableValueMetadata.convertHoodieTypeToRangeType(localDateTime, valueMetadata);

    assertEquals(ValueType.fromLocalTimestampMicros(localDateTime, valueMetadata), result);
  }

  @Test
  void testConvertHoodieTypeToRangeTypeForLocalDateTimeWithInvalidType() {
    LocalDateTime localDateTime = LocalDateTime.now();
    ValueMetadata valueMetadata =
        XTableValueMetadata.getValueMetadata(ValueType.STRING, HoodieIndexVersion.V2);

    assertThrows(
        IllegalArgumentException.class,
        () -> XTableValueMetadata.convertHoodieTypeToRangeType(localDateTime, valueMetadata));
  }

  @Test
  void testConvertHoodieTypeToRangeTypeForLocalDate() {
    LocalDate localDate = LocalDate.now();
    ValueMetadata valueMetadata =
        XTableValueMetadata.getValueMetadata(ValueType.DATE, HoodieIndexVersion.V2);

    Comparable<?> result =
        XTableValueMetadata.convertHoodieTypeToRangeType(localDate, valueMetadata);

    assertEquals(ValueType.fromDate(localDate, valueMetadata), result);
  }

  @Test
  void testConvertHoodieTypeToRangeTypeForLocalDateWithInvalidType() {
    LocalDate localDate = LocalDate.now();
    ValueMetadata valueMetadata =
        XTableValueMetadata.getValueMetadata(ValueType.STRING, HoodieIndexVersion.V2);

    assertThrows(
        IllegalArgumentException.class,
        () -> XTableValueMetadata.convertHoodieTypeToRangeType(localDate, valueMetadata));
  }

  @Test
  void testConvertHoodieTypeToRangeTypeForNonTemporalType() {
    String value = "test_string";
    ValueMetadata valueMetadata =
        XTableValueMetadata.getValueMetadata(ValueType.STRING, HoodieIndexVersion.V2);

    Comparable<?> result = XTableValueMetadata.convertHoodieTypeToRangeType(value, valueMetadata);

    assertEquals(value, result);
  }

  @Test
  void testConvertHoodieTypeToRangeTypeForInteger() {
    Integer value = 42;
    ValueMetadata valueMetadata =
        XTableValueMetadata.getValueMetadata(ValueType.INT, HoodieIndexVersion.V2);

    Comparable<?> result = XTableValueMetadata.convertHoodieTypeToRangeType(value, valueMetadata);

    assertEquals(value, result);
  }
}
