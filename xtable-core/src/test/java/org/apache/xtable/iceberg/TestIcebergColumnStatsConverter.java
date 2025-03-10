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
 
package org.apache.xtable.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.iceberg.Metrics;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;

public class TestIcebergColumnStatsConverter {
  private static final CharsetEncoder ENCODER = StandardCharsets.UTF_8.newEncoder();

  @Test
  public void testMetricsCreation() throws Exception {
    long totalRowCount = 98776;
    Schema icebergSchema = IcebergTestUtils.SCHEMA;
    InternalField timestampField =
        InternalField.builder()
            .name("timestamp_field")
            .schema(
                InternalSchema.builder().name("time").dataType(InternalType.TIMESTAMP_NTZ).build())
            .build();
    InternalField dateField =
        InternalField.builder()
            .name("date_field")
            .schema(InternalSchema.builder().name("date").dataType(InternalType.DATE).build())
            .build();
    InternalField groupId =
        InternalField.builder()
            .name("group_id")
            .schema(InternalSchema.builder().name("int").dataType(InternalType.INT).build())
            .build();
    InternalField stringField =
        InternalField.builder()
            .name("string_field")
            .parentPath("record")
            .schema(InternalSchema.builder().name("string").dataType(InternalType.STRING).build())
            .build();
    InternalField mapFieldKey =
        InternalField.builder()
            .name(InternalField.Constants.MAP_KEY_FIELD_NAME)
            .parentPath("map_field")
            .schema(InternalSchema.builder().name("string").dataType(InternalType.STRING).build())
            .build();
    InternalField mapFieldValue =
        InternalField.builder()
            .name(InternalField.Constants.MAP_VALUE_FIELD_NAME)
            .parentPath("map_field")
            .schema(InternalSchema.builder().name("int").dataType(InternalType.INT).build())
            .build();
    InternalField arrayFieldElement =
        InternalField.builder()
            .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
            .parentPath("array_field")
            .schema(InternalSchema.builder().name("int").dataType(InternalType.INT).build())
            .build();
    ColumnStat timestampColumnStats =
        ColumnStat.builder()
            .field(timestampField)
            .numValues(123)
            .numNulls(32)
            .totalSize(13)
            .range(Range.vector(10000000L, 20000000L))
            .build();
    ColumnStat dateColumnStats =
        ColumnStat.builder()
            .field(dateField)
            .numValues(555)
            .numNulls(15)
            .totalSize(53)
            .range(Range.vector(18181, 18182))
            .build();
    ColumnStat groupIdStats =
        ColumnStat.builder()
            .field(groupId)
            .numValues(510)
            .numNulls(200)
            .totalSize(10)
            .range(Range.vector(-3, 3))
            .build();
    ColumnStat stringFieldStats =
        ColumnStat.builder()
            .field(stringField)
            .numValues(50)
            .numNulls(0)
            .totalSize(12)
            .range(Range.vector("yyyy", "zzzz"))
            .build();
    ColumnStat mapFieldKeyStats =
        ColumnStat.builder()
            .field(mapFieldKey)
            .numValues(10)
            .numNulls(0)
            .totalSize(12)
            .range(Range.vector("aaa", "abc"))
            .build();
    ColumnStat mapFieldValueStats =
        ColumnStat.builder()
            .field(mapFieldValue)
            .numValues(10)
            .numNulls(0)
            .totalSize(24)
            .range(Range.vector(0, 10))
            .build();
    ColumnStat arrayFieldElementStats =
        ColumnStat.builder()
            .field(arrayFieldElement)
            .numValues(5)
            .numNulls(0)
            .totalSize(48)
            .range(Range.vector(100, 1000))
            .build();
    List<ColumnStat> columnStats =
        Arrays.asList(
            timestampColumnStats,
            dateColumnStats,
            groupIdStats,
            stringFieldStats,
            mapFieldKeyStats,
            mapFieldValueStats,
            arrayFieldElementStats);

    Map<Integer, Long> columnSizes =
        getStatMap(
            timestampColumnStats.getTotalSize(),
            dateColumnStats.getTotalSize(),
            groupIdStats.getTotalSize(),
            stringFieldStats.getTotalSize(),
            mapFieldKeyStats.getTotalSize(),
            mapFieldValueStats.getTotalSize(),
            arrayFieldElementStats.getTotalSize());
    Map<Integer, Long> expectedValueCounts =
        getStatMap(
            timestampColumnStats.getNumValues(),
            dateColumnStats.getNumValues(),
            groupIdStats.getNumValues(),
            stringFieldStats.getNumValues(),
            mapFieldKeyStats.getNumValues(),
            mapFieldValueStats.getNumValues(),
            arrayFieldElementStats.getNumValues());
    Map<Integer, Long> expectedNullValueCounts =
        getStatMap(
            timestampColumnStats.getNumNulls(),
            dateColumnStats.getNumNulls(),
            groupIdStats.getNumNulls(),
            stringFieldStats.getNumNulls(),
            mapFieldKeyStats.getNumNulls(),
            mapFieldValueStats.getNumNulls(),
            arrayFieldElementStats.getNumNulls());
    Map<Integer, ByteBuffer> expectedUpperBounds =
        getBoundsMap(
            timestampColumnStats.getRange().getMaxValue(),
            dateColumnStats.getRange().getMaxValue(),
            groupIdStats.getRange().getMaxValue(),
            stringFieldStats.getRange().getMaxValue(),
            mapFieldKeyStats.getRange().getMaxValue(),
            mapFieldValueStats.getRange().getMaxValue(),
            arrayFieldElementStats.getRange().getMaxValue());
    Map<Integer, ByteBuffer> expectedLowerBounds =
        getBoundsMap(
            timestampColumnStats.getRange().getMinValue(),
            dateColumnStats.getRange().getMinValue(),
            groupIdStats.getRange().getMinValue(),
            stringFieldStats.getRange().getMinValue(),
            mapFieldKeyStats.getRange().getMinValue(),
            mapFieldValueStats.getRange().getMinValue(),
            arrayFieldElementStats.getRange().getMinValue());
    Metrics expected =
        new Metrics(
            totalRowCount,
            columnSizes,
            expectedValueCounts,
            expectedNullValueCounts,
            null,
            expectedLowerBounds,
            expectedUpperBounds);

    Metrics actual =
        IcebergColumnStatsConverter.getInstance()
            .toIceberg(icebergSchema, totalRowCount, columnStats);
    // Metrics does not implement equals, so we need to manually compare fields
    assertEquals(expected.columnSizes(), actual.columnSizes());
    assertEquals(expected.nanValueCounts(), actual.nanValueCounts());
    assertEquals(expected.nullValueCounts(), actual.nullValueCounts());
    assertEquals(expected.recordCount(), actual.recordCount());
    assertEquals(expected.valueCounts(), actual.valueCounts());
    assertEquals(expected.upperBounds(), actual.upperBounds());
    assertEquals(expected.lowerBounds(), actual.lowerBounds());
  }

  @Test
  public void testNullMinMaxValues() {
    long totalRowCount = 98776;
    Schema icebergSchema =
        new Schema(Types.NestedField.required(1, "date_field", Types.DateType.get()));

    InternalField dateField =
        InternalField.builder()
            .name("date_field")
            .schema(InternalSchema.builder().name("date").dataType(InternalType.DATE).build())
            .build();
    ColumnStat dateColumnStats =
        ColumnStat.builder()
            .field(dateField)
            .numValues(555)
            .numNulls(15)
            .totalSize(53)
            .range(Range.vector(null, null))
            .build();
    List<ColumnStat> columnStats = Collections.singletonList(dateColumnStats);

    Map<Integer, Long> columnSizes = getStatMap(dateColumnStats.getTotalSize());
    Map<Integer, Long> expectedValueCounts = getStatMap(dateColumnStats.getNumValues());
    Map<Integer, Long> expectedNullValueCounts = getStatMap(dateColumnStats.getNumNulls());
    Map<Integer, ByteBuffer> expectedUpperBounds = new HashMap<>();
    Map<Integer, ByteBuffer> expectedLowerBounds = new HashMap<>();
    Metrics expected =
        new Metrics(
            totalRowCount,
            columnSizes,
            expectedValueCounts,
            expectedNullValueCounts,
            null,
            expectedLowerBounds,
            expectedUpperBounds);

    Metrics actual =
        IcebergColumnStatsConverter.getInstance()
            .toIceberg(icebergSchema, totalRowCount, columnStats);
    // Metrics does not implement equals, so we need to manually compare fields
    assertEquals(expected.columnSizes(), actual.columnSizes());
    assertEquals(expected.nanValueCounts(), actual.nanValueCounts());
    assertEquals(expected.nullValueCounts(), actual.nullValueCounts());
    assertEquals(expected.recordCount(), actual.recordCount());
    assertEquals(expected.valueCounts(), actual.valueCounts());
    assertEquals(expected.upperBounds(), actual.upperBounds());
    assertEquals(expected.lowerBounds(), actual.lowerBounds());
  }

  @Test
  public void fromIceberg() {
    Map<Integer, Long> valueCounts = new HashMap<>();
    valueCounts.put(1, 123L);
    valueCounts.put(3, 456L);
    valueCounts.put(4, 1000L);
    valueCounts.put(5, 1000L);
    valueCounts.put(6, 1000L);
    Map<Integer, Long> nullCounts = new HashMap<>();
    nullCounts.put(1, 32L);
    nullCounts.put(3, 456L);
    nullCounts.put(4, 789L);
    nullCounts.put(5, 789L);
    nullCounts.put(6, 789L);
    Map<Integer, Long> columnSizes = new HashMap<>();
    columnSizes.put(1, 13L);
    columnSizes.put(3, 31L);
    columnSizes.put(4, 42L);
    columnSizes.put(5, 42L);
    columnSizes.put(6, 42L);
    Map<Integer, ByteBuffer> lowerBounds = new HashMap<>();
    lowerBounds.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 1));
    lowerBounds.put(4, Conversions.toByteBuffer(Types.StringType.get(), "a"));
    lowerBounds.put(5, Conversions.toByteBuffer(Types.DateType.get(), 18181));
    lowerBounds.put(6, Conversions.toByteBuffer(Types.TimestampType.withZone(), 10000000L));
    Map<Integer, ByteBuffer> upperBounds = new HashMap<>();
    upperBounds.put(1, Conversions.toByteBuffer(Types.IntegerType.get(), 2));
    upperBounds.put(4, Conversions.toByteBuffer(Types.StringType.get(), "zzz"));
    upperBounds.put(5, Conversions.toByteBuffer(Types.DateType.get(), 18182));
    upperBounds.put(6, Conversions.toByteBuffer(Types.TimestampType.withZone(), 20000000L));

    List<InternalField> fields =
        Arrays.asList(
            InternalField.builder()
                .fieldId(1)
                .name("int_field")
                .schema(InternalSchema.builder().dataType(InternalType.INT).build())
                .build(),
            InternalField.builder()
                .fieldId(2)
                .name("not_tracked_field")
                .schema(InternalSchema.builder().dataType(InternalType.DATE).build())
                .build(),
            InternalField.builder()
                .fieldId(3)
                .name("null_field")
                .schema(InternalSchema.builder().dataType(InternalType.INT).build())
                .build(),
            InternalField.builder()
                .fieldId(4)
                .name("string_field")
                .schema(InternalSchema.builder().dataType(InternalType.STRING).build())
                .build(),
            InternalField.builder()
                .fieldId(5)
                .name("date_field")
                .schema(InternalSchema.builder().dataType(InternalType.DATE).build())
                .build(),
            InternalField.builder()
                .fieldId(6)
                .name("timestamp_field")
                .schema(InternalSchema.builder().dataType(InternalType.TIMESTAMP).build())
                .build());

    List<ColumnStat> actual =
        IcebergColumnStatsConverter.getInstance()
            .fromIceberg(fields, valueCounts, nullCounts, columnSizes, lowerBounds, upperBounds);
    List<ColumnStat> expected =
        Arrays.asList(
            ColumnStat.builder()
                .field(fields.get(0))
                .numValues(123)
                .numNulls(32)
                .totalSize(13)
                .range(Range.vector(1, 2))
                .build(),
            ColumnStat.builder()
                .field(fields.get(2))
                .numValues(456)
                .numNulls(456)
                .totalSize(31)
                .range(Range.vector(null, null))
                .build(),
            ColumnStat.builder()
                .field(fields.get(3))
                .numValues(1000L)
                .numNulls(789L)
                .totalSize(42)
                .range(Range.vector("a", "zzz"))
                .build(),
            ColumnStat.builder()
                .field(fields.get(4))
                .numValues(1000L)
                .numNulls(789L)
                .totalSize(42)
                .range(Range.vector(18181, 18182))
                .build(),
            ColumnStat.builder()
                .field(fields.get(5))
                .numValues(1000L)
                .numNulls(789L)
                .totalSize(42)
                .range(Range.vector(10000000L, 20000000L))
                .build());
    assertEquals(expected, actual);
  }

  private Map<Integer, ByteBuffer> getBoundsMap(
      Object timestampColumnStatsValue,
      Object dateColumnStatsValue,
      Object groupIdStatsValue,
      Object stringFieldStatsValue,
      Object mapFieldKeyStatsValue,
      Object mapFieldValueStatsValue,
      Object arrayFieldElementStatsValue)
      throws CharacterCodingException {
    Map<Integer, ByteBuffer> expectedLowerBounds = new HashMap<>();
    expectedLowerBounds.put(
        1,
        ByteBuffer.allocate(8)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putLong(0, (long) timestampColumnStatsValue));
    expectedLowerBounds.put(
        2,
        ByteBuffer.allocate(4)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putInt(0, (int) dateColumnStatsValue));
    expectedLowerBounds.put(
        3,
        ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(0, (int) groupIdStatsValue));
    expectedLowerBounds.put(
        5, ENCODER.encode(CharBuffer.wrap((CharSequence) stringFieldStatsValue)));
    expectedLowerBounds.put(
        7, ENCODER.encode(CharBuffer.wrap((CharSequence) mapFieldKeyStatsValue)));
    expectedLowerBounds.put(
        8,
        ByteBuffer.allocate(4)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putInt(0, (int) mapFieldValueStatsValue));
    expectedLowerBounds.put(
        10,
        ByteBuffer.allocate(4)
            .order(ByteOrder.LITTLE_ENDIAN)
            .putInt(0, (int) arrayFieldElementStatsValue));
    return expectedLowerBounds;
  }

  private Map<Integer, Long> getStatMap(long dateColumnStats) {
    Map<Integer, Long> columnSizes = new HashMap<>();
    columnSizes.put(1, dateColumnStats);
    return columnSizes;
  }

  private Map<Integer, Long> getStatMap(
      long timestampColumnStats,
      long dateColumnStats,
      long groupIdStats,
      long stringFieldStats,
      long mapFieldKeyStats,
      long mapFieldValueStats,
      long arrayFieldElementStats) {
    Map<Integer, Long> columnSizes = new HashMap<>();
    columnSizes.put(1, timestampColumnStats);
    columnSizes.put(2, dateColumnStats);
    columnSizes.put(3, groupIdStats);
    columnSizes.put(5, stringFieldStats);
    columnSizes.put(7, mapFieldKeyStats);
    columnSizes.put(8, mapFieldValueStats);
    columnSizes.put(10, arrayFieldElementStats);
    return columnSizes;
  }
}
