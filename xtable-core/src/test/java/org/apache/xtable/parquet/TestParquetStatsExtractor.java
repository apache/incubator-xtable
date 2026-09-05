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

import static org.apache.parquet.column.Encoding.BIT_PACKED;
import static org.apache.parquet.column.Encoding.PLAIN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.ColumnStat;

class TestParquetStatsExtractor {

  private final Configuration conf = new Configuration();
  @TempDir static java.nio.file.Path tempDir = Paths.get("./");

  /** Write a two-row single-column Parquet file; Parquet computes min/max automatically. */
  private Path writeParquetFile(File file, MessageType schema, Object minVal, Object maxVal)
      throws IOException {
    Path path = new Path(file.toURI());
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    String fieldName = schema.getFields().get(0).getName();
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withConf(conf).build()) {
      writer.write(addValue(factory.newGroup(), fieldName, minVal));
      writer.write(addValue(factory.newGroup(), fieldName, maxVal));
    }
    return path;
  }

  private static Group addValue(Group group, String fieldName, Object value) {
    if (value instanceof Integer) {
      group.add(fieldName, (Integer) value);
    } else if (value instanceof Long) {
      group.add(fieldName, (Long) value);
    } else if (value instanceof Float) {
      group.add(fieldName, (Float) value);
    } else if (value instanceof Double) {
      group.add(fieldName, (Double) value);
    } else if (value instanceof Boolean) {
      group.add(fieldName, (Boolean) value);
    } else if (value instanceof Binary) {
      group.add(fieldName, (Binary) value);
    } else {
      throw new IllegalArgumentException("Unsupported: " + value.getClass());
    }
    return group;
  }

  /** Read all column stats from a written file. */
  private List<ColumnStat> readStats(Path path) {
    ParquetMetadata footer = ParquetMetadataExtractor.getInstance().readParquetMetadata(conf, path);
    return ParquetStatsExtractor.getStatsForFile(
        footer,
        ParquetSchemaExtractor.getInstance()
            .toInternalSchema(footer.getFileMetaData().getSchema(), ""));
  }

  private static IntStatistics intStats(int min, int max) {
    IntStatistics s = new IntStatistics();
    s.updateStats(min);
    s.updateStats(max);
    return s;
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("primitiveTypeTestCases")
  void testPrimitiveTypeStats(
      String name,
      MessageType schema,
      Object minWrite,
      Object maxWrite,
      InternalType expectedType,
      Object expectedMin,
      Object expectedMax)
      throws IOException {
    File file = tempDir.resolve("parquet-test-" + name).toFile();
    Path path = writeParquetFile(file, schema, minWrite, maxWrite);
    List<ColumnStat> columnStats = readStats(path);
    assertEquals(1, columnStats.size());
    ColumnStat stat = columnStats.get(0);
    assertEquals(expectedType, stat.getField().getSchema().getDataType());
    assertEquals(expectedMin, stat.getRange().getMinValue());
    assertEquals(expectedMax, stat.getRange().getMaxValue());
  }

  static Stream<Arguments> primitiveTypeTestCases() {
    return Stream.of(
        // BOOLEAN
        Arguments.of(
            "boolean",
            new MessageType("m", Types.required(PrimitiveTypeName.BOOLEAN).named("b")),
            true, // minWrite
            false, // maxWrite — Parquet stores {false, true} as {min=false, max=true}
            InternalType.BOOLEAN,
            false,
            true),

        // INT32 plain (no logical type) → INT
        Arguments.of(
            "int32-plain",
            new MessageType("m", Types.required(PrimitiveTypeName.INT32).named("b")),
            1,
            100,
            InternalType.INT,
            1,
            100),

        // INT64 plain (no logical type) → LONG
        Arguments.of(
            "int64-plain",
            new MessageType("message", Types.required(PrimitiveTypeName.INT64).named("field1")),
            100L,
            500L,
            InternalType.LONG,
            100L,
            500L),

        // INT64 + Timestamp UTC MICROS → TIMESTAMP
        Arguments.of(
            "int64-timestamp-utc-micros",
            new MessageType(
                "message",
                Types.required(PrimitiveTypeName.INT64)
                    .as(
                        LogicalTypeAnnotation.timestampType(
                            true, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .named("field1")),
            1_000_000L,
            2_000_000L,
            InternalType.TIMESTAMP,
            1_000_000L,
            2_000_000L),

        // INT64 + Timestamp UTC MILLIS → TIMESTAMP
        Arguments.of(
            "int64-timestamp-utc-millis",
            new MessageType(
                "message",
                Types.required(PrimitiveTypeName.INT64)
                    .as(
                        LogicalTypeAnnotation.timestampType(
                            true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                    .named("field1")),
            1000L,
            2000L,
            InternalType.TIMESTAMP,
            1000L,
            2000L),

        // INT64 + Timestamp UTC NANOS → TIMESTAMP
        Arguments.of(
            "int64-timestamp-utc-nanos",
            new MessageType(
                "message",
                Types.required(PrimitiveTypeName.INT64)
                    .as(
                        LogicalTypeAnnotation.timestampType(
                            true, LogicalTypeAnnotation.TimeUnit.NANOS))
                    .named("field1")),
            1_000_000_000L,
            2_000_000_000L,
            InternalType.TIMESTAMP,
            1_000_000_000L,
            2_000_000_000L),

        // INT64 + Timestamp NTZ MICROS → TIMESTAMP_NTZ
        Arguments.of(
            "int64-timestamp-ntz-micros",
            new MessageType(
                "message",
                Types.required(PrimitiveTypeName.INT64)
                    .as(
                        LogicalTypeAnnotation.timestampType(
                            false, LogicalTypeAnnotation.TimeUnit.MICROS))
                    .named("field1")),
            1_000_000L,
            2_000_000L,
            InternalType.TIMESTAMP_NTZ,
            1_000_000L,
            2_000_000L),

        // DOUBLE
        Arguments.of(
            "double",
            new MessageType("message", Types.required(PrimitiveTypeName.DOUBLE).named("field1")),
            1.5,
            9.9,
            InternalType.DOUBLE,
            1.5,
            9.9),

        // FLOAT
        Arguments.of(
            "float",
            new MessageType("message", Types.required(PrimitiveTypeName.FLOAT).named("field1")),
            1.0f,
            3.14f,
            InternalType.FLOAT,
            1.0f,
            3.14f),

        // INT32 + Date → DATE
        Arguments.of(
            "int32-date",
            new MessageType(
                "message",
                Types.required(PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.dateType())
                    .named("field1")),
            18000,
            19000,
            InternalType.DATE,
            18000,
            19000),

        // INT32 + Time MILLIS → INT
        Arguments.of(
            "int32-time-millis",
            new MessageType(
                "message",
                Types.required(PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                    .named("field1")),
            0,
            86_400_000,
            InternalType.INT,
            0,
            86_400_000),

        // BINARY plain (no logical type) → BYTES; range stays Binary
        Arguments.of(
            "binary-plain",
            new MessageType("message", Types.required(PrimitiveTypeName.BINARY).named("field1")),
            Binary.fromString("aaa"),
            Binary.fromString("zzz"),
            InternalType.BYTES,
            Binary.fromString("aaa"),
            Binary.fromString("zzz")),

        // BINARY + STRING → STRING; range values are Java String
        Arguments.of(
            "binary-string",
            new MessageType(
                "message",
                Types.required(PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.stringType())
                    .named("field1")),
            Binary.fromString("apple"),
            Binary.fromString("zebra"),
            InternalType.STRING,
            "apple",
            "zebra"),

        // BINARY + JSON → BYTES; range stays Binary
        Arguments.of(
            "binary-json",
            new MessageType(
                "message",
                Types.required(PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.jsonType())
                    .named("field1")),
            Binary.fromString("{\"a\":1}"),
            Binary.fromString("{\"z\":99}"),
            InternalType.BYTES,
            Binary.fromString("{\"a\":1}"),
            Binary.fromString("{\"z\":99}")),

        // FIXED_LEN_BYTE_ARRAY(16) + UUID → UUID; range stays Binary
        Arguments.of(
            "fixed-uuid",
            new MessageType(
                "message",
                Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                    .length(16)
                    .as(LogicalTypeAnnotation.uuidType())
                    .named("field1")),
            Binary.fromConstantByteArray(new byte[16]),
            Binary.fromConstantByteArray(
                new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}),
            InternalType.UUID,
            Binary.fromConstantByteArray(new byte[16]),
            Binary.fromConstantByteArray(
                new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1})),

        // FIXED_LEN_BYTE_ARRAY(8) → BYTES; range stays Binary
        Arguments.of(
            "fixed-bytes",
            new MessageType(
                "message",
                Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY).length(8).named("field1")),
            Binary.fromConstantByteArray(new byte[] {0, 0, 0, 0, 0, 0, 0, 1}),
            Binary.fromConstantByteArray(new byte[] {0, 0, 0, 0, 0, 1, 0, 0}),
            InternalType.FIXED,
            Binary.fromConstantByteArray(new byte[] {0, 0, 0, 0, 0, 0, 0, 1}),
            Binary.fromConstantByteArray(new byte[] {0, 0, 0, 0, 0, 1, 0, 0})));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("decimalTypeTestCases")
  void testDecimalTypeStats(
      String name,
      MessageType schema,
      Object minWrite,
      Object maxWrite,
      int expectedPrecision,
      int expectedScale,
      Object expectedMin,
      Object expectedMax)
      throws IOException {
    File file = tempDir.resolve("parquet-test-" + name).toFile();
    Path path = writeParquetFile(file, schema, minWrite, maxWrite);
    List<ColumnStat> columnStats = readStats(path);
    assertEquals(1, columnStats.size());
    ColumnStat stat = columnStats.get(0);
    assertEquals(InternalType.DECIMAL, stat.getField().getSchema().getDataType(), name);
    Map<InternalSchema.MetadataKey, Object> metadata = stat.getField().getSchema().getMetadata();
    assertEquals(
        expectedPrecision,
        metadata.get(InternalSchema.MetadataKey.DECIMAL_PRECISION),
        name + " precision");
    assertEquals(expectedScale, metadata.get(InternalSchema.MetadataKey.DECIMAL_SCALE));
    assertEquals(expectedMin, stat.getRange().getMinValue());
    assertEquals(expectedMax, stat.getRange().getMaxValue());
  }

  static Stream<Arguments> decimalTypeTestCases() {
    // Note: LogicalTypeAnnotation.decimalType(scale, precision) — scale is first argument
    return Stream.of(
        // INT32 + Decimal(precision=9, scale=2) — INT32 supports precision up to 9
        Arguments.of(
            "int32-decimal",
            new MessageType(
                "message",
                Types.required(PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.decimalType(2, 9))
                    .named("decimal_field")),
            100,
            9999,
            9,
            2,
            new BigDecimal("1.00"),
            new BigDecimal("99.99")),

        // INT64 + Decimal(precision=18, scale=6)
        Arguments.of(
            "int64-decimal",
            new MessageType(
                "message",
                Types.required(PrimitiveTypeName.INT64)
                    .as(LogicalTypeAnnotation.decimalType(6, 18))
                    .named("decimal_field")),
            1_000_000L,
            9_999_999L,
            18,
            6,
            new BigDecimal("1.000000"),
            new BigDecimal("9.999999")),

        // FIXED_LEN_BYTE_ARRAY(8) + Decimal(precision=18, scale=6); range is BigDecimal
        Arguments.of(
            "fixed-decimal",
            new MessageType(
                "message",
                Types.required(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY)
                    .length(8)
                    .as(LogicalTypeAnnotation.decimalType(6, 18))
                    .named("decimal_field")),
            Binary.fromConstantByteArray(new byte[] {0, 0, 0, 0, 0, 0, 0, 1}),
            Binary.fromConstantByteArray(new byte[] {0, 0, 0, 0, 0, 1, 0, 0}),
            18,
            6,
            new BigDecimal("0.000001"),
            new BigDecimal("0.065536")),

        // BINARY + Decimal; range is BigDecimal
        Arguments.of(
            "binary-decimal",
            new MessageType(
                "message",
                Types.required(PrimitiveTypeName.BINARY)
                    .as(LogicalTypeAnnotation.decimalType(4, 20))
                    .named("decimal_field")),
            Binary.fromConstantByteArray(new byte[] {0, 0, 0, 100}),
            Binary.fromConstantByteArray(new byte[] {0, 0, 3, (byte) 0xe8}),
            20,
            4,
            new BigDecimal("0.0100"),
            new BigDecimal("0.1000")));
  }

  @Test
  void testMultipleColumnsStat() throws IOException {
    MessageType schema =
        new MessageType(
            "message",
            Types.required(PrimitiveTypeName.INT32).named("col_int"),
            Types.required(PrimitiveTypeName.DOUBLE).named("col_double"));

    File file = tempDir.resolve("parquet-test-multi-col").toFile();
    Path path = new Path(file.toURI());
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withConf(conf).build()) {
      Group row1 = factory.newGroup();
      row1.add("col_int", 5);
      row1.add("col_double", 1.5);
      Group row2 = factory.newGroup();
      row2.add("col_int", 50);
      row2.add("col_double", 9.9);
      writer.write(row1);
      writer.write(row2);
    }

    List<ColumnStat> columnStats = readStats(path);
    assertEquals(2, columnStats.size());

    ColumnStat intStat =
        columnStats.stream()
            .filter(s -> s.getField().getName().equals("col_int"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("col_int not found"));
    ColumnStat doubleStat =
        columnStats.stream()
            .filter(s -> s.getField().getName().equals("col_double"))
            .findFirst()
            .orElseThrow(() -> new RuntimeException("col_double not found"));

    assertEquals(InternalType.INT, intStat.getField().getSchema().getDataType());
    assertEquals(5, intStat.getRange().getMinValue());
    assertEquals(50, intStat.getRange().getMaxValue());

    assertEquals(InternalType.DOUBLE, doubleStat.getField().getSchema().getDataType());
    assertEquals(1.5, doubleStat.getRange().getMinValue());
    assertEquals(9.9, doubleStat.getRange().getMaxValue());
  }

  @Test
  void testMultipleRowGroupsStat() throws IOException {
    MessageType schema =
        new MessageType("message", Types.required(PrimitiveTypeName.INT32).named("field1"));
    ColumnDescriptor col = schema.getColumns().get(0);
    IntStatistics stats1 = intStats(10, 100);
    IntStatistics stats2 = intStats(200, 500);

    File file = tempDir.resolve("parquet-test-multi-rg").toFile();
    Path path = new Path(file.toURI());
    ParquetFileWriter w = new ParquetFileWriter(conf, schema, path);
    w.start();
    w.startBlock(10);
    w.startColumn(col, 10, CompressionCodecName.UNCOMPRESSED);
    w.writeDataPage(10, 4, BytesInput.fromInt(0), stats1, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.endBlock();
    w.startBlock(5);
    w.startColumn(col, 5, CompressionCodecName.UNCOMPRESSED);
    w.writeDataPage(5, 4, BytesInput.fromInt(0), stats2, BIT_PACKED, BIT_PACKED, PLAIN);
    w.endColumn();
    w.endBlock();
    w.end(new HashMap<>());

    List<ColumnStat> columnStats = readStats(path);
    assertEquals(1, columnStats.size());
    ColumnStat columnStat = columnStats.get(0);
    assertEquals(10, columnStat.getRange().getMinValue());
    assertEquals(500, columnStat.getRange().getMaxValue());
    assertEquals(15L, columnStat.getNumValues());
  }

  @Test
  void testAllNullColumnStats() throws IOException {
    MessageType schema =
        new MessageType("message", Types.optional(PrimitiveTypeName.INT32).named("col"));
    File file = tempDir.resolve("parquet-all-null").toFile();
    Path path = new Path(file.toURI());
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withConf(conf).build()) {
      writer.write(factory.newGroup());
      writer.write(factory.newGroup());
      writer.write(factory.newGroup());
    }

    List<ColumnStat> columnStats = readStats(path);
    assertEquals(1, columnStats.size());
    ColumnStat stat = columnStats.get(0);
    assertEquals(3L, stat.getNumNulls());
    assertNull(stat.getRange().getMinValue());
    assertNull(stat.getRange().getMaxValue());
  }

  @Test
  void testNumNullsIsSet() throws IOException {
    MessageType schema =
        new MessageType("message", Types.optional(PrimitiveTypeName.INT32).named("col"));
    File file = tempDir.resolve("parquet-partial-null").toFile();
    Path path = new Path(file.toURI());
    GroupWriteSupport.setSchema(schema, conf);
    SimpleGroupFactory factory = new SimpleGroupFactory(schema);
    try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(path).withConf(conf).build()) {
      Group row1 = factory.newGroup();
      row1.add("col", 10);
      writer.write(row1);
      writer.write(factory.newGroup()); // null row
      Group row3 = factory.newGroup();
      row3.add("col", 50);
      writer.write(row3);
    }

    List<ColumnStat> columnStats = readStats(path);
    assertEquals(1, columnStats.size());
    ColumnStat stat = columnStats.get(0);
    assertEquals(1L, stat.getNumNulls());
    assertEquals(10, stat.getRange().getMinValue());
    assertEquals(50, stat.getRange().getMaxValue());
  }
}
