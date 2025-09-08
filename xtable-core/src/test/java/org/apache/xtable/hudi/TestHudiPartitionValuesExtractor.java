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
 
package org.apache.xtable.hudi;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.common.base.Strings;

import org.apache.xtable.exception.PartitionValuesExtractorException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;

public class TestHudiPartitionValuesExtractor {

  private static final InternalSchema INT_SCHEMA =
      InternalSchema.builder().name("int").dataType(InternalType.INT).build();

  private static final InternalSchema STRING_SCHEMA =
      InternalSchema.builder().name("string").dataType(InternalType.STRING).build();

  @Test
  public void testSingleColumn() {
    InternalPartitionField column =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("column1")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();

    List<PartitionValue> expected =
        Collections.singletonList(
            PartitionValue.builder().partitionField(column).range(Range.scalar("foo")).build());

    List<PartitionValue> actual =
        new HudiPartitionValuesExtractor(Collections.emptyMap())
            .extractPartitionValues(Collections.singletonList(column), "foo");
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testSingleColumnValueWithSlashes() {
    InternalPartitionField column =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("column1")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();

    List<PartitionValue> expected =
        Collections.singletonList(
            PartitionValue.builder().partitionField(column).range(Range.scalar("foo/bar")).build());

    List<PartitionValue> actual =
        new HudiPartitionValuesExtractor(Collections.emptyMap())
            .extractPartitionValues(Collections.singletonList(column), "foo/bar");
    Assertions.assertEquals(expected, actual);
  }

  private static Stream<Arguments> dateAndTimeParameters() {
    return Stream.of(
        // date string field translated to month granularity
        Arguments.of(
            Instant.parse("2022-10-01T00:00:00.00Z").toEpochMilli(),
            "yyyy-MM",
            "2022-10",
            InternalType.STRING,
            PartitionTransformType.MONTH),
        // date logical type translated to day granularity
        Arguments.of(
            Instant.parse("2022-10-02T00:00:00.00Z").toEpochMilli(),
            "yyyy-MM-dd",
            "2022-10-02",
            InternalType.DATE,
            PartitionTransformType.DAY),
        // date string with year granularity
        Arguments.of(
            Instant.parse("2022-01-01T00:00:00.00Z").toEpochMilli(),
            "yyyy",
            "2022",
            InternalType.STRING,
            PartitionTransformType.YEAR),
        // long field with day granularity
        Arguments.of(
            Instant.parse("2022-10-02T00:00:00.00Z").toEpochMilli(),
            "yyyy-MM-dd",
            "2022-10-02",
            InternalType.LONG,
            PartitionTransformType.DAY),
        // timestamp field with day granularity
        Arguments.of(
            Instant.parse("2022-10-02T00:00:00.00Z").toEpochMilli(),
            "yyyy-MM-dd",
            "2022-10-02",
            InternalType.TIMESTAMP,
            PartitionTransformType.DAY),
        // timestamp field with month granularity
        Arguments.of(
            Instant.parse("2022-10-01T00:00:00.00Z").toEpochMilli(),
            "yyyy-MM",
            "2022-10",
            InternalType.TIMESTAMP,
            PartitionTransformType.MONTH),
        // hour granularity with slashes
        Arguments.of(
            Instant.parse("2022-10-02T11:00:00.00Z").toEpochMilli(),
            "yyyy/MM/dd/hh",
            "2022/10/02/11",
            InternalType.STRING,
            PartitionTransformType.HOUR),
        // day granularity with slashes
        Arguments.of(
            Instant.parse("2022-10-02T00:00:00.00Z").toEpochMilli(),
            "yyyy/MM/dd",
            "2022/10/02",
            InternalType.STRING,
            PartitionTransformType.DAY),
        // month granularity with slashes
        Arguments.of(
            Instant.parse("2022-10-01T00:00:00.00Z").toEpochMilli(),
            "yyyy/MM",
            "2022/10",
            InternalType.STRING,
            PartitionTransformType.MONTH),
        // day granularity with slashes
        Arguments.of(
            Instant.parse("2022-01-01T00:00:00.00Z").toEpochMilli(),
            "yyyy",
            "2022",
            InternalType.STRING,
            PartitionTransformType.YEAR),
        // hour granularity concatenated
        Arguments.of(
            Instant.parse("2022-10-02T11:00:00.00Z").toEpochMilli(),
            "yyyyMMddhh",
            "2022100211",
            InternalType.STRING,
            PartitionTransformType.HOUR),
        // day granularity concatenated
        Arguments.of(
            Instant.parse("2022-10-02T00:00:00.00Z").toEpochMilli(),
            "yyyyMMdd",
            "20221002",
            InternalType.STRING,
            PartitionTransformType.DAY),
        // month granularity concatenated
        Arguments.of(
            Instant.parse("2022-10-01T00:00:00.00Z").toEpochMilli(),
            "yyyyMM",
            "202210",
            InternalType.STRING,
            PartitionTransformType.MONTH),
        // day granularity concatenated
        Arguments.of(
            Instant.parse("2022-01-01T00:00:00.00Z").toEpochMilli(),
            "yyyy",
            "2022",
            InternalType.STRING,
            PartitionTransformType.YEAR));
  }

  @ParameterizedTest
  @MethodSource(value = "dateAndTimeParameters")
  public void testDateAndTimeFormats(
      long partitionValue,
      String format,
      String partitionString,
      InternalType columnType,
      PartitionTransformType partitionTransformType) {
    InternalPartitionField column =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("col")
                    .schema(
                        InternalSchema.builder()
                            .name("partition-column")
                            .dataType(columnType)
                            .build())
                    .build())
            .transformType(partitionTransformType)
            .build();

    List<PartitionValue> expected =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(column)
                .range(Range.scalar(partitionValue))
                .build());

    Map<String, String> pathToPartitionFieldFormat = new HashMap<>();
    pathToPartitionFieldFormat.put(column.getSourceField().getPath(), format);
    List<PartitionValue> actual =
        new HudiPartitionValuesExtractor(pathToPartitionFieldFormat)
            .extractPartitionValues(Collections.singletonList(column), partitionString);
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testMultipleColumns() {
    Map<InternalSchema.MetadataKey, Object> timeFieldMetadata = new HashMap<>();
    timeFieldMetadata.put(
        InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MILLIS);
    InternalPartitionField column1 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("column1")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    InternalPartitionField column2 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("column2")
                    .schema(
                        InternalSchema.builder()
                            .name("time")
                            .dataType(InternalType.TIMESTAMP)
                            .metadata(timeFieldMetadata)
                            .build())
                    .build())
            .transformType(PartitionTransformType.MONTH)
            .build();
    InternalPartitionField column3 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("column3")
                    .schema(InternalSchema.builder().name("int").dataType(InternalType.INT).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();

    List<PartitionValue> expected =
        Arrays.asList(
            PartitionValue.builder().partitionField(column1).range(Range.scalar("foo")).build(),
            PartitionValue.builder()
                .partitionField(column2)
                .range(Range.scalar(Instant.parse("2022-10-02T00:00:00.00Z").toEpochMilli()))
                .build(),
            PartitionValue.builder().partitionField(column3).range(Range.scalar(32)).build());

    Map<String, String> pathToPartitionFieldFormat = new HashMap<>();
    pathToPartitionFieldFormat.put(column2.getSourceField().getPath(), "yyyy/MM/dd");
    List<PartitionValue> actual =
        new HudiPartitionValuesExtractor(pathToPartitionFieldFormat)
            .extractPartitionValues(Arrays.asList(column1, column2, column3), "foo/2022/10/02/32");
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testMultipleColumnsWithDefaultHivePartition() {
    InternalPartitionField column1 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("column1")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    InternalPartitionField column2 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("column2")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .build())
                    .build())
            .transformType(PartitionTransformType.DAY)
            .build();
    InternalPartitionField column3 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("column3")
                    .schema(InternalSchema.builder().name("int").dataType(InternalType.INT).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();

    List<PartitionValue> expected =
        Arrays.asList(
            PartitionValue.builder().partitionField(column1).range(Range.scalar("foo")).build(),
            PartitionValue.builder().partitionField(column2).range(Range.scalar(null)).build(),
            PartitionValue.builder().partitionField(column3).range(Range.scalar(32)).build());

    Map<String, String> pathToPartitionFieldFormat = new HashMap<>();
    pathToPartitionFieldFormat.put(column2.getSourceField().getPath(), "yyyy-MM-dd");
    List<PartitionValue> actual =
        new HudiPartitionValuesExtractor(pathToPartitionFieldFormat)
            .extractPartitionValues(
                Arrays.asList(column1, column2, column3), "foo/__HIVE_DEFAULT_PARTITION__/32");
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testHiveStyle() {
    InternalPartitionField column1 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("column1")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    InternalPartitionField column2 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("column2")
                    .schema(
                        InternalSchema.builder().name("long").dataType(InternalType.LONG).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();

    List<PartitionValue> expected =
        Arrays.asList(
            PartitionValue.builder().partitionField(column1).range(Range.scalar("foo")).build(),
            PartitionValue.builder().partitionField(column2).range(Range.scalar(32L)).build());

    List<PartitionValue> actual =
        new HudiPartitionValuesExtractor(Collections.emptyMap())
            .extractPartitionValues(Arrays.asList(column1, column2), "column1=foo/column2=32");
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testHiveStyleWithDefaultPartition() {
    InternalPartitionField column1 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("column1")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    InternalPartitionField column2 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("column2")
                    .schema(
                        InternalSchema.builder().name("long").dataType(InternalType.LONG).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();

    List<PartitionValue> expected =
        Arrays.asList(
            PartitionValue.builder().partitionField(column2).range(Range.scalar(32L)).build(),
            PartitionValue.builder().partitionField(column1).range(Range.scalar(null)).build());

    List<PartitionValue> actual =
        new HudiPartitionValuesExtractor(Collections.emptyMap())
            .extractPartitionValues(
                Arrays.asList(column2, column1), "column2=32/column1=__HIVE_DEFAULT_PARTITION__");
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testPartitionCountMismatch() {
    InternalPartitionField column1 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("column1")
                    .schema(
                        InternalSchema.builder()
                            .name("string")
                            .dataType(InternalType.STRING)
                            .build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    InternalPartitionField column2 =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("column2")
                    .parentPath("base")
                    .schema(InternalSchema.builder().name("int").dataType(InternalType.INT).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();

    // partition path only contains a single value
    Assertions.assertThrows(
        PartitionValuesExtractorException.class,
        () ->
            new HudiPartitionValuesExtractor(Collections.emptyMap())
                .extractPartitionValues(Arrays.asList(column1, column2), "foo"));
  }

  @Test
  public void testNoPartitionColumnsConfigured() {
    List<PartitionValue> actual =
        new HudiPartitionValuesExtractor(Collections.emptyMap())
            .extractPartitionValues(Collections.emptyList(), "column1=foo/column2=32");
    Assertions.assertTrue(actual.isEmpty());
  }

  @Test
  public void testNullPartitionColumns() {
    List<PartitionValue> actual =
        new HudiPartitionValuesExtractor(Collections.emptyMap())
            .extractPartitionValues(null, "column1=foo/column2=32");
    Assertions.assertTrue(actual.isEmpty());
  }

  @Test
  public void testPartitionFormatMismatch() {
    InternalPartitionField column =
        InternalPartitionField.builder()
            .sourceField(
                InternalField.builder()
                    .name("column1")
                    .schema(
                        InternalSchema.builder()
                            .name("time")
                            .dataType(InternalType.TIMESTAMP_NTZ)
                            .build())
                    .build())
            .transformType(PartitionTransformType.DAY)
            .build();
    Map<String, String> pathToPartitionFieldFormat = new HashMap<>();
    pathToPartitionFieldFormat.put(column.getSourceField().getPath(), "yyyy/MM/dd");

    // partition path format is not as expected
    Assertions.assertThrows(
        PartitionValuesExtractorException.class,
        () ->
            new HudiPartitionValuesExtractor(pathToPartitionFieldFormat)
                .extractPartitionValues(Collections.singletonList(column), "2022-10-02"));
  }

  static Stream<Arguments> nestedColumnPartitioning_testArgs() {
    InternalPartitionField p1 = createSimplePartitionField("year", "partition.date", INT_SCHEMA);
    InternalPartitionField p2 = createSimplePartitionField("month", "partition.date", INT_SCHEMA);
    InternalPartitionField p3 = createSimplePartitionField("day", "partition.date", INT_SCHEMA);
    InternalPartitionField p4 = createSimplePartitionField("country", null, STRING_SCHEMA);

    return Stream.of(
        // nested column partition, hive style enabled
        Arguments.of(
            Collections.singletonList(p1),
            Collections.singletonList(Range.scalar(2022)),
            "partition.date.year=2022"),
        Arguments.of(
            Arrays.asList(p1, p2),
            Arrays.asList(Range.scalar(2022), Range.scalar(10)),
            "partition.date.year=2022/partition.date.month=10"),
        Arguments.of(
            Arrays.asList(p1, p2, p3),
            Arrays.asList(Range.scalar(2022), Range.scalar(10), Range.scalar(2)),
            "partition.date.year=2022/partition.date.month=10/partition.date.day=2"),
        Arguments.of(
            Arrays.asList(p1, p4),
            Arrays.asList(Range.scalar(2022), Range.scalar("US")),
            "partition.date.year=2022/country=US"),

        // nested column partition, hive style disabled
        Arguments.of(
            Collections.singletonList(p1), Collections.singletonList(Range.scalar(2022)), "2022"),
        Arguments.of(
            Arrays.asList(p1, p2), Arrays.asList(Range.scalar(2022), Range.scalar(10)), "2022/10"),
        Arguments.of(
            Arrays.asList(p1, p2, p3),
            Arrays.asList(Range.scalar(2022), Range.scalar(10), Range.scalar(2)),
            "2022/10/2"),
        Arguments.of(
            Arrays.asList(p1, p4),
            Arrays.asList(Range.scalar(2022), Range.scalar("US")),
            "2022/US"));
  }

  @ParameterizedTest
  @MethodSource("nestedColumnPartitioning_testArgs")
  void testNestedColumnPartitioning(
      List<InternalPartitionField> partitionFields,
      List<Range> partitionRanges,
      String partitionPath) {
    List<PartitionValue> expected =
        IntStream.range(0, partitionFields.size())
            .mapToObj(
                i ->
                    PartitionValue.builder()
                        .partitionField(partitionFields.get(i))
                        .range(partitionRanges.get(i))
                        .build())
            .collect(Collectors.toList());

    List<PartitionValue> actual =
        new HudiPartitionValuesExtractor(Collections.emptyMap())
            .extractPartitionValues(partitionFields, partitionPath);
    Assertions.assertEquals(expected, actual);
  }

  private static InternalPartitionField createSimplePartitionField(
      String name, String parentPath, InternalSchema schema) {
    InternalField.InternalFieldBuilder sourceFieldBuilder =
        InternalField.builder().name(name).schema(schema);
    if (!Strings.isNullOrEmpty(parentPath)) {
      sourceFieldBuilder.parentPath(parentPath);
    }
    return InternalPartitionField.builder()
        .sourceField(sourceFieldBuilder.build())
        .transformType(PartitionTransformType.VALUE)
        .build();
  }
}
