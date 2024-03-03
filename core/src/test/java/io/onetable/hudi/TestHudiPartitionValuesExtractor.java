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
package io.onetable.hudi;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.onetable.exception.PartitionValuesExtractorException;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.stat.PartitionValue;
import io.onetable.model.stat.Range;

public class TestHudiPartitionValuesExtractor {

  @Test
  public void testSingleColumn() {
    OnePartitionField column =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("column1")
                    .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
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
    OnePartitionField column =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("column1")
                    .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
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
            OneType.STRING,
            PartitionTransformType.MONTH),
        // date logical type translated to day granularity
        Arguments.of(
            Instant.parse("2022-10-02T00:00:00.00Z").toEpochMilli(),
            "yyyy-MM-dd",
            "2022-10-02",
            OneType.DATE,
            PartitionTransformType.DAY),
        // date string with year granularity
        Arguments.of(
            Instant.parse("2022-01-01T00:00:00.00Z").toEpochMilli(),
            "yyyy",
            "2022",
            OneType.STRING,
            PartitionTransformType.YEAR),
        // long field with day granularity
        Arguments.of(
            Instant.parse("2022-10-02T00:00:00.00Z").toEpochMilli(),
            "yyyy-MM-dd",
            "2022-10-02",
            OneType.LONG,
            PartitionTransformType.DAY),
        // timestamp field with day granularity
        Arguments.of(
            Instant.parse("2022-10-02T00:00:00.00Z").toEpochMilli(),
            "yyyy-MM-dd",
            "2022-10-02",
            OneType.TIMESTAMP,
            PartitionTransformType.DAY),
        // timestamp field with month granularity
        Arguments.of(
            Instant.parse("2022-10-01T00:00:00.00Z").toEpochMilli(),
            "yyyy-MM",
            "2022-10",
            OneType.TIMESTAMP,
            PartitionTransformType.MONTH),
        // hour granularity with slashes
        Arguments.of(
            Instant.parse("2022-10-02T11:00:00.00Z").toEpochMilli(),
            "yyyy/MM/dd/hh",
            "2022/10/02/11",
            OneType.STRING,
            PartitionTransformType.HOUR),
        // day granularity with slashes
        Arguments.of(
            Instant.parse("2022-10-02T00:00:00.00Z").toEpochMilli(),
            "yyyy/MM/dd",
            "2022/10/02",
            OneType.STRING,
            PartitionTransformType.DAY),
        // month granularity with slashes
        Arguments.of(
            Instant.parse("2022-10-01T00:00:00.00Z").toEpochMilli(),
            "yyyy/MM",
            "2022/10",
            OneType.STRING,
            PartitionTransformType.MONTH),
        // day granularity with slashes
        Arguments.of(
            Instant.parse("2022-01-01T00:00:00.00Z").toEpochMilli(),
            "yyyy",
            "2022",
            OneType.STRING,
            PartitionTransformType.YEAR),
        // hour granularity concatenated
        Arguments.of(
            Instant.parse("2022-10-02T11:00:00.00Z").toEpochMilli(),
            "yyyyMMddhh",
            "2022100211",
            OneType.STRING,
            PartitionTransformType.HOUR),
        // day granularity concatenated
        Arguments.of(
            Instant.parse("2022-10-02T00:00:00.00Z").toEpochMilli(),
            "yyyyMMdd",
            "20221002",
            OneType.STRING,
            PartitionTransformType.DAY),
        // month granularity concatenated
        Arguments.of(
            Instant.parse("2022-10-01T00:00:00.00Z").toEpochMilli(),
            "yyyyMM",
            "202210",
            OneType.STRING,
            PartitionTransformType.MONTH),
        // day granularity concatenated
        Arguments.of(
            Instant.parse("2022-01-01T00:00:00.00Z").toEpochMilli(),
            "yyyy",
            "2022",
            OneType.STRING,
            PartitionTransformType.YEAR));
  }

  @ParameterizedTest
  @MethodSource(value = "dateAndTimeParameters")
  public void testDateAndTimeFormats(
      long partitionValue,
      String format,
      String partitionString,
      OneType columnType,
      PartitionTransformType partitionTransformType) {
    OnePartitionField column =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("col")
                    .schema(
                        OneSchema.builder().name("partition-column").dataType(columnType).build())
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
    Map<OneSchema.MetadataKey, Object> timeFieldMetadata = new HashMap<>();
    timeFieldMetadata.put(
        OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MILLIS);
    OnePartitionField column1 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("column1")
                    .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    OnePartitionField column2 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("column2")
                    .schema(
                        OneSchema.builder()
                            .name("time")
                            .dataType(OneType.TIMESTAMP)
                            .metadata(timeFieldMetadata)
                            .build())
                    .build())
            .transformType(PartitionTransformType.MONTH)
            .build();
    OnePartitionField column3 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("column3")
                    .schema(OneSchema.builder().name("int").dataType(OneType.INT).build())
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
    OnePartitionField column1 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("column1")
                    .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    OnePartitionField column2 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("column2")
                    .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
                    .build())
            .transformType(PartitionTransformType.DAY)
            .build();
    OnePartitionField column3 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("column3")
                    .schema(OneSchema.builder().name("int").dataType(OneType.INT).build())
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
    OnePartitionField column1 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("column1")
                    .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    OnePartitionField column2 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("column2")
                    .parentPath("base")
                    .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
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
    OnePartitionField column1 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("column1")
                    .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    OnePartitionField column2 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("column2")
                    .parentPath("base")
                    .schema(OneSchema.builder().name("long").dataType(OneType.LONG).build())
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
    OnePartitionField column1 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("column1")
                    .schema(OneSchema.builder().name("string").dataType(OneType.STRING).build())
                    .build())
            .transformType(PartitionTransformType.VALUE)
            .build();
    OnePartitionField column2 =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("column2")
                    .parentPath("base")
                    .schema(OneSchema.builder().name("int").dataType(OneType.INT).build())
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
    OnePartitionField column =
        OnePartitionField.builder()
            .sourceField(
                OneField.builder()
                    .name("column1")
                    .schema(
                        OneSchema.builder().name("time").dataType(OneType.TIMESTAMP_NTZ).build())
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
}
