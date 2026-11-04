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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.xtable.exception.PartitionValuesExtractorException;
import org.apache.xtable.hudi.PathBasedPartitionSpecExtractor;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.PartitionValue;

public class TestParquetPartitionValueExtractor {

  private static final InternalSchema SCHEMA =
      InternalSchema.builder()
          .name("record")
          .dataType(InternalType.RECORD)
          .fields(
              Arrays.asList(
                  field("region", InternalType.STRING),
                  field("bucket", InternalType.INT),
                  field("order_date", InternalType.DATE),
                  field("ts", InternalType.TIMESTAMP)))
          .build();

  @Test
  void testStringValuePartition() {
    List<PartitionValue> values = extract("region:VALUE", "region=us-west");
    assertEquals(
        PartitionTransformType.VALUE, values.get(0).getPartitionField().getTransformType());
    assertEquals("us-west", values.get(0).getRange().getMaxValue());
  }

  @Test
  void testDateValuePartitionIsDaysSinceEpoch() {
    List<PartitionValue> values = extract("order_date:VALUE", "order_date=2026-08-01");
    assertEquals((int) LocalDate.parse("2026-08-01").toEpochDay(), scalars(values).get(0));
  }

  @Test
  void testMultipleValuePartitions() {
    List<PartitionValue> values = extract("region:VALUE,bucket:VALUE", "region=us-west/bucket=7");
    assertEquals(Arrays.asList("us-west", 7), scalars(values));
  }

  @Test
  void testValuePartitionIsUnescaped() {
    List<PartitionValue> values = extract("region:VALUE", "region=a%3Ab%2Fc");
    assertEquals("a:b/c", scalars(values).get(0));
  }

  @Test
  void testHiveDefaultPartitionIsNull() {
    List<PartitionValue> values =
        extract("region:VALUE,bucket:VALUE", "region=__HIVE_DEFAULT_PARTITION__/bucket=7");
    assertNull(scalars(values).get(0));
    assertEquals(7, scalars(values).get(1));
  }

  @Test
  void testValueAndTimePartitions() {
    List<PartitionValue> values =
        extract("region:VALUE,ts:DAY:ts=yyyy-MM-dd", "region=us-west/ts=2026-08-01");
    assertEquals(
        Arrays.asList("us-west", Instant.parse("2026-08-01T00:00:00Z").toEpochMilli()),
        scalars(values));
  }

  @Test
  void testTimePartitionAcrossDirectories() {
    List<PartitionValue> values = extract("ts:MONTH:year=yyyy/month=MM", "year=2026/month=08");
    assertEquals(Instant.parse("2026-08-01T00:00:00Z").toEpochMilli(), scalars(values).get(0));
  }

  @Test
  void testUnparsableValueNamesTheField() {
    PartitionValuesExtractorException e =
        assertThrows(
            PartitionValuesExtractorException.class, () -> extract("bucket:VALUE", "bucket=seven"));
    assertTrue(e.getMessage().contains("bucket"), e.getMessage());
  }

  @Test
  void testTimeTransformWithoutFormatIsRejected() {
    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> config("ts:DAY"));
    assertTrue(e.getMessage().contains("ts:DAY:ts=yyyy-MM-dd"), e.getMessage());
  }

  private static List<PartitionValue> extract(String partitionSpec, String partitionPath) {
    PathBasedPartitionSpecExtractor specExtractor =
        config(partitionSpec).loadSourcePartitionSpecExtractor();
    List<InternalPartitionField> partitionFields = specExtractor.spec(SCHEMA);
    return new ParquetPartitionValueExtractor(specExtractor.getPathToPartitionFieldFormat())
        .extractPartitionValues(partitionFields, partitionPath);
  }

  private static ParquetSourceConfig config(String partitionSpec) {
    Properties properties = new Properties();
    properties.put(ParquetSourceConfig.PARTITION_FIELD_SPEC_CONFIG, partitionSpec);
    return ParquetSourceConfig.fromProperties(properties);
  }

  private static List<Object> scalars(List<PartitionValue> values) {
    return values.stream().map(v -> v.getRange().getMaxValue()).collect(Collectors.toList());
  }

  private static InternalField field(String name, InternalType type) {
    return InternalField.builder()
        .name(name)
        .schema(InternalSchema.builder().name(type.name()).dataType(type).build())
        .build();
  }
}
