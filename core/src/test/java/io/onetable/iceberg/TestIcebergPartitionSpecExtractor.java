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
 
package io.onetable.iceberg;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import io.onetable.model.schema.PartitionTransformType;

public class TestIcebergPartitionSpecExtractor {
  // TODO assert error cases and add wrap errors in Onetable exceptions
  private static final Schema TEST_SCHEMA =
      new Schema(
          Types.NestedField.required(0, "timestamp_hour", Types.TimestampType.withZone()),
          Types.NestedField.required(1, "timestamp_day", Types.TimestampType.withZone()),
          Types.NestedField.required(2, "timestamp_month", Types.TimestampType.withoutZone()),
          Types.NestedField.required(3, "timestamp_year", Types.DateType.get()),
          Types.NestedField.required(4, "string_field", Types.StringType.get()));

  @Test
  public void testUnpartitioned() {
    Schema icebergSchema =
        new Schema(Types.NestedField.required(0, "timestamp", Types.TimestampType.withZone()));
    PartitionSpec actual =
        IcebergPartitionSpecExtractor.getInstance().toPartitionSpec(null, icebergSchema);
    PartitionSpec expected = PartitionSpec.unpartitioned();
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testMultiplePartitions() {
    List<OnePartitionField> partitionFieldList =
        Arrays.asList(
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("timestamp_hour")
                        .schema(OneSchema.builder().dataType(OneType.TIMESTAMP).build())
                        .build())
                .transformType(PartitionTransformType.HOUR)
                .build(),
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("string_field")
                        .schema(OneSchema.builder().dataType(OneType.STRING).build())
                        .build())
                .transformType(PartitionTransformType.VALUE)
                .build());
    PartitionSpec actual =
        IcebergPartitionSpecExtractor.getInstance()
            .toPartitionSpec(partitionFieldList, TEST_SCHEMA);
    PartitionSpec expected =
        PartitionSpec.builderFor(TEST_SCHEMA)
            .hour("timestamp_hour")
            .identity("string_field")
            .build();
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testYearPartitioning() {
    List<OnePartitionField> partitionFieldList =
        Collections.singletonList(
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("timestamp_year")
                        .schema(OneSchema.builder().dataType(OneType.TIMESTAMP_NTZ).build())
                        .build())
                .transformType(PartitionTransformType.YEAR)
                .build());
    PartitionSpec actual =
        IcebergPartitionSpecExtractor.getInstance()
            .toPartitionSpec(partitionFieldList, TEST_SCHEMA);
    PartitionSpec expected = PartitionSpec.builderFor(TEST_SCHEMA).year("timestamp_year").build();
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testMonthPartitioning() {
    List<OnePartitionField> partitionFieldList =
        Collections.singletonList(
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("timestamp_month")
                        .schema(OneSchema.builder().dataType(OneType.TIMESTAMP_NTZ).build())
                        .build())
                .transformType(PartitionTransformType.MONTH)
                .build());
    PartitionSpec actual =
        IcebergPartitionSpecExtractor.getInstance()
            .toPartitionSpec(partitionFieldList, TEST_SCHEMA);
    PartitionSpec expected = PartitionSpec.builderFor(TEST_SCHEMA).month("timestamp_month").build();
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testDayPartitioning() {
    List<OnePartitionField> partitionFieldList =
        Collections.singletonList(
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("timestamp_day")
                        .schema(OneSchema.builder().dataType(OneType.TIMESTAMP_NTZ).build())
                        .build())
                .transformType(PartitionTransformType.DAY)
                .build());
    PartitionSpec actual =
        IcebergPartitionSpecExtractor.getInstance()
            .toPartitionSpec(partitionFieldList, TEST_SCHEMA);
    PartitionSpec expected = PartitionSpec.builderFor(TEST_SCHEMA).day("timestamp_day").build();
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testHourPartitioning() {
    List<OnePartitionField> partitionFieldList =
        Collections.singletonList(
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("timestamp_hour")
                        .schema(OneSchema.builder().dataType(OneType.TIMESTAMP_NTZ).build())
                        .build())
                .transformType(PartitionTransformType.HOUR)
                .build());
    PartitionSpec actual =
        IcebergPartitionSpecExtractor.getInstance()
            .toPartitionSpec(partitionFieldList, TEST_SCHEMA);
    PartitionSpec expected = PartitionSpec.builderFor(TEST_SCHEMA).hour("timestamp_hour").build();
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testNestedPartitionField() {
    List<OnePartitionField> partitionFieldList =
        Collections.singletonList(
            OnePartitionField.builder()
                .sourceField(
                    OneField.builder()
                        .name("nested")
                        .parentPath("data")
                        .schema(OneSchema.builder().dataType(OneType.STRING).build())
                        .build())
                .transformType(PartitionTransformType.VALUE)
                .build());
    Schema icebergSchema =
        new Schema(
            Types.NestedField.required(
                0,
                "data",
                Types.StructType.of(
                    Types.NestedField.required(1, "nested", Types.StringType.get()))));
    PartitionSpec actual =
        IcebergPartitionSpecExtractor.getInstance()
            .toPartitionSpec(partitionFieldList, icebergSchema);
    PartitionSpec expected =
        PartitionSpec.builderFor(icebergSchema).identity("data.nested").build();
    Assertions.assertEquals(expected, actual);
  }
}
