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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Types;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;

public class TestIcebergPartitionSpecExtractor {
  // TODO assert error cases and add wrap errors in XTable exceptions
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
        IcebergPartitionSpecExtractor.getInstance().toIceberg(null, icebergSchema);
    PartitionSpec expected = PartitionSpec.unpartitioned();
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testMultiplePartitions() {
    List<InternalPartitionField> partitionFieldList =
        Arrays.asList(
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder()
                        .name("timestamp_hour")
                        .schema(InternalSchema.builder().dataType(InternalType.TIMESTAMP).build())
                        .build())
                .transformType(PartitionTransformType.HOUR)
                .build(),
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder()
                        .name("string_field")
                        .schema(InternalSchema.builder().dataType(InternalType.STRING).build())
                        .build())
                .transformType(PartitionTransformType.VALUE)
                .build());
    PartitionSpec actual =
        IcebergPartitionSpecExtractor.getInstance().toIceberg(partitionFieldList, TEST_SCHEMA);
    PartitionSpec expected =
        PartitionSpec.builderFor(TEST_SCHEMA)
            .hour("timestamp_hour")
            .identity("string_field")
            .build();
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testYearPartitioning() {
    List<InternalPartitionField> partitionFieldList =
        Collections.singletonList(
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder()
                        .name("timestamp_year")
                        .schema(
                            InternalSchema.builder().dataType(InternalType.TIMESTAMP_NTZ).build())
                        .build())
                .transformType(PartitionTransformType.YEAR)
                .build());
    PartitionSpec actual =
        IcebergPartitionSpecExtractor.getInstance().toIceberg(partitionFieldList, TEST_SCHEMA);
    PartitionSpec expected = PartitionSpec.builderFor(TEST_SCHEMA).year("timestamp_year").build();
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testMonthPartitioning() {
    List<InternalPartitionField> partitionFieldList =
        Collections.singletonList(
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder()
                        .name("timestamp_month")
                        .schema(
                            InternalSchema.builder().dataType(InternalType.TIMESTAMP_NTZ).build())
                        .build())
                .transformType(PartitionTransformType.MONTH)
                .build());
    PartitionSpec actual =
        IcebergPartitionSpecExtractor.getInstance().toIceberg(partitionFieldList, TEST_SCHEMA);
    PartitionSpec expected = PartitionSpec.builderFor(TEST_SCHEMA).month("timestamp_month").build();
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testDayPartitioning() {
    List<InternalPartitionField> partitionFieldList =
        Collections.singletonList(
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder()
                        .name("timestamp_day")
                        .schema(
                            InternalSchema.builder().dataType(InternalType.TIMESTAMP_NTZ).build())
                        .build())
                .transformType(PartitionTransformType.DAY)
                .build());
    PartitionSpec actual =
        IcebergPartitionSpecExtractor.getInstance().toIceberg(partitionFieldList, TEST_SCHEMA);
    PartitionSpec expected = PartitionSpec.builderFor(TEST_SCHEMA).day("timestamp_day").build();
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testHourPartitioning() {
    List<InternalPartitionField> partitionFieldList =
        Collections.singletonList(
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder()
                        .name("timestamp_hour")
                        .schema(
                            InternalSchema.builder().dataType(InternalType.TIMESTAMP_NTZ).build())
                        .build())
                .transformType(PartitionTransformType.HOUR)
                .build());
    PartitionSpec actual =
        IcebergPartitionSpecExtractor.getInstance().toIceberg(partitionFieldList, TEST_SCHEMA);
    PartitionSpec expected = PartitionSpec.builderFor(TEST_SCHEMA).hour("timestamp_hour").build();
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testNestedPartitionField() {
    List<InternalPartitionField> partitionFieldList =
        Collections.singletonList(
            InternalPartitionField.builder()
                .sourceField(
                    InternalField.builder()
                        .name("nested")
                        .parentPath("data")
                        .schema(InternalSchema.builder().dataType(InternalType.STRING).build())
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
        IcebergPartitionSpecExtractor.getInstance().toIceberg(partitionFieldList, icebergSchema);
    PartitionSpec expected =
        PartitionSpec.builderFor(icebergSchema).identity("data.nested").build();
    Assertions.assertEquals(expected, actual);
  }

  @Test
  public void testFromIcebergUnPartitioned() {
    IcebergPartitionSpecExtractor extractor = IcebergPartitionSpecExtractor.getInstance();
    List<InternalPartitionField> fields =
        extractor.fromIceberg(PartitionSpec.unpartitioned(), null, null);
    Assertions.assertEquals(0, fields.size());
  }

  @Test
  public void testFromIcebergSingleColumn() {
    IcebergPartitionSpecExtractor extractor = IcebergPartitionSpecExtractor.getInstance();

    Schema iceSchema =
        new Schema(
            Types.NestedField.required(0, "data_int", Types.IntegerType.get()),
            Types.NestedField.required(1, "key_string", Types.StringType.get()));
    PartitionSpec icePartitionSpec =
        PartitionSpec.builderFor(iceSchema).identity("key_string").build();

    InternalSchema irSchema =
        InternalSchema.builder()
            .name("test_schema")
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("data_int")
                        .schema(InternalSchema.builder().dataType(InternalType.INT).build())
                        .build(),
                    InternalField.builder()
                        .name("key_string")
                        .fieldId(1)
                        .schema(InternalSchema.builder().dataType(InternalType.STRING).build())
                        .build()))
            .build();

    List<InternalPartitionField> irPartitionSpec =
        extractor.fromIceberg(icePartitionSpec, iceSchema, irSchema);
    Assertions.assertEquals(1, irPartitionSpec.size());
    InternalField sourceField = irPartitionSpec.get(0).getSourceField();
    Assertions.assertEquals("key_string", sourceField.getName());
    Assertions.assertEquals(1, sourceField.getFieldId());
    Assertions.assertEquals(InternalType.STRING, sourceField.getSchema().getDataType());
    Assertions.assertEquals(
        PartitionTransformType.VALUE, irPartitionSpec.get(0).getTransformType());
  }

  @Test
  public void testFromIcebergMultiColumn() {
    IcebergPartitionSpecExtractor extractor = IcebergPartitionSpecExtractor.getInstance();

    Schema iceSchema =
        new Schema(
            Types.NestedField.required(0, "key_year", Types.DateType.get()),
            Types.NestedField.required(1, "key_string", Types.StringType.get()));
    PartitionSpec icePartitionSpec =
        PartitionSpec.builderFor(iceSchema).identity("key_string").year("key_year").build();

    InternalSchema irSchema =
        InternalSchema.builder()
            .name("test_schema")
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("key_year")
                        .fieldId(10)
                        .schema(InternalSchema.builder().dataType(InternalType.DATE).build())
                        .build(),
                    InternalField.builder()
                        .name("key_string")
                        .fieldId(11)
                        .schema(InternalSchema.builder().dataType(InternalType.STRING).build())
                        .build()))
            .build();

    List<InternalPartitionField> irPartitionSpec =
        extractor.fromIceberg(icePartitionSpec, iceSchema, irSchema);
    Assertions.assertEquals(2, irPartitionSpec.size());

    InternalField sourceField = irPartitionSpec.get(0).getSourceField();
    Assertions.assertEquals("key_string", sourceField.getName());
    Assertions.assertEquals(11, sourceField.getFieldId());
    Assertions.assertEquals(InternalType.STRING, sourceField.getSchema().getDataType());
    Assertions.assertEquals(
        PartitionTransformType.VALUE, irPartitionSpec.get(0).getTransformType());

    sourceField = irPartitionSpec.get(1).getSourceField();
    Assertions.assertEquals("key_year", sourceField.getName());
    Assertions.assertEquals(10, sourceField.getFieldId());
    Assertions.assertEquals(InternalType.DATE, sourceField.getSchema().getDataType());
    Assertions.assertEquals(PartitionTransformType.YEAR, irPartitionSpec.get(1).getTransformType());
  }

  @Test
  public void fromIcebergTransformType() {
    IcebergPartitionSpecExtractor extractor = IcebergPartitionSpecExtractor.getInstance();
    Assertions.assertEquals(
        PartitionTransformType.YEAR, extractor.fromIcebergTransform(Transforms.year()));
    Assertions.assertEquals(
        PartitionTransformType.MONTH, extractor.fromIcebergTransform(Transforms.month()));
    Assertions.assertEquals(
        PartitionTransformType.DAY, extractor.fromIcebergTransform(Transforms.day()));
    Assertions.assertEquals(
        PartitionTransformType.HOUR, extractor.fromIcebergTransform(Transforms.hour()));
    Assertions.assertEquals(
        PartitionTransformType.VALUE, extractor.fromIcebergTransform(Transforms.identity()));

    Assertions.assertThrows(
        NotSupportedException.class, () -> extractor.fromIcebergTransform(Transforms.bucket(10)));
  }
}
