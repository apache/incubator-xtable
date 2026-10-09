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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Types;

import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;

public class TestIcebergPartitionValueConverter {
  private IcebergPartitionValueConverter partitionValueConverter =
      IcebergPartitionValueConverter.getInstance();
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "birthDate", Types.TimestampType.withZone()));
  private static final InternalSchema ONE_SCHEMA =
      IcebergSchemaExtractor.getInstance().fromIceberg(SCHEMA);

  @Test
  public void testToXTableNotPartitioned() {
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    List<PartitionValue> partitionValues =
        partitionValueConverter.toXTable(
            buildInternalTable(false), partitionData(partitionSpec), partitionSpec);
    assertTrue(partitionValues.isEmpty());
  }

  @Test
  public void testToXTableValuePartitioned() {
    List<PartitionValue> expectedPartitionValues =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(getPartitionField("name", PartitionTransformType.VALUE))
                .range(Range.scalar("abc"))
                .build());
    PartitionSpec partitionSpec = PartitionSpec.builderFor(SCHEMA).identity("name").build();
    List<PartitionValue> partitionValues =
        partitionValueConverter.toXTable(
            buildInternalTable(true, "name", PartitionTransformType.VALUE),
            partitionData(partitionSpec, "abc"),
            partitionSpec);
    assertEquals(1, partitionValues.size());
    assertEquals(expectedPartitionValues, partitionValues);
  }

  @Test
  public void testToXTablePartitionData() {
    PartitionSpec partitionSpec =
        PartitionSpec.builderFor(SCHEMA).identity("name").year("birthDate").build();
    PartitionData partitionData = new PartitionData(partitionSpec.partitionType());
    partitionData.set(0, "abc");
    partitionData.set(1, 51);
    List<PartitionValue> partitionValues =
        partitionValueConverter.toXTable(
            InternalTable.builder()
                .readSchema(ONE_SCHEMA)
                .partitioningFields(
                    Arrays.asList(
                        getPartitionField("name", PartitionTransformType.VALUE),
                        getPartitionField("birthDate", PartitionTransformType.YEAR)))
                .build(),
            partitionData,
            partitionSpec);
    assertEquals(
        Arrays.asList(
            PartitionValue.builder()
                .partitionField(getPartitionField("name", PartitionTransformType.VALUE))
                .range(Range.scalar("abc"))
                .build(),
            PartitionValue.builder()
                .partitionField(getPartitionField("birthDate", PartitionTransformType.YEAR))
                .range(Range.scalar(1609459200000L))
                .build()),
        partitionValues);
  }

  @Test
  public void testToXTableYearPartitioned() {
    List<PartitionValue> expectedPartitionValues =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(getPartitionField("birthDate", PartitionTransformType.YEAR))
                .range(Range.scalar(1609459200000L))
                .build());
    PartitionSpec partitionSpec = PartitionSpec.builderFor(SCHEMA).year("birthDate").build();
    List<PartitionValue> partitionValues =
        partitionValueConverter.toXTable(
            buildInternalTable(true, "birthDate", PartitionTransformType.YEAR),
            partitionData(partitionSpec, 51 /* Iceberg represents year as diff from 1970 */),
            partitionSpec);
    assertEquals(1, partitionValues.size());
    assertEquals(expectedPartitionValues, partitionValues);
  }

  @Test
  void testToXTableBucketPartitioned() {
    List<PartitionValue> expectedPartitionValues =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(getPartitionField("name", PartitionTransformType.BUCKET))
                .range(Range.scalar(5))
                .build());
    PartitionSpec partitionSpec = PartitionSpec.builderFor(SCHEMA).bucket("name", 8).build();
    List<PartitionValue> partitionValues =
        partitionValueConverter.toXTable(
            buildInternalTable(true, "name", PartitionTransformType.BUCKET),
            partitionData(partitionSpec, 5),
            partitionSpec);
    assertEquals(1, partitionValues.size());
    assertEquals(expectedPartitionValues, partitionValues);
  }

  private InternalTable buildInternalTable(boolean isPartitioned) {
    return buildInternalTable(isPartitioned, null, null);
  }

  private InternalTable buildInternalTable(
      boolean isPartitioned, String sourceField, PartitionTransformType transformType) {
    return InternalTable.builder()
        .readSchema(IcebergSchemaExtractor.getInstance().fromIceberg(SCHEMA))
        .partitioningFields(
            isPartitioned
                ? Collections.singletonList(getPartitionField(sourceField, transformType))
                : Collections.emptyList())
        .build();
  }

  private InternalPartitionField getPartitionField(
      String sourceField, PartitionTransformType transformType) {
    InternalField internalField =
        ONE_SCHEMA.getFields().stream()
            .filter(f -> f.getName().equals(sourceField))
            .findFirst()
            .get();
    return InternalPartitionField.builder()
        .sourceField(internalField)
        .transformType(transformType)
        .build();
  }

  private static StructLike partitionData(PartitionSpec partitionSpec, Object... values) {
    PartitionData partitionData = new PartitionData(partitionSpec.partitionType());
    for (int position = 0; position < values.length; position++) {
      partitionData.set(position, values[position]);
    }
    return partitionData;
  }
}
