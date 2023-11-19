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

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.Test;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Types;

import io.onetable.model.OneTable;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.stat.PartitionValue;
import io.onetable.model.stat.Range;

public class TestIcebergPartitionValueConverter {
  private IcebergPartitionValueConverter partitionValueConverter =
      IcebergPartitionValueConverter.getInstance();
  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.optional(1, "key", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "birthDate", Types.TimestampType.withZone()));
  private static final Schema SCHEMA_WITH_PARTITION =
      new Schema(
          Types.NestedField.optional(1, "key", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "birthDate", Types.TimestampType.withZone()),
          Types.NestedField.optional(4, "birthDate_year", Types.IntegerType.get()));
  private static final StructLike STRUCT_LIKE_RECORD =
      Row.of(
          SCHEMA_WITH_PARTITION,
          1,
          "abc",
          1614556800000L,
          51 /* Iceberg represents year as diff from 1970 */);
  private static final OneSchema ONE_SCHEMA =
      IcebergSchemaExtractor.getInstance().fromIceberg(SCHEMA);

  @Test
  public void testToOneTableNotPartitioned() {
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    List<PartitionValue> partitionValues =
        partitionValueConverter.toOneTable(buildOnetable(false), STRUCT_LIKE_RECORD, partitionSpec);
    assertTrue(partitionValues.isEmpty());
  }

  @Test
  public void testToOneTableValuePartitioned() {
    List<PartitionValue> expectedPartitionValues =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(getPartitionField("name", PartitionTransformType.VALUE))
                .range(Range.scalar("abc"))
                .build());
    PartitionSpec partitionSpec = PartitionSpec.builderFor(SCHEMA).identity("name").build();
    List<PartitionValue> partitionValues =
        partitionValueConverter.toOneTable(
            buildOnetable(true, "name", PartitionTransformType.VALUE),
            STRUCT_LIKE_RECORD,
            partitionSpec);
    assertEquals(1, partitionValues.size());
    assertEquals(expectedPartitionValues, partitionValues);
  }

  @Test
  public void testToOneTableYearPartitioned() {
    List<PartitionValue> expectedPartitionValues =
        Collections.singletonList(
            PartitionValue.builder()
                .partitionField(getPartitionField("birthDate", PartitionTransformType.YEAR))
                .range(Range.scalar(1609459200000L))
                .build());
    PartitionSpec partitionSpec = PartitionSpec.builderFor(SCHEMA).year("birthDate").build();
    List<PartitionValue> partitionValues =
        partitionValueConverter.toOneTable(
            buildOnetable(true, "birthDate", PartitionTransformType.YEAR),
            STRUCT_LIKE_RECORD,
            partitionSpec);
    assertEquals(1, partitionValues.size());
    assertEquals(expectedPartitionValues, partitionValues);
  }

  private OneTable buildOnetable(boolean isPartitioned) {
    return buildOnetable(isPartitioned, null, null);
  }

  private OneTable buildOnetable(
      boolean isPartitioned, String sourceField, PartitionTransformType transformType) {
    return OneTable.builder()
        .readSchema(IcebergSchemaExtractor.getInstance().fromIceberg(SCHEMA))
        .partitioningFields(
            isPartitioned
                ? Collections.singletonList(getPartitionField(sourceField, transformType))
                : Collections.emptyList())
        .build();
  }

  private OnePartitionField getPartitionField(
      String sourceField, PartitionTransformType transformType) {
    OneField oneField =
        ONE_SCHEMA.getFields().stream()
            .filter(f -> f.getName().equals(sourceField))
            .findFirst()
            .get();
    return OnePartitionField.builder().sourceField(oneField).transformType(transformType).build();
  }

  public static class Row implements StructLike, IndexedRecord {
    public static Row of(Schema schema, Object... values) {
      return new Row(schema, values);
    }

    private final Object[] values;
    private final Schema schema;

    private Row(Schema schema, Object... values) {
      this.schema = schema;
      this.values = values;
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      values[pos] = value;
    }

    @Override
    public void put(int i, Object v) {
      values[i] = v;
    }

    @Override
    public Object get(int i) {
      return values[i];
    }

    @Override
    public org.apache.avro.Schema getSchema() {
      return AvroSchemaUtil.convert(schema, "testSchema");
    }
  }
}
