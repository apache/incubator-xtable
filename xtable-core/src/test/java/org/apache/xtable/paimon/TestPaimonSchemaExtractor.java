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
 
package org.apache.xtable.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;

public class TestPaimonSchemaExtractor {
  private static final PaimonSchemaExtractor schemaExtractor = PaimonSchemaExtractor.getInstance();

  private void assertField(DataField paimonField, InternalField expectedInternalField) {
    assertField(paimonField, expectedInternalField, Collections.emptyList());
  }

  private void assertField(
      DataField paimonField, InternalField expectedInternalField, List<String> primaryKeys) {
    TableSchema paimonSchema =
        new TableSchema(
            0,
            Collections.singletonList(paimonField),
            0,
            Collections.emptyList(),
            primaryKeys,
            new HashMap<>(),
            "");
    InternalSchema internalSchema = schemaExtractor.toInternalSchema(paimonSchema);
    List<InternalField> recordKeyFields =
        primaryKeys.isEmpty()
            ? Collections.emptyList()
            : Collections.singletonList(expectedInternalField);
    InternalSchema expectedSchema =
        InternalSchema.builder()
            .name("record")
            .dataType(InternalType.RECORD)
            .fields(Collections.singletonList(expectedInternalField))
            .recordKeyFields(recordKeyFields)
            .build();
    assertEquals(expectedSchema, internalSchema);
  }

  @Test
  void testCharField() {
    DataField paimonField = new DataField(0, "char_field", new CharType(10));
    InternalField expectedField =
        InternalField.builder()
            .name("char_field")
            .fieldId(0)
            .schema(
                InternalSchema.builder()
                    .name("CHAR(10)")
                    .dataType(InternalType.STRING)
                    .isNullable(true)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    assertField(paimonField, expectedField);
  }

  @Test
  void testVarcharField() {
    DataField paimonField = new DataField(1, "varchar_field", new VarCharType(255));
    InternalField expectedField =
        InternalField.builder()
            .name("varchar_field")
            .fieldId(1)
            .schema(
                InternalSchema.builder()
                    .name("VARCHAR(255)")
                    .dataType(InternalType.STRING)
                    .isNullable(true)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    assertField(paimonField, expectedField);
  }

  @Test
  void testBooleanField() {
    DataField paimonField = new DataField(2, "boolean_field", new BooleanType());
    InternalField expectedField =
        InternalField.builder()
            .name("boolean_field")
            .fieldId(2)
            .schema(
                InternalSchema.builder()
                    .name("BOOLEAN")
                    .dataType(InternalType.BOOLEAN)
                    .isNullable(true)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    assertField(paimonField, expectedField);
  }

  @Test
  void testTinyIntField() {
    DataField paimonField = new DataField(3, "tinyint_field", new TinyIntType());
    InternalField expectedField =
        InternalField.builder()
            .name("tinyint_field")
            .fieldId(3)
            .schema(
                InternalSchema.builder()
                    .name("TINYINT")
                    .dataType(InternalType.INT)
                    .isNullable(true)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    assertField(paimonField, expectedField);
  }

  @Test
  void testSmallIntField() {
    DataField paimonField = new DataField(4, "smallint_field", new SmallIntType());
    InternalField expectedField =
        InternalField.builder()
            .name("smallint_field")
            .fieldId(4)
            .schema(
                InternalSchema.builder()
                    .name("SMALLINT")
                    .dataType(InternalType.INT)
                    .isNullable(true)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    assertField(paimonField, expectedField);
  }

  @Test
  void testIntField() {
    DataField paimonField = new DataField(5, "int_field", new IntType());
    InternalField expectedField =
        InternalField.builder()
            .name("int_field")
            .fieldId(5)
            .schema(
                InternalSchema.builder()
                    .name("INT")
                    .dataType(InternalType.INT)
                    .isNullable(true)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    assertField(paimonField, expectedField);
  }

  @Test
  void testBigIntField() {
    DataField paimonField = new DataField(6, "bigint_field", new BigIntType());
    InternalField expectedField =
        InternalField.builder()
            .name("bigint_field")
            .fieldId(6)
            .schema(
                InternalSchema.builder()
                    .name("BIGINT")
                    .dataType(InternalType.LONG)
                    .isNullable(true)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    assertField(paimonField, expectedField);
  }

  @Test
  void testFloatField() {
    DataField paimonField = new DataField(7, "float_field", new FloatType());
    InternalField expectedField =
        InternalField.builder()
            .name("float_field")
            .fieldId(7)
            .schema(
                InternalSchema.builder()
                    .name("FLOAT")
                    .dataType(InternalType.FLOAT)
                    .isNullable(true)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    assertField(paimonField, expectedField);
  }

  @Test
  void testDoubleField() {
    DataField paimonField = new DataField(8, "double_field", new DoubleType());
    InternalField expectedField =
        InternalField.builder()
            .name("double_field")
            .fieldId(8)
            .schema(
                InternalSchema.builder()
                    .name("DOUBLE")
                    .dataType(InternalType.DOUBLE)
                    .isNullable(true)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    assertField(paimonField, expectedField);
  }

  @Test
  void testDateField() {
    DataField paimonField = new DataField(9, "date_field", new DateType());
    InternalField expectedField =
        InternalField.builder()
            .name("date_field")
            .fieldId(9)
            .schema(
                InternalSchema.builder()
                    .name("DATE")
                    .dataType(InternalType.DATE)
                    .isNullable(true)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    assertField(paimonField, expectedField);
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9})
  void testTimestampField(int precision) {
    DataField paimonField = new DataField(10, "timestamp_field", new TimestampType(precision));
    
    InternalSchema.MetadataValue expectedPrecision;
    if (precision <= 3) {
      expectedPrecision = InternalSchema.MetadataValue.MILLIS;
    } else if (precision <= 6) {
      expectedPrecision = InternalSchema.MetadataValue.MICROS;
    } else {
      expectedPrecision = InternalSchema.MetadataValue.NANOS;
    }
    
    Map<InternalSchema.MetadataKey, Object> timestampMetadata =
        Collections.singletonMap(
            InternalSchema.MetadataKey.TIMESTAMP_PRECISION, expectedPrecision);
    InternalField expectedField =
        InternalField.builder()
            .name("timestamp_field")
            .fieldId(10)
            .schema(
                InternalSchema.builder()
                    .name("TIMESTAMP(" + precision + ")")
                    .dataType(InternalType.TIMESTAMP)
                    .isNullable(true)
                    .metadata(timestampMetadata)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    assertField(paimonField, expectedField);
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 10})
  void testInvalidTimestampPrecisionField(int invalidPrecision) {
    assertThrows(IllegalArgumentException.class, () -> new TimestampType(invalidPrecision));
  }

  @Test
  void testDecimalField() {
    DataField paimonField = new DataField(11, "decimal_field", new DecimalType(10, 2));
    Map<InternalSchema.MetadataKey, Object> decimalMetadata = new HashMap<>();
    decimalMetadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, 10);
    decimalMetadata.put(InternalSchema.MetadataKey.DECIMAL_SCALE, 2);
    InternalField expectedField =
        InternalField.builder()
            .name("decimal_field")
            .fieldId(11)
            .schema(
                InternalSchema.builder()
                    .name("DECIMAL(10, 2)")
                    .dataType(InternalType.DECIMAL)
                    .isNullable(true)
                    .metadata(decimalMetadata)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    assertField(paimonField, expectedField);
  }

  @Test
  void testStructField() {
    DataField paimonField =
        new DataField(
            12,
            "struct_field",
            RowType.of(
                new DataType[] {
                  new IntType(),
                  new VarCharType(255),
                  RowType.of(new DataType[] {new DoubleType()}, new String[] {"very_nested_double"})
                },
                new String[] {"nested_int", "nested_varchar", "nested_struct"}));
    InternalField nestedIntField =
        InternalField.builder()
            .name("nested_int")
            .fieldId(0)
            .parentPath("struct_field")
            .schema(
                InternalSchema.builder()
                    .name("INT")
                    .dataType(InternalType.INT)
                    .isNullable(true)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    InternalField nestedVarcharField =
        InternalField.builder()
            .name("nested_varchar")
            .fieldId(1)
            .parentPath("struct_field")
            .schema(
                InternalSchema.builder()
                    .name("VARCHAR(255)")
                    .dataType(InternalType.STRING)
                    .isNullable(true)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    InternalField veryNestedDoubleField =
        InternalField.builder()
            .name("very_nested_double")
            .fieldId(0)
            .parentPath("struct_field.nested_struct")
            .schema(
                InternalSchema.builder()
                    .name("DOUBLE")
                    .dataType(InternalType.DOUBLE)
                    .isNullable(true)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    InternalField nestedStructField =
        InternalField.builder()
            .name("nested_struct")
            .fieldId(2)
            .parentPath("struct_field")
            .schema(
                InternalSchema.builder()
                    .name("ROW<`very_nested_double` DOUBLE>")
                    .dataType(InternalType.RECORD)
                    .isNullable(true)
                    .fields(Collections.singletonList(veryNestedDoubleField))
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    InternalField expectedField =
        InternalField.builder()
            .name("struct_field")
            .fieldId(12)
            .schema(
                InternalSchema.builder()
                    .name(
                        "ROW<`nested_int` INT, `nested_varchar` VARCHAR(255), `nested_struct` ROW<`very_nested_double` DOUBLE>>")
                    .dataType(InternalType.RECORD)
                    .isNullable(true)
                    .fields(Arrays.asList(nestedIntField, nestedVarcharField, nestedStructField))
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    assertField(paimonField, expectedField);
  }

  @Test
  void testArrayField() {
    DataField paimonField = new DataField(13, "array_field", new ArrayType(new IntType()));
    InternalField arrayElementField =
        InternalField.builder()
            .name("_one_field_element")
            .parentPath("array_field")
            .schema(
                InternalSchema.builder()
                    .name("INT")
                    .dataType(InternalType.INT)
                    .isNullable(true)
                    .build())
            .build();
    InternalField expectedField =
        InternalField.builder()
            .name("array_field")
            .fieldId(13)
            .schema(
                InternalSchema.builder()
                    .name("ARRAY<INT>")
                    .dataType(InternalType.LIST)
                    .isNullable(true)
                    .fields(Collections.singletonList(arrayElementField))
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    assertField(paimonField, expectedField);
  }

  @Test
  void testMapField() {
    DataField paimonField =
        new DataField(14, "map_field", new MapType(new VarCharType(255), new IntType()));
    InternalField mapKeyField =
        InternalField.builder()
            .name("_one_field_key")
            .parentPath("map_field")
            .schema(
                InternalSchema.builder()
                    .name("VARCHAR(255)")
                    .dataType(InternalType.STRING)
                    .isNullable(false)
                    .build())
            .build();
    InternalField mapValueField =
        InternalField.builder()
            .name("_one_field_value")
            .parentPath("map_field")
            .schema(
                InternalSchema.builder()
                    .name("INT")
                    .dataType(InternalType.INT)
                    .isNullable(true)
                    .build())
            .build();
    InternalField expectedField =
        InternalField.builder()
            .name("map_field")
            .fieldId(14)
            .schema(
                InternalSchema.builder()
                    .name("MAP<VARCHAR(255), INT>")
                    .dataType(InternalType.MAP)
                    .isNullable(true)
                    .fields(Arrays.asList(mapKeyField, mapValueField))
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    assertField(paimonField, expectedField);
  }

  @Test
  void testPrimaryKey() {
    DataField paimonField = new DataField(0, "pk_field", new IntType());
    InternalField expectedField =
        InternalField.builder()
            .name("pk_field")
            .fieldId(0)
            .schema(
                InternalSchema.builder()
                    .name("INT")
                    .dataType(InternalType.INT)
                    .isNullable(true)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    assertField(paimonField, expectedField, Collections.singletonList("pk_field"));
  }

  @Test
  void testMultiplePrimaryKeys() {
    DataField intField = new DataField(0, "int_pk", new IntType());
    DataField stringField = new DataField(1, "string_pk", new VarCharType(255));
    List<DataField> paimonFields = Arrays.asList(intField, stringField);
    List<String> primaryKeys = Arrays.asList("int_pk", "string_pk");
    TableSchema paimonSchema =
        new TableSchema(
            0, paimonFields, 0, Collections.emptyList(), primaryKeys, new HashMap<>(), "");
    InternalSchema internalSchema = schemaExtractor.toInternalSchema(paimonSchema);

    InternalField expectedIntField =
        InternalField.builder()
            .name("int_pk")
            .fieldId(0)
            .schema(
                InternalSchema.builder()
                    .name("INT")
                    .dataType(InternalType.INT)
                    .isNullable(true)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    InternalField expectedStringField =
        InternalField.builder()
            .name("string_pk")
            .fieldId(1)
            .schema(
                InternalSchema.builder()
                    .name("VARCHAR(255)")
                    .dataType(InternalType.STRING)
                    .isNullable(true)
                    .build())
            .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
            .build();
    List<InternalField> expectedFields = Arrays.asList(expectedIntField, expectedStringField);
    InternalSchema expectedSchema =
        InternalSchema.builder()
            .name("record")
            .dataType(InternalType.RECORD)
            .fields(expectedFields)
            .recordKeyFields(expectedFields)
            .build();
    assertEquals(expectedSchema, internalSchema);
  }
}
