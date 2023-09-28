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
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;

public class TestIcebergSchemaExtractor {

  @Test
  public void testPrimitiveTypes() {
    String schemaName = "testRecord";
    String doc = "What's up doc";
    Map<OneSchema.MetadataKey, Object> requiredEnumMetadata = new HashMap<>();
    requiredEnumMetadata.put(OneSchema.MetadataKey.ENUM_VALUES, Arrays.asList("ONE", "TWO"));
    Map<OneSchema.MetadataKey, Object> optionalEnumMetadata = new HashMap<>();
    optionalEnumMetadata.put(OneSchema.MetadataKey.ENUM_VALUES, Arrays.asList("THREE", "FOUR"));

    int precision = 10;
    int scale = 5;
    Map<OneSchema.MetadataKey, Object> doubleMetadata = new HashMap<>();
    doubleMetadata.put(OneSchema.MetadataKey.DECIMAL_PRECISION, precision);
    doubleMetadata.put(OneSchema.MetadataKey.DECIMAL_SCALE, scale);

    int fixedSize = 8;
    Map<OneSchema.MetadataKey, Object> fixedMetadata = new HashMap<>();
    fixedMetadata.put(OneSchema.MetadataKey.FIXED_BYTES_SIZE, fixedSize);

    Map<OneSchema.MetadataKey, Object> millisTimestamp = new HashMap<>();
    millisTimestamp.put(OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MILLIS);

    Map<OneSchema.MetadataKey, Object> microsTimestamp = new HashMap<>();
    microsTimestamp.put(OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MICROS);

    OneSchema oneSchemaRepresentation =
        OneSchema.builder()
            .name(schemaName)
            .comment(doc)
            .dataType(OneType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("requiredBoolean")
                        .schema(
                            OneSchema.builder()
                                .name("boolean")
                                .dataType(OneType.BOOLEAN)
                                .isNullable(false)
                                .build())
                        .defaultValue(false)
                        .build(),
                    OneField.builder()
                        .name("optionalBoolean")
                        .schema(
                            OneSchema.builder()
                                .name("boolean")
                                .dataType(OneType.BOOLEAN)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("requiredInt")
                        .schema(
                            OneSchema.builder()
                                .name("int")
                                .dataType(OneType.INT)
                                .isNullable(false)
                                .build())
                        .defaultValue(123)
                        .build(),
                    OneField.builder()
                        .name("optionalInt")
                        .schema(
                            OneSchema.builder()
                                .name("int")
                                .dataType(OneType.INT)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("requiredLong")
                        .schema(
                            OneSchema.builder()
                                .name("long")
                                .dataType(OneType.LONG)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalLong")
                        .schema(
                            OneSchema.builder()
                                .name("long")
                                .dataType(OneType.LONG)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("requiredDouble")
                        .schema(
                            OneSchema.builder()
                                .name("double")
                                .dataType(OneType.DOUBLE)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalDouble")
                        .schema(
                            OneSchema.builder()
                                .name("double")
                                .dataType(OneType.DOUBLE)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("requiredFloat")
                        .schema(
                            OneSchema.builder()
                                .name("float")
                                .dataType(OneType.FLOAT)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalFloat")
                        .schema(
                            OneSchema.builder()
                                .name("float")
                                .dataType(OneType.FLOAT)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("requiredString")
                        .schema(
                            OneSchema.builder()
                                .name("string")
                                .dataType(OneType.STRING)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalString")
                        .schema(
                            OneSchema.builder()
                                .name("string")
                                .dataType(OneType.STRING)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("requiredBytes")
                        .schema(
                            OneSchema.builder()
                                .name("bytes")
                                .dataType(OneType.BYTES)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalBytes")
                        .schema(
                            OneSchema.builder()
                                .name("bytes")
                                .dataType(OneType.BYTES)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("requiredEnum")
                        .schema(
                            OneSchema.builder()
                                .name("REQUIRED_ENUM")
                                .dataType(OneType.ENUM)
                                .isNullable(false)
                                .metadata(requiredEnumMetadata)
                                .build())
                        .defaultValue("ONE")
                        .build(),
                    OneField.builder()
                        .name("optionalEnum")
                        .schema(
                            OneSchema.builder()
                                .name("OPTIONAL_ENUM")
                                .dataType(OneType.ENUM)
                                .isNullable(true)
                                .metadata(optionalEnumMetadata)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("requiredDate")
                        .schema(
                            OneSchema.builder()
                                .name("date")
                                .dataType(OneType.DATE)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalDate")
                        .schema(
                            OneSchema.builder()
                                .name("date")
                                .dataType(OneType.DATE)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("requiredTimestampMillis")
                        .schema(
                            OneSchema.builder()
                                .name("timestamp")
                                .dataType(OneType.TIMESTAMP)
                                .metadata(millisTimestamp)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalTimestampMillis")
                        .schema(
                            OneSchema.builder()
                                .name("timestamp")
                                .dataType(OneType.TIMESTAMP)
                                .metadata(millisTimestamp)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("requiredTimestampNtzMillis")
                        .schema(
                            OneSchema.builder()
                                .name("timestampNtz")
                                .dataType(OneType.TIMESTAMP_NTZ)
                                .isNullable(false)
                                .metadata(millisTimestamp)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalTimestampNtzMillis")
                        .schema(
                            OneSchema.builder()
                                .name("timestampNtz")
                                .dataType(OneType.TIMESTAMP_NTZ)
                                .metadata(millisTimestamp)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("requiredTimestampMicros")
                        .schema(
                            OneSchema.builder()
                                .name("timestamp")
                                .dataType(OneType.TIMESTAMP)
                                .metadata(microsTimestamp)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalTimestampMicros")
                        .schema(
                            OneSchema.builder()
                                .name("timestamp")
                                .dataType(OneType.TIMESTAMP)
                                .metadata(microsTimestamp)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("requiredTimestampNtzMicros")
                        .schema(
                            OneSchema.builder()
                                .name("timestampNtz")
                                .dataType(OneType.TIMESTAMP_NTZ)
                                .isNullable(false)
                                .metadata(microsTimestamp)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalTimestampNtzMicros")
                        .schema(
                            OneSchema.builder()
                                .name("timestampNtz")
                                .dataType(OneType.TIMESTAMP_NTZ)
                                .metadata(microsTimestamp)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("requiredFixed")
                        .schema(
                            OneSchema.builder()
                                .name("fixed")
                                .dataType(OneType.FIXED)
                                .isNullable(false)
                                .metadata(fixedMetadata)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalFixed")
                        .schema(
                            OneSchema.builder()
                                .name("fixed")
                                .dataType(OneType.FIXED)
                                .isNullable(true)
                                .metadata(fixedMetadata)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("requiredDecimal")
                        .schema(
                            OneSchema.builder()
                                .name("decimal")
                                .dataType(OneType.DECIMAL)
                                .isNullable(false)
                                .metadata(doubleMetadata)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalDecimal")
                        .schema(
                            OneSchema.builder()
                                .name("decimal")
                                .dataType(OneType.DECIMAL)
                                .isNullable(true)
                                .metadata(doubleMetadata)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    Schema expectedSchema =
        new Schema(
            Types.NestedField.required(1, "requiredBoolean", Types.BooleanType.get()),
            Types.NestedField.optional(2, "optionalBoolean", Types.BooleanType.get()),
            Types.NestedField.required(3, "requiredInt", Types.IntegerType.get()),
            Types.NestedField.optional(4, "optionalInt", Types.IntegerType.get()),
            Types.NestedField.required(5, "requiredLong", Types.LongType.get()),
            Types.NestedField.optional(6, "optionalLong", Types.LongType.get()),
            Types.NestedField.required(7, "requiredDouble", Types.DoubleType.get()),
            Types.NestedField.optional(8, "optionalDouble", Types.DoubleType.get()),
            Types.NestedField.required(9, "requiredFloat", Types.FloatType.get()),
            Types.NestedField.optional(10, "optionalFloat", Types.FloatType.get()),
            Types.NestedField.required(11, "requiredString", Types.StringType.get()),
            Types.NestedField.optional(12, "optionalString", Types.StringType.get()),
            Types.NestedField.required(13, "requiredBytes", Types.BinaryType.get()),
            Types.NestedField.optional(14, "optionalBytes", Types.BinaryType.get()),
            Types.NestedField.required(15, "requiredEnum", Types.StringType.get()),
            Types.NestedField.optional(16, "optionalEnum", Types.StringType.get()),
            Types.NestedField.required(17, "requiredDate", Types.DateType.get()),
            Types.NestedField.optional(18, "optionalDate", Types.DateType.get()),
            Types.NestedField.required(
                19, "requiredTimestampMillis", Types.TimestampType.withZone()),
            Types.NestedField.optional(
                20, "optionalTimestampMillis", Types.TimestampType.withZone()),
            Types.NestedField.required(21, "requiredTimestampNtzMillis", Types.LongType.get()),
            Types.NestedField.optional(22, "optionalTimestampNtzMillis", Types.LongType.get()),
            Types.NestedField.required(
                23, "requiredTimestampMicros", Types.TimestampType.withZone()),
            Types.NestedField.optional(
                24, "optionalTimestampMicros", Types.TimestampType.withZone()),
            Types.NestedField.required(25, "requiredTimestampNtzMicros", Types.LongType.get()),
            Types.NestedField.optional(26, "optionalTimestampNtzMicros", Types.LongType.get()),
            Types.NestedField.required(27, "requiredFixed", Types.FixedType.ofLength(fixedSize)),
            Types.NestedField.optional(28, "optionalFixed", Types.FixedType.ofLength(fixedSize)),
            Types.NestedField.required(
                29, "requiredDecimal", Types.DecimalType.of(precision, scale)),
            Types.NestedField.optional(
                30, "optionalDecimal", Types.DecimalType.of(precision, scale)));

    Schema actual = IcebergSchemaExtractor.getInstance().toIceberg(oneSchemaRepresentation);
    Assertions.assertTrue(expectedSchema.sameSchema(actual));
  }

  @Test
  public void testMaps() {
    OneSchema recordMapElementSchema =
        OneSchema.builder()
            .name("element")
            .isNullable(true)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("requiredDouble")
                        .parentPath("recordMap._one_field_value")
                        .schema(
                            OneSchema.builder()
                                .name("double")
                                .dataType(OneType.DOUBLE)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalString")
                        .parentPath("recordMap._one_field_value")
                        .schema(
                            OneSchema.builder()
                                .name("string")
                                .dataType(OneType.STRING)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .dataType(OneType.RECORD)
            .build();
    OneSchema oneSchemaRepresentation =
        OneSchema.builder()
            .name("testRecord")
            .dataType(OneType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("intMap")
                        .schema(
                            OneSchema.builder()
                                .name("map")
                                .isNullable(false)
                                .dataType(OneType.MAP)
                                .fields(
                                    Arrays.asList(
                                        OneField.builder()
                                            .name(OneField.Constants.MAP_KEY_FIELD_NAME)
                                            .parentPath("intMap")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("map_key")
                                                    .dataType(OneType.STRING)
                                                    .isNullable(false)
                                                    .build())
                                            .defaultValue("")
                                            .build(),
                                        OneField.builder()
                                            .name(OneField.Constants.MAP_VALUE_FIELD_NAME)
                                            .parentPath("intMap")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("int")
                                                    .dataType(OneType.INT)
                                                    .isNullable(false)
                                                    .build())
                                            .build()))
                                .build())
                        .build(),
                    OneField.builder()
                        .name("recordMap")
                        .schema(
                            OneSchema.builder()
                                .name("map")
                                .isNullable(true)
                                .dataType(OneType.MAP)
                                .fields(
                                    Arrays.asList(
                                        OneField.builder()
                                            .name(OneField.Constants.MAP_KEY_FIELD_NAME)
                                            .parentPath("recordMap")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("map_key")
                                                    .dataType(OneType.INT)
                                                    .isNullable(false)
                                                    .build())
                                            .defaultValue("")
                                            .build(),
                                        OneField.builder()
                                            .name(OneField.Constants.MAP_VALUE_FIELD_NAME)
                                            .parentPath("recordMap")
                                            .schema(recordMapElementSchema)
                                            .build()))
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    Schema expectedSchema =
        new Schema(
            Types.NestedField.required(
                1,
                "intMap",
                Types.MapType.ofRequired(3, 4, Types.StringType.get(), Types.IntegerType.get())),
            Types.NestedField.optional(
                2,
                "recordMap",
                Types.MapType.ofOptional(
                    5,
                    6,
                    Types.IntegerType.get(),
                    Types.StructType.of(
                        Types.NestedField.required(7, "requiredDouble", Types.DoubleType.get()),
                        Types.NestedField.optional(8, "optionalString", Types.StringType.get())))));

    Schema actual = IcebergSchemaExtractor.getInstance().toIceberg(oneSchemaRepresentation);
    Assertions.assertTrue(expectedSchema.sameSchema(actual));
  }

  @Test
  public void testLists() {
    OneSchema recordListElementSchema =
        OneSchema.builder()
            .name("element")
            .isNullable(true)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("requiredDouble")
                        .parentPath("recordList._one_field_element")
                        .schema(
                            OneSchema.builder()
                                .name("double")
                                .dataType(OneType.DOUBLE)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalString")
                        .parentPath("recordList._one_field_element")
                        .schema(
                            OneSchema.builder()
                                .name("string")
                                .dataType(OneType.STRING)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .dataType(OneType.RECORD)
            .build();
    OneSchema oneSchemaRepresentation =
        OneSchema.builder()
            .name("testRecord")
            .dataType(OneType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("intList")
                        .schema(
                            OneSchema.builder()
                                .name("array")
                                .isNullable(false)
                                .dataType(OneType.ARRAY)
                                .fields(
                                    Arrays.asList(
                                        OneField.builder()
                                            .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .parentPath("intList")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("int")
                                                    .dataType(OneType.INT)
                                                    .isNullable(false)
                                                    .build())
                                            .build()))
                                .build())
                        .build(),
                    OneField.builder()
                        .name("recordList")
                        .schema(
                            OneSchema.builder()
                                .name("array")
                                .isNullable(true)
                                .dataType(OneType.ARRAY)
                                .fields(
                                    Arrays.asList(
                                        OneField.builder()
                                            .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .parentPath("recordList")
                                            .schema(recordListElementSchema)
                                            .build()))
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    Schema expectedSchema =
        new Schema(
            Types.NestedField.required(
                1, "intList", Types.ListType.ofRequired(3, Types.IntegerType.get())),
            Types.NestedField.optional(
                2,
                "recordList",
                Types.ListType.ofOptional(
                    4,
                    Types.StructType.of(
                        Types.NestedField.required(5, "requiredDouble", Types.DoubleType.get()),
                        Types.NestedField.optional(6, "optionalString", Types.StringType.get())))));

    Schema actual = IcebergSchemaExtractor.getInstance().toIceberg(oneSchemaRepresentation);
    Assertions.assertTrue(expectedSchema.sameSchema(actual));
  }

  @Test
  public void testNestedRecords() {
    OneSchema oneSchemaRepresentation =
        OneSchema.builder()
            .name("testRecord")
            .dataType(OneType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("nestedOne")
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .schema(
                            OneSchema.builder()
                                .name("nestedOneType")
                                .dataType(OneType.RECORD)
                                .isNullable(true)
                                .fields(
                                    Arrays.asList(
                                        OneField.builder()
                                            .name("nestedOptionalInt")
                                            .parentPath("nestedOne")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("int")
                                                    .dataType(OneType.INT)
                                                    .isNullable(true)
                                                    .build())
                                            .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                                            .build(),
                                        OneField.builder()
                                            .name("nestedRequiredDouble")
                                            .parentPath("nestedOne")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("double")
                                                    .dataType(OneType.DOUBLE)
                                                    .isNullable(false)
                                                    .build())
                                            .build(),
                                        OneField.builder()
                                            .name("nestedTwo")
                                            .parentPath("nestedOne")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("nestedTwoType")
                                                    .dataType(OneType.RECORD)
                                                    .isNullable(false)
                                                    .fields(
                                                        Arrays.asList(
                                                            OneField.builder()
                                                                .name("doublyNestedString")
                                                                .parentPath("nestedOne.nestedTwo")
                                                                .schema(
                                                                    OneSchema.builder()
                                                                        .name("string")
                                                                        .dataType(OneType.STRING)
                                                                        .isNullable(true)
                                                                        .build())
                                                                .defaultValue(
                                                                    OneField.Constants
                                                                        .NULL_DEFAULT_VALUE)
                                                                .build()))
                                                    .build())
                                            .build()))
                                .build())
                        .build()))
            .build();

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(
                1,
                "nestedOne",
                Types.StructType.of(
                    Types.NestedField.optional(2, "nestedOptionalInt", Types.IntegerType.get()),
                    Types.NestedField.required(3, "nestedRequiredDouble", Types.DoubleType.get()),
                    Types.NestedField.required(
                        4,
                        "nestedTwo",
                        Types.StructType.of(
                            Types.NestedField.optional(
                                5, "doublyNestedString", Types.StringType.get()))))));
    Schema actual = IcebergSchemaExtractor.getInstance().toIceberg(oneSchemaRepresentation);
    Assertions.assertTrue(expectedSchema.sameSchema(actual));
  }

  @Test
  public void testIdHandling() {
    OneSchema recordListElementSchema =
        OneSchema.builder()
            .name("element")
            .isNullable(true)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("requiredDouble")
                        .fieldId(3)
                        .parentPath("recordList._one_field_element")
                        .schema(
                            OneSchema.builder()
                                .name("double")
                                .dataType(OneType.DOUBLE)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalString")
                        .parentPath("recordList._one_field_element")
                        .fieldId(9)
                        .schema(
                            OneSchema.builder()
                                .name("string")
                                .dataType(OneType.STRING)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .dataType(OneType.RECORD)
            .build();
    OneSchema recordMapElementSchema =
        OneSchema.builder()
            .name("element")
            .isNullable(true)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("requiredDouble")
                        .parentPath("recordMap._one_field_value")
                        .fieldId(7)
                        .schema(
                            OneSchema.builder()
                                .name("double")
                                .dataType(OneType.DOUBLE)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalString")
                        .parentPath("recordMap._one_field_value")
                        .fieldId(10)
                        .schema(
                            OneSchema.builder()
                                .name("string")
                                .dataType(OneType.STRING)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .dataType(OneType.RECORD)
            .build();
    OneSchema oneSchemaRepresentation =
        OneSchema.builder()
            .name("testRecord")
            .dataType(OneType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("recordList")
                        .fieldId(1)
                        .schema(
                            OneSchema.builder()
                                .name("array")
                                .isNullable(true)
                                .dataType(OneType.ARRAY)
                                .fields(
                                    Arrays.asList(
                                        OneField.builder()
                                            .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .parentPath("recordList")
                                            .fieldId(2)
                                            .schema(recordListElementSchema)
                                            .build()))
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("recordMap")
                        .fieldId(4)
                        .schema(
                            OneSchema.builder()
                                .name("map")
                                .isNullable(true)
                                .dataType(OneType.MAP)
                                .fields(
                                    Arrays.asList(
                                        OneField.builder()
                                            .name(OneField.Constants.MAP_KEY_FIELD_NAME)
                                            .parentPath("recordMap")
                                            .fieldId(5)
                                            .schema(
                                                OneSchema.builder()
                                                    .name("map_key")
                                                    .dataType(OneType.INT)
                                                    .isNullable(false)
                                                    .build())
                                            .defaultValue("")
                                            .build(),
                                        OneField.builder()
                                            .fieldId(6)
                                            .name(OneField.Constants.MAP_VALUE_FIELD_NAME)
                                            .parentPath("recordMap")
                                            .schema(recordMapElementSchema)
                                            .build()))
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    Schema expectedSchema =
        new Schema(
            Types.NestedField.optional(
                1,
                "recordList",
                Types.ListType.ofOptional(
                    2,
                    Types.StructType.of(
                        Types.NestedField.required(3, "requiredDouble", Types.DoubleType.get()),
                        Types.NestedField.optional(9, "optionalString", Types.StringType.get())))),
            Types.NestedField.optional(
                4,
                "recordMap",
                Types.MapType.ofOptional(
                    5,
                    6,
                    Types.IntegerType.get(),
                    Types.StructType.of(
                        Types.NestedField.required(7, "requiredDouble", Types.DoubleType.get()),
                        Types.NestedField.optional(
                            10, "optionalString", Types.StringType.get())))));

    Schema actual = IcebergSchemaExtractor.getInstance().toIceberg(oneSchemaRepresentation);
    Assertions.assertTrue(expectedSchema.sameSchema(actual));
  }
}
