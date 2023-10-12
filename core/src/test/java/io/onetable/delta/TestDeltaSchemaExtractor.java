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
 
package io.onetable.delta;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;

public class TestDeltaSchemaExtractor {

  @Test
  public void testPrimitiveTypes() {
    Map<OneSchema.MetadataKey, Object> decimalMetadata = new HashMap<>();
    decimalMetadata.put(OneSchema.MetadataKey.DECIMAL_PRECISION, 10);
    decimalMetadata.put(OneSchema.MetadataKey.DECIMAL_SCALE, 2);

    OneSchema oneSchemaRepresentation =
        OneSchema.builder()
            .name("struct")
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
                                .name("integer")
                                .dataType(OneType.INT)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalInt")
                        .schema(
                            OneSchema.builder()
                                .name("integer")
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
                                .name("byte")
                                .dataType(OneType.BYTES)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalBytes")
                        .schema(
                            OneSchema.builder()
                                .name("byte")
                                .dataType(OneType.BYTES)
                                .isNullable(true)
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
                        .name("requiredDecimal")
                        .schema(
                            OneSchema.builder()
                                .name("decimal")
                                .dataType(OneType.DECIMAL)
                                .isNullable(false)
                                .metadata(decimalMetadata)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalDecimal")
                        .schema(
                            OneSchema.builder()
                                .name("decimal")
                                .dataType(OneType.DECIMAL)
                                .isNullable(true)
                                .metadata(decimalMetadata)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    StructType structRepresentation =
        new StructType()
            .add("requiredBoolean", DataTypes.BooleanType, false)
            .add("optionalBoolean", DataTypes.BooleanType, true)
            .add("requiredInt", DataTypes.IntegerType, false)
            .add("optionalInt", DataTypes.IntegerType, true)
            .add("requiredLong", DataTypes.LongType, false)
            .add("optionalLong", DataTypes.LongType, true)
            .add("requiredDouble", DataTypes.DoubleType, false)
            .add("optionalDouble", DataTypes.DoubleType, true)
            .add("requiredFloat", DataTypes.FloatType, false)
            .add("optionalFloat", DataTypes.FloatType, true)
            .add("requiredString", DataTypes.StringType, false)
            .add("optionalString", DataTypes.StringType, true)
            .add("requiredBytes", DataTypes.ByteType, false)
            .add("optionalBytes", DataTypes.ByteType, true)
            .add("requiredDate", DataTypes.DateType, false)
            .add("optionalDate", DataTypes.DateType, true)
            .add("requiredDecimal", DataTypes.createDecimalType(10, 2), false)
            .add("optionalDecimal", DataTypes.createDecimalType(10, 2), true);

    Assertions.assertEquals(
        structRepresentation,
        DeltaSchemaExtractor.getInstance().fromOneSchema(oneSchemaRepresentation));
    Assertions.assertEquals(
        oneSchemaRepresentation,
        DeltaSchemaExtractor.getInstance().toOneSchema(structRepresentation));
  }

  @Test
  public void testFixedBytes() {
    OneSchema oneSchemaRepresentationOriginal =
        OneSchema.builder()
            .name("struct")
            .dataType(OneType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("requiredFixed")
                        .schema(
                            OneSchema.builder()
                                .name("fixed")
                                .dataType(OneType.FIXED)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalFixed")
                        .schema(
                            OneSchema.builder()
                                .name("fixed")
                                .dataType(OneType.FIXED)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    OneSchema oneSchemaRepresentationAfterRoundTrip =
        OneSchema.builder()
            .name("struct")
            .dataType(OneType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("requiredFixed")
                        .schema(
                            OneSchema.builder()
                                .name("byte")
                                .dataType(OneType.BYTES)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalFixed")
                        .schema(
                            OneSchema.builder()
                                .name("byte")
                                .dataType(OneType.BYTES)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();
    StructType structRepresentation =
        new StructType()
            .add("requiredFixed", DataTypes.ByteType, false)
            .add("optionalFixed", DataTypes.ByteType, true);

    Assertions.assertEquals(
        structRepresentation,
        DeltaSchemaExtractor.getInstance().fromOneSchema(oneSchemaRepresentationOriginal));
    Assertions.assertEquals(
        oneSchemaRepresentationAfterRoundTrip,
        DeltaSchemaExtractor.getInstance().toOneSchema(structRepresentation));
  }

  @Test
  public void testTimestamps() {
    OneSchema oneSchemaRepresentationTimestamp =
        OneSchema.builder()
            .name("struct")
            .dataType(OneType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("requiredTimestamp")
                        .schema(
                            OneSchema.builder()
                                .name("timestamp")
                                .dataType(OneType.TIMESTAMP)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalTimestamp")
                        .schema(
                            OneSchema.builder()
                                .name("timestamp")
                                .dataType(OneType.TIMESTAMP)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    OneSchema oneSchemaRepresentationTimestampNtz =
        OneSchema.builder()
            .name("struct")
            .dataType(OneType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("requiredTimestampNtz")
                        .schema(
                            OneSchema.builder()
                                .name("timestampNtz")
                                .dataType(OneType.TIMESTAMP_NTZ)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("optionalTimestampNtz")
                        .schema(
                            OneSchema.builder()
                                .name("timestampNtz")
                                .dataType(OneType.TIMESTAMP_NTZ)
                                .isNullable(true)
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    StructType structRepresentationTimestamp =
        new StructType()
            .add("requiredTimestamp", DataTypes.TimestampType, false)
            .add("optionalTimestamp", DataTypes.TimestampType, true);

    StructType structRepresentationTimestampNtz =
        new StructType()
            .add("requiredTimestampNtz", DataTypes.LongType, false)
            .add("optionalTimestampNtz", DataTypes.LongType, true);

    Assertions.assertEquals(
        structRepresentationTimestamp,
        DeltaSchemaExtractor.getInstance().fromOneSchema(oneSchemaRepresentationTimestamp));
    Assertions.assertEquals(
        oneSchemaRepresentationTimestamp,
        DeltaSchemaExtractor.getInstance().toOneSchema(structRepresentationTimestamp));
    Assertions.assertEquals(
        structRepresentationTimestampNtz,
        DeltaSchemaExtractor.getInstance().fromOneSchema(oneSchemaRepresentationTimestampNtz));
  }

  @Test
  public void testEnums() {
    Map<OneSchema.MetadataKey, Object> requiredEnumMetadata = new HashMap<>();
    requiredEnumMetadata.put(OneSchema.MetadataKey.ENUM_VALUES, Arrays.asList("ONE", "TWO"));
    Map<OneSchema.MetadataKey, Object> optionalEnumMetadata = new HashMap<>();
    optionalEnumMetadata.put(OneSchema.MetadataKey.ENUM_VALUES, Arrays.asList("THREE", "FOUR"));

    OneSchema oneSchemaRepresentation =
        OneSchema.builder()
            .name("struct")
            .dataType(OneType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("requiredEnum")
                        .schema(
                            OneSchema.builder()
                                .name("REQUIRED_ENUM")
                                .dataType(OneType.ENUM)
                                .isNullable(false)
                                .metadata(requiredEnumMetadata)
                                .build())
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
                        .build()))
            .build();

    StructType structRepresentation =
        new StructType()
            .add("requiredEnum", DataTypes.StringType, false)
            .add("optionalEnum", DataTypes.StringType, true);

    Assertions.assertEquals(
        structRepresentation,
        DeltaSchemaExtractor.getInstance().fromOneSchema(oneSchemaRepresentation));
  }

  @Test
  public void testMaps() {
    OneSchema recordMapElementSchema =
        OneSchema.builder()
            .name("struct")
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
            .name("struct")
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
                                                    .name("string")
                                                    .dataType(OneType.STRING)
                                                    .isNullable(false)
                                                    .build())
                                            .build(),
                                        OneField.builder()
                                            .name(OneField.Constants.MAP_VALUE_FIELD_NAME)
                                            .parentPath("intMap")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("integer")
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
                                                    .name("integer")
                                                    .dataType(OneType.INT)
                                                    .isNullable(false)
                                                    .build())
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

    StructType mapElement =
        new StructType()
            .add("requiredDouble", DataTypes.DoubleType, false)
            .add("optionalString", DataTypes.StringType, true);
    StructType structRepresentation =
        new StructType()
            .add(
                "intMap",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType, false),
                false)
            .add("recordMap", DataTypes.createMapType(DataTypes.IntegerType, mapElement, true));

    Assertions.assertEquals(
        structRepresentation,
        DeltaSchemaExtractor.getInstance().fromOneSchema(oneSchemaRepresentation));
    Assertions.assertEquals(
        oneSchemaRepresentation,
        DeltaSchemaExtractor.getInstance().toOneSchema(structRepresentation));
  }

  @Test
  public void testLists() {
    OneSchema recordListElementSchema =
        OneSchema.builder()
            .name("struct")
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
            .name("struct")
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
                                    Collections.singletonList(
                                        OneField.builder()
                                            .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .parentPath("intList")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("integer")
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
                                    Collections.singletonList(
                                        OneField.builder()
                                            .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .parentPath("recordList")
                                            .schema(recordListElementSchema)
                                            .build()))
                                .build())
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();
    StructType elementSchema =
        new StructType()
            .add("requiredDouble", DataTypes.DoubleType, false)
            .add("optionalString", DataTypes.StringType, true);
    StructType structRepresentation =
        new StructType()
            .add("intList", DataTypes.createArrayType(DataTypes.IntegerType, false), false)
            .add("recordList", DataTypes.createArrayType(elementSchema, true), true);

    Assertions.assertEquals(
        structRepresentation,
        DeltaSchemaExtractor.getInstance().fromOneSchema(oneSchemaRepresentation));
    Assertions.assertEquals(
        oneSchemaRepresentation,
        DeltaSchemaExtractor.getInstance().toOneSchema(structRepresentation));
  }

  @Test
  public void testNestedRecords() {
    OneSchema oneSchemaRepresentation =
        OneSchema.builder()
            .name("struct")
            .dataType(OneType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("nestedOne")
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .schema(
                            OneSchema.builder()
                                .name("struct")
                                .dataType(OneType.RECORD)
                                .isNullable(true)
                                .fields(
                                    Arrays.asList(
                                        OneField.builder()
                                            .name("nestedOptionalInt")
                                            .parentPath("nestedOne")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("integer")
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
                                                    .name("struct")
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

    StructType structRepresentation =
        new StructType()
            .add(
                "nestedOne",
                new StructType()
                    .add("nestedOptionalInt", DataTypes.IntegerType, true)
                    .add("nestedRequiredDouble", DataTypes.DoubleType, false)
                    .add(
                        "nestedTwo",
                        new StructType().add("doublyNestedString", DataTypes.StringType, true),
                        false),
                true);
    Assertions.assertEquals(
        structRepresentation,
        DeltaSchemaExtractor.getInstance().fromOneSchema(oneSchemaRepresentation));
    Assertions.assertEquals(
        oneSchemaRepresentation,
        DeltaSchemaExtractor.getInstance().toOneSchema(structRepresentation));
  }
}
