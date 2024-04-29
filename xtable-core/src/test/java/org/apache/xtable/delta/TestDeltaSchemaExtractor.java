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
 
package org.apache.xtable.delta;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;

public class TestDeltaSchemaExtractor {

  @Test
  public void testPrimitiveTypes() {
    Map<InternalSchema.MetadataKey, Object> decimalMetadata = new HashMap<>();
    decimalMetadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, 10);
    decimalMetadata.put(InternalSchema.MetadataKey.DECIMAL_SCALE, 2);

    InternalSchema internalSchema =
        InternalSchema.builder()
            .name("struct")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("requiredBoolean")
                        .schema(
                            InternalSchema.builder()
                                .name("boolean")
                                .dataType(InternalType.BOOLEAN)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalBoolean")
                        .schema(
                            InternalSchema.builder()
                                .name("boolean")
                                .dataType(InternalType.BOOLEAN)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    InternalField.builder()
                        .name("requiredInt")
                        .schema(
                            InternalSchema.builder()
                                .name("integer")
                                .dataType(InternalType.INT)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalInt")
                        .schema(
                            InternalSchema.builder()
                                .name("integer")
                                .dataType(InternalType.INT)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    InternalField.builder()
                        .name("requiredLong")
                        .schema(
                            InternalSchema.builder()
                                .name("long")
                                .dataType(InternalType.LONG)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalLong")
                        .schema(
                            InternalSchema.builder()
                                .name("long")
                                .dataType(InternalType.LONG)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    InternalField.builder()
                        .name("requiredDouble")
                        .schema(
                            InternalSchema.builder()
                                .name("double")
                                .dataType(InternalType.DOUBLE)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalDouble")
                        .schema(
                            InternalSchema.builder()
                                .name("double")
                                .dataType(InternalType.DOUBLE)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    InternalField.builder()
                        .name("requiredFloat")
                        .schema(
                            InternalSchema.builder()
                                .name("float")
                                .dataType(InternalType.FLOAT)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalFloat")
                        .schema(
                            InternalSchema.builder()
                                .name("float")
                                .dataType(InternalType.FLOAT)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    InternalField.builder()
                        .name("requiredString")
                        .schema(
                            InternalSchema.builder()
                                .name("string")
                                .dataType(InternalType.STRING)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalString")
                        .schema(
                            InternalSchema.builder()
                                .name("string")
                                .dataType(InternalType.STRING)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    InternalField.builder()
                        .name("requiredBytes")
                        .schema(
                            InternalSchema.builder()
                                .name("binary")
                                .dataType(InternalType.BYTES)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalBytes")
                        .schema(
                            InternalSchema.builder()
                                .name("binary")
                                .dataType(InternalType.BYTES)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    InternalField.builder()
                        .name("requiredDate")
                        .schema(
                            InternalSchema.builder()
                                .name("date")
                                .dataType(InternalType.DATE)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalDate")
                        .schema(
                            InternalSchema.builder()
                                .name("date")
                                .dataType(InternalType.DATE)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    InternalField.builder()
                        .name("requiredDecimal")
                        .schema(
                            InternalSchema.builder()
                                .name("decimal")
                                .dataType(InternalType.DECIMAL)
                                .isNullable(false)
                                .metadata(decimalMetadata)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalDecimal")
                        .schema(
                            InternalSchema.builder()
                                .name("decimal")
                                .dataType(InternalType.DECIMAL)
                                .isNullable(true)
                                .metadata(decimalMetadata)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
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
            .add("requiredBytes", DataTypes.BinaryType, false)
            .add("optionalBytes", DataTypes.BinaryType, true)
            .add("requiredDate", DataTypes.DateType, false)
            .add("optionalDate", DataTypes.DateType, true)
            .add("requiredDecimal", DataTypes.createDecimalType(10, 2), false)
            .add("optionalDecimal", DataTypes.createDecimalType(10, 2), true);

    Assertions.assertEquals(
        structRepresentation,
        DeltaSchemaExtractor.getInstance().fromInternalSchema(internalSchema));
    Assertions.assertEquals(
        internalSchema, DeltaSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
  }

  @Test
  public void testFixedBytes() {
    InternalSchema internalSchemaOriginal =
        InternalSchema.builder()
            .name("struct")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("requiredFixed")
                        .schema(
                            InternalSchema.builder()
                                .name("fixed")
                                .dataType(InternalType.FIXED)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalFixed")
                        .schema(
                            InternalSchema.builder()
                                .name("fixed")
                                .dataType(InternalType.FIXED)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    InternalSchema internalSchemaAfterRoundTrip =
        InternalSchema.builder()
            .name("struct")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("requiredFixed")
                        .schema(
                            InternalSchema.builder()
                                .name("binary")
                                .dataType(InternalType.BYTES)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalFixed")
                        .schema(
                            InternalSchema.builder()
                                .name("binary")
                                .dataType(InternalType.BYTES)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();
    StructType structRepresentation =
        new StructType()
            .add("requiredFixed", DataTypes.BinaryType, false)
            .add("optionalFixed", DataTypes.BinaryType, true);

    Assertions.assertEquals(
        structRepresentation,
        DeltaSchemaExtractor.getInstance().fromInternalSchema(internalSchemaOriginal));
    Assertions.assertEquals(
        internalSchemaAfterRoundTrip,
        DeltaSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
  }

  @Test
  public void testTimestamps() {
    Map<InternalSchema.MetadataKey, Object> metadata =
        Collections.singletonMap(
            InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MICROS);
    InternalSchema internalSchemaTimestamp =
        InternalSchema.builder()
            .name("struct")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("requiredTimestamp")
                        .schema(
                            InternalSchema.builder()
                                .name("timestamp")
                                .dataType(InternalType.TIMESTAMP)
                                .isNullable(false)
                                .metadata(metadata)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalTimestamp")
                        .schema(
                            InternalSchema.builder()
                                .name("timestamp")
                                .dataType(InternalType.TIMESTAMP)
                                .isNullable(true)
                                .metadata(metadata)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    InternalSchema internalSchemaTimestampNtz =
        InternalSchema.builder()
            .name("struct")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("requiredTimestampNtz")
                        .schema(
                            InternalSchema.builder()
                                .name("timestampNtz")
                                .dataType(InternalType.TIMESTAMP_NTZ)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalTimestampNtz")
                        .schema(
                            InternalSchema.builder()
                                .name("timestampNtz")
                                .dataType(InternalType.TIMESTAMP_NTZ)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
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
        DeltaSchemaExtractor.getInstance().fromInternalSchema(internalSchemaTimestamp));
    Assertions.assertEquals(
        internalSchemaTimestamp,
        DeltaSchemaExtractor.getInstance().toInternalSchema(structRepresentationTimestamp));
    Assertions.assertEquals(
        structRepresentationTimestampNtz,
        DeltaSchemaExtractor.getInstance().fromInternalSchema(internalSchemaTimestampNtz));
  }

  @Test
  public void testEnums() {
    Map<InternalSchema.MetadataKey, Object> requiredEnumMetadata = new HashMap<>();
    requiredEnumMetadata.put(InternalSchema.MetadataKey.ENUM_VALUES, Arrays.asList("ONE", "TWO"));
    Map<InternalSchema.MetadataKey, Object> optionalEnumMetadata = new HashMap<>();
    optionalEnumMetadata.put(
        InternalSchema.MetadataKey.ENUM_VALUES, Arrays.asList("THREE", "FOUR"));

    InternalSchema internalSchema =
        InternalSchema.builder()
            .name("struct")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("requiredEnum")
                        .schema(
                            InternalSchema.builder()
                                .name("REQUIRED_ENUM")
                                .dataType(InternalType.ENUM)
                                .isNullable(false)
                                .metadata(requiredEnumMetadata)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalEnum")
                        .schema(
                            InternalSchema.builder()
                                .name("OPTIONAL_ENUM")
                                .dataType(InternalType.ENUM)
                                .isNullable(true)
                                .metadata(optionalEnumMetadata)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    StructType structRepresentation =
        new StructType()
            .add("requiredEnum", DataTypes.StringType, false)
            .add("optionalEnum", DataTypes.StringType, true);

    Assertions.assertEquals(
        structRepresentation,
        DeltaSchemaExtractor.getInstance().fromInternalSchema(internalSchema));
  }

  @Test
  public void testMaps() {
    InternalSchema recordMapElementSchema =
        InternalSchema.builder()
            .name("struct")
            .isNullable(true)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("requiredDouble")
                        .parentPath("recordMap._one_field_value")
                        .schema(
                            InternalSchema.builder()
                                .name("double")
                                .dataType(InternalType.DOUBLE)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalString")
                        .parentPath("recordMap._one_field_value")
                        .schema(
                            InternalSchema.builder()
                                .name("string")
                                .dataType(InternalType.STRING)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .dataType(InternalType.RECORD)
            .build();
    InternalSchema internalSchema =
        InternalSchema.builder()
            .name("struct")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("intMap")
                        .schema(
                            InternalSchema.builder()
                                .name("map")
                                .isNullable(false)
                                .dataType(InternalType.MAP)
                                .fields(
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.MAP_KEY_FIELD_NAME)
                                            .parentPath("intMap")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("string")
                                                    .dataType(InternalType.STRING)
                                                    .isNullable(false)
                                                    .build())
                                            .build(),
                                        InternalField.builder()
                                            .name(InternalField.Constants.MAP_VALUE_FIELD_NAME)
                                            .parentPath("intMap")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("integer")
                                                    .dataType(InternalType.INT)
                                                    .isNullable(false)
                                                    .build())
                                            .build()))
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("recordMap")
                        .schema(
                            InternalSchema.builder()
                                .name("map")
                                .isNullable(true)
                                .dataType(InternalType.MAP)
                                .fields(
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.MAP_KEY_FIELD_NAME)
                                            .parentPath("recordMap")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("integer")
                                                    .dataType(InternalType.INT)
                                                    .isNullable(false)
                                                    .build())
                                            .build(),
                                        InternalField.builder()
                                            .name(InternalField.Constants.MAP_VALUE_FIELD_NAME)
                                            .parentPath("recordMap")
                                            .schema(recordMapElementSchema)
                                            .build()))
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
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
        DeltaSchemaExtractor.getInstance().fromInternalSchema(internalSchema));
    Assertions.assertEquals(
        internalSchema, DeltaSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
  }

  @Test
  public void testLists() {
    InternalSchema recordListElementSchema =
        InternalSchema.builder()
            .name("struct")
            .isNullable(true)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("requiredDouble")
                        .parentPath("recordList._one_field_element")
                        .schema(
                            InternalSchema.builder()
                                .name("double")
                                .dataType(InternalType.DOUBLE)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalString")
                        .parentPath("recordList._one_field_element")
                        .schema(
                            InternalSchema.builder()
                                .name("string")
                                .dataType(InternalType.STRING)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .dataType(InternalType.RECORD)
            .build();
    InternalSchema internalSchema =
        InternalSchema.builder()
            .name("struct")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("intList")
                        .schema(
                            InternalSchema.builder()
                                .name("array")
                                .isNullable(false)
                                .dataType(InternalType.LIST)
                                .fields(
                                    Collections.singletonList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .parentPath("intList")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("integer")
                                                    .dataType(InternalType.INT)
                                                    .isNullable(false)
                                                    .build())
                                            .build()))
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("recordList")
                        .schema(
                            InternalSchema.builder()
                                .name("array")
                                .isNullable(true)
                                .dataType(InternalType.LIST)
                                .fields(
                                    Collections.singletonList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .parentPath("recordList")
                                            .schema(recordListElementSchema)
                                            .build()))
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
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
        DeltaSchemaExtractor.getInstance().fromInternalSchema(internalSchema));
    Assertions.assertEquals(
        internalSchema, DeltaSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
  }

  @Test
  public void testNestedRecords() {
    InternalSchema internalSchema =
        InternalSchema.builder()
            .name("struct")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("nestedOne")
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .schema(
                            InternalSchema.builder()
                                .name("struct")
                                .dataType(InternalType.RECORD)
                                .isNullable(true)
                                .fields(
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name("nestedOptionalInt")
                                            .parentPath("nestedOne")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("integer")
                                                    .dataType(InternalType.INT)
                                                    .isNullable(true)
                                                    .build())
                                            .defaultValue(
                                                InternalField.Constants.NULL_DEFAULT_VALUE)
                                            .build(),
                                        InternalField.builder()
                                            .name("nestedRequiredDouble")
                                            .parentPath("nestedOne")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("double")
                                                    .dataType(InternalType.DOUBLE)
                                                    .isNullable(false)
                                                    .build())
                                            .build(),
                                        InternalField.builder()
                                            .name("nestedTwo")
                                            .parentPath("nestedOne")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("struct")
                                                    .dataType(InternalType.RECORD)
                                                    .isNullable(false)
                                                    .fields(
                                                        Arrays.asList(
                                                            InternalField.builder()
                                                                .name("doublyNestedString")
                                                                .parentPath("nestedOne.nestedTwo")
                                                                .schema(
                                                                    InternalSchema.builder()
                                                                        .name("string")
                                                                        .dataType(
                                                                            InternalType.STRING)
                                                                        .isNullable(true)
                                                                        .build())
                                                                .defaultValue(
                                                                    InternalField.Constants
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
        DeltaSchemaExtractor.getInstance().fromInternalSchema(internalSchema));
    Assertions.assertEquals(
        internalSchema, DeltaSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
  }

  @Test
  public void testFieldIdsInDeltaSchema() {
    StructType structRepresentation =
        new StructType()
            .add(
                "nestedOne",
                new StructType()
                    .add(
                        "nestedOptionalInt",
                        DataTypes.IntegerType,
                        true,
                        Metadata.fromJson("{\"delta.columnMapping.id\": 3}"))
                    .add(
                        "nestedRequiredDouble",
                        DataTypes.DoubleType,
                        false,
                        Metadata.fromJson("{\"delta.columnMapping.id\": 5}"))
                    .add(
                        "nestedTwo",
                        new StructType()
                            .add(
                                "doublyNestedString",
                                DataTypes.StringType,
                                true,
                                Metadata.fromJson("{\"delta.columnMapping.id\": 12}")),
                        false,
                        Metadata.fromJson("{\"delta.columnMapping.id\": 10}")),
                true,
                Metadata.fromJson("{\"delta.columnMapping.id\": 2}"));

    InternalSchema internalSchema =
        InternalSchema.builder()
            .name("struct")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Collections.singletonList(
                    InternalField.builder()
                        .name("nestedOne")
                        .fieldId(2)
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .schema(
                            InternalSchema.builder()
                                .name("struct")
                                .dataType(InternalType.RECORD)
                                .isNullable(true)
                                .fields(
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name("nestedOptionalInt")
                                            .fieldId(3)
                                            .parentPath("nestedOne")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("integer")
                                                    .dataType(InternalType.INT)
                                                    .isNullable(true)
                                                    .build())
                                            .defaultValue(
                                                InternalField.Constants.NULL_DEFAULT_VALUE)
                                            .build(),
                                        InternalField.builder()
                                            .name("nestedRequiredDouble")
                                            .fieldId(5)
                                            .parentPath("nestedOne")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("double")
                                                    .dataType(InternalType.DOUBLE)
                                                    .isNullable(false)
                                                    .build())
                                            .build(),
                                        InternalField.builder()
                                            .name("nestedTwo")
                                            .fieldId(10)
                                            .parentPath("nestedOne")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("struct")
                                                    .dataType(InternalType.RECORD)
                                                    .isNullable(false)
                                                    .fields(
                                                        Collections.singletonList(
                                                            InternalField.builder()
                                                                .name("doublyNestedString")
                                                                .fieldId(12)
                                                                .parentPath("nestedOne.nestedTwo")
                                                                .schema(
                                                                    InternalSchema.builder()
                                                                        .name("string")
                                                                        .dataType(
                                                                            InternalType.STRING)
                                                                        .isNullable(true)
                                                                        .build())
                                                                .defaultValue(
                                                                    InternalField.Constants
                                                                        .NULL_DEFAULT_VALUE)
                                                                .build()))
                                                    .build())
                                            .build()))
                                .build())
                        .build()))
            .build();
    Assertions.assertEquals(
        internalSchema, DeltaSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
  }

  @Test
  void generateColumnsAreNotTranslatedToInternalSchema() {
    StructType structRepresentation =
        new StructType()
            .add("birthDate", DataTypes.TimestampType, false)
            .add(
                "birthYear",
                DataTypes.TimestampType,
                true,
                Metadata.fromJson("{\"delta.generationExpression\":\"YEAR(birthDate)\"}"));
    InternalSchema internalSchema =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
            .name("struct")
            .fields(
                Collections.singletonList(
                    InternalField.builder()
                        .schema(
                            InternalSchema.builder()
                                .name("timestamp")
                                .dataType(InternalType.TIMESTAMP)
                                .metadata(
                                    Collections.singletonMap(
                                        InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                                        InternalSchema.MetadataValue.MICROS))
                                .build())
                        .name("birthDate")
                        .build()))
            .build();
    Assertions.assertEquals(
        internalSchema, DeltaSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
  }
}
