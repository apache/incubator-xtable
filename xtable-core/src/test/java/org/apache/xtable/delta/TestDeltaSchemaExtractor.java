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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
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
                                .comment("requiredBooleanComment")
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
            .add("requiredBoolean", DataTypes.BooleanType, false, "requiredBooleanComment")
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
        internalSchema, DeltaSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
  }

  @Test
  public void testFixedBytes() {
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
                                .comment("comment")
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
            .add("requiredFixed", DataTypes.BinaryType, false, "comment")
            .add("optionalFixed", DataTypes.BinaryType, true);

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
                        .build(),
                    InternalField.builder()
                        .name("requiredTimestampNtz")
                        .schema(
                            InternalSchema.builder()
                                .name("timestamp_ntz")
                                .dataType(InternalType.TIMESTAMP_NTZ)
                                .isNullable(false)
                                .metadata(metadata)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalTimestampNtz")
                        .schema(
                            InternalSchema.builder()
                                .name("timestamp_ntz")
                                .dataType(InternalType.TIMESTAMP_NTZ)
                                .isNullable(true)
                                .metadata(metadata)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    StructType structRepresentationTimestamp =
        new StructType()
            .add("requiredTimestamp", DataTypes.TimestampType, false)
            .add("optionalTimestamp", DataTypes.TimestampType, true)
            .add("requiredTimestampNtz", DataTypes.TimestampNTZType, false)
            .add("optionalTimestampNtz", DataTypes.TimestampNTZType, true);

    Assertions.assertEquals(
        internalSchemaTimestamp,
        DeltaSchemaExtractor.getInstance().toInternalSchema(structRepresentationTimestamp));
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
        internalSchema, DeltaSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
  }

  @Test
  public void testMapWithStructKey() {
    InternalSchema structKeySchema =
        InternalSchema.builder()
            .name("struct")
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("id")
                        .parentPath("structKeyMap._one_field_key")
                        .schema(
                            InternalSchema.builder()
                                .name("long")
                                .dataType(InternalType.LONG)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("region")
                        .parentPath("structKeyMap._one_field_key")
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
    InternalSchema structValueSchema =
        InternalSchema.builder()
            .name("struct")
            .isNullable(true)
            .fields(
                Collections.singletonList(
                    InternalField.builder()
                        .name("payload")
                        .parentPath("structKeyMap._one_field_value")
                        .schema(
                            InternalSchema.builder()
                                .name("string")
                                .dataType(InternalType.STRING)
                                .isNullable(false)
                                .build())
                        .build()))
            .dataType(InternalType.RECORD)
            .build();
    InternalSchema internalSchema =
        InternalSchema.builder()
            .name("struct")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Collections.singletonList(
                    InternalField.builder()
                        .name("structKeyMap")
                        .schema(
                            InternalSchema.builder()
                                .name("map")
                                .isNullable(true)
                                .dataType(InternalType.MAP)
                                .fields(
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.MAP_KEY_FIELD_NAME)
                                            .parentPath("structKeyMap")
                                            .schema(structKeySchema)
                                            .build(),
                                        InternalField.builder()
                                            .name(InternalField.Constants.MAP_VALUE_FIELD_NAME)
                                            .parentPath("structKeyMap")
                                            .schema(structValueSchema)
                                            .build()))
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    StructType keyStruct =
        new StructType()
            .add("id", DataTypes.LongType, false)
            .add("region", DataTypes.StringType, true);
    StructType valueStruct = new StructType().add("payload", DataTypes.StringType, false);
    StructType structRepresentation =
        new StructType().add("structKeyMap", DataTypes.createMapType(keyStruct, valueStruct, true));

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
        internalSchema, DeltaSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
  }

  @Test
  public void testBinaryInMapAndArrayWithoutMetadata() {
    InternalSchema expectedSchema =
        InternalSchema.builder()
            .name("struct")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("binaryList")
                        .schema(
                            InternalSchema.builder()
                                .name("array")
                                .isNullable(false)
                                .dataType(InternalType.LIST)
                                .fields(
                                    Collections.singletonList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .parentPath("binaryList")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("binary")
                                                    .dataType(InternalType.BYTES)
                                                    .isNullable(false)
                                                    .build())
                                            .build()))
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("binaryMap")
                        .schema(
                            InternalSchema.builder()
                                .name("map")
                                .isNullable(false)
                                .dataType(InternalType.MAP)
                                .fields(
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.MAP_KEY_FIELD_NAME)
                                            .parentPath("binaryMap")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("string")
                                                    .dataType(InternalType.STRING)
                                                    .isNullable(false)
                                                    .build())
                                            .build(),
                                        InternalField.builder()
                                            .name(InternalField.Constants.MAP_VALUE_FIELD_NAME)
                                            .parentPath("binaryMap")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("binary")
                                                    .dataType(InternalType.BYTES)
                                                    .isNullable(false)
                                                    .build())
                                            .build()))
                                .build())
                        .build()))
            .build();

    StructType structRepresentation =
        new StructType()
            .add("binaryList", DataTypes.createArrayType(DataTypes.BinaryType, false), false)
            .add(
                "binaryMap",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.BinaryType, false),
                false);

    Assertions.assertEquals(
        expectedSchema, DeltaSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
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
                                .comment("comment")
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
                                                    .comment("nestedOptionalIntComment")
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
                    .add(
                        "nestedOptionalInt",
                        DataTypes.IntegerType,
                        true,
                        "nestedOptionalIntComment")
                    .add("nestedRequiredDouble", DataTypes.DoubleType, false)
                    .add(
                        "nestedTwo",
                        new StructType().add("doublyNestedString", DataTypes.StringType, true),
                        false),
                true,
                "comment");
    Assertions.assertEquals(
        internalSchema, DeltaSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
  }

  @Test
  public void testNestedFieldIdsInDeltaSchema() {
    // Delta writes these ids for a collection's children when IcebergCompatV2 is enabled, keyed
    // by the path the child takes in the parquet file.
    Metadata mapMetadata =
        Metadata.fromJson(
            "{\"delta.columnMapping.id\": 1, \"delta.columnMapping.physicalName\": \"col-map\","
                + " \"delta.columnMapping.nested.ids\": {\"col-map.key\": 7, \"col-map.value\": 8}}");
    Metadata listMetadata =
        Metadata.fromJson(
            "{\"delta.columnMapping.id\": 2, \"delta.columnMapping.physicalName\": \"col-list\","
                + " \"delta.columnMapping.nested.ids\": {\"col-list.element\": 9}}");
    Metadata plainMetadata =
        Metadata.fromJson(
            "{\"delta.columnMapping.id\": 3, \"delta.columnMapping.physicalName\": \"col-plain\"}");
    StructType structType =
        new StructType()
            .add(
                "map_field",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType),
                true,
                mapMetadata)
            .add("list_field", DataTypes.createArrayType(DataTypes.IntegerType), true, listMetadata)
            .add(
                "plain_map",
                DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType),
                true,
                plainMetadata);

    InternalSchema internalSchema = DeltaSchemaExtractor.getInstance().toInternalSchema(structType);

    Assertions.assertEquals(7, fieldId(internalSchema, "map_field", "_one_field_key"));
    Assertions.assertEquals(8, fieldId(internalSchema, "map_field", "_one_field_value"));
    Assertions.assertEquals(9, fieldId(internalSchema, "list_field", "_one_field_element"));
    // A field without the metadata leaves its children unassigned, as before.
    Assertions.assertNull(fieldId(internalSchema, "plain_map", "_one_field_key"));
    Assertions.assertNull(fieldId(internalSchema, "plain_map", "_one_field_value"));
  }

  @Test
  public void testNestedFieldIdsForComplexCollectionChildren() {
    // Delta keys the nested ids by the path the child takes in the parquet file, relative to the
    // nearest parent struct field's physical name, so a collection nested in a collection adds to
    // the same field's metadata while a collection nested in a struct gets metadata of its own.
    Metadata structListMetadata =
        Metadata.fromJson(
            "{\"delta.columnMapping.id\": 1, \"delta.columnMapping.physicalName\":"
                + " \"col-struct-list\", \"delta.columnMapping.nested.ids\":"
                + " {\"col-struct-list.element\": 20}}");
    Metadata structMapMetadata =
        Metadata.fromJson(
            "{\"delta.columnMapping.id\": 2, \"delta.columnMapping.physicalName\":"
                + " \"col-struct-map\", \"delta.columnMapping.nested.ids\":"
                + " {\"col-struct-map.key\": 30, \"col-struct-map.value\": 31}}");
    Metadata nestedMapMetadata =
        Metadata.fromJson(
            "{\"delta.columnMapping.id\": 3, \"delta.columnMapping.physicalName\":"
                + " \"col-nested-map\", \"delta.columnMapping.nested.ids\":"
                + " {\"col-nested-map.key\": 40, \"col-nested-map.value\": 41,"
                + " \"col-nested-map.value.element\": 42}}");
    Metadata memberListMetadata =
        Metadata.fromJson(
            "{\"delta.columnMapping.id\": 6, \"delta.columnMapping.physicalName\":"
                + " \"col-member-list\", \"delta.columnMapping.nested.ids\":"
                + " {\"col-member-list.element\": 60}}");
    StructType structType =
        new StructType()
            // list of structs: the element position carries the id, the struct's members carry
            // their own column mapping ids
            .add(
                "struct_list",
                DataTypes.createArrayType(
                    new StructType()
                        .add(
                            "name",
                            DataTypes.StringType,
                            true,
                            Metadata.fromJson("{\"delta.columnMapping.id\": 21}"))
                        .add(
                            "quant",
                            DataTypes.IntegerType,
                            false,
                            Metadata.fromJson("{\"delta.columnMapping.id\": 22}"))),
                true,
                structListMetadata)
            // map with a struct value: the key and value positions carry ids, the value struct's
            // members carry their own
            .add(
                "struct_map",
                DataTypes.createMapType(
                    DataTypes.StringType,
                    new StructType()
                        .add(
                            "price",
                            DataTypes.DoubleType,
                            true,
                            Metadata.fromJson("{\"delta.columnMapping.id\": 32}"))),
                true,
                structMapMetadata)
            // map of lists: with no struct field in between, all positions stay keyed under the
            // map field's own physical name
            .add(
                "nested_map",
                DataTypes.createMapType(
                    DataTypes.StringType, DataTypes.createArrayType(DataTypes.IntegerType)),
                true,
                nestedMapMetadata)
            // map whose value struct holds a list: the list is a struct field of its own, so its
            // element id comes from the list field's own metadata, not from the map's
            .add(
                "map_with_member_list",
                DataTypes.createMapType(
                    DataTypes.StringType,
                    new StructType()
                        .add(
                            "tags",
                            DataTypes.createArrayType(DataTypes.StringType),
                            true,
                            memberListMetadata)),
                true,
                Metadata.fromJson(
                    "{\"delta.columnMapping.id\": 5, \"delta.columnMapping.physicalName\":"
                        + " \"col-map-with-member-list\", \"delta.columnMapping.nested.ids\":"
                        + " {\"col-map-with-member-list.key\": 50,"
                        + " \"col-map-with-member-list.value\": 51}}"));

    InternalSchema internalSchema = DeltaSchemaExtractor.getInstance().toInternalSchema(structType);

    Assertions.assertEquals(20, fieldId(internalSchema, "struct_list", "_one_field_element"));
    Assertions.assertEquals(
        21, fieldId(internalSchema, "struct_list", "_one_field_element", "name"));
    Assertions.assertEquals(
        22, fieldId(internalSchema, "struct_list", "_one_field_element", "quant"));
    Assertions.assertEquals(30, fieldId(internalSchema, "struct_map", "_one_field_key"));
    Assertions.assertEquals(31, fieldId(internalSchema, "struct_map", "_one_field_value"));
    Assertions.assertEquals(32, fieldId(internalSchema, "struct_map", "_one_field_value", "price"));
    Assertions.assertEquals(40, fieldId(internalSchema, "nested_map", "_one_field_key"));
    Assertions.assertEquals(41, fieldId(internalSchema, "nested_map", "_one_field_value"));
    Assertions.assertEquals(
        42, fieldId(internalSchema, "nested_map", "_one_field_value", "_one_field_element"));
    Assertions.assertEquals(50, fieldId(internalSchema, "map_with_member_list", "_one_field_key"));
    Assertions.assertEquals(
        51, fieldId(internalSchema, "map_with_member_list", "_one_field_value"));
    Assertions.assertEquals(
        60,
        fieldId(
            internalSchema,
            "map_with_member_list",
            "_one_field_value",
            "tags",
            "_one_field_element"));
  }

  private static Integer fieldId(InternalSchema schema, String fieldName, String childName) {
    return fieldId(schema, fieldName, new String[] {childName});
  }

  private static Integer fieldId(InternalSchema schema, String fieldName, String... childNames) {
    InternalSchema current = schema;
    InternalField found = null;
    List<String> path = new ArrayList<>(childNames.length + 1);
    path.add(fieldName);
    path.addAll(Arrays.asList(childNames));
    for (String name : path) {
      found =
          current.getFields().stream()
              .filter(field -> name.equals(field.getName()))
              .findFirst()
              .orElseThrow(() -> new AssertionError("missing " + name));
      current = found.getSchema();
    }
    return found.getFieldId();
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

  @Test
  public void testIcebergToDeltaUUIDSupport() {
    Metadata metadata =
        new MetadataBuilder().putString(InternalSchema.XTABLE_LOGICAL_TYPE, "uuid").build();
    StructType structRepresentation =
        new StructType()
            .add("requiredUUID", DataTypes.BinaryType, false, metadata)
            .add("optionalUUID", DataTypes.BinaryType, true, metadata);
    InternalSchema internalSchema =
        InternalSchema.builder()
            .name("struct")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("requiredUUID")
                        .schema(
                            InternalSchema.builder()
                                .name("binary")
                                .dataType(InternalType.UUID)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalUUID")
                        .schema(
                            InternalSchema.builder()
                                .name("binary")
                                .dataType(InternalType.UUID)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();
    Assertions.assertEquals(
        internalSchema, DeltaSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
  }
}
