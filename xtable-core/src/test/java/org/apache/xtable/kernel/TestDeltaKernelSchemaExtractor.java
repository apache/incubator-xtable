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
 
package org.apache.xtable.kernel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.BinaryType;
import io.delta.kernel.types.BooleanType;
import io.delta.kernel.types.DateType;
import io.delta.kernel.types.DecimalType;
import io.delta.kernel.types.DoubleType;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.FloatType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;
import io.delta.kernel.types.TimestampNTZType;
import io.delta.kernel.types.TimestampType;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;

public class TestDeltaKernelSchemaExtractor {

  private final DeltaKernelSchemaExtractor extractor = DeltaKernelSchemaExtractor.getInstance();

  // ========== Tests for toInternalSchema() ==========

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
            .add(
                "requiredBoolean",
                BooleanType.BOOLEAN,
                false,
                FieldMetadata.builder().putString("comment", "requiredBooleanComment").build())
            .add("optionalBoolean", BooleanType.BOOLEAN, true)
            .add("requiredInt", IntegerType.INTEGER, false)
            .add("optionalInt", IntegerType.INTEGER, true)
            .add("requiredLong", LongType.LONG, false)
            .add("optionalLong", LongType.LONG, true)
            .add("requiredDouble", DoubleType.DOUBLE, false)
            .add("optionalDouble", DoubleType.DOUBLE, true)
            .add("requiredFloat", FloatType.FLOAT, false)
            .add("optionalFloat", FloatType.FLOAT, true)
            .add("requiredString", StringType.STRING, false)
            .add("optionalString", StringType.STRING, true)
            .add("requiredBytes", BinaryType.BINARY, false)
            .add("optionalBytes", BinaryType.BINARY, true)
            .add("requiredDate", DateType.DATE, false)
            .add("optionalDate", DateType.DATE, true)
            .add("requiredDecimal", new DecimalType(10, 2), false)
            .add("optionalDecimal", new DecimalType(10, 2), true);

    Assertions.assertEquals(
        internalSchema,
        DeltaKernelSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
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
    io.delta.kernel.types.StructType structRepresentation =
        new io.delta.kernel.types.StructType()
            .add(
                "requiredFixed",
                BinaryType.BINARY,
                false,
                FieldMetadata.builder().putString("comment", "comment").build())
            .add("optionalFixed", BinaryType.BINARY, true);

    Assertions.assertEquals(
        internalSchemaAfterRoundTrip,
        DeltaKernelSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
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

    io.delta.kernel.types.StructType structRepresentationTimestamp =
        new StructType()
            .add("requiredTimestamp", TimestampType.TIMESTAMP, false)
            .add("optionalTimestamp", TimestampType.TIMESTAMP, true)
            .add("requiredTimestampNtz", TimestampNTZType.TIMESTAMP_NTZ, false)
            .add("optionalTimestampNtz", TimestampNTZType.TIMESTAMP_NTZ, true);

    Assertions.assertEquals(
        internalSchemaTimestamp,
        DeltaKernelSchemaExtractor.getInstance().toInternalSchema(structRepresentationTimestamp));
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

    io.delta.kernel.types.StructType mapElement =
        new StructType()
            .add("requiredDouble", DoubleType.DOUBLE, false)
            .add("optionalString", StringType.STRING, true);
    io.delta.kernel.types.StructType structRepresentation =
        new StructType()
            .add("intMap", new MapType(StringType.STRING, IntegerType.INTEGER, false), false)
            .add("recordMap", new MapType(IntegerType.INTEGER, mapElement, true));

    Assertions.assertEquals(
        internalSchema,
        DeltaKernelSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
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
    io.delta.kernel.types.StructType elementSchema =
        new StructType()
            .add("requiredDouble", DoubleType.DOUBLE, false)
            .add("optionalString", StringType.STRING, true);
    io.delta.kernel.types.StructType structRepresentation =
        new StructType()
            .add("intList", new ArrayType(IntegerType.INTEGER, false), false)
            .add("recordList", new ArrayType(elementSchema, true), true);

    Assertions.assertEquals(
        internalSchema,
        DeltaKernelSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
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

    io.delta.kernel.types.StructType structRepresentation =
        new StructType()
            .add(
                "nestedOne",
                new StructType()
                    .add(
                        "nestedOptionalInt",
                        IntegerType.INTEGER,
                        true,
                        FieldMetadata.builder()
                            .putString("comment", "nestedOptionalIntComment")
                            .build())
                    .add("nestedRequiredDouble", DoubleType.DOUBLE, false)
                    .add(
                        "nestedTwo",
                        new StructType().add("doublyNestedString", StringType.STRING, true),
                        false),
                true,
                FieldMetadata.builder().putString("comment", "comment").build());
    Assertions.assertEquals(
        internalSchema,
        DeltaKernelSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
  }

  @Test
  public void testFieldIdsInDeltaSchema() {
    io.delta.kernel.types.StructType structRepresentation =
        new StructType()
            .add(
                "nestedOne",
                new StructType()
                    .add(
                        "nestedOptionalInt",
                        IntegerType.INTEGER,
                        true,
                        FieldMetadata.builder().putLong("delta.columnMapping.id", 3).build())
                    .add(
                        "nestedRequiredDouble",
                        DoubleType.DOUBLE,
                        false,
                        FieldMetadata.builder().putLong("delta.columnMapping.id", 5).build())
                    .add(
                        "nestedTwo",
                        new StructType()
                            .add(
                                "doublyNestedString",
                                StringType.STRING,
                                true,
                                FieldMetadata.builder()
                                    .putLong("delta.columnMapping.id", 12)
                                    .build()),
                        false,
                        FieldMetadata.builder().putLong("delta.columnMapping.id", 10).build()),
                true,
                FieldMetadata.builder().putLong("delta.columnMapping.id", 2).build());

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
        internalSchema,
        DeltaKernelSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
  }

  @Test
  void generateColumnsAreNotTranslatedToInternalSchema() {
    io.delta.kernel.types.StructType structRepresentation =
        new StructType()
            .add("birthDate", TimestampType.TIMESTAMP, false)
            .add(
                "birthYear",
                TimestampType.TIMESTAMP,
                true,
                FieldMetadata.builder()
                    .putString("delta.generationExpression", "YEAR(birthDate)")
                    .build());
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
        internalSchema,
        DeltaKernelSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
  }

  @Test
  public void testIcebergToDeltaUUIDSupport() {

    io.delta.kernel.types.StructType structRepresentation =
        new StructType()
            .add(
                "requiredUUID",
                BinaryType.BINARY,
                false,
                FieldMetadata.builder()
                    .putString(InternalSchema.XTABLE_LOGICAL_TYPE, "uuid")
                    .build())
            .add(
                "optionalUUID",
                BinaryType.BINARY,
                true,
                FieldMetadata.builder()
                    .putString(InternalSchema.XTABLE_LOGICAL_TYPE, "uuid")
                    .build());
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
        internalSchema,
        DeltaKernelSchemaExtractor.getInstance().toInternalSchema(structRepresentation));
  }

  // ========== Tests for fromInternalSchema() - New Tests ==========

  @Test
  public void testFromInternalSchemaSimpleTypes() {
    // Create an InternalSchema with simple types
    InternalField idField =
        InternalField.builder()
            .name("id")
            .schema(
                InternalSchema.builder()
                    .name("integer")
                    .dataType(InternalType.INT)
                    .isNullable(false)
                    .build())
            .build();

    InternalField nameField =
        InternalField.builder()
            .name("name")
            .schema(
                InternalSchema.builder()
                    .name("string")
                    .dataType(InternalType.STRING)
                    .isNullable(true)
                    .build())
            .build();

    InternalField activeField =
        InternalField.builder()
            .name("active")
            .schema(
                InternalSchema.builder()
                    .name("boolean")
                    .dataType(InternalType.BOOLEAN)
                    .isNullable(false)
                    .build())
            .build();

    InternalSchema internalSchema =
        InternalSchema.builder()
            .name("record")
            .dataType(InternalType.RECORD)
            .fields(Arrays.asList(idField, nameField, activeField))
            .build();

    // Convert to Delta Kernel StructType
    StructType deltaSchema = extractor.fromInternalSchema(internalSchema);

    // Verify
    assertNotNull(deltaSchema);
    assertEquals(3, deltaSchema.fields().size());

    // Check id field
    StructField idDeltaField = deltaSchema.fields().get(0);
    assertEquals("id", idDeltaField.getName());
    assertEquals(IntegerType.INTEGER, idDeltaField.getDataType());
    assertEquals(false, idDeltaField.isNullable());

    // Check name field
    StructField nameDeltaField = deltaSchema.fields().get(1);
    assertEquals("name", nameDeltaField.getName());
    assertEquals(StringType.STRING, nameDeltaField.getDataType());
    assertEquals(true, nameDeltaField.isNullable());

    // Check active field
    StructField activeDeltaField = deltaSchema.fields().get(2);
    assertEquals("active", activeDeltaField.getName());
    assertEquals(BooleanType.BOOLEAN, activeDeltaField.getDataType());
    assertEquals(false, activeDeltaField.isNullable());
  }

  @Test
  public void testFromInternalSchemaWithUUID() {
    // Create an InternalSchema with UUID type
    InternalField uuidField =
        InternalField.builder()
            .name("userId")
            .schema(
                InternalSchema.builder()
                    .name("binary")
                    .dataType(InternalType.UUID)
                    .isNullable(false)
                    .build())
            .build();

    InternalSchema internalSchema =
        InternalSchema.builder()
            .name("record")
            .dataType(InternalType.RECORD)
            .fields(Collections.singletonList(uuidField))
            .build();

    // Convert to Delta Kernel StructType
    StructType deltaSchema = extractor.fromInternalSchema(internalSchema);

    // Verify
    assertNotNull(deltaSchema);
    assertEquals(1, deltaSchema.fields().size());

    StructField uuidDeltaField = deltaSchema.fields().get(0);
    assertEquals("userId", uuidDeltaField.getName());
    assertTrue(uuidDeltaField.getDataType() instanceof BinaryType);
    assertEquals(false, uuidDeltaField.isNullable());

    // Check metadata contains UUID marker
    FieldMetadata metadata = uuidDeltaField.getMetadata();
    assertTrue(metadata.contains(InternalSchema.XTABLE_LOGICAL_TYPE));
    assertEquals("uuid", metadata.getString(InternalSchema.XTABLE_LOGICAL_TYPE));
  }

  @Test
  public void testRoundTripConversion() {
    // Create a Delta Kernel StructType
    StructType originalDeltaSchema =
        new StructType(
            Arrays.asList(
                new StructField("id", IntegerType.INTEGER, false),
                new StructField("name", StringType.STRING, true),
                new StructField("score", DoubleType.DOUBLE, false)));

    // Convert to InternalSchema
    InternalSchema internalSchema = extractor.toInternalSchema(originalDeltaSchema);

    // Convert back to Delta Kernel StructType
    StructType convertedDeltaSchema = extractor.fromInternalSchema(internalSchema);

    // Verify structure matches
    assertEquals(originalDeltaSchema.fields().size(), convertedDeltaSchema.fields().size());

    for (int i = 0; i < originalDeltaSchema.fields().size(); i++) {
      StructField original = originalDeltaSchema.fields().get(i);
      StructField converted = convertedDeltaSchema.fields().get(i);

      assertEquals(original.getName(), converted.getName());
      assertEquals(original.getDataType(), converted.getDataType());
      assertEquals(original.isNullable(), converted.isNullable());
    }
  }
}
