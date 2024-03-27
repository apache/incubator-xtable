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

import java.util.*;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;

public class TestIcebergSchemaExtractor {

  private static final IcebergSchemaExtractor SCHEMA_EXTRACTOR =
      IcebergSchemaExtractor.getInstance();

  @Test
  public void testPrimitiveTypes() {
    int precision = 10;
    int scale = 5;
    Map<InternalSchema.MetadataKey, Object> doubleMetadata = new HashMap<>();
    doubleMetadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, precision);
    doubleMetadata.put(InternalSchema.MetadataKey.DECIMAL_SCALE, scale);

    int fixedSize = 8;
    Map<InternalSchema.MetadataKey, Object> fixedMetadata =
        Collections.singletonMap(InternalSchema.MetadataKey.FIXED_BYTES_SIZE, fixedSize);

    InternalSchema internalSchema =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .name("record")
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
                        .fieldId(1)
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
                        .fieldId(2)
                        .build(),
                    InternalField.builder()
                        .name("requiredInt")
                        .schema(
                            InternalSchema.builder()
                                .name("integer")
                                .dataType(InternalType.INT)
                                .isNullable(false)
                                .build())
                        .fieldId(3)
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
                        .fieldId(4)
                        .build(),
                    InternalField.builder()
                        .name("requiredLong")
                        .schema(
                            InternalSchema.builder()
                                .name("long")
                                .dataType(InternalType.LONG)
                                .isNullable(false)
                                .build())
                        .fieldId(5)
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
                        .fieldId(6)
                        .build(),
                    InternalField.builder()
                        .name("requiredDouble")
                        .schema(
                            InternalSchema.builder()
                                .name("double")
                                .dataType(InternalType.DOUBLE)
                                .isNullable(false)
                                .build())
                        .fieldId(7)
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
                        .fieldId(8)
                        .build(),
                    InternalField.builder()
                        .name("requiredFloat")
                        .schema(
                            InternalSchema.builder()
                                .name("float")
                                .dataType(InternalType.FLOAT)
                                .isNullable(false)
                                .build())
                        .fieldId(9)
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
                        .fieldId(10)
                        .build(),
                    InternalField.builder()
                        .name("requiredString")
                        .schema(
                            InternalSchema.builder()
                                .name("string")
                                .dataType(InternalType.STRING)
                                .isNullable(false)
                                .build())
                        .fieldId(11)
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
                        .fieldId(12)
                        .build(),
                    InternalField.builder()
                        .name("requiredBytes")
                        .schema(
                            InternalSchema.builder()
                                .name("binary")
                                .dataType(InternalType.BYTES)
                                .isNullable(false)
                                .build())
                        .fieldId(13)
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
                        .fieldId(14)
                        .build(),
                    InternalField.builder()
                        .name("requiredDate")
                        .schema(
                            InternalSchema.builder()
                                .name("date")
                                .dataType(InternalType.DATE)
                                .isNullable(false)
                                .build())
                        .fieldId(17)
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
                        .fieldId(18)
                        .build(),
                    InternalField.builder()
                        .name("requiredFixed")
                        .schema(
                            InternalSchema.builder()
                                .name("fixed")
                                .dataType(InternalType.FIXED)
                                .isNullable(false)
                                .metadata(fixedMetadata)
                                .build())
                        .fieldId(27)
                        .build(),
                    InternalField.builder()
                        .name("optionalFixed")
                        .schema(
                            InternalSchema.builder()
                                .name("fixed")
                                .dataType(InternalType.FIXED)
                                .isNullable(true)
                                .metadata(fixedMetadata)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .fieldId(28)
                        .build(),
                    InternalField.builder()
                        .name("requiredDecimal")
                        .schema(
                            InternalSchema.builder()
                                .name("decimal")
                                .dataType(InternalType.DECIMAL)
                                .isNullable(false)
                                .metadata(doubleMetadata)
                                .build())
                        .fieldId(29)
                        .build(),
                    InternalField.builder()
                        .name("optionalDecimal")
                        .schema(
                            InternalSchema.builder()
                                .name("decimal")
                                .dataType(InternalType.DECIMAL)
                                .isNullable(true)
                                .metadata(doubleMetadata)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .fieldId(30)
                        .build()))
            .build();

    Schema icebergRepresentation =
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
            Types.NestedField.required(17, "requiredDate", Types.DateType.get()),
            Types.NestedField.optional(18, "optionalDate", Types.DateType.get()),
            Types.NestedField.required(27, "requiredFixed", Types.FixedType.ofLength(fixedSize)),
            Types.NestedField.optional(28, "optionalFixed", Types.FixedType.ofLength(fixedSize)),
            Types.NestedField.required(
                29, "requiredDecimal", Types.DecimalType.of(precision, scale)),
            Types.NestedField.optional(
                30, "optionalDecimal", Types.DecimalType.of(precision, scale)));

    Assertions.assertTrue(
        icebergRepresentation.sameSchema(SCHEMA_EXTRACTOR.toIceberg(internalSchema)));
    assertEquals(internalSchema, SCHEMA_EXTRACTOR.fromIceberg(icebergRepresentation));
  }

  @Test
  public void testEnums() {
    // there are no enums in iceberg so we convert them to string
    Map<InternalSchema.MetadataKey, Object> requiredEnumMetadata =
        Collections.singletonMap(
            InternalSchema.MetadataKey.ENUM_VALUES, Arrays.asList("ONE", "TWO"));
    Map<InternalSchema.MetadataKey, Object> optionalEnumMetadata =
        Collections.singletonMap(
            InternalSchema.MetadataKey.ENUM_VALUES, Arrays.asList("THREE", "FOUR"));

    InternalSchema schemaWithEnums =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
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
                        .defaultValue("ONE")
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

    Schema expectedSchema =
        new Schema(
            Types.NestedField.required(1, "requiredEnum", Types.StringType.get()),
            Types.NestedField.optional(2, "optionalEnum", Types.StringType.get()));
    assertTrue(expectedSchema.sameSchema(SCHEMA_EXTRACTOR.toIceberg(schemaWithEnums)));
  }

  @Test
  public void testUuids() {
    // UUIDs are represented as fixed length byte arrays
    Schema inputSchema =
        new Schema(
            Types.NestedField.required(1, "requiredUuid", Types.UUIDType.get()),
            Types.NestedField.optional(2, "optionalUuid", Types.UUIDType.get()));

    int fixedSize = 16;
    Map<InternalSchema.MetadataKey, Object> fixedMetadata = new HashMap<>();
    fixedMetadata.put(InternalSchema.MetadataKey.FIXED_BYTES_SIZE, fixedSize);
    InternalSchema expectedSchema =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
            .name("record")
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("requiredUuid")
                        .fieldId(1)
                        .schema(
                            InternalSchema.builder()
                                .name("uuid")
                                .dataType(InternalType.FIXED)
                                .isNullable(false)
                                .metadata(fixedMetadata)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalUuid")
                        .fieldId(2)
                        .schema(
                            InternalSchema.builder()
                                .name("uuid")
                                .dataType(InternalType.FIXED)
                                .isNullable(true)
                                .metadata(fixedMetadata)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();
    assertEquals(expectedSchema, (SCHEMA_EXTRACTOR.fromIceberg(inputSchema)));
  }

  @Test
  public void testTimestamps() {
    Map<InternalSchema.MetadataKey, Object> millisTimestamp =
        Collections.singletonMap(
            InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MILLIS);

    Map<InternalSchema.MetadataKey, Object> microsTimestamp =
        Collections.singletonMap(
            InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MICROS);

    InternalSchema irSchema =
        InternalSchema.builder()
            .name("record")
            .dataType(InternalType.RECORD)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("requiredTimestampMillis")
                        .schema(
                            InternalSchema.builder()
                                .name("timestamp")
                                .dataType(InternalType.TIMESTAMP)
                                .metadata(millisTimestamp)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalTimestampMillis")
                        .schema(
                            InternalSchema.builder()
                                .name("timestamp")
                                .dataType(InternalType.TIMESTAMP)
                                .metadata(millisTimestamp)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    InternalField.builder()
                        .name("requiredTimestampNtzMillis")
                        .schema(
                            InternalSchema.builder()
                                .name("timestampNtz")
                                .dataType(InternalType.TIMESTAMP_NTZ)
                                .isNullable(false)
                                .metadata(millisTimestamp)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalTimestampNtzMillis")
                        .schema(
                            InternalSchema.builder()
                                .name("timestampNtz")
                                .dataType(InternalType.TIMESTAMP_NTZ)
                                .metadata(millisTimestamp)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    InternalField.builder()
                        .name("requiredTimestampMicros")
                        .schema(
                            InternalSchema.builder()
                                .name("timestamp")
                                .dataType(InternalType.TIMESTAMP)
                                .metadata(microsTimestamp)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalTimestampMicros")
                        .schema(
                            InternalSchema.builder()
                                .name("timestamp")
                                .dataType(InternalType.TIMESTAMP)
                                .metadata(microsTimestamp)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    InternalField.builder()
                        .name("requiredTimestampNtzMicros")
                        .schema(
                            InternalSchema.builder()
                                .name("timestampNtz")
                                .dataType(InternalType.TIMESTAMP_NTZ)
                                .isNullable(false)
                                .metadata(microsTimestamp)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalTimestampNtzMicros")
                        .schema(
                            InternalSchema.builder()
                                .name("timestampNtz")
                                .dataType(InternalType.TIMESTAMP_NTZ)
                                .metadata(microsTimestamp)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();
    Schema expectedTargetSchema =
        new Schema(
            Types.NestedField.required(
                1, "requiredTimestampMillis", Types.TimestampType.withZone()),
            Types.NestedField.optional(
                2, "optionalTimestampMillis", Types.TimestampType.withZone()),
            Types.NestedField.required(3, "requiredTimestampNtzMillis", Types.LongType.get()),
            Types.NestedField.optional(4, "optionalTimestampNtzMillis", Types.LongType.get()),
            Types.NestedField.required(
                5, "requiredTimestampMicros", Types.TimestampType.withZone()),
            Types.NestedField.optional(
                6, "optionalTimestampMicros", Types.TimestampType.withZone()),
            Types.NestedField.required(7, "requiredTimestampNtzMicros", Types.LongType.get()),
            Types.NestedField.optional(8, "optionalTimestampNtzMicros", Types.LongType.get()));
    assertTrue(expectedTargetSchema.sameSchema(SCHEMA_EXTRACTOR.toIceberg(irSchema)));

    Schema sourceSchema =
        new Schema(
            Types.NestedField.required(
                4, "requiredTimestampWithZone", Types.TimestampType.withZone()),
            Types.NestedField.optional(
                5, "optionalTimestampWithZone", Types.TimestampType.withZone()),
            Types.NestedField.required(
                6, "requiredTimestampWithoutZone", Types.TimestampType.withoutZone()),
            Types.NestedField.optional(
                7, "optionalTimestampWithoutZone", Types.TimestampType.withoutZone()));
    InternalSchema expectedIrSchema =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
            .name("record")
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("requiredTimestampWithZone")
                        .fieldId(4)
                        .schema(
                            InternalSchema.builder()
                                .name("timestamp")
                                .dataType(InternalType.TIMESTAMP)
                                .metadata(microsTimestamp)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalTimestampWithZone")
                        .fieldId(5)
                        .schema(
                            InternalSchema.builder()
                                .name("timestamp")
                                .dataType(InternalType.TIMESTAMP)
                                .metadata(microsTimestamp)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    InternalField.builder()
                        .name("requiredTimestampWithoutZone")
                        .fieldId(6)
                        .schema(
                            InternalSchema.builder()
                                .name("timestamp")
                                .dataType(InternalType.TIMESTAMP_NTZ)
                                .metadata(microsTimestamp)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalTimestampWithoutZone")
                        .fieldId(7)
                        .schema(
                            InternalSchema.builder()
                                .name("timestamp")
                                .dataType(InternalType.TIMESTAMP_NTZ)
                                .metadata(microsTimestamp)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();
    assertEquals(expectedIrSchema, SCHEMA_EXTRACTOR.fromIceberg(sourceSchema));
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
                        .fieldId(7)
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
                        .fieldId(8)
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
            .name("record")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("intMap")
                        .fieldId(1)
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
                                            .fieldId(3)
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
                                            .fieldId(4)
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
                        .fieldId(2)
                        .schema(
                            InternalSchema.builder()
                                .name("map")
                                .isNullable(true)
                                .dataType(InternalType.MAP)
                                .fields(
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.MAP_KEY_FIELD_NAME)
                                            .fieldId(5)
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
                                            .fieldId(6)
                                            .parentPath("recordMap")
                                            .schema(recordMapElementSchema)
                                            .build()))
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    Schema icebergRepresentation =
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

    Assertions.assertTrue(
        icebergRepresentation.sameSchema(SCHEMA_EXTRACTOR.toIceberg(internalSchema)));
    assertEquals(internalSchema, SCHEMA_EXTRACTOR.fromIceberg(icebergRepresentation));
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
                        .fieldId(5)
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
                        .fieldId(6)
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
            .dataType(InternalType.RECORD)
            .name("record")
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("intList")
                        .fieldId(1)
                        .schema(
                            InternalSchema.builder()
                                .name("list")
                                .isNullable(false)
                                .dataType(InternalType.LIST)
                                .fields(
                                    Collections.singletonList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .fieldId(3)
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
                        .fieldId(2)
                        .schema(
                            InternalSchema.builder()
                                .name("list")
                                .isNullable(true)
                                .dataType(InternalType.LIST)
                                .fields(
                                    Collections.singletonList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .fieldId(4)
                                            .parentPath("recordList")
                                            .schema(recordListElementSchema)
                                            .build()))
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    Schema icebergRepresentation =
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

    Assertions.assertTrue(
        icebergRepresentation.sameSchema(SCHEMA_EXTRACTOR.toIceberg(internalSchema)));
    assertEquals(internalSchema, SCHEMA_EXTRACTOR.fromIceberg(icebergRepresentation));
  }

  @Test
  public void testNestedRecords() {
    InternalSchema internalSchema =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
            .name("record")
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("nestedOne")
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .fieldId(1)
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
                                            .fieldId(2)
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
                                            .fieldId(3)
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
                                            .fieldId(4)
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("struct")
                                                    .dataType(InternalType.RECORD)
                                                    .isNullable(false)
                                                    .fields(
                                                        Collections.singletonList(
                                                            InternalField.builder()
                                                                .name("doublyNestedString")
                                                                .parentPath("nestedOne.nestedTwo")
                                                                .fieldId(5)
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

    Schema icebergRepresentation =
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
    Assertions.assertTrue(
        icebergRepresentation.sameSchema(SCHEMA_EXTRACTOR.toIceberg(internalSchema)));
    assertEquals(internalSchema, SCHEMA_EXTRACTOR.fromIceberg(icebergRepresentation));
  }

  @Test
  public void testToIcebergWithNoFieldIdsSet() {
    InternalSchema recordListElementSchema =
        InternalSchema.builder()
            .name("element")
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
    InternalSchema recordMapElementSchema =
        InternalSchema.builder()
            .name("element")
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
            .name("testRecord")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("recordList")
                        .schema(
                            InternalSchema.builder()
                                .name("array")
                                .isNullable(true)
                                .dataType(InternalType.LIST)
                                .fields(
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .parentPath("recordList")
                                            .schema(recordListElementSchema)
                                            .build()))
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
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
                                                    .name("map_key")
                                                    .dataType(InternalType.INT)
                                                    .isNullable(false)
                                                    .build())
                                            .defaultValue("")
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

    Schema icebergRepresentation =
        new Schema(
            Types.NestedField.optional(
                1,
                "recordList",
                Types.ListType.ofOptional(
                    3,
                    Types.StructType.of(
                        Types.NestedField.required(4, "requiredDouble", Types.DoubleType.get()),
                        Types.NestedField.optional(5, "optionalString", Types.StringType.get())))),
            Types.NestedField.optional(
                2,
                "recordMap",
                Types.MapType.ofOptional(
                    6,
                    7,
                    Types.IntegerType.get(),
                    Types.StructType.of(
                        Types.NestedField.required(8, "requiredDouble", Types.DoubleType.get()),
                        Types.NestedField.optional(9, "optionalString", Types.StringType.get())))));

    Assertions.assertTrue(
        icebergRepresentation.sameSchema(SCHEMA_EXTRACTOR.toIceberg(internalSchema)));
  }
}
