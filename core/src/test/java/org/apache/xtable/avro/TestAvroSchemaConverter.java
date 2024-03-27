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
 
package org.apache.xtable.avro;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import org.apache.hudi.common.util.Option;

import org.apache.xtable.hudi.idtracking.IdTracker;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;

public class TestAvroSchemaConverter {
  @Test
  public void testPrimitiveTypes() {
    String schemaName = "testRecord";
    String doc = "What's up doc";
    Schema avroRepresentation =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"testRecord\",\"doc\":\"What's up doc\",\"fields\":[{\"name\":\"requiredBoolean\",\"type\":\"boolean\",\"default\":false},{\"name\":\"optionalBoolean\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"requiredInt\",\"type\":\"int\",\"default\":123},{\"name\":\"optionalInt\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"requiredLong\",\"type\":\"long\"},{\"name\":\"optionalLong\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"requiredDouble\",\"type\":\"double\"},{\"name\":\"optionalDouble\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"requiredFloat\",\"type\":\"float\"},{\"name\":\"optionalFloat\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"requiredString\",\"type\":\"string\"},{\"name\":\"optionalString\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"requiredBytes\",\"type\":\"bytes\"},{\"name\":\"optionalBytes\",\"type\":[\"null\",\"bytes\"],\"default\":null},{\"name\":\"requiredEnum\",\"type\":{\"type\":\"enum\",\"name\":\"REQUIRED_ENUM\",\"symbols\":[\"ONE\",\"TWO\"]},\"default\":\"ONE\"},{\"name\":\"optionalEnum\",\"type\":[\"null\",{\"type\":\"enum\",\"name\":\"OPTIONAL_ENUM\",\"symbols\":[\"THREE\",\"FOUR\"]}],\"default\":null}]}");

    Map<InternalSchema.MetadataKey, Object> requiredEnumMetadata = new HashMap<>();
    requiredEnumMetadata.put(InternalSchema.MetadataKey.ENUM_VALUES, Arrays.asList("ONE", "TWO"));
    Map<InternalSchema.MetadataKey, Object> optionalEnumMetadata = new HashMap<>();
    optionalEnumMetadata.put(
        InternalSchema.MetadataKey.ENUM_VALUES, Arrays.asList("THREE", "FOUR"));

    InternalSchema internalSchema =
        InternalSchema.builder()
            .name(schemaName)
            .comment(doc)
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
                        .defaultValue(false)
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
                                .name("int")
                                .dataType(InternalType.INT)
                                .isNullable(false)
                                .build())
                        .defaultValue(123)
                        .build(),
                    InternalField.builder()
                        .name("optionalInt")
                        .schema(
                            InternalSchema.builder()
                                .name("int")
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
                                .name("bytes")
                                .dataType(InternalType.BYTES)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("optionalBytes")
                        .schema(
                            InternalSchema.builder()
                                .name("bytes")
                                .dataType(InternalType.BYTES)
                                .isNullable(true)
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
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

    assertEquals(
        internalSchema, AvroSchemaConverter.getInstance().toInternalSchema(avroRepresentation));
    assertEquals(
        avroRepresentation, AvroSchemaConverter.getInstance().fromInternalSchema(internalSchema));
  }

  @Test
  public void testAvroNestedRecords() {
    Schema avroRepresentation =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"testRecord\",\"fields\":[{\"name\":\"nestedOne\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"nestedOneType\",\"namespace\":\"nestedOne\",\"fields\":[{\"name\":\"nestedOptionalInt\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"nestedRequiredDouble\",\"type\":\"double\"},{\"name\":\"nestedTwo\",\"type\":{\"type\":\"record\",\"name\":\"nestedTwoType\",\"namespace\":\"nestedOne.nestedTwo\",\"fields\":[{\"name\":\"doublyNestedString\",\"type\":[\"null\",\"string\"],\"default\":null}]}}]}],\"default\":null}]}");

    InternalSchema internalSchema =
        InternalSchema.builder()
            .name("testRecord")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("nestedOne")
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .schema(
                            InternalSchema.builder()
                                .name("nestedOneType")
                                .dataType(InternalType.RECORD)
                                .isNullable(true)
                                .fields(
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name("nestedOptionalInt")
                                            .parentPath("nestedOne")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("int")
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
                                                    .name("nestedTwoType")
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

    assertEquals(
        internalSchema, AvroSchemaConverter.getInstance().toInternalSchema(avroRepresentation));
    assertEquals(
        avroRepresentation, AvroSchemaConverter.getInstance().fromInternalSchema(internalSchema));
  }

  @Test
  public void testAvroLists() {
    Schema avroRepresentation =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"testRecord\",\"fields\":[{\"name\":\"intList\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"recordList\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"element\",\"namespace\":\"recordList._one_field_element\",\"fields\":[{\"name\":\"requiredDouble\",\"type\":\"double\"},{\"name\":\"optionalString\",\"type\":[\"null\",\"string\"],\"default\":null}]}}],\"default\":null}]}");

    InternalSchema recordListElementSchema =
        InternalSchema.builder()
            .name("element")
            .isNullable(false)
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
            .name("testRecord")
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
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .parentPath("intList")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("int")
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
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .parentPath("recordList")
                                            .schema(recordListElementSchema)
                                            .build()))
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    assertEquals(
        internalSchema, AvroSchemaConverter.getInstance().toInternalSchema(avroRepresentation));
    assertEquals(
        avroRepresentation, AvroSchemaConverter.getInstance().fromInternalSchema(internalSchema));
  }

  @Test
  public void testAvroMaps() {
    Schema avroRepresentation =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"testRecord\",\"fields\":[{\"name\":\"intMap\",\"type\":{\"type\":\"map\",\"values\":\"int\"}},{\"name\":\"recordMap\",\"type\":[\"null\",{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"element\",\"namespace\":\"recordMap._one_field_value\",\"fields\":[{\"name\":\"requiredDouble\",\"type\":\"double\"},{\"name\":\"optionalString\",\"type\":[\"null\",\"string\"],\"default\":null}]}}],\"default\":null}]}");

    InternalSchema recordMapElementSchema =
        InternalSchema.builder()
            .name("element")
            .isNullable(false)
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
                                                    .name("map_key")
                                                    .dataType(InternalType.STRING)
                                                    .isNullable(false)
                                                    .build())
                                            .defaultValue("")
                                            .build(),
                                        InternalField.builder()
                                            .name(InternalField.Constants.MAP_VALUE_FIELD_NAME)
                                            .parentPath("intMap")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("int")
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
                                                    .name("map_key")
                                                    .dataType(InternalType.STRING)
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

    assertEquals(
        internalSchema, AvroSchemaConverter.getInstance().toInternalSchema(avroRepresentation));
    assertEquals(
        avroRepresentation, AvroSchemaConverter.getInstance().fromInternalSchema(internalSchema));
  }

  @Test
  public void testAvroLogicalTypes() {
    Schema avroRepresentation =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"logicalTypes\",\"fields\":[{\"name\":\"int_date\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},"
                    + "{\"name\":\"long_timestamp_millis\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
                    + "{\"name\":\"long_timestamp_micros\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-micros\"}},"
                    + "{\"name\":\"long_timestamp_millis_local\",\"type\":{\"type\":\"long\",\"logicalType\":\"local-timestamp-millis\"}},"
                    + "{\"name\":\"long_timestamp_micros_local\",\"type\":{\"type\":\"long\",\"logicalType\":\"local-timestamp-micros\"}},"
                    + "{\"name\":\"bytes_decimal\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\", \"precision\": 4, \"scale\": 2}},"
                    + "{\"name\":\"fixed_decimal\",\"type\":{\"type\":\"fixed\",\"logicalType\":\"decimal\",\"name\":\"fixed_field\",\"size\":10,\"precision\":5,\"scale\":3}},"
                    + "{\"name\":\"fixed_plain\",\"type\":{\"type\":\"fixed\",\"name\":\"fixed_plain_field\",\"size\":10}}]}");

    Map<InternalSchema.MetadataKey, Object> bytesMetadata = new HashMap<>();
    bytesMetadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, 4);
    bytesMetadata.put(InternalSchema.MetadataKey.DECIMAL_SCALE, 2);
    Map<InternalSchema.MetadataKey, Object> fixedDecimalMetadata = new HashMap<>();
    fixedDecimalMetadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, 5);
    fixedDecimalMetadata.put(InternalSchema.MetadataKey.DECIMAL_SCALE, 3);
    fixedDecimalMetadata.put(InternalSchema.MetadataKey.FIXED_BYTES_SIZE, 10);
    Map<InternalSchema.MetadataKey, Object> fixedMetadata = new HashMap<>();
    fixedMetadata.put(InternalSchema.MetadataKey.FIXED_BYTES_SIZE, 10);
    Map<InternalSchema.MetadataKey, Object> millisMetadata = new HashMap<>();
    millisMetadata.put(
        InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MILLIS);
    Map<InternalSchema.MetadataKey, Object> microsMetadata = new HashMap<>();
    microsMetadata.put(
        InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MICROS);
    InternalSchema internalSchema =
        InternalSchema.builder()
            .name("logicalTypes")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("int_date")
                        .schema(
                            InternalSchema.builder()
                                .name("int")
                                .dataType(InternalType.DATE)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("long_timestamp_millis")
                        .schema(
                            InternalSchema.builder()
                                .name("long")
                                .dataType(InternalType.TIMESTAMP)
                                .isNullable(false)
                                .metadata(millisMetadata)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("long_timestamp_micros")
                        .schema(
                            InternalSchema.builder()
                                .name("long")
                                .dataType(InternalType.TIMESTAMP)
                                .isNullable(false)
                                .metadata(microsMetadata)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("long_timestamp_millis_local")
                        .schema(
                            InternalSchema.builder()
                                .name("long")
                                .dataType(InternalType.TIMESTAMP_NTZ)
                                .isNullable(false)
                                .metadata(millisMetadata)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("long_timestamp_micros_local")
                        .schema(
                            InternalSchema.builder()
                                .name("long")
                                .dataType(InternalType.TIMESTAMP_NTZ)
                                .isNullable(false)
                                .metadata(microsMetadata)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("bytes_decimal")
                        .schema(
                            InternalSchema.builder()
                                .name("bytes")
                                .dataType(InternalType.DECIMAL)
                                .isNullable(false)
                                .metadata(bytesMetadata)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("fixed_decimal")
                        .schema(
                            InternalSchema.builder()
                                .name("fixed_field")
                                .dataType(InternalType.DECIMAL)
                                .isNullable(false)
                                .metadata(fixedDecimalMetadata)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("fixed_plain")
                        .schema(
                            InternalSchema.builder()
                                .name("fixed_plain_field")
                                .dataType(InternalType.FIXED)
                                .isNullable(false)
                                .metadata(fixedMetadata)
                                .build())
                        .build()))
            .build();

    assertEquals(
        internalSchema, AvroSchemaConverter.getInstance().toInternalSchema(avroRepresentation));
    assertEquals(
        avroRepresentation, AvroSchemaConverter.getInstance().fromInternalSchema(internalSchema));
  }

  @Test
  public void testIdSupport() {
    Schema avroRepresentation =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"Sample\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"},{\"name\":\"ts\",\"type\":\"long\"},"
                    + "{\"name\":\"nested_record\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Nested\",\"fields\":[{\"name\":\"nested_int\",\"type\":\"int\",\"default\":0}]}],\"default\":null},"
                    + "{\"name\":\"nullable_map_field\",\"type\":[\"null\",{\"type\":\"map\",\"values\":\"Nested\"}],\"default\":null},{\"name\":\"primitive_map_field\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},"
                    + "{\"name\":\"array_field\",\"type\":{\"type\":\"array\",\"items\":\"Nested\"},\"default\":[]},{\"name\":\"primitive_array_field\",\"type\":{\"type\":\"array\",\"items\":\"string\"}}]}");
    Schema schemaWithIds =
        IdTracker.getInstance().addIdTracking(avroRepresentation, Option.empty(), false);

    InternalSchema internalSchema =
        InternalSchema.builder()
            .name("Sample")
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("key")
                        .schema(
                            InternalSchema.builder()
                                .name("string")
                                .dataType(InternalType.STRING)
                                .isNullable(false)
                                .build())
                        .fieldId(1)
                        .build(),
                    InternalField.builder()
                        .name("ts")
                        .schema(
                            InternalSchema.builder()
                                .name("long")
                                .dataType(InternalType.LONG)
                                .isNullable(false)
                                .build())
                        .fieldId(2)
                        .build(),
                    InternalField.builder()
                        .name("nested_record")
                        .schema(
                            InternalSchema.builder()
                                .name("Nested")
                                .dataType(InternalType.RECORD)
                                .isNullable(true)
                                .fields(
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name("nested_int")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("int")
                                                    .dataType(InternalType.INT)
                                                    .isNullable(false)
                                                    .build())
                                            .parentPath("nested_record")
                                            .defaultValue(0)
                                            .fieldId(8)
                                            .build()))
                                .build())
                        .fieldId(3)
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    InternalField.builder()
                        .name("nullable_map_field")
                        .schema(
                            InternalSchema.builder()
                                .name("map")
                                .isNullable(true)
                                .dataType(InternalType.MAP)
                                .fields(
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.MAP_KEY_FIELD_NAME)
                                            .parentPath("nullable_map_field")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("map_key")
                                                    .dataType(InternalType.STRING)
                                                    .isNullable(false)
                                                    .build())
                                            .defaultValue("")
                                            .fieldId(9)
                                            .build(),
                                        InternalField.builder()
                                            .name(InternalField.Constants.MAP_VALUE_FIELD_NAME)
                                            .parentPath("nullable_map_field")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("Nested")
                                                    .dataType(InternalType.RECORD)
                                                    .isNullable(false)
                                                    .fields(
                                                        Arrays.asList(
                                                            InternalField.builder()
                                                                .name("nested_int")
                                                                .schema(
                                                                    InternalSchema.builder()
                                                                        .name("int")
                                                                        .dataType(InternalType.INT)
                                                                        .isNullable(false)
                                                                        .build())
                                                                .parentPath(
                                                                    "nullable_map_field._one_field_value")
                                                                .defaultValue(0)
                                                                .fieldId(11)
                                                                .build()))
                                                    .build())
                                            .fieldId(10)
                                            .build()))
                                .build())
                        .fieldId(4)
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    InternalField.builder()
                        .name("primitive_map_field")
                        .schema(
                            InternalSchema.builder()
                                .name("map")
                                .isNullable(false)
                                .dataType(InternalType.MAP)
                                .fields(
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.MAP_KEY_FIELD_NAME)
                                            .parentPath("primitive_map_field")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("map_key")
                                                    .dataType(InternalType.STRING)
                                                    .isNullable(false)
                                                    .build())
                                            .defaultValue("")
                                            .fieldId(12)
                                            .build(),
                                        InternalField.builder()
                                            .name(InternalField.Constants.MAP_VALUE_FIELD_NAME)
                                            .parentPath("primitive_map_field")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("string")
                                                    .dataType(InternalType.STRING)
                                                    .isNullable(false)
                                                    .build())
                                            .fieldId(13)
                                            .build()))
                                .build())
                        .fieldId(5)
                        .build(),
                    InternalField.builder()
                        .name("array_field")
                        .schema(
                            InternalSchema.builder()
                                .name("array")
                                .isNullable(false)
                                .dataType(InternalType.LIST)
                                .fields(
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .parentPath("array_field")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("Nested")
                                                    .dataType(InternalType.RECORD)
                                                    .isNullable(false)
                                                    .fields(
                                                        Arrays.asList(
                                                            InternalField.builder()
                                                                .name("nested_int")
                                                                .schema(
                                                                    InternalSchema.builder()
                                                                        .name("int")
                                                                        .dataType(InternalType.INT)
                                                                        .isNullable(false)
                                                                        .build())
                                                                .parentPath(
                                                                    "array_field._one_field_element")
                                                                .defaultValue(0)
                                                                .fieldId(15)
                                                                .build()))
                                                    .build())
                                            .fieldId(14)
                                            .build()))
                                .build())
                        .defaultValue(new ArrayList<>())
                        .fieldId(6)
                        .build(),
                    InternalField.builder()
                        .name("primitive_array_field")
                        .schema(
                            InternalSchema.builder()
                                .name("array")
                                .isNullable(false)
                                .dataType(InternalType.LIST)
                                .fields(
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .parentPath("primitive_array_field")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("string")
                                                    .dataType(InternalType.STRING)
                                                    .isNullable(false)
                                                    .build())
                                            .fieldId(16)
                                            .build()))
                                .build())
                        .fieldId(7)
                        .build()))
            .build();
    assertEquals(internalSchema, AvroSchemaConverter.getInstance().toInternalSchema(schemaWithIds));
  }
}
