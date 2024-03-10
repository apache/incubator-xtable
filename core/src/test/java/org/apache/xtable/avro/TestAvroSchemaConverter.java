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

import org.apache.xtable.hudi.idtracking.IdTracker;
import org.apache.xtable.model.schema.OneField;
import org.apache.xtable.model.schema.OneSchema;
import org.apache.xtable.model.schema.OneType;

import org.apache.hudi.common.util.Option;

public class TestAvroSchemaConverter {
  @Test
  public void testPrimitiveTypes() {
    String schemaName = "testRecord";
    String doc = "What's up doc";
    Schema avroRepresentation =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"testRecord\",\"doc\":\"What's up doc\",\"fields\":[{\"name\":\"requiredBoolean\",\"type\":\"boolean\",\"default\":false},{\"name\":\"optionalBoolean\",\"type\":[\"null\",\"boolean\"],\"default\":null},{\"name\":\"requiredInt\",\"type\":\"int\",\"default\":123},{\"name\":\"optionalInt\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"requiredLong\",\"type\":\"long\"},{\"name\":\"optionalLong\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"requiredDouble\",\"type\":\"double\"},{\"name\":\"optionalDouble\",\"type\":[\"null\",\"double\"],\"default\":null},{\"name\":\"requiredFloat\",\"type\":\"float\"},{\"name\":\"optionalFloat\",\"type\":[\"null\",\"float\"],\"default\":null},{\"name\":\"requiredString\",\"type\":\"string\"},{\"name\":\"optionalString\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"requiredBytes\",\"type\":\"bytes\"},{\"name\":\"optionalBytes\",\"type\":[\"null\",\"bytes\"],\"default\":null},{\"name\":\"requiredEnum\",\"type\":{\"type\":\"enum\",\"name\":\"REQUIRED_ENUM\",\"symbols\":[\"ONE\",\"TWO\"]},\"default\":\"ONE\"},{\"name\":\"optionalEnum\",\"type\":[\"null\",{\"type\":\"enum\",\"name\":\"OPTIONAL_ENUM\",\"symbols\":[\"THREE\",\"FOUR\"]}],\"default\":null}]}");

    Map<OneSchema.MetadataKey, Object> requiredEnumMetadata = new HashMap<>();
    requiredEnumMetadata.put(OneSchema.MetadataKey.ENUM_VALUES, Arrays.asList("ONE", "TWO"));
    Map<OneSchema.MetadataKey, Object> optionalEnumMetadata = new HashMap<>();
    optionalEnumMetadata.put(OneSchema.MetadataKey.ENUM_VALUES, Arrays.asList("THREE", "FOUR"));

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
                        .build()))
            .build();

    assertEquals(
        oneSchemaRepresentation, AvroSchemaConverter.getInstance().toOneSchema(avroRepresentation));
    assertEquals(
        avroRepresentation,
        AvroSchemaConverter.getInstance().fromOneSchema(oneSchemaRepresentation));
  }

  @Test
  public void testAvroNestedRecords() {
    Schema avroRepresentation =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"testRecord\",\"fields\":[{\"name\":\"nestedOne\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"nestedOneType\",\"namespace\":\"nestedOne\",\"fields\":[{\"name\":\"nestedOptionalInt\",\"type\":[\"null\",\"int\"],\"default\":null},{\"name\":\"nestedRequiredDouble\",\"type\":\"double\"},{\"name\":\"nestedTwo\",\"type\":{\"type\":\"record\",\"name\":\"nestedTwoType\",\"namespace\":\"nestedOne.nestedTwo\",\"fields\":[{\"name\":\"doublyNestedString\",\"type\":[\"null\",\"string\"],\"default\":null}]}}]}],\"default\":null}]}");

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

    assertEquals(
        oneSchemaRepresentation, AvroSchemaConverter.getInstance().toOneSchema(avroRepresentation));
    assertEquals(
        avroRepresentation,
        AvroSchemaConverter.getInstance().fromOneSchema(oneSchemaRepresentation));
  }

  @Test
  public void testAvroLists() {
    Schema avroRepresentation =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"testRecord\",\"fields\":[{\"name\":\"intList\",\"type\":{\"type\":\"array\",\"items\":\"int\"}},{\"name\":\"recordList\",\"type\":[\"null\",{\"type\":\"array\",\"items\":{\"type\":\"record\",\"name\":\"element\",\"namespace\":\"recordList._one_field_element\",\"fields\":[{\"name\":\"requiredDouble\",\"type\":\"double\"},{\"name\":\"optionalString\",\"type\":[\"null\",\"string\"],\"default\":null}]}}],\"default\":null}]}");

    OneSchema recordListElementSchema =
        OneSchema.builder()
            .name("element")
            .isNullable(false)
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
                                .dataType(OneType.LIST)
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
                                .dataType(OneType.LIST)
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

    assertEquals(
        oneSchemaRepresentation, AvroSchemaConverter.getInstance().toOneSchema(avroRepresentation));
    assertEquals(
        avroRepresentation,
        AvroSchemaConverter.getInstance().fromOneSchema(oneSchemaRepresentation));
  }

  @Test
  public void testAvroMaps() {
    Schema avroRepresentation =
        new Schema.Parser()
            .parse(
                "{\"type\":\"record\",\"name\":\"testRecord\",\"fields\":[{\"name\":\"intMap\",\"type\":{\"type\":\"map\",\"values\":\"int\"}},{\"name\":\"recordMap\",\"type\":[\"null\",{\"type\":\"map\",\"values\":{\"type\":\"record\",\"name\":\"element\",\"namespace\":\"recordMap._one_field_value\",\"fields\":[{\"name\":\"requiredDouble\",\"type\":\"double\"},{\"name\":\"optionalString\",\"type\":[\"null\",\"string\"],\"default\":null}]}}],\"default\":null}]}");

    OneSchema recordMapElementSchema =
        OneSchema.builder()
            .name("element")
            .isNullable(false)
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
                                                    .dataType(OneType.STRING)
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

    assertEquals(
        oneSchemaRepresentation, AvroSchemaConverter.getInstance().toOneSchema(avroRepresentation));
    assertEquals(
        avroRepresentation,
        AvroSchemaConverter.getInstance().fromOneSchema(oneSchemaRepresentation));
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

    Map<OneSchema.MetadataKey, Object> bytesMetadata = new HashMap<>();
    bytesMetadata.put(OneSchema.MetadataKey.DECIMAL_PRECISION, 4);
    bytesMetadata.put(OneSchema.MetadataKey.DECIMAL_SCALE, 2);
    Map<OneSchema.MetadataKey, Object> fixedDecimalMetadata = new HashMap<>();
    fixedDecimalMetadata.put(OneSchema.MetadataKey.DECIMAL_PRECISION, 5);
    fixedDecimalMetadata.put(OneSchema.MetadataKey.DECIMAL_SCALE, 3);
    fixedDecimalMetadata.put(OneSchema.MetadataKey.FIXED_BYTES_SIZE, 10);
    Map<OneSchema.MetadataKey, Object> fixedMetadata = new HashMap<>();
    fixedMetadata.put(OneSchema.MetadataKey.FIXED_BYTES_SIZE, 10);
    Map<OneSchema.MetadataKey, Object> millisMetadata = new HashMap<>();
    millisMetadata.put(OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MILLIS);
    Map<OneSchema.MetadataKey, Object> microsMetadata = new HashMap<>();
    microsMetadata.put(OneSchema.MetadataKey.TIMESTAMP_PRECISION, OneSchema.MetadataValue.MICROS);
    OneSchema oneSchemaRepresentation =
        OneSchema.builder()
            .name("logicalTypes")
            .dataType(OneType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("int_date")
                        .schema(
                            OneSchema.builder()
                                .name("int")
                                .dataType(OneType.DATE)
                                .isNullable(false)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("long_timestamp_millis")
                        .schema(
                            OneSchema.builder()
                                .name("long")
                                .dataType(OneType.TIMESTAMP)
                                .isNullable(false)
                                .metadata(millisMetadata)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("long_timestamp_micros")
                        .schema(
                            OneSchema.builder()
                                .name("long")
                                .dataType(OneType.TIMESTAMP)
                                .isNullable(false)
                                .metadata(microsMetadata)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("long_timestamp_millis_local")
                        .schema(
                            OneSchema.builder()
                                .name("long")
                                .dataType(OneType.TIMESTAMP_NTZ)
                                .isNullable(false)
                                .metadata(millisMetadata)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("long_timestamp_micros_local")
                        .schema(
                            OneSchema.builder()
                                .name("long")
                                .dataType(OneType.TIMESTAMP_NTZ)
                                .isNullable(false)
                                .metadata(microsMetadata)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("bytes_decimal")
                        .schema(
                            OneSchema.builder()
                                .name("bytes")
                                .dataType(OneType.DECIMAL)
                                .isNullable(false)
                                .metadata(bytesMetadata)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("fixed_decimal")
                        .schema(
                            OneSchema.builder()
                                .name("fixed_field")
                                .dataType(OneType.DECIMAL)
                                .isNullable(false)
                                .metadata(fixedDecimalMetadata)
                                .build())
                        .build(),
                    OneField.builder()
                        .name("fixed_plain")
                        .schema(
                            OneSchema.builder()
                                .name("fixed_plain_field")
                                .dataType(OneType.FIXED)
                                .isNullable(false)
                                .metadata(fixedMetadata)
                                .build())
                        .build()))
            .build();

    assertEquals(
        oneSchemaRepresentation, AvroSchemaConverter.getInstance().toOneSchema(avroRepresentation));
    assertEquals(
        avroRepresentation,
        AvroSchemaConverter.getInstance().fromOneSchema(oneSchemaRepresentation));
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

    OneSchema oneSchemaRepresentation =
        OneSchema.builder()
            .name("Sample")
            .dataType(OneType.RECORD)
            .isNullable(false)
            .fields(
                Arrays.asList(
                    OneField.builder()
                        .name("key")
                        .schema(
                            OneSchema.builder()
                                .name("string")
                                .dataType(OneType.STRING)
                                .isNullable(false)
                                .build())
                        .fieldId(1)
                        .build(),
                    OneField.builder()
                        .name("ts")
                        .schema(
                            OneSchema.builder()
                                .name("long")
                                .dataType(OneType.LONG)
                                .isNullable(false)
                                .build())
                        .fieldId(2)
                        .build(),
                    OneField.builder()
                        .name("nested_record")
                        .schema(
                            OneSchema.builder()
                                .name("Nested")
                                .dataType(OneType.RECORD)
                                .isNullable(true)
                                .fields(
                                    Arrays.asList(
                                        OneField.builder()
                                            .name("nested_int")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("int")
                                                    .dataType(OneType.INT)
                                                    .isNullable(false)
                                                    .build())
                                            .parentPath("nested_record")
                                            .defaultValue(0)
                                            .fieldId(8)
                                            .build()))
                                .build())
                        .fieldId(3)
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("nullable_map_field")
                        .schema(
                            OneSchema.builder()
                                .name("map")
                                .isNullable(true)
                                .dataType(OneType.MAP)
                                .fields(
                                    Arrays.asList(
                                        OneField.builder()
                                            .name(OneField.Constants.MAP_KEY_FIELD_NAME)
                                            .parentPath("nullable_map_field")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("map_key")
                                                    .dataType(OneType.STRING)
                                                    .isNullable(false)
                                                    .build())
                                            .defaultValue("")
                                            .fieldId(9)
                                            .build(),
                                        OneField.builder()
                                            .name(OneField.Constants.MAP_VALUE_FIELD_NAME)
                                            .parentPath("nullable_map_field")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("Nested")
                                                    .dataType(OneType.RECORD)
                                                    .isNullable(false)
                                                    .fields(
                                                        Arrays.asList(
                                                            OneField.builder()
                                                                .name("nested_int")
                                                                .schema(
                                                                    OneSchema.builder()
                                                                        .name("int")
                                                                        .dataType(OneType.INT)
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
                        .defaultValue(OneField.Constants.NULL_DEFAULT_VALUE)
                        .build(),
                    OneField.builder()
                        .name("primitive_map_field")
                        .schema(
                            OneSchema.builder()
                                .name("map")
                                .isNullable(false)
                                .dataType(OneType.MAP)
                                .fields(
                                    Arrays.asList(
                                        OneField.builder()
                                            .name(OneField.Constants.MAP_KEY_FIELD_NAME)
                                            .parentPath("primitive_map_field")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("map_key")
                                                    .dataType(OneType.STRING)
                                                    .isNullable(false)
                                                    .build())
                                            .defaultValue("")
                                            .fieldId(12)
                                            .build(),
                                        OneField.builder()
                                            .name(OneField.Constants.MAP_VALUE_FIELD_NAME)
                                            .parentPath("primitive_map_field")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("string")
                                                    .dataType(OneType.STRING)
                                                    .isNullable(false)
                                                    .build())
                                            .fieldId(13)
                                            .build()))
                                .build())
                        .fieldId(5)
                        .build(),
                    OneField.builder()
                        .name("array_field")
                        .schema(
                            OneSchema.builder()
                                .name("array")
                                .isNullable(false)
                                .dataType(OneType.LIST)
                                .fields(
                                    Arrays.asList(
                                        OneField.builder()
                                            .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .parentPath("array_field")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("Nested")
                                                    .dataType(OneType.RECORD)
                                                    .isNullable(false)
                                                    .fields(
                                                        Arrays.asList(
                                                            OneField.builder()
                                                                .name("nested_int")
                                                                .schema(
                                                                    OneSchema.builder()
                                                                        .name("int")
                                                                        .dataType(OneType.INT)
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
                    OneField.builder()
                        .name("primitive_array_field")
                        .schema(
                            OneSchema.builder()
                                .name("array")
                                .isNullable(false)
                                .dataType(OneType.LIST)
                                .fields(
                                    Arrays.asList(
                                        OneField.builder()
                                            .name(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .parentPath("primitive_array_field")
                                            .schema(
                                                OneSchema.builder()
                                                    .name("string")
                                                    .dataType(OneType.STRING)
                                                    .isNullable(false)
                                                    .build())
                                            .fieldId(16)
                                            .build()))
                                .build())
                        .fieldId(7)
                        .build()))
            .build();
    assertEquals(
        oneSchemaRepresentation, AvroSchemaConverter.getInstance().toOneSchema(schemaWithIds));
  }
}
