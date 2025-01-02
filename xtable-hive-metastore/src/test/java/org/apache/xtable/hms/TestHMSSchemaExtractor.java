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
 
package org.apache.xtable.hms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.junit.jupiter.api.Test;

import org.apache.xtable.catalog.TestSchemaExtractorBase;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.storage.TableFormat;

public class TestHMSSchemaExtractor extends TestSchemaExtractorBase {

  private FieldSchema getFieldSchema(String name, String type) {
    return new FieldSchema(name, type, null);
  }

  @Test
  void testPrimitiveTypes() {
    int precision = 10;
    int scale = 5;
    Map<InternalSchema.MetadataKey, Object> doubleMetadata = new HashMap<>();
    doubleMetadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, precision);
    doubleMetadata.put(InternalSchema.MetadataKey.DECIMAL_SCALE, scale);
    String tableFormat = TableFormat.ICEBERG;

    InternalSchema oneSchema =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveInternalField(
                        "requiredBoolean", "boolean", InternalType.BOOLEAN, false, 1),
                    getPrimitiveInternalField(
                        "optionalBoolean", "boolean", InternalType.BOOLEAN, true, 2),
                    getPrimitiveInternalField("requiredInt", "integer", InternalType.INT, false, 3),
                    getPrimitiveInternalField("requiredLong", "long", InternalType.LONG, false, 4),
                    getPrimitiveInternalField(
                        "requiredDouble", "double", InternalType.DOUBLE, false, 5),
                    getPrimitiveInternalField(
                        "requiredFloat", "float", InternalType.FLOAT, false, 6),
                    getPrimitiveInternalField(
                        "requiredString", "string", InternalType.STRING, false, 7),
                    getPrimitiveInternalField(
                        "requiredBytes", "binary", InternalType.BYTES, false, 8),
                    getPrimitiveInternalField("requiredDate", "date", InternalType.DATE, false, 9),
                    getPrimitiveInternalField(
                        "requiredDecimal",
                        "decimal",
                        InternalType.DECIMAL,
                        false,
                        10,
                        doubleMetadata),
                    getPrimitiveInternalField(
                        "requiredTimestamp", "timestamp", InternalType.TIMESTAMP, false, 11),
                    getPrimitiveInternalField(
                        "requiredTimestampNTZ",
                        "timestamp_ntz",
                        InternalType.TIMESTAMP_NTZ,
                        false,
                        12)))
            .build();

    List<FieldSchema> expected =
        Arrays.asList(
            getFieldSchema("requiredBoolean", "boolean"),
            getFieldSchema("optionalBoolean", "boolean"),
            getFieldSchema("requiredInt", "int"),
            getFieldSchema("requiredLong", "bigint"),
            getFieldSchema("requiredDouble", "double"),
            getFieldSchema("requiredFloat", "float"),
            getFieldSchema("requiredString", "string"),
            getFieldSchema("requiredBytes", "binary"),
            getFieldSchema("requiredDate", "date"),
            getFieldSchema("requiredDecimal", String.format("decimal(%s,%s)", precision, scale)),
            getFieldSchema("requiredTimestamp", "timestamp"),
            getFieldSchema("requiredTimestampNTZ", "timestamp"));

    assertEquals(expected, HMSSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
  }

  @Test
  void testTimestamps() {
    String tableFormat = TableFormat.ICEBERG;
    Map<InternalSchema.MetadataKey, Object> millisTimestamp =
        Collections.singletonMap(
            InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MILLIS);

    Map<InternalSchema.MetadataKey, Object> microsTimestamp =
        Collections.singletonMap(
            InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MICROS);

    InternalSchema oneSchema =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveInternalField(
                        "requiredTimestampMillis",
                        "timestamp",
                        InternalType.TIMESTAMP,
                        false,
                        1,
                        millisTimestamp),
                    getPrimitiveInternalField(
                        "requiredTimestampMicros",
                        "timestamp",
                        InternalType.TIMESTAMP,
                        false,
                        2,
                        microsTimestamp),
                    getPrimitiveInternalField(
                        "requiredTimestampNTZMillis",
                        "timestamp_ntz",
                        InternalType.TIMESTAMP_NTZ,
                        false,
                        3,
                        millisTimestamp),
                    getPrimitiveInternalField(
                        "requiredTimestampNTZMicros",
                        "timestamp_ntz",
                        InternalType.TIMESTAMP_NTZ,
                        false,
                        4,
                        microsTimestamp)))
            .build();

    List<FieldSchema> expected =
        Arrays.asList(
            getFieldSchema("requiredTimestampMillis", "timestamp"),
            getFieldSchema("requiredTimestampMicros", "timestamp"),
            getFieldSchema("requiredTimestampNTZMillis", "timestamp"),
            getFieldSchema("requiredTimestampNTZMicros", "timestamp"));

    assertEquals(expected, HMSSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
  }

  @Test
  void testMaps() {
    String tableFormat = TableFormat.ICEBERG;
    InternalSchema recordMapElementSchema =
        InternalSchema.builder()
            .name("struct")
            .isNullable(true)
            .fields(
                Arrays.asList(
                    getPrimitiveInternalField(
                        "requiredDouble",
                        "double",
                        InternalType.DOUBLE,
                        false,
                        1,
                        "recordMap._one_field_value"),
                    getPrimitiveInternalField(
                        "optionalString",
                        "string",
                        InternalType.STRING,
                        true,
                        2,
                        "recordMap._one_field_value")))
            .dataType(InternalType.RECORD)
            .build();

    InternalSchema oneSchema =
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
                                        getPrimitiveInternalField(
                                            InternalField.Constants.MAP_KEY_FIELD_NAME,
                                            "string",
                                            InternalType.STRING,
                                            false,
                                            3,
                                            "intMap"),
                                        getPrimitiveInternalField(
                                            InternalField.Constants.MAP_VALUE_FIELD_NAME,
                                            "integer",
                                            InternalType.INT,
                                            false,
                                            4,
                                            "intMap")))
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
                                        getPrimitiveInternalField(
                                            InternalField.Constants.MAP_KEY_FIELD_NAME,
                                            "integer",
                                            InternalType.INT,
                                            false,
                                            5,
                                            "recordMap"),
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

    List<FieldSchema> expected =
        Arrays.asList(
            getFieldSchema("intMap", "map<string,int>"),
            getFieldSchema(
                "recordMap", "map<int,struct<requiredDouble:double,optionalString:string>>"));

    assertEquals(expected, HMSSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
  }

  @Test
  void testLists() {
    String tableFormat = TableFormat.ICEBERG;
    InternalSchema recordListElementSchema =
        InternalSchema.builder()
            .name("struct")
            .isNullable(true)
            .fields(
                Arrays.asList(
                    getPrimitiveInternalField(
                        "requiredDouble",
                        "double",
                        InternalType.DOUBLE,
                        false,
                        11,
                        "recordMap._one_field_value"),
                    getPrimitiveInternalField(
                        "optionalString",
                        "string",
                        InternalType.STRING,
                        true,
                        12,
                        "recordMap._one_field_value")))
            .dataType(InternalType.RECORD)
            .build();

    InternalSchema oneSchema =
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
                                        getPrimitiveInternalField(
                                            InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME,
                                            "integer",
                                            InternalType.INT,
                                            false,
                                            13,
                                            "intList")))
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
                                            .fieldId(14)
                                            .parentPath("recordList")
                                            .schema(recordListElementSchema)
                                            .build()))
                                .build())
                        .defaultValue(InternalField.Constants.NULL_DEFAULT_VALUE)
                        .build()))
            .build();

    List<FieldSchema> expected =
        Arrays.asList(
            getFieldSchema("intList", "array<int>"),
            getFieldSchema(
                "recordList", "array<struct<requiredDouble:double,optionalString:string>>"));

    assertEquals(expected, HMSSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
  }

  @Test
  void testNestedRecords() {
    String tableFormat = TableFormat.ICEBERG;
    InternalSchema oneSchema =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
            .name("record")
            .isNullable(false)
            .fields(
                Collections.singletonList(
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
                                        getPrimitiveInternalField(
                                            "nestedOptionalInt",
                                            "integer",
                                            InternalType.INT,
                                            true,
                                            11,
                                            "nestedOne"),
                                        getPrimitiveInternalField(
                                            "nestedRequiredDouble",
                                            "double",
                                            InternalType.DOUBLE,
                                            false,
                                            12,
                                            "nestedOne"),
                                        InternalField.builder()
                                            .name("nestedTwo")
                                            .parentPath("nestedOne")
                                            .fieldId(13)
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("struct")
                                                    .dataType(InternalType.RECORD)
                                                    .isNullable(false)
                                                    .fields(
                                                        Collections.singletonList(
                                                            getPrimitiveInternalField(
                                                                "doublyNestedString",
                                                                "string",
                                                                InternalType.STRING,
                                                                true,
                                                                14,
                                                                "nestedOne.nestedTwo")))
                                                    .build())
                                            .build()))
                                .build())
                        .build()))
            .build();

    List<FieldSchema> expected =
        Arrays.asList(
            getFieldSchema(
                "nestedOne",
                "struct<nestedOptionalInt:int,nestedRequiredDouble:double,nestedTwo:struct<doublyNestedString:string>>"));
    assertEquals(expected, HMSSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
  }

  @Test
  void testUnsupportedType() {
    String tableFormat = TableFormat.ICEBERG;
    // Unknown "UNION" type
    InternalSchema oneSchema =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveInternalField(
                        "optionalBoolean", "boolean", InternalType.BOOLEAN, true, 2),
                    InternalField.builder()
                        .name("unionField")
                        .schema(
                            InternalSchema.builder()
                                .name("unionSchema")
                                .dataType(InternalType.UNION)
                                .isNullable(true)
                                .build())
                        .fieldId(2)
                        .build()))
            .build();

    NotSupportedException exception =
        assertThrows(
            NotSupportedException.class,
            () -> HMSSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
    assertEquals("Unsupported type: InternalType.UNION(name=union)", exception.getMessage());

    // Invalid decimal type (precision and scale metadata is missing)
    InternalSchema oneSchema2 =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveInternalField(
                        "optionalBoolean", "boolean", InternalType.BOOLEAN, true, 1),
                    getPrimitiveInternalField(
                        "optionalDecimal", "decimal", InternalType.DECIMAL, true, 2)))
            .build();

    exception =
        assertThrows(
            NotSupportedException.class,
            () -> HMSSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema2));
    assertEquals("Invalid decimal type, precision and scale is missing", exception.getMessage());

    // Invalid decimal type (scale metadata is missing)
    Map<InternalSchema.MetadataKey, Object> doubleMetadata = new HashMap<>();
    doubleMetadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, 10);
    InternalSchema oneSchema3 =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveInternalField(
                        "optionalBoolean", "boolean", InternalType.BOOLEAN, true, 1),
                    getPrimitiveInternalField(
                        "optionalDecimal",
                        "decimal",
                        InternalType.DECIMAL,
                        true,
                        2,
                        doubleMetadata)))
            .build();

    exception =
        assertThrows(
            NotSupportedException.class,
            () -> HMSSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema3));
    assertEquals("Invalid decimal type, scale is missing", exception.getMessage());
  }
}
