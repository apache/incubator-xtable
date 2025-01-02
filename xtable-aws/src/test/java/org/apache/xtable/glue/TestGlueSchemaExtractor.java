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
 
package org.apache.xtable.glue;

import static org.apache.xtable.glue.GlueSchemaExtractor.getColumnProperty;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.xtable.catalog.TestSchemaExtractorBase;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.storage.TableFormat;

import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

public class TestGlueSchemaExtractor extends TestSchemaExtractorBase {

  private Column getCurrentGlueTableColumn(
      String tableFormat, String colName, String colType, Integer fieldId, boolean isNullable) {
    fieldId = fieldId != null ? fieldId : -1;
    return Column.builder()
        .name(colName)
        .type(colType)
        .parameters(
            ImmutableMap.of(
                getColumnProperty(tableFormat, "field.id"), Integer.toString(fieldId),
                getColumnProperty(tableFormat, "field.optional"), Boolean.toString(isNullable),
                getColumnProperty(tableFormat, "field.current"), "true"))
        .build();
  }

  private Column getPreviousGlueTableColumn(String tableFormat, String colName, String colType) {
    return Column.builder()
        .name(colName)
        .type(colType)
        .parameters(ImmutableMap.of(getColumnProperty(tableFormat, "field.current"), "false"))
        .build();
  }

  @Test
  void testPrimitiveTypes_NoExistingTable() {
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
                    getPrimitiveOneField(
                        "requiredBoolean", "boolean", InternalType.BOOLEAN, false, 1),
                    getPrimitiveOneField(
                        "optionalBoolean", "boolean", InternalType.BOOLEAN, true, 2),
                    getPrimitiveOneField("requiredInt", "integer", InternalType.INT, false, 3),
                    getPrimitiveOneField("requiredLong", "long", InternalType.LONG, false, 4),
                    getPrimitiveOneField("requiredDouble", "double", InternalType.DOUBLE, false, 5),
                    getPrimitiveOneField("requiredFloat", "float", InternalType.FLOAT, false, 6),
                    getPrimitiveOneField("requiredString", "string", InternalType.STRING, false, 7),
                    getPrimitiveOneField("requiredBytes", "binary", InternalType.BYTES, false, 8),
                    getPrimitiveOneField("requiredDate", "date", InternalType.DATE, false, 9),
                    getPrimitiveOneField(
                        "requiredDecimal",
                        "decimal",
                        InternalType.DECIMAL,
                        false,
                        10,
                        doubleMetadata),
                    getPrimitiveOneField(
                        "requiredTimestamp", "timestamp", InternalType.TIMESTAMP, false, 11),
                    getPrimitiveOneField(
                        "requiredTimestampNTZ",
                        "timestamp_ntz",
                        InternalType.TIMESTAMP_NTZ,
                        false,
                        12)))
            .build();

    List<Column> expectedGlueColumns =
        Arrays.asList(
            getCurrentGlueTableColumn(tableFormat, "requiredBoolean", "boolean", 1, false),
            getCurrentGlueTableColumn(tableFormat, "optionalBoolean", "boolean", 2, true),
            getCurrentGlueTableColumn(tableFormat, "requiredInt", "int", 3, false),
            getCurrentGlueTableColumn(tableFormat, "requiredLong", "bigint", 4, false),
            getCurrentGlueTableColumn(tableFormat, "requiredDouble", "double", 5, false),
            getCurrentGlueTableColumn(tableFormat, "requiredFloat", "float", 6, false),
            getCurrentGlueTableColumn(tableFormat, "requiredString", "string", 7, false),
            getCurrentGlueTableColumn(tableFormat, "requiredBytes", "binary", 8, false),
            getCurrentGlueTableColumn(tableFormat, "requiredDate", "date", 9, false),
            getCurrentGlueTableColumn(
                tableFormat,
                "requiredDecimal",
                String.format("decimal(%s,%s)", precision, scale),
                10,
                false),
            getCurrentGlueTableColumn(tableFormat, "requiredTimestamp", "timestamp", 11, false),
            getCurrentGlueTableColumn(tableFormat, "requiredTimestampNTZ", "timestamp", 12, false));

    assertEquals(
        expectedGlueColumns, GlueSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
  }

  @Test
  void testTimestamps_NoExistingTable() {
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
                    getPrimitiveOneField(
                        "requiredTimestampMillis",
                        "timestamp",
                        InternalType.TIMESTAMP,
                        false,
                        1,
                        millisTimestamp),
                    getPrimitiveOneField(
                        "requiredTimestampMicros",
                        "timestamp",
                        InternalType.TIMESTAMP,
                        false,
                        2,
                        microsTimestamp),
                    getPrimitiveOneField(
                        "requiredTimestampNTZMillis",
                        "timestamp_ntz",
                        InternalType.TIMESTAMP_NTZ,
                        false,
                        3,
                        millisTimestamp),
                    getPrimitiveOneField(
                        "requiredTimestampNTZMicros",
                        "timestamp_ntz",
                        InternalType.TIMESTAMP_NTZ,
                        false,
                        4,
                        microsTimestamp)))
            .build();

    List<Column> expectedGlueColumns =
        Arrays.asList(
            getCurrentGlueTableColumn(
                tableFormat, "requiredTimestampMillis", "timestamp", 1, false),
            getCurrentGlueTableColumn(
                tableFormat, "requiredTimestampMicros", "timestamp", 2, false),
            getCurrentGlueTableColumn(
                tableFormat, "requiredTimestampNTZMillis", "timestamp", 3, false),
            getCurrentGlueTableColumn(
                tableFormat, "requiredTimestampNTZMicros", "timestamp", 4, false));

    assertEquals(
        expectedGlueColumns, GlueSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
  }

  @Test
  void testMaps_NoExistingTable() {
    String tableFormat = TableFormat.ICEBERG;
    InternalSchema recordMapElementSchema =
        InternalSchema.builder()
            .name("struct")
            .isNullable(true)
            .fields(
                Arrays.asList(
                    getPrimitiveOneField(
                        "requiredDouble",
                        "double",
                        InternalType.DOUBLE,
                        false,
                        1,
                        "recordMap._one_field_value"),
                    getPrimitiveOneField(
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
                                        getPrimitiveOneField(
                                            InternalField.Constants.MAP_KEY_FIELD_NAME,
                                            "string",
                                            InternalType.STRING,
                                            false,
                                            3,
                                            "intMap"),
                                        getPrimitiveOneField(
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
                                        getPrimitiveOneField(
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

    List<Column> expectedGlueColumns =
        Arrays.asList(
            getCurrentGlueTableColumn(tableFormat, "intMap", "map<string,int>", 1, false),
            getCurrentGlueTableColumn(
                tableFormat,
                "recordMap",
                "map<int,struct<requiredDouble:double,optionalString:string>>",
                2,
                true));

    assertEquals(
        expectedGlueColumns, GlueSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
  }

  @Test
  void testLists_NoExistingTable() {
    String tableFormat = TableFormat.ICEBERG;
    InternalSchema recordListElementSchema =
        InternalSchema.builder()
            .name("struct")
            .isNullable(true)
            .fields(
                Arrays.asList(
                    getPrimitiveOneField(
                        "requiredDouble",
                        "double",
                        InternalType.DOUBLE,
                        false,
                        11,
                        "recordMap._one_field_value"),
                    getPrimitiveOneField(
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
                                        getPrimitiveOneField(
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

    List<Column> expectedGlueColumns =
        Arrays.asList(
            getCurrentGlueTableColumn(tableFormat, "intList", "array<int>", 1, false),
            getCurrentGlueTableColumn(
                tableFormat,
                "recordList",
                "array<struct<requiredDouble:double,optionalString:string>>",
                2,
                true));

    assertEquals(
        expectedGlueColumns, GlueSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
  }

  @Test
  void testNestedRecords_NoExistingTable() {
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
                                        getPrimitiveOneField(
                                            "nestedOptionalInt",
                                            "integer",
                                            InternalType.INT,
                                            true,
                                            11,
                                            "nestedOne"),
                                        getPrimitiveOneField(
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
                                                            getPrimitiveOneField(
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

    List<Column> expectedGlueColumns =
        Arrays.asList(
            getCurrentGlueTableColumn(
                tableFormat,
                "nestedOne",
                "struct<nestedOptionalInt:int,nestedRequiredDouble:double,nestedTwo:struct<doublyNestedString:string>>",
                1,
                true));
    assertEquals(
        expectedGlueColumns, GlueSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
  }

  @Test
  void testToColumns_NoColumnsFromExistingTable() {
    String tableFormat = TableFormat.ICEBERG;
    InternalSchema oneSchema =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveOneField(
                        "optionalBoolean", "boolean", InternalType.BOOLEAN, true, 2),
                    getPrimitiveOneField("requiredInt", "integer", InternalType.INT, false, 3)))
            .build();

    List<Table> tableList =
        Arrays.asList(
            // table is null
            null,
            // storageDescriptor is null
            Table.builder().build(),
            // no columns present
            Table.builder().storageDescriptor(StorageDescriptor.builder().build()).build());

    List<Column> expectedGlueColumns =
        Arrays.asList(
            getCurrentGlueTableColumn(tableFormat, "optionalBoolean", "boolean", 2, true),
            getCurrentGlueTableColumn(tableFormat, "requiredInt", "int", 3, false));

    for (Table table : tableList) {
      assertEquals(
          expectedGlueColumns,
          GlueSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema, table));
    }
  }

  @Test
  void testToColumns_ValidExistingTable() {
    String tableFormat = TableFormat.ICEBERG;
    InternalSchema oneSchema =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveOneField(
                        "optionalBoolean", "boolean", InternalType.BOOLEAN, true, 2),
                    getPrimitiveOneField("requiredInt", "integer", InternalType.INT, false, 3)))
            .build();

    Table existingTable =
        Table.builder()
            .storageDescriptor(
                StorageDescriptor.builder()
                    .columns(
                        ImmutableList.of(
                            Column.builder().name("prev_x").type("string").build(),
                            Column.builder().name("prev_y").type("string").build()))
                    .build())
            .build();

    List<Column> expectedGlueColumns =
        Arrays.asList(
            getCurrentGlueTableColumn(tableFormat, "optionalBoolean", "boolean", 2, true),
            getCurrentGlueTableColumn(tableFormat, "requiredInt", "int", 3, false),
            getPreviousGlueTableColumn(tableFormat, "prev_x", "string"),
            getPreviousGlueTableColumn(tableFormat, "prev_y", "string"));

    assertEquals(
        expectedGlueColumns,
        GlueSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema, existingTable));
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
                    getPrimitiveOneField(
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
            () -> GlueSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema));
    assertEquals("Unsupported type: InternalType.UNION(name=union)", exception.getMessage());

    // Invalid decimal type (precision and scale metadata is missing)
    InternalSchema oneSchema2 =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveOneField(
                        "optionalBoolean", "boolean", InternalType.BOOLEAN, true, 1),
                    getPrimitiveOneField(
                        "optionalDecimal", "decimal", InternalType.DECIMAL, true, 2)))
            .build();

    exception =
        assertThrows(
            NotSupportedException.class,
            () -> GlueSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema2));
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
                    getPrimitiveOneField(
                        "optionalBoolean", "boolean", InternalType.BOOLEAN, true, 1),
                    getPrimitiveOneField(
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
            () -> GlueSchemaExtractor.getInstance().toColumns(tableFormat, oneSchema3));
    assertEquals("Invalid decimal type, scale is missing", exception.getMessage());
  }
}
