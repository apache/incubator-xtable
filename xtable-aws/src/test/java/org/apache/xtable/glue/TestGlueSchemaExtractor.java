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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.xtable.catalog.TestSchemaExtractorBase;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.storage.TableFormat;

import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

public class TestGlueSchemaExtractor extends TestSchemaExtractorBase {

  private static final List<Column> testColumns =
      Arrays.asList(
          getColumn("id", "string"),
          getColumn("firstName", "string"),
          getColumn("gender", "string"),
          getColumn("birthTs", "timestamp"),
          getColumn("dateOfBirth", "int"));

  private static final Map<String, Column> testColumnMap =
      testColumns.stream().collect(Collectors.toMap(Column::name, c -> c));

  private static final InternalPartitionField simplePartitionField =
      InternalPartitionField.builder()
          .sourceField(
              InternalField.builder()
                  .name("gender")
                  .schema(
                      InternalSchema.builder().name("string").dataType(InternalType.STRING).build())
                  .build())
          .transformType(PartitionTransformType.VALUE)
          .build();

  private static final InternalPartitionField complexPartitionField =
      InternalPartitionField.builder()
          .sourceField(
              InternalField.builder()
                  .name("birthTimestamp")
                  .schema(
                      InternalSchema.builder()
                          .name("timestamp")
                          .dataType(InternalType.TIMESTAMP)
                          .build())
                  .build())
          .transformType(PartitionTransformType.DAY)
          .partitionFieldNames(Collections.singletonList("dateOfBirth"))
          .build();

  public static Column getColumn(String name, String type) {
    return Column.builder().name(name).type(type).build();
  }

  public static Column getColumn(String tableFormat, String name, String type) {
    return getCurrentGlueTableColumn(tableFormat, name, type, null, false);
  }

  private static Column getCurrentGlueTableColumn(
      String tableFormat, String colName, String colType, Integer fieldId, boolean isNullable) {
    fieldId = fieldId != null ? fieldId : -1;
    return getCurrentGlueTableColumn(tableFormat, colName, colType, null, fieldId, isNullable);
  }

  private static Column getCurrentGlueTableColumn(
      String tableFormat,
      String colName,
      String colType,
      String comment,
      Integer fieldId,
      boolean isNullable) {
    fieldId = fieldId != null ? fieldId : -1;
    return Column.builder()
        .name(colName)
        .type(colType)
        .comment(comment)
        .parameters(
            ImmutableMap.of(
                getColumnProperty(tableFormat, "field.id"), Integer.toString(fieldId),
                getColumnProperty(tableFormat, "field.optional"), Boolean.toString(isNullable),
                getColumnProperty(tableFormat, "field.current"), "true"))
        .build();
  }

  private static Column getPreviousGlueTableColumn(
      String tableFormat, String colName, String colType) {
    return Column.builder()
        .name(colName)
        .type(colType)
        .parameters(ImmutableMap.of(getColumnProperty(tableFormat, "field.current"), "false"))
        .build();
  }

  private static InternalTable getInternalTable(List<InternalPartitionField> partitionFields) {
    return InternalTable.builder().partitioningFields(partitionFields).build();
  }

  @Test
  void testPrimitiveTypes_NoExistingTable() {
    int precision = 10;
    int scale = 5;
    Map<InternalSchema.MetadataKey, Object> doubleMetadata = new HashMap<>();
    doubleMetadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, precision);
    doubleMetadata.put(InternalSchema.MetadataKey.DECIMAL_SCALE, scale);
    String tableFormat = TableFormat.ICEBERG;

    InternalSchema internalSchema =
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
        expectedGlueColumns,
        GlueSchemaExtractor.getInstance().toColumns(tableFormat, internalSchema));
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

    InternalSchema internalSchema =
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
        expectedGlueColumns,
        GlueSchemaExtractor.getInstance().toColumns(tableFormat, internalSchema));
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
        expectedGlueColumns,
        GlueSchemaExtractor.getInstance().toColumns(tableFormat, internalSchema));
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
        expectedGlueColumns,
        GlueSchemaExtractor.getInstance().toColumns(tableFormat, internalSchema));
  }

  @Test
  void testNestedRecords_NoExistingTable() {
    String tableFormat = TableFormat.ICEBERG;
    InternalSchema internalSchema =
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

    List<Column> expectedGlueColumns =
        Arrays.asList(
            getCurrentGlueTableColumn(
                tableFormat,
                "nestedOne",
                "struct<nestedOptionalInt:int,nestedRequiredDouble:double,nestedTwo:struct<doublyNestedString:string>>",
                1,
                true));
    assertEquals(
        expectedGlueColumns,
        GlueSchemaExtractor.getInstance().toColumns(tableFormat, internalSchema));
  }

  @Test
  void testToColumns_NoColumnsFromExistingTable() {
    String tableFormat = TableFormat.ICEBERG;
    InternalSchema internalSchema =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveInternalField(
                        "optionalBoolean", "boolean", InternalType.BOOLEAN, true, 2),
                    getPrimitiveInternalField(
                        "requiredInt", "integer", InternalType.INT, false, 3)))
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
          GlueSchemaExtractor.getInstance().toColumns(tableFormat, internalSchema, table));
    }
  }

  @Test
  void testToColumns_ValidExistingTable() {
    String tableFormat = TableFormat.ICEBERG;
    InternalSchema internalSchema =
        InternalSchema.builder()
            .dataType(InternalType.RECORD)
            .isNullable(false)
            .name("record")
            .fields(
                Arrays.asList(
                    getPrimitiveInternalField(
                        "optionalBoolean", "boolean", InternalType.BOOLEAN, true, 2),
                    getPrimitiveInternalField(
                        "requiredInt", "integer", InternalType.INT, false, 3)))
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
        GlueSchemaExtractor.getInstance().toColumns(tableFormat, internalSchema, existingTable));
  }

  @Test
  void testUnsupportedType() {
    String tableFormat = TableFormat.ICEBERG;
    // Unknown "UNION" type
    InternalSchema internalSchema =
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
            () -> GlueSchemaExtractor.getInstance().toColumns(tableFormat, internalSchema));
    assertEquals("Unsupported type: InternalType.UNION(name=union)", exception.getMessage());

    // Invalid decimal type (precision and scale metadata is missing)
    InternalSchema internalSchema2 =
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
            () -> GlueSchemaExtractor.getInstance().toColumns(tableFormat, internalSchema2));
    assertEquals("Invalid decimal type, precision and scale is missing", exception.getMessage());

    // Invalid decimal type (scale metadata is missing)
    Map<InternalSchema.MetadataKey, Object> doubleMetadata = new HashMap<>();
    doubleMetadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, 10);
    InternalSchema internalSchema3 =
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
            () -> GlueSchemaExtractor.getInstance().toColumns(tableFormat, internalSchema3));
    assertEquals("Invalid decimal type, scale is missing", exception.getMessage());
  }

  @Test
  void testColumnComment() {
    String tableFormat = TableFormat.ICEBERG;
    // comment exceeding 255 chars
    String comment = String.join("", java.util.Collections.nCopies(260, "a"));
    InternalField field =
        getPrimitiveInternalField(
            "booleanField",
            "boolean",
            InternalType.BOOLEAN,
            comment,
            false,
            1,
            null,
            Collections.emptyMap());
    comment = String.join("", java.util.Collections.nCopies(255, "a"));
    Column column =
        getCurrentGlueTableColumn(tableFormat, "booleanField", "boolean", comment, 1, false);
    assertEquals(column, GlueSchemaExtractor.getInstance().toColumn(field, tableFormat));

    // comment has 255 chars
    field =
        getPrimitiveInternalField(
            "booleanField",
            "boolean",
            InternalType.BOOLEAN,
            comment,
            false,
            1,
            null,
            Collections.emptyMap());
    column = getCurrentGlueTableColumn(tableFormat, "booleanField", "boolean", comment, 1, false);
    assertEquals(column, GlueSchemaExtractor.getInstance().toColumn(field, tableFormat));

    // comment has less than 255 chars
    comment = String.join("", java.util.Collections.nCopies(50, "a"));
    field =
        getPrimitiveInternalField(
            "booleanField",
            "boolean",
            InternalType.BOOLEAN,
            comment,
            false,
            1,
            null,
            Collections.emptyMap());
    column = getCurrentGlueTableColumn(tableFormat, "booleanField", "boolean", comment, 1, false);
    assertEquals(column, GlueSchemaExtractor.getInstance().toColumn(field, tableFormat));
  }

  static Stream<Arguments> getNonPartitionColumnsTestArgs() {
    return Stream.of(
        // table with no partition fields
        Arguments.of(getInternalTable(Collections.emptyList()), testColumns),
        // table with simple partition field
        Arguments.of(
            getInternalTable(Collections.singletonList(simplePartitionField)),
            Arrays.asList(
                testColumns.get(0), testColumns.get(1), testColumns.get(3), testColumns.get(4))),
        // table with complex partition field
        Arguments.of(
            getInternalTable(Collections.singletonList(complexPartitionField)),
            Arrays.asList(
                testColumns.get(0), testColumns.get(1), testColumns.get(2), testColumns.get(3))),
        // table with multiple partition field
        Arguments.of(
            getInternalTable(Arrays.asList(simplePartitionField, complexPartitionField)),
            Arrays.asList(testColumns.get(0), testColumns.get(1), testColumns.get(3))));
  }

  @ParameterizedTest
  @MethodSource("getNonPartitionColumnsTestArgs")
  void testGetNonPartitionColumns(InternalTable table, List<Column> expected) {
    List<Column> output =
        GlueSchemaExtractor.getInstance().getNonPartitionColumns(table, testColumnMap);
    assertEquals(new HashSet<>(expected), new HashSet<>(output));
  }

  static Stream<Arguments> getPartitionColumnsTestArgs() {
    return Stream.of(
        // table with no partition fields
        Arguments.of(getInternalTable(Collections.emptyList()), Collections.emptyList()),
        // table with simple partition field
        Arguments.of(
            getInternalTable(Collections.singletonList(simplePartitionField)),
            Collections.singletonList(testColumns.get(2))),
        // table with complex partition field
        Arguments.of(
            getInternalTable(Collections.singletonList(complexPartitionField)),
            Collections.singletonList(testColumns.get(4))),
        // table with multiple partition field
        Arguments.of(
            getInternalTable(Arrays.asList(simplePartitionField, complexPartitionField)),
            Arrays.asList(testColumns.get(2), testColumns.get(4))));
  }

  @ParameterizedTest
  @MethodSource("getPartitionColumnsTestArgs")
  void testGetPartitionColumns(InternalTable table, List<Column> expected) {
    List<Column> output =
        GlueSchemaExtractor.getInstance().getPartitionColumns(table, testColumnMap);
    assertEquals(new HashSet<>(expected), new HashSet<>(output));
  }
}
