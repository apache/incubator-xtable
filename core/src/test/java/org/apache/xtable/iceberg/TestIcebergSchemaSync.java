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

import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import org.apache.xtable.exception.NotSupportedException;

public class TestIcebergSchemaSync {

  private static final Schema SCHEMA =
      new Schema(
          Types.NestedField.required(1, "timestamp_field", Types.TimestampType.withoutZone()),
          Types.NestedField.optional(2, "date_field", Types.DateType.get()),
          Types.NestedField.required(3, "group_id", Types.IntegerType.get()),
          Types.NestedField.required(
              4,
              "record",
              Types.StructType.of(
                  Types.NestedField.required(5, "string_field", Types.StringType.get()),
                  Types.NestedField.required(6, "int_field", Types.IntegerType.get()))),
          Types.NestedField.required(
              7,
              "map_field",
              Types.MapType.ofRequired(
                  8,
                  9,
                  Types.StructType.of(
                      Types.NestedField.required(12, "key_string", Types.StringType.get())),
                  Types.StructType.of(
                      Types.NestedField.required(13, "value_string", Types.StringType.get()),
                      Types.NestedField.optional(14, "value_int", Types.IntegerType.get())))),
          Types.NestedField.required(
              10,
              "array_field",
              Types.ListType.ofRequired(
                  11,
                  Types.StructType.of(
                      Types.NestedField.required(15, "element_string", Types.StringType.get()),
                      Types.NestedField.optional(16, "element_int", Types.IntegerType.get())))));

  private final IcebergSchemaSync schemaSync = IcebergSchemaSync.getInstance();
  private final Transaction mockTransaction = Mockito.mock(Transaction.class);

  @Test
  public void testSameSchema() {
    schemaSync.sync(SCHEMA, SCHEMA, mockTransaction);

    Mockito.verify(mockTransaction, Mockito.never()).updateSchema();
  }

  @Test
  public void testAddRequiredColumn() {
    String columnName = "addedColumn";
    String doc = "doc";
    Type type = Types.BooleanType.get();
    UpdateSchema mockUpdateSchema = Mockito.mock(UpdateSchema.class);
    when(mockTransaction.updateSchema()).thenReturn(mockUpdateSchema);
    Types.NestedField columnToAdd = Types.NestedField.required(20, columnName, type, doc);
    schemaSync.sync(SCHEMA, addColumnToDefault(SCHEMA, columnToAdd, null), mockTransaction);

    verify(mockUpdateSchema).addRequiredColumn(null, columnName, type, doc);
    verify(mockUpdateSchema).commit();
  }

  @Test
  public void testAddOptionalColumn() {
    String columnName = "addedColumn";
    String doc = "doc";
    Type type = Types.BooleanType.get();
    UpdateSchema mockUpdateSchema = Mockito.mock(UpdateSchema.class);
    when(mockTransaction.updateSchema()).thenReturn(mockUpdateSchema);
    Types.NestedField columnToAdd = Types.NestedField.optional(20, columnName, type, doc);
    schemaSync.sync(SCHEMA, addColumnToDefault(SCHEMA, columnToAdd, null), mockTransaction);

    verify(mockUpdateSchema).addColumn(null, columnName, type, doc);
    verify(mockUpdateSchema).commit();
  }

  @Test
  public void testFieldOrderingCarriesThroughUpdate() {
    String columnName = "addedColumn";
    String nestedColumnName = "addedNestedColumn";
    String doc = "doc";
    Type type = Types.BooleanType.get();
    UpdateSchema mockUpdateSchema = Mockito.mock(UpdateSchema.class);
    when(mockTransaction.updateSchema()).thenReturn(mockUpdateSchema);
    Types.NestedField columnToAdd = Types.NestedField.optional(20, columnName, type, doc);
    Types.NestedField nestedColumnToAdd =
        Types.NestedField.required(21, nestedColumnName, type, doc);
    Schema schemaWithColumn = addColumnToDefault(SCHEMA, columnToAdd, null);
    Schema schemaWithNestedColumn = addColumnToDefault(schemaWithColumn, nestedColumnToAdd, 4);
    schemaSync.sync(SCHEMA, schemaWithNestedColumn, mockTransaction);

    InOrder inOrder = Mockito.inOrder(mockUpdateSchema);
    inOrder.verify(mockUpdateSchema).addColumn(null, columnName, type, doc);
    inOrder.verify(mockUpdateSchema).addRequiredColumn("record", nestedColumnName, type, doc);
    inOrder.verify(mockUpdateSchema).commit();
  }

  @Test
  public void testDropColumn() {
    UpdateSchema mockUpdateSchema = Mockito.mock(UpdateSchema.class);
    when(mockTransaction.updateSchema()).thenReturn(mockUpdateSchema);

    schemaSync.sync(SCHEMA, dropColumnInDefault(1, null), mockTransaction);

    verify(mockUpdateSchema).deleteColumn("timestamp_field");
    verify(mockUpdateSchema).commit();
  }

  @Test
  public void testAddNestedColumn() {
    String columnName = "addedColumn";
    String doc = "doc";
    Type type = Types.BooleanType.get();
    UpdateSchema mockUpdateSchema = Mockito.mock(UpdateSchema.class);
    when(mockTransaction.updateSchema()).thenReturn(mockUpdateSchema);
    Types.NestedField columnToAdd = Types.NestedField.required(20, columnName, type, doc);
    schemaSync.sync(SCHEMA, addColumnToDefault(SCHEMA, columnToAdd, 4), mockTransaction);

    verify(mockUpdateSchema).addRequiredColumn("record", columnName, type, doc);
    verify(mockUpdateSchema).commit();
  }

  @Test
  public void testDropNestedColumn() {
    UpdateSchema mockUpdateSchema = Mockito.mock(UpdateSchema.class);
    when(mockTransaction.updateSchema()).thenReturn(mockUpdateSchema);

    schemaSync.sync(SCHEMA, dropColumnInDefault(5, 4), mockTransaction);

    verify(mockUpdateSchema).deleteColumn("record.string_field");
    verify(mockUpdateSchema).commit();
  }

  @Test
  public void testUpdateListElement() {
    UpdateSchema mockUpdateSchema = Mockito.mock(UpdateSchema.class);
    when(mockTransaction.updateSchema()).thenReturn(mockUpdateSchema);

    schemaSync.sync(SCHEMA, addAndDropFieldsInListElement(), mockTransaction);

    verify(mockUpdateSchema).deleteColumn("array_field.element.element_string");
    verify(mockUpdateSchema)
        .addColumn("array_field.element", "element_double", Types.DoubleType.get(), null);
    verify(mockUpdateSchema).commit();
  }

  @Test
  public void testUpdateMapValue() {
    UpdateSchema mockUpdateSchema = Mockito.mock(UpdateSchema.class);
    when(mockTransaction.updateSchema()).thenReturn(mockUpdateSchema);

    schemaSync.sync(SCHEMA, addAndDropFieldsInMapValue(), mockTransaction);

    verify(mockUpdateSchema).deleteColumn("map_field.value.value_string");
    verify(mockUpdateSchema)
        .addRequiredColumn("map_field.value", "value_double", Types.DoubleType.get(), null);
    verify(mockUpdateSchema).commit();
  }

  @Test
  public void testUpdateMapKey() {
    // iceberg does not allow the key to be updated
    UpdateSchema mockUpdateSchema = Mockito.mock(UpdateSchema.class);
    when(mockTransaction.updateSchema()).thenReturn(mockUpdateSchema);

    Assertions.assertThrows(
        NotSupportedException.class,
        () -> schemaSync.sync(SCHEMA, addFieldsInMapKey(), mockTransaction));

    verify(mockUpdateSchema, never()).commit();
  }

  @Test
  public void testMakeExistingColumnOptional() {
    UpdateSchema mockUpdateSchema = Mockito.mock(UpdateSchema.class);
    when(mockTransaction.updateSchema()).thenReturn(mockUpdateSchema);

    schemaSync.sync(SCHEMA, updateFieldRequired(1), mockTransaction);

    verify(mockUpdateSchema).makeColumnOptional("timestamp_field");
    verify(mockUpdateSchema).commit();
  }

  @Test
  public void testMakeExistingColumnRequired() {
    UpdateSchema mockUpdateSchema = Mockito.mock(UpdateSchema.class);
    when(mockTransaction.updateSchema()).thenReturn(mockUpdateSchema);

    schemaSync.sync(SCHEMA, updateFieldRequired(2), mockTransaction);

    verify(mockUpdateSchema).requireColumn("date_field");
    verify(mockUpdateSchema).commit();
  }

  private Schema addColumnToDefault(Schema schema, Types.NestedField field, Integer parentId) {
    List<Types.NestedField> fields = new ArrayList<>();
    for (Types.NestedField existingField : schema.columns()) {
      if (parentId != null && existingField.fieldId() == parentId) {
        List<Types.NestedField> nestedFields =
            new ArrayList<>(existingField.type().asStructType().fields());
        nestedFields.add(field);
        fields.add(
            Types.NestedField.of(
                existingField.fieldId(),
                existingField.isOptional(),
                existingField.name(),
                Types.StructType.of(nestedFields)));
      } else {
        fields.add(existingField);
      }
    }
    if (parentId == null) {
      fields.add(field);
    }
    return new Schema(fields);
  }

  private Schema updateFieldRequired(int fieldId) {
    List<Types.NestedField> fields = new ArrayList<>();
    for (Types.NestedField existingField : SCHEMA.columns()) {
      if (existingField.fieldId() == fieldId) {
        fields.add(
            Types.NestedField.of(
                existingField.fieldId(),
                !existingField.isOptional(),
                existingField.name(),
                existingField.type()));
      } else {
        fields.add(existingField);
      }
    }
    return new Schema(fields);
  }

  private Schema dropColumnInDefault(Integer fieldId, Integer parentId) {
    List<Types.NestedField> fields = new ArrayList<>();
    for (Types.NestedField existingField : SCHEMA.columns()) {
      if (parentId != null && existingField.fieldId() == parentId) {
        List<Types.NestedField> nestedFields =
            existingField.type().asStructType().fields().stream()
                .filter(field -> field.fieldId() != fieldId)
                .collect(Collectors.toList());
        fields.add(
            Types.NestedField.of(
                existingField.fieldId(),
                existingField.isOptional(),
                existingField.name(),
                Types.StructType.of(nestedFields)));
      } else if (existingField.fieldId() != fieldId) {
        fields.add(existingField);
      }
    }
    return new Schema(fields);
  }

  private Schema addAndDropFieldsInListElement() {
    Types.NestedField updatedArrayField =
        Types.NestedField.required(
            10,
            "array_field",
            Types.ListType.ofRequired(
                11,
                Types.StructType.of(
                    Types.NestedField.optional(17, "element_double", Types.DoubleType.get()),
                    Types.NestedField.optional(16, "element_int", Types.IntegerType.get()))));
    List<Types.NestedField> fields = new ArrayList<>();
    for (Types.NestedField existingField : SCHEMA.columns()) {
      if (existingField.fieldId() == 10) {
        fields.add(updatedArrayField);
      } else {
        fields.add(existingField);
      }
    }
    return new Schema(fields);
  }

  private Schema addAndDropFieldsInMapValue() {
    Types.NestedField updatedMapField =
        Types.NestedField.required(
            7,
            "map_field",
            Types.MapType.ofRequired(
                8,
                9,
                Types.StructType.of(
                    Types.NestedField.required(12, "key_string", Types.StringType.get())),
                Types.StructType.of(
                    Types.NestedField.required(20, "value_double", Types.DoubleType.get()),
                    Types.NestedField.optional(14, "value_int", Types.IntegerType.get()))));
    List<Types.NestedField> fields = new ArrayList<>();
    for (Types.NestedField existingField : SCHEMA.columns()) {
      if (existingField.fieldId() == 7) {
        fields.add(updatedMapField);
      } else {
        fields.add(existingField);
      }
    }
    return new Schema(fields);
  }

  private Schema addFieldsInMapKey() {
    Types.NestedField updatedMapField =
        Types.NestedField.required(
            7,
            "map_field",
            Types.MapType.ofRequired(
                8,
                9,
                Types.StructType.of(
                    Types.NestedField.required(12, "key_string", Types.StringType.get()),
                    Types.NestedField.required(20, "key_int", Types.IntegerType.get())),
                Types.StructType.of(
                    Types.NestedField.required(13, "value_string", Types.StringType.get()),
                    Types.NestedField.optional(14, "value_int", Types.IntegerType.get()))));
    List<Types.NestedField> fields = new ArrayList<>();
    for (Types.NestedField existingField : SCHEMA.columns()) {
      if (existingField.fieldId() == 7) {
        fields.add(updatedMapField);
      } else {
        fields.add(existingField);
      }
    }
    return new Schema(fields);
  }
}
