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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.types.Types;

import org.apache.xtable.exception.NotSupportedException;

/** Syncs schema updates for Iceberg. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IcebergSchemaSync {
  private static final IcebergSchemaSync INSTANCE = new IcebergSchemaSync();

  public static IcebergSchemaSync getInstance() {
    return INSTANCE;
  }

  public void sync(Schema current, Schema latest, Transaction transaction) {
    if (!current.sameSchema(latest)) {
      // Schema change
      UpdateSchema updateSchema = transaction.updateSchema();
      Map<Integer, Supplier<UpdateSchema>> fieldIdToUpdate =
          addUpdates(current.asStruct(), latest.asStruct(), updateSchema, null);
      fieldIdToUpdate.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          // execute updates in order
          .forEach(entry -> entry.getValue().get());
      updateSchema.commit();
    }
  }

  /**
   * Return a mapping of fieldId in the latest schema to an update action to perform. This allows
   * updates to happen in the same order as the source system.
   *
   * @param current The current schema in the iceberg table
   * @param latest The latest schema provided by XTable (reflects source schema's state)
   * @param updateSchema An updateSchema object for the current transaction
   * @param parentPath dot separated path for nested field's parent
   * @return map of fieldId to action
   */
  private static Map<Integer, Supplier<UpdateSchema>> addUpdates(
      Types.StructType current,
      Types.StructType latest,
      UpdateSchema updateSchema,
      String parentPath) {
    Map<Integer, Supplier<UpdateSchema>> updates = new HashMap<>();
    Set<String> allColumnNames = new HashSet<>();
    current.fields().stream().map(Types.NestedField::name).forEach(allColumnNames::add);
    latest.fields().stream().map(Types.NestedField::name).forEach(allColumnNames::add);
    for (String columnName : allColumnNames) {
      Types.NestedField latestColumn = latest.field(columnName);
      Types.NestedField currentColumn = current.field(columnName);
      if (currentColumn == null) {
        // add a new column
        if (latestColumn.isOptional()) {
          updates.put(
              latestColumn.fieldId(),
              () ->
                  updateSchema.addColumn(
                      parentPath, latestColumn.name(), latestColumn.type(), latestColumn.doc()));
        } else {
          updates.put(
              latestColumn.fieldId(),
              () ->
                  updateSchema.addRequiredColumn(
                      parentPath, latestColumn.name(), latestColumn.type(), latestColumn.doc()));
        }
      } else if (latestColumn == null) {
        // drop the existing column, use fieldId 0 to perform deletes first
        updates.put(
            0,
            () -> updateSchema.deleteColumn(constructFullyQualifiedName(columnName, parentPath)));
      } else {
        updates.putAll(updateColumn(latestColumn, currentColumn, updateSchema, parentPath));
      }
    }
    return updates;
  }

  private static Map<Integer, Supplier<UpdateSchema>> updateColumn(
      Types.NestedField latestColumn,
      Types.NestedField currentColumn,
      UpdateSchema updateSchema,
      String parentPath) {
    Map<Integer, Supplier<UpdateSchema>> updates = new HashMap<>();
    if (!latestColumn.equals(currentColumn)) {
      // update the type of the column
      if (latestColumn.type().isPrimitiveType()
          && !latestColumn.type().equals(currentColumn.type())) {
        updates.put(
            latestColumn.fieldId(),
            () ->
                updateSchema.updateColumn(
                    latestColumn.name(), latestColumn.type().asPrimitiveType()));
      }
      // update whether the column is required
      if (latestColumn.isOptional() != currentColumn.isOptional()) {
        if (latestColumn.isOptional()) {
          updates.put(
              latestColumn.fieldId(), () -> updateSchema.makeColumnOptional(latestColumn.name()));
        } else {
          updates.put(
              latestColumn.fieldId(), () -> updateSchema.requireColumn(latestColumn.name()));
        }
      }
      if (latestColumn.type().isStructType()) {
        updates.putAll(
            addUpdates(
                currentColumn.type().asStructType(),
                latestColumn.type().asStructType(),
                updateSchema,
                constructFullyQualifiedName(latestColumn.name(), parentPath)));
      } else if (latestColumn.type().isListType()) {
        Types.NestedField latestListField = latestColumn.type().asListType().fields().get(0);
        Types.NestedField currentListField = currentColumn.type().asListType().fields().get(0);
        updates.putAll(
            updateColumn(
                latestListField,
                currentListField,
                updateSchema,
                constructFullyQualifiedName(latestColumn.name(), parentPath)));
      } else if (latestColumn.type().isMapType()) {
        Types.NestedField latestKeyField = latestColumn.type().asMapType().fields().get(0);
        Types.NestedField currentKeyField = currentColumn.type().asMapType().fields().get(0);
        if (latestKeyField.type().isStructType()
            && latestKeyField.type().asStructType().fields().size()
                != currentKeyField.type().asStructType().fields().size()) {
          // map keys do not support adding or dropping struct fields
          throw new NotSupportedException(
              "Cannot add or remove fields in map key struct for Iceberg tables");
        }
        updates.putAll(
            updateColumn(
                latestKeyField,
                currentKeyField,
                updateSchema,
                constructFullyQualifiedName(latestColumn.name(), parentPath)));
        Types.NestedField latestValueField = latestColumn.type().asMapType().fields().get(1);
        Types.NestedField currentValueField = currentColumn.type().asMapType().fields().get(1);
        updates.putAll(
            updateColumn(
                latestValueField,
                currentValueField,
                updateSchema,
                constructFullyQualifiedName(latestColumn.name(), parentPath)));
      }
    }
    return updates;
  }

  private static String constructFullyQualifiedName(String columnName, String parentPath) {
    return parentPath == null ? columnName : parentPath + "." + columnName;
  }
}
