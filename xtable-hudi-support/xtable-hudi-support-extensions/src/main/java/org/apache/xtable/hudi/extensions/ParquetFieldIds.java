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
 
package org.apache.xtable.hudi.extensions;

import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;

/**
 * Sets Iceberg field IDs on a Parquet schema from a name mapping, with the same path rules as
 * Iceberg's {@code ParquetSchemaUtil.applyNameMapping}: a list element is matched as {@code
 * element}, a map's entries as {@code key} and {@code value}, and legacy two-level lists follow the
 * Parquet backward-compatibility rules. Unlike Iceberg's version it keeps the schema's structure
 * and only adds IDs, and it uses the unshaded Parquet classes the writer already has, so it works
 * next to an Iceberg runtime jar that relocates Parquet.
 */
final class ParquetFieldIds {
  private static final String LIST_ELEMENT_NAME = "element";
  private static final String MAP_KEY_NAME = "key";
  private static final String MAP_VALUE_NAME = "value";

  private ParquetFieldIds() {}

  static MessageType apply(MessageType message, NameMapping mapping) {
    List<Type> fields = new ArrayList<>(message.getFieldCount());
    for (Type field : message.getFields()) {
      fields.add(apply(field, mapping, path(new ArrayList<>(), field.getName())));
    }
    return new MessageType(message.getName(), fields);
  }

  private static Type apply(Type type, NameMapping mapping, List<String> path) {
    Type result = type;
    if (!type.isPrimitive()) {
      GroupType group = type.asGroupType();
      LogicalTypeAnnotation annotation = group.getLogicalTypeAnnotation();
      if (LogicalTypeAnnotation.listType().equals(annotation)) {
        result = group.withNewFields(applyToList(group, mapping, path));
      } else if (LogicalTypeAnnotation.mapType().equals(annotation)) {
        result = group.withNewFields(applyToMap(group.getType(0).asGroupType(), mapping, path));
      } else {
        List<Type> fields = new ArrayList<>(group.getFieldCount());
        for (Type field : group.getFields()) {
          fields.add(apply(field, mapping, path(path, field.getName())));
        }
        result = group.withNewFields(fields);
      }
    }
    MappedField field = mapping.find(path);
    return field == null ? result : result.withId(field.id());
  }

  private static Type applyToList(GroupType list, NameMapping mapping, List<String> path) {
    Type repeated = list.getType(0);
    List<String> elementPath = path(path, LIST_ELEMENT_NAME);
    if (isOldListElementType(list)) {
      // the repeated field is the element itself
      return apply(repeated, mapping, elementPath);
    }
    GroupType repeatedGroup = repeated.asGroupType();
    return repeatedGroup.withNewFields(apply(repeatedGroup.getType(0), mapping, elementPath));
  }

  private static Type applyToMap(GroupType keyValue, NameMapping mapping, List<String> path) {
    List<Type> fields = new ArrayList<>(keyValue.getFieldCount());
    for (Type field : keyValue.getFields()) {
      boolean isKey =
          keyValue.getFieldCount() == 2
              ? field == keyValue.getType(0)
              : field.getName().equalsIgnoreCase(MAP_KEY_NAME);
      fields.add(apply(field, mapping, path(path, isKey ? MAP_KEY_NAME : MAP_VALUE_NAME)));
    }
    return keyValue.withNewFields(fields);
  }

  // Parquet LIST backward-compatibility rules, as in Iceberg's ParquetSchemaUtil.
  private static boolean isOldListElementType(GroupType list) {
    Type repeated = list.getType(0);
    return repeated.isPrimitive()
        || repeated.asGroupType().getFieldCount() > 1
        || repeated.getName().equals("array")
        || repeated.getName().equals(list.getName() + "_tuple");
  }

  private static List<String> path(List<String> parent, String name) {
    List<String> path = new ArrayList<>(parent.size() + 1);
    path.addAll(parent);
    path.add(name);
    return path;
  }
}
