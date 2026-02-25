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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;

/**
 * A Parquet schema visitor that applies a NameMapping to add field IDs to a Parquet schema.
 *
 * <h2>Why this class exists</h2>
 *
 * <p>This is a local implementation adapted from Iceberg's {@code
 * org.apache.iceberg.parquet.ApplyNameMapping} to work around a dependency conflict introduced in
 * Iceberg 1.10+.
 *
 * <p>The issue: Iceberg 1.10 added Parquet variant type support, which requires parquet 1.16.0's
 * {@code LogicalTypeAnnotation.variantType()} method. However, other dependencies in the XTable
 * ecosystem (Hudi 0.14.0, Paimon 1.3.1) are not yet compatible with parquet 1.16:
 *
 * <ul>
 *   <li>Hudi 0.14.0 - compiled against parquet 1.10.1, fails with {@code
 *       GeospatialStatistics.writePage()} errors on parquet 1.16
 *   <li>Paimon 1.3.1 - bundles shaded parquet with older fastutil, causing {@code LongList.of()}
 *       conflicts
 * </ul>
 *
 * <p>The original {@code iceberg-parquet} module's {@code ParquetSchemaUtil.applyNameMapping()}
 * internally uses {@code ParquetTypeVisitor.visit()}, which unconditionally checks for variant
 * types even when not processing variants - triggering the parquet 1.16 requirement.
 *
 * <p>This implementation avoids extending Iceberg's {@code ParquetTypeVisitor} and instead
 * implements the traversal logic directly, skipping variant type handling entirely since it's not
 * needed for the field ID mapping use case.
 *
 * <h2>Deprecation Plan</h2>
 *
 * <p>This class should be removed in favor of using {@code
 * org.apache.iceberg.parquet.ParquetSchemaUtil.applyNameMapping()} directly once the following
 * conditions are met:
 *
 * <ol>
 *   <li>Hudi upgrades to a version compatible with parquet 1.16+ (likely Hudi 0.16+)
 *   <li>Paimon upgrades to a version compatible with parquet 1.16+
 *   <li>XTable can safely set {@code parquet.version} to 1.16.0 or later globally
 * </ol>
 *
 * <p>To migrate back to iceberg-parquet:
 *
 * <ol>
 *   <li>Add {@code iceberg-parquet} dependency to xtable-hudi-support-extensions/pom.xml
 *   <li>Replace usage of {@code ApplyNameMappingForParquet.applyNameMapping()} with {@code
 *       ParquetSchemaUtil.applyNameMapping()}
 *   <li>Delete this class
 *   <li>Update parquet.version to 1.16.0+ in the root pom.xml
 * </ol>
 *
 * @see <a href="https://github.com/apache/iceberg/pull/14588">Iceberg PR #14588 - Add variant type
 *     support to ParquetTypeVisitor</a>
 */
class ApplyNameMappingForParquet {

  private static final String LIST_ELEMENT_NAME = "element";
  private static final String MAP_KEY_NAME = "key";
  private static final String MAP_VALUE_NAME = "value";

  private ApplyNameMappingForParquet() {}

  /**
   * Applies a NameMapping to a Parquet MessageType, adding field IDs based on the mapping.
   *
   * @param fileSchema the Parquet schema to apply the mapping to
   * @param nameMapping the NameMapping containing field ID assignments
   * @return a new MessageType with field IDs applied
   */
  public static MessageType applyNameMapping(MessageType fileSchema, NameMapping nameMapping) {
    Visitor visitor = new Visitor(nameMapping);
    return (MessageType) visit(fileSchema, visitor);
  }

  private static Type visit(Type type, Visitor visitor) {
    if (type instanceof MessageType) {
      MessageType message = (MessageType) type;
      List<Type> fields = new ArrayList<>();
      for (Type field : message.getFields()) {
        visitor.beforeField(field);
        try {
          fields.add(visit(field, visitor));
        } finally {
          visitor.afterField(field);
        }
      }
      return visitor.message(message, fields);
    } else if (type.isPrimitive()) {
      return visitor.primitive(type.asPrimitiveType());
    } else {
      GroupType group = type.asGroupType();
      LogicalTypeAnnotation annotation = group.getLogicalTypeAnnotation();

      if (LogicalTypeAnnotation.listType().equals(annotation)) {
        return visitList(group, visitor);
      } else if (LogicalTypeAnnotation.mapType().equals(annotation)) {
        return visitMap(group, visitor);
      }

      // Regular struct
      List<Type> fields = new ArrayList<>();
      for (Type field : group.getFields()) {
        visitor.beforeField(field);
        try {
          fields.add(visit(field, visitor));
        } finally {
          visitor.afterField(field);
        }
      }
      return visitor.struct(group, fields);
    }
  }

  private static Type visitList(GroupType list, Visitor visitor) {
    if (list.getFieldCount() != 1) {
      throw new IllegalArgumentException("Invalid list: " + list);
    }

    Type repeatedElement = list.getType(0);
    Type elementResult;

    if (isElementType(list, repeatedElement)) {
      visitor.beforeElementField(repeatedElement);
      try {
        elementResult = visit(repeatedElement, visitor);
      } finally {
        visitor.afterField(repeatedElement);
      }
    } else {
      GroupType repeated = repeatedElement.asGroupType();
      Type element = repeated.getType(0);
      visitor.beforeElementField(element);
      try {
        elementResult = visit(element, visitor);
      } finally {
        visitor.afterField(element);
      }
    }

    return visitor.list(list, elementResult);
  }

  private static boolean isElementType(GroupType list, Type repeatedElement) {
    // Check for 2-level list encoding
    return repeatedElement.isPrimitive()
        || repeatedElement.asGroupType().getFieldCount() > 1
        || repeatedElement.getName().equals("array")
        || repeatedElement.getName().equals(list.getName() + "_tuple");
  }

  private static Type visitMap(GroupType map, Visitor visitor) {
    if (map.getFieldCount() != 1) {
      throw new IllegalArgumentException("Invalid map: " + map);
    }

    GroupType keyValue = map.getType(0).asGroupType();
    if (keyValue.getFieldCount() != 2) {
      throw new IllegalArgumentException("Invalid map key-value: " + keyValue);
    }

    Type key = keyValue.getType(0);
    Type value = keyValue.getType(1);

    visitor.beforeKeyField(key);
    Type keyResult;
    try {
      keyResult = visit(key, visitor);
    } finally {
      visitor.afterField(key);
    }

    visitor.beforeValueField(value);
    Type valueResult;
    try {
      valueResult = visit(value, visitor);
    } finally {
      visitor.afterField(value);
    }

    return visitor.map(map, keyResult, valueResult);
  }

  private static class Visitor {
    private final NameMapping nameMapping;
    private final Deque<String> fieldNames = new ArrayDeque<>();

    Visitor(NameMapping nameMapping) {
      this.nameMapping = nameMapping;
    }

    Type message(MessageType message, List<Type> fields) {
      Types.MessageTypeBuilder builder = Types.buildMessage();
      fields.stream().filter(Objects::nonNull).forEach(builder::addField);
      return builder.named(message.getName());
    }

    Type struct(GroupType struct, List<Type> types) {
      MappedField field = nameMapping.find(currentPath());
      List<Type> actualTypes = types.stream().filter(Objects::nonNull).collect(Collectors.toList());
      Type structType = struct.withNewFields(actualTypes);
      return field == null ? structType : structType.withId(field.id());
    }

    Type list(GroupType list, Type elementType) {
      if (elementType == null) {
        throw new IllegalArgumentException("List type must have element field");
      }

      Type listElement = determineListElementType(list);
      MappedField field = nameMapping.find(currentPath());

      Types.GroupBuilder<GroupType> listBuilder =
          Types.buildGroup(list.getRepetition()).as(LogicalTypeAnnotation.listType());
      if (listElement.isRepetition(Type.Repetition.REPEATED)) {
        listBuilder.addFields(elementType);
      } else {
        listBuilder.repeatedGroup().addFields(elementType).named(list.getFieldName(0));
      }
      Type listType = listBuilder.named(list.getName());

      return field == null ? listType : listType.withId(field.id());
    }

    Type map(GroupType map, Type keyType, Type valueType) {
      if (keyType == null || valueType == null) {
        throw new IllegalArgumentException("Map type must have both key field and value field");
      }

      MappedField field = nameMapping.find(currentPath());
      Type mapType =
          Types.buildGroup(map.getRepetition())
              .as(LogicalTypeAnnotation.mapType())
              .repeatedGroup()
              .addFields(keyType, valueType)
              .named(map.getFieldName(0))
              .named(map.getName());

      return field == null ? mapType : mapType.withId(field.id());
    }

    Type primitive(PrimitiveType primitive) {
      MappedField field = nameMapping.find(currentPath());
      return field == null ? primitive : primitive.withId(field.id());
    }

    void beforeField(Type type) {
      fieldNames.push(type.getName());
    }

    void afterField(Type type) {
      fieldNames.pop();
    }

    void beforeElementField(Type element) {
      fieldNames.push(LIST_ELEMENT_NAME);
    }

    void beforeKeyField(Type key) {
      fieldNames.push(MAP_KEY_NAME);
    }

    void beforeValueField(Type value) {
      fieldNames.push(MAP_VALUE_NAME);
    }

    private String[] currentPath() {
      List<String> path = new ArrayList<>(fieldNames);
      java.util.Collections.reverse(path);
      return path.toArray(new String[0]);
    }

    private static Type determineListElementType(GroupType list) {
      Type repeated = list.getType(0);
      if (isElementType(list, repeated)) {
        return repeated;
      }
      return repeated.asGroupType().getType(0);
    }
  }
}
