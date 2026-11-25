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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import org.apache.hudi.avro.HoodieAvroWriteSupport;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.xtable.hudi.idtracking.IdTracker;
import org.apache.xtable.hudi.idtracking.models.IdMapping;
import org.apache.xtable.hudi.idtracking.models.IdTracking;

/**
 * An extension of the standard {@link HoodieAvroWriteSupport} that adds field IDs to the parquet
 * schema. When used with {@link AddFieldIdsClientInitCallback}, ID values will be set on the fields
 * in the parquet file making them compatible with Iceberg readers that do not support the default
 * field id mapping.
 */
public class HoodieAvroWriteSupportWithFieldIds extends HoodieAvroWriteSupport {
  private static final IdTracker ID_TRACKER = IdTracker.getInstance();
  private static final String ELEMENT = "element";
  private static final String KEY = "key";
  private static final String VALUE = "value";

  // Hudi 1.2.0 reflectively instantiates the write support with the schema as a HoodieSchema, so
  // this constructor must mirror that signature (see HoodieAvroWriteSupport).
  public HoodieAvroWriteSupportWithFieldIds(
      MessageType schema,
      HoodieSchema avroSchema,
      Option<BloomFilter> bloomFilterOpt,
      Properties properties) {
    super(
        addFieldIdsToParquetSchema(schema, avroSchema.toAvroSchema(), properties),
        avroSchema,
        bloomFilterOpt,
        properties);
  }

  private static MessageType addFieldIdsToParquetSchema(
      MessageType messageType, Schema schema, Properties properties) {
    Option<IdTracking> idTrackingOption = ID_TRACKER.getIdTracking(schema);
    if (!idTrackingOption.isPresent()) {
      HoodieWriteConfig writeConfig =
          HoodieWriteConfig.newBuilder().withProperties(properties).build();
      String writeSchemaStr = writeConfig.getWriteSchema();
      // if there is a schema with ID tracking specified in the properties, fall back to inferring
      // the proper ID tracking on provided schema
      if (writeSchemaStr != null && !writeSchemaStr.isEmpty()) {
        Schema writeSchema = new Schema.Parser().parse(writeSchemaStr);
        if (ID_TRACKER.hasIdTracking(writeSchema)) {
          idTrackingOption =
              Option.of(
                  ID_TRACKER.getIdTracking(
                      schema, Option.of(writeSchema), writeConfig.populateMetaFields()));
        }
      }
    }
    return idTrackingOption
        .map(idTracking -> applyFieldIds(messageType, idTracking.getIdMappings()))
        .orElse(messageType);
  }

  /**
   * Sets the field id of every field of the schema that the mappings name, matching fields by name
   * at each level, the element of a list as {@code element} and the entries of a map as {@code key}
   * and {@code value}, as Iceberg's name mapping does. A field without a mapping keeps no id. This
   * uses Parquet's own schema classes rather than Iceberg's, whose Parquet support needs a newer
   * Parquet than the engines that load this write support provide.
   */
  static MessageType applyFieldIds(MessageType schema, List<IdMapping> mappings) {
    return new MessageType(schema.getName(), applyFieldIds(schema.getFields(), mappings));
  }

  private static List<Type> applyFieldIds(List<Type> fields, List<IdMapping> mappings) {
    Map<String, IdMapping> byName = byName(mappings);
    return fields.stream()
        .map(field -> applyFieldId(field, byName.get(field.getName())))
        .collect(Collectors.toList());
  }

  private static Type applyFieldId(Type type, IdMapping mapping) {
    if (mapping == null) {
      return type;
    }
    return applyNestedFieldIds(type, mapping.getFields()).withId(mapping.getId());
  }

  private static Type applyNestedFieldIds(Type type, List<IdMapping> nested) {
    if (type.isPrimitive()) {
      return type;
    }
    GroupType group = type.asGroupType();
    LogicalTypeAnnotation annotation = group.getLogicalTypeAnnotation();
    Map<String, IdMapping> byName = byName(nested);
    if (annotation instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation
        && group.getFieldCount() == 1) {
      Type repeated = group.getType(0);
      IdMapping element = byName.get(ELEMENT);
      if (isElementType(repeated, group.getName())) {
        return group.withNewFields(applyFieldId(repeated, element));
      }
      GroupType repeatedGroup = repeated.asGroupType();
      return group.withNewFields(
          repeatedGroup.withNewFields(applyFieldId(repeatedGroup.getType(0), element)));
    }
    if ((annotation instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation
            || annotation instanceof LogicalTypeAnnotation.MapKeyValueTypeAnnotation)
        && group.getFieldCount() == 1
        && !group.getType(0).isPrimitive()) {
      GroupType keyValue = group.getType(0).asGroupType();
      List<Type> entries =
          keyValue.getFields().stream()
              .map(
                  entry ->
                      applyFieldId(entry, byName.get(entry == keyValue.getType(0) ? KEY : VALUE)))
              .collect(Collectors.toList());
      return group.withNewFields(keyValue.withNewFields(entries));
    }
    return group.withNewFields(applyFieldIds(group.getFields(), nested));
  }

  /**
   * Whether the repeated field of a list is the element itself, as in Parquet's two-level list
   * layout, rather than a group holding the element.
   */
  private static boolean isElementType(Type repeated, String listName) {
    return repeated.isPrimitive()
        || repeated.asGroupType().getFieldCount() > 1
        || repeated.getName().equals("array")
        || repeated.getName().equals(listName + "_tuple");
  }

  private static Map<String, IdMapping> byName(List<IdMapping> mappings) {
    if (mappings == null) {
      return Collections.emptyMap();
    }
    Map<String, IdMapping> byName = new HashMap<>();
    for (IdMapping mapping : mappings) {
      byName.put(mapping.getName(), mapping);
    }
    return byName;
  }
}
