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
 
package org.apache.xtable.hudi.idtracking;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.avro.Schema;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;

import org.apache.xtable.hudi.idtracking.models.IdMapping;
import org.apache.xtable.hudi.idtracking.models.IdTracking;

/** Utility class for reading and writing the IdTracking information from a schema. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IdTracker {
  private static final String ARRAY_FIELD = "element";
  private static final String KEY_FIELD = "key";
  private static final String VALUE_FIELD = "value";
  private static final String ID_TRACKING = "hudi_id_tracking";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final ObjectReader ID_TRACKING_READER = OBJECT_MAPPER.readerFor(IdTracking.class);

  private static final IdTracker INSTANCE = new IdTracker();

  public static IdTracker getInstance() {
    return INSTANCE;
  }

  /**
   * Reads the IdTracking information from the provided schema if it is present.
   *
   * @param schema to get IdTracking information from
   * @return Option containing the IdTracking if it is present in the schema properties, otherwise
   *     returns an empty Option.
   */
  public Option<IdTracking> getIdTracking(Schema schema) {
    try {
      Object propValue = schema.getObjectProp(ID_TRACKING);
      if (propValue == null) {
        return Option.empty();
      }
      return Option.of(
          ID_TRACKING_READER.readValue((JsonNode) OBJECT_MAPPER.valueToTree(propValue)));
    } catch (IOException ex) {
      throw new UncheckedIOException(ex);
    }
  }

  /**
   * Determines whether the ID tracking metadata is set.
   *
   * @param schema schema to inspect
   * @return true if ID tracking is set, false otherwise
   */
  public boolean hasIdTracking(Schema schema) {
    return schema.getObjectProp(ID_TRACKING) != null;
  }

  /**
   * Adds IdTracking information to the properties of a schema and returns a schema with the
   * property set.
   *
   * @param schema The schema to update the IdTracking property
   * @param previousSchema Schema previously used, if any, to ensure that IDs are consistent between
   *     commits.
   * @param includeMetaFields Whether hoodie meta fields will be included in the table
   * @return a new or updated schema
   */
  public Schema addIdTracking(
      Schema schema, Option<Schema> previousSchema, boolean includeMetaFields) {
    IdTracking newIdTracking = getIdTracking(schema, previousSchema, includeMetaFields);
    return addIdMappings(schema, newIdTracking);
  }

  /**
   * Returns IdTracking information for the provided schema
   *
   * @param schema The schema to update the IdTracking property
   * @param previousSchema Schema previously used, if any, to ensure that IDs are consistent between
   *     commits.
   * @param includeMetaFields Whether hoodie meta fields will be included in the table
   * @return the IdTracking information
   */
  public IdTracking getIdTracking(
      Schema schema, Option<Schema> previousSchema, boolean includeMetaFields) {
    IdTracking existingState = previousSchema.flatMap(this::getIdTracking).orElse(IdTracking.EMPTY);
    AtomicInteger currentId = new AtomicInteger(existingState.getLastIdUsed());
    // add meta fields to the schema in order to ensure they will be assigned IDs
    Schema schemaForIdMapping =
        includeMetaFields ? HoodieAvroUtils.addMetadataFields(schema) : schema;
    List<IdMapping> newMappings =
        generateIdMappings(schemaForIdMapping, currentId, existingState.getIdMappings());
    return new IdTracking(newMappings, currentId.get());
  }

  private static List<IdMapping> generateIdMappings(
      Schema schema, AtomicInteger lastFieldId, List<IdMapping> existingMappings) {
    Map<String, IdMapping> fieldNameToExistingMapping =
        existingMappings == null
            ? new HashMap<>()
            : existingMappings.stream()
                .collect(Collectors.toMap(IdMapping::getName, Function.identity()));
    List<IdMapping> mappings = new ArrayList<>();
    List<Pair<IdMapping, Schema>> nested = new ArrayList<>();
    if (schema.getType() == Schema.Type.ARRAY) {
      // add a single field "element"
      IdMapping fieldMapping =
          fieldNameToExistingMapping.computeIfAbsent(
              ARRAY_FIELD, key -> new IdMapping(key, lastFieldId.incrementAndGet()));
      mappings.add(fieldMapping);
      nested.add(Pair.of(fieldMapping, getFieldSchema(schema.getElementType())));
    } else if (schema.getType() == Schema.Type.MAP) {
      // add a field for the key and value
      IdMapping keyFieldMapping =
          fieldNameToExistingMapping.computeIfAbsent(
              KEY_FIELD, key -> new IdMapping(key, lastFieldId.incrementAndGet()));
      IdMapping valueFieldMapping =
          fieldNameToExistingMapping.computeIfAbsent(
              VALUE_FIELD, key -> new IdMapping(key, lastFieldId.incrementAndGet()));
      mappings.add(keyFieldMapping);
      mappings.add(valueFieldMapping);
      nested.add(Pair.of(valueFieldMapping, getFieldSchema(schema.getValueType())));
    } else if (schema.getType() == Schema.Type.RECORD) {
      for (Schema.Field field : schema.getFields()) {
        Schema fieldSchema = getFieldSchema(field.schema());
        IdMapping fieldMapping =
            fieldNameToExistingMapping.computeIfAbsent(
                field.name(), key -> new IdMapping(key, lastFieldId.incrementAndGet()));
        mappings.add(fieldMapping);
        if (fieldSchema.getType() == Schema.Type.RECORD
            || fieldSchema.getType() == Schema.Type.ARRAY
            || fieldSchema.getType() == Schema.Type.MAP) {
          nested.add(Pair.of(fieldMapping, fieldSchema));
        }
      }
    }
    nested.forEach(
        pair ->
            pair.getLeft()
                .setFields(
                    generateIdMappings(pair.getRight(), lastFieldId, pair.getLeft().getFields())));
    return mappings.stream()
        .sorted(Comparator.comparing(IdMapping::getId))
        .collect(Collectors.toList());
  }

  private static Schema getFieldSchema(Schema fieldSchema) {
    if (fieldSchema.isUnion()) {
      // assumes union for nullable
      return fieldSchema.getTypes().get(0).getType() == Schema.Type.NULL
          ? fieldSchema.getTypes().get(1)
          : fieldSchema.getTypes().get(0);
    }
    return fieldSchema;
  }

  private static Schema addIdMappings(Schema schema, IdTracking idTracking) {
    Schema updatedSchema = schema;
    if (schema.getObjectProps().containsKey(ID_TRACKING)) {
      updatedSchema = copySchemaWithoutIdTrackingProp(schema);
    }
    updatedSchema.addProp(ID_TRACKING, OBJECT_MAPPER.valueToTree(idTracking));
    return updatedSchema;
  }

  private static Schema copySchemaWithoutIdTrackingProp(Schema input) {
    List<Schema.Field> parentFields = new ArrayList<>();

    for (Schema.Field field : input.getFields()) {
      Schema.Field newField =
          new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal());
      for (Map.Entry<String, Object> prop : field.getObjectProps().entrySet()) {
        newField.addProp(prop.getKey(), prop.getValue());
      }
      parentFields.add(newField);
    }

    Schema mergedSchema =
        Schema.createRecord(input.getName(), input.getDoc(), input.getNamespace(), false);
    if (input.hasProps()) {
      input
          .getObjectProps()
          .forEach(
              (key, value) -> {
                if (!key.equals(ID_TRACKING)) {
                  mergedSchema.addProp(key, value);
                }
              });
    }
    mergedSchema.setFields(parentFields);
    return mergedSchema;
  }
}
