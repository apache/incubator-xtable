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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.hadoop.hive.metastore.api.FieldSchema;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.SchemaExtractorException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HMSSchemaExtractor {

  private static final HMSSchemaExtractor INSTANCE = new HMSSchemaExtractor();

  public static HMSSchemaExtractor getInstance() {
    return INSTANCE;
  }

  /**
   * Extract HMS schema from InternalTable schema
   *
   * @param tableFormat tableFormat to handle format specific type conversion
   * @param tableSchema InternalTable schema
   * @return HMS Field schema list
   */
  public List<FieldSchema> toColumns(String tableFormat, InternalSchema tableSchema) {
    return tableSchema.getFields().stream()
        .map(
            field ->
                new FieldSchema(
                    field.getName(),
                    convertToTypeString(field.getSchema()),
                    field.getSchema().getComment()))
        .collect(Collectors.toList());
  }

  private String convertToTypeString(InternalSchema fieldSchema) {
    switch (fieldSchema.getDataType()) {
      case BOOLEAN:
        return "boolean";
      case INT:
        return "int";
      case LONG:
        return "bigint";
      case FLOAT:
        return "float";
      case DOUBLE:
        return "double";
      case DATE:
        return "date";
      case ENUM:
      case STRING:
        return "string";
      case TIMESTAMP:
      case TIMESTAMP_NTZ:
        return "timestamp";
      case FIXED:
      case BYTES:
        return "binary";
      case DECIMAL:
        Map<InternalSchema.MetadataKey, Object> metadata = fieldSchema.getMetadata();
        if (metadata == null || metadata.isEmpty()) {
          throw new NotSupportedException("Invalid decimal type, precision and scale is missing");
        }
        int precision =
            (int)
                metadata.computeIfAbsent(
                    InternalSchema.MetadataKey.DECIMAL_PRECISION,
                    k -> {
                      throw new NotSupportedException("Invalid decimal type, precision is missing");
                    });
        int scale =
            (int)
                metadata.computeIfAbsent(
                    InternalSchema.MetadataKey.DECIMAL_SCALE,
                    k -> {
                      throw new NotSupportedException("Invalid decimal type, scale is missing");
                    });
        return String.format("decimal(%s,%s)", precision, scale);
      case RECORD:
        final String nameToType =
            fieldSchema.getFields().stream()
                .map(f -> String.format("%s:%s", f.getName(), convertToTypeString(f.getSchema())))
                .collect(Collectors.joining(","));
        return String.format("struct<%s>", nameToType);
      case LIST:
        InternalField arrayElement =
            fieldSchema.getFields().stream()
                .filter(
                    arrayField ->
                        InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME.equals(
                            arrayField.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid array schema"));
        return String.format("array<%s>", convertToTypeString(arrayElement.getSchema()));
      case MAP:
        InternalField key =
            fieldSchema.getFields().stream()
                .filter(
                    mapField ->
                        InternalField.Constants.MAP_KEY_FIELD_NAME.equals(mapField.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid map schema"));
        InternalField value =
            fieldSchema.getFields().stream()
                .filter(
                    mapField ->
                        InternalField.Constants.MAP_VALUE_FIELD_NAME.equals(mapField.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid map schema"));
        return String.format(
            "map<%s,%s>",
            convertToTypeString(key.getSchema()), convertToTypeString(value.getSchema()));
      default:
        throw new NotSupportedException("Unsupported type: " + fieldSchema.getDataType());
    }
  }
}
