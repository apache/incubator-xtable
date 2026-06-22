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
 
package org.apache.xtable.orc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.orc.TypeDescription;

import org.apache.xtable.exception.UnsupportedSchemaTypeException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.schema.SchemaUtils;

/** Class that converts ORC Schema to Canonical Schema {@link InternalSchema} and vice-versa. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ORCSchemaExtractor {
  private static final org.apache.xtable.orc.ORCSchemaExtractor INSTANCE =
      new org.apache.xtable.orc.ORCSchemaExtractor();

  public static org.apache.xtable.orc.ORCSchemaExtractor getInstance() {
    return INSTANCE;
  }

  private static boolean isNullable(TypeDescription schema) {
    List<TypeDescription> subFields = schema.getChildren();
    if (subFields == null) return false;

    for (TypeDescription subField : subFields) {
      if (subField == null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Converts the ORC to {@link InternalSchema}.
   *
   * @param schema The schema being converted
   * @param parentPath If this schema is nested within another, this will be a dot separated string
   *     representing the path from the top most field to the current schema.
   * @return a converted schema
   */
  private InternalSchema toInternalSchema(TypeDescription schema, String parentPath) {
    InternalType newDataType;
    Map<InternalSchema.MetadataKey, Object> metadata = new HashMap<>();
    switch (schema.getCategory()) {
      case INT:
        newDataType = InternalType.INT;
        break;
      case DATE:
        newDataType = InternalType.DATE;
        break;
      case LONG:
        newDataType = InternalType.LONG;
        break;
      case TIMESTAMP:
        newDataType = InternalType.TIMESTAMP;
        metadata.put(
            InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MICROS);
        break;
      case DECIMAL:
        newDataType = InternalType.DECIMAL;
        metadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, schema.getPrecision());
        metadata.put(InternalSchema.MetadataKey.DECIMAL_SCALE, schema.getScale());
        break;
      case STRUCT:
        List<TypeDescription> fieldTypes = schema.getChildren();
        List<String> fieldNames = schema.getFieldNames();
        List<InternalField> subFields = new ArrayList<>(fieldTypes.size());

        for (int i = 0; i < fieldTypes.size(); i++) {
          TypeDescription childSchema = fieldTypes.get(i);
          String fieldName = fieldNames.get(i);

          InternalSchema subFieldSchema =
              toInternalSchema(
                  childSchema, SchemaUtils.getFullyQualifiedPath(parentPath, fieldName));

          subFields.add(
              InternalField.builder()
                  .parentPath(parentPath)
                  .name(fieldName)
                  .schema(subFieldSchema)
                  .fieldId(childSchema.getId())
                  .build());
        }
        return InternalSchema.builder()
            .name(schema.getFullFieldName())
            .dataType(InternalType.RECORD)
            .fields(subFields)
            .isNullable(true)
            .build();
      default:
        throw new UnsupportedSchemaTypeException(
            String.format("Unsupported schema type category %s", schema.getCategory()));
    }

    return InternalSchema.builder()
        .name(schema.getFullFieldName())
        .dataType(newDataType)
        .comment(schema.toString())
        .isNullable(isNullable(schema))
        .metadata(metadata.isEmpty() ? null : metadata)
        .build();
  }

  public TypeDescription fromInternalSchema(InternalSchema internalSchema, String currentPath) {
    if (internalSchema == null) {
      return null;
    }

    TypeDescription type;
    InternalType internalType = internalSchema.getDataType();

    switch (internalType) {
      case BOOLEAN:
        type = TypeDescription.createBoolean();
        break;
      case INT:
        type = TypeDescription.createInt();
        break;
      case LONG:
        type = TypeDescription.createLong();
        break;
      case STRING:
        type = TypeDescription.createString();
        break;
      case FLOAT:
        type = TypeDescription.createFloat();
        break;
      case DOUBLE:
        type = TypeDescription.createDouble();
        break;
      case DATE:
        type = TypeDescription.createDate();
        break;
      case TIMESTAMP:
      case TIMESTAMP_NTZ:
        type = TypeDescription.createTimestamp();
        break;
      case DECIMAL:
        int precision =
            (int)
                internalSchema
                    .getMetadata()
                    .getOrDefault(InternalSchema.MetadataKey.DECIMAL_PRECISION, 38);
        int scale =
            (int)
                internalSchema
                    .getMetadata()
                    .getOrDefault(InternalSchema.MetadataKey.DECIMAL_SCALE, 10);
        type = TypeDescription.createDecimal().withPrecision(precision).withScale(scale);
        break;
      case RECORD:
        type = TypeDescription.createStruct();
        if (internalSchema.getFields() != null) {
          for (InternalField field : internalSchema.getFields()) {
            String fieldPath = SchemaUtils.getFullyQualifiedPath(currentPath, field.getName());
            TypeDescription fieldType = fromInternalSchema(field.getSchema(), fieldPath);
            type.addField(field.getName(), fieldType);
          }
        }
        break;
      case LIST:
        InternalField elementField =
            internalSchema.getFields().stream()
                .filter(
                    field ->
                        InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME.equals(field.getName()))
                .findFirst()
                .orElseThrow(() -> new UnsupportedSchemaTypeException("Invalid array schema"));

        TypeDescription elementType =
            fromInternalSchema(elementField.getSchema(), elementField.getPath());
        type = TypeDescription.createList(elementType);
        break;
      case MAP:
        InternalField valueField =
            internalSchema.getFields().stream()
                .filter(
                    field -> InternalField.Constants.MAP_VALUE_FIELD_NAME.equals(field.getName()))
                .findFirst()
                .orElseThrow(() -> new UnsupportedSchemaTypeException("Invalid map schema"));

        TypeDescription keyType = TypeDescription.createString();
        TypeDescription valueType =
            fromInternalSchema(valueField.getSchema(), valueField.getPath());
        type = TypeDescription.createMap(keyType, valueType);
        break;
      case BYTES:
      case FIXED:
      case UUID:
        type = TypeDescription.createBinary();
        break;
      default:
        throw new UnsupportedSchemaTypeException(
            "Encountered unhandled type during InternalSchema to ORC conversion: " + internalType);
    }
    return type;
  }
}
