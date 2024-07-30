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
 
package org.apache.xtable.avro;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import org.apache.xtable.collectors.CustomCollectors;
import org.apache.xtable.exception.SchemaExtractorException;
import org.apache.xtable.exception.UnsupportedSchemaTypeException;
import org.apache.xtable.hudi.idtracking.IdTracker;
import org.apache.xtable.hudi.idtracking.models.IdMapping;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.schema.SchemaUtils;

/**
 * Class that converts Avro Schema {@link Schema} to Canonical Schema {@link InternalSchema} and
 * vice-versa. This conversion is fully reversible and there is a strict 1 to 1 mapping between avro
 * data types and canonical data types.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AvroSchemaConverter {
  // avro only supports string keys in maps
  private static final InternalField MAP_KEY_FIELD =
      InternalField.builder()
          .name(InternalField.Constants.MAP_KEY_FIELD_NAME)
          .schema(
              InternalSchema.builder()
                  .name("map_key")
                  .dataType(InternalType.STRING)
                  .isNullable(false)
                  .build())
          .defaultValue("")
          .build();
  private static final AvroSchemaConverter INSTANCE = new AvroSchemaConverter();
  private static final String ELEMENT = "element";
  private static final String KEY = "key";
  private static final String VALUE = "value";

  public static AvroSchemaConverter getInstance() {
    return INSTANCE;
  }

  public InternalSchema toInternalSchema(Schema schema) {
    Map<String, IdMapping> fieldNameToIdMapping =
        IdTracker.getInstance()
            .getIdTracking(schema)
            .map(
                idTracking ->
                    idTracking.getIdMappings().stream()
                        .collect(Collectors.toMap(IdMapping::getName, Function.identity())))
            .orElse(Collections.emptyMap());
    return toInternalSchema(schema, null, fieldNameToIdMapping);
  }

  /**
   * Converts the Avro {@link Schema} to {@link InternalSchema}.
   *
   * @param schema The schema being converted
   * @param parentPath If this schema is nested within another, this will be a dot separated string
   *     representing the path from the top most field to the current schema.
   * @param fieldNameToIdMapping map of fieldName to IdMapping to track field IDs provided by the
   *     source schema. If source schema does not contain IdMappings, map will be empty.
   * @return a converted schema
   */
  private InternalSchema toInternalSchema(
      Schema schema, String parentPath, Map<String, IdMapping> fieldNameToIdMapping) {
    // TODO - Does not handle recursion in Avro schema
    InternalType newDataType;
    Map<InternalSchema.MetadataKey, Object> metadata = new HashMap<>();
    switch (schema.getType()) {
      case INT:
        LogicalType logicalType = schema.getLogicalType();
        if (logicalType instanceof LogicalTypes.Date) {
          newDataType = InternalType.DATE;
        } else {
          newDataType = InternalType.INT;
        }
        break;
      case STRING:
        newDataType = InternalType.STRING;
        break;
      case BOOLEAN:
        newDataType = InternalType.BOOLEAN;
        break;
      case BYTES:
      case FIXED:
        logicalType = schema.getLogicalType();
        if (logicalType instanceof LogicalTypes.Decimal) {
          metadata.put(
              InternalSchema.MetadataKey.DECIMAL_PRECISION,
              ((LogicalTypes.Decimal) logicalType).getPrecision());
          metadata.put(
              InternalSchema.MetadataKey.DECIMAL_SCALE,
              ((LogicalTypes.Decimal) logicalType).getScale());
          if (schema.getType() == Schema.Type.FIXED) {
            metadata.put(InternalSchema.MetadataKey.FIXED_BYTES_SIZE, schema.getFixedSize());
          }
          newDataType = InternalType.DECIMAL;
          break;
        }
        if (schema.getType() == Schema.Type.FIXED) {
          metadata.put(InternalSchema.MetadataKey.FIXED_BYTES_SIZE, schema.getFixedSize());
          newDataType = InternalType.FIXED;
        } else {
          newDataType = InternalType.BYTES;
        }
        break;
      case DOUBLE:
        newDataType = InternalType.DOUBLE;
        break;
      case FLOAT:
        newDataType = InternalType.FLOAT;
        break;
      case LONG:
        logicalType = schema.getLogicalType();
        if (logicalType instanceof LogicalTypes.TimestampMillis) {
          newDataType = InternalType.TIMESTAMP;
          metadata.put(
              InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MILLIS);
        } else if (logicalType instanceof LogicalTypes.TimestampMicros) {
          newDataType = InternalType.TIMESTAMP;
          metadata.put(
              InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MICROS);
        } else if (logicalType instanceof LogicalTypes.LocalTimestampMillis) {
          newDataType = InternalType.TIMESTAMP_NTZ;
          metadata.put(
              InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MILLIS);
        } else if (logicalType instanceof LogicalTypes.LocalTimestampMicros) {
          newDataType = InternalType.TIMESTAMP_NTZ;
          metadata.put(
              InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MICROS);
        } else {
          newDataType = InternalType.LONG;
        }
        break;
      case ENUM:
        metadata.put(InternalSchema.MetadataKey.ENUM_VALUES, schema.getEnumSymbols());
        newDataType = InternalType.ENUM;
        break;
      case NULL:
        newDataType = InternalType.NULL;
        break;
      case RECORD:
        List<InternalField> subFields = new ArrayList<>(schema.getFields().size());
        for (Schema.Field avroField : schema.getFields()) {
          IdMapping idMapping = fieldNameToIdMapping.get(avroField.name());
          InternalSchema subFieldSchema =
              toInternalSchema(
                  avroField.schema(),
                  SchemaUtils.getFullyQualifiedPath(parentPath, avroField.name()),
                  getChildIdMap(idMapping));
          Object defaultValue = getDefaultValue(avroField);
          subFields.add(
              InternalField.builder()
                  .parentPath(parentPath)
                  .name(avroField.name())
                  .schema(subFieldSchema)
                  .defaultValue(defaultValue)
                  .fieldId(idMapping == null ? null : idMapping.getId())
                  .build());
        }
        return InternalSchema.builder()
            .name(schema.getName())
            .comment(schema.getDoc())
            .dataType(InternalType.RECORD)
            .fields(subFields)
            .isNullable(schema.isNullable())
            .build();
      case ARRAY:
        IdMapping elementMapping = fieldNameToIdMapping.get(ELEMENT);
        InternalSchema elementSchema =
            toInternalSchema(
                schema.getElementType(),
                SchemaUtils.getFullyQualifiedPath(
                    parentPath, InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME),
                getChildIdMap(elementMapping));
        InternalField elementField =
            InternalField.builder()
                .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                .parentPath(parentPath)
                .schema(elementSchema)
                .fieldId(elementMapping == null ? null : elementMapping.getId())
                .build();
        return InternalSchema.builder()
            .name(schema.getName())
            .dataType(InternalType.LIST)
            .comment(schema.getDoc())
            .isNullable(schema.isNullable())
            .fields(Collections.singletonList(elementField))
            .build();
      case MAP:
        IdMapping keyMapping = fieldNameToIdMapping.get(KEY);
        IdMapping valueMapping = fieldNameToIdMapping.get(VALUE);
        InternalSchema valueSchema =
            toInternalSchema(
                schema.getValueType(),
                SchemaUtils.getFullyQualifiedPath(
                    parentPath, InternalField.Constants.MAP_VALUE_FIELD_NAME),
                getChildIdMap(valueMapping));
        InternalField valueField =
            InternalField.builder()
                .name(InternalField.Constants.MAP_VALUE_FIELD_NAME)
                .parentPath(parentPath)
                .schema(valueSchema)
                .fieldId(valueMapping == null ? null : valueMapping.getId())
                .build();
        return InternalSchema.builder()
            .name(schema.getName())
            .dataType(InternalType.MAP)
            .comment(schema.getDoc())
            .isNullable(schema.isNullable())
            .fields(
                Arrays.asList(
                    MAP_KEY_FIELD.toBuilder()
                        .parentPath(parentPath)
                        .fieldId(keyMapping == null ? null : keyMapping.getId())
                        .build(),
                    valueField))
            .build();
      case UNION:
        boolean containsUnionWithNull =
            schema.getTypes().stream().anyMatch(t -> t.getType() == Schema.Type.NULL);
        if (containsUnionWithNull) {
          List<Schema> remainingSchemas =
              schema.getTypes().stream()
                  .filter(t -> t.getType() != Schema.Type.NULL)
                  .collect(Collectors.toList());
          if (remainingSchemas.size() == 1) {
            InternalSchema restSchema =
                toInternalSchema(remainingSchemas.get(0), parentPath, fieldNameToIdMapping);
            return InternalSchema.builderFrom(restSchema).isNullable(true).build();
          } else {
            return InternalSchema.builderFrom(
                    toInternalSchema(Schema.createUnion(remainingSchemas)))
                .isNullable(true)
                .build();
          }
        } else {
          throw new UnsupportedSchemaTypeException(
              String.format("Unsupported complex union type %s", schema));
        }
      default:
        throw new UnsupportedSchemaTypeException(
            String.format("Unsupported schema type %s", schema));
    }
    return InternalSchema.builder()
        .name(schema.getName())
        .dataType(newDataType)
        .comment(schema.getDoc())
        .isNullable(schema.isNullable())
        .metadata(metadata.isEmpty() ? null : metadata)
        .build();
  }

  private Map<String, IdMapping> getChildIdMap(IdMapping idMapping) {
    if (idMapping == null) {
      return Collections.emptyMap();
    }
    return idMapping.getFields().stream()
        .collect(Collectors.toMap(IdMapping::getName, Function.identity()));
  }

  private static Object getDefaultValue(Schema.Field avroField) {
    return Schema.Field.NULL_VALUE.equals(avroField.defaultVal())
        ? InternalField.Constants.NULL_DEFAULT_VALUE
        : avroField.defaultVal();
  }

  /**
   * Converts the {@link InternalSchema} to Avro {@link Schema}.
   *
   * @param internalSchema internal schema representation
   * @return an Avro schema
   */
  public Schema fromInternalSchema(InternalSchema internalSchema) {
    return fromInternalSchema(internalSchema, null);
  }

  /**
   * Internal method for converting the {@link InternalSchema} to Avro {@link Schema}.
   *
   * @param internalSchema internal schema representation
   * @param currentPath If this schema is nested within another, this will be a dot separated
   *     string. This is used for the avro namespace to guarantee unique names for nested records.
   * @return an Avro schema
   */
  private Schema fromInternalSchema(InternalSchema internalSchema, String currentPath) {
    switch (internalSchema.getDataType()) {
      case RECORD:
        List<Schema.Field> fields =
            internalSchema.getFields().stream()
                .map(
                    field ->
                        new Schema.Field(
                            field.getName(),
                            fromInternalSchema(
                                field.getSchema(),
                                SchemaUtils.getFullyQualifiedPath(currentPath, field.getName())),
                            field.getSchema().getComment(),
                            InternalField.Constants.NULL_DEFAULT_VALUE == field.getDefaultValue()
                                ? Schema.Field.NULL_VALUE
                                : field.getDefaultValue()))
                .collect(CustomCollectors.toList(internalSchema.getFields().size()));
        return finalizeSchema(
            Schema.createRecord(
                internalSchema.getName(), internalSchema.getComment(), currentPath, false, fields),
            internalSchema);
      case BYTES:
        return finalizeSchema(Schema.create(Schema.Type.BYTES), internalSchema);
      case BOOLEAN:
        return finalizeSchema(Schema.create(Schema.Type.BOOLEAN), internalSchema);
      case INT:
        return finalizeSchema(Schema.create(Schema.Type.INT), internalSchema);
      case LONG:
        return finalizeSchema(Schema.create(Schema.Type.LONG), internalSchema);
      case STRING:
        return finalizeSchema(Schema.create(Schema.Type.STRING), internalSchema);
      case FLOAT:
        return finalizeSchema(Schema.create(Schema.Type.FLOAT), internalSchema);
      case DOUBLE:
        return finalizeSchema(Schema.create(Schema.Type.DOUBLE), internalSchema);
      case ENUM:
        return finalizeSchema(
            Schema.createEnum(
                internalSchema.getName(),
                internalSchema.getComment(),
                null,
                (List<String>)
                    internalSchema.getMetadata().get(InternalSchema.MetadataKey.ENUM_VALUES),
                null),
            internalSchema);
      case DATE:
        return finalizeSchema(
            LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)), internalSchema);
      case TIMESTAMP:
        if (internalSchema.getMetadata().get(InternalSchema.MetadataKey.TIMESTAMP_PRECISION)
            == InternalSchema.MetadataValue.MICROS) {
          return finalizeSchema(
              LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)),
              internalSchema);
        } else {
          return finalizeSchema(
              LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)),
              internalSchema);
        }
      case TIMESTAMP_NTZ:
        if (internalSchema.getMetadata().get(InternalSchema.MetadataKey.TIMESTAMP_PRECISION)
            == InternalSchema.MetadataValue.MICROS) {
          return finalizeSchema(
              LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG)),
              internalSchema);
        } else {
          return finalizeSchema(
              LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)),
              internalSchema);
        }
      case LIST:
        InternalField elementField =
            internalSchema.getFields().stream()
                .filter(
                    field ->
                        InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME.equals(field.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid array schema"));
        return finalizeSchema(
            Schema.createArray(
                fromInternalSchema(elementField.getSchema(), elementField.getPath())),
            internalSchema);
      case MAP:
        InternalField valueField =
            internalSchema.getFields().stream()
                .filter(
                    field -> InternalField.Constants.MAP_VALUE_FIELD_NAME.equals(field.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid map schema"));
        return finalizeSchema(
            Schema.createMap(fromInternalSchema(valueField.getSchema(), valueField.getPath())),
            internalSchema);
      case DECIMAL:
        int precision =
            (int) internalSchema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_PRECISION);
        int scale =
            (int) internalSchema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_SCALE);
        Integer size =
            (Integer) internalSchema.getMetadata().get(InternalSchema.MetadataKey.FIXED_BYTES_SIZE);
        if (size == null) {
          return finalizeSchema(
              LogicalTypes.decimal(precision, scale).addToSchema(Schema.create(Schema.Type.BYTES)),
              internalSchema);
        } else {
          return finalizeSchema(
              LogicalTypes.decimal(precision, scale)
                  .addToSchema(
                      Schema.createFixed(
                          internalSchema.getName(), internalSchema.getComment(), null, size)),
              internalSchema);
        }
      case FIXED:
        Integer fixedSize =
            (Integer) internalSchema.getMetadata().get(InternalSchema.MetadataKey.FIXED_BYTES_SIZE);
        return finalizeSchema(
            Schema.createFixed(
                internalSchema.getName(), internalSchema.getComment(), null, fixedSize),
            internalSchema);
      default:
        throw new UnsupportedSchemaTypeException(
            "Encountered unhandled type during InternalSchema to Avro conversion: "
                + internalSchema.getDataType());
    }
  }

  private String buildCurrentPath(InternalField field, String parentPath) {
    return Optional.ofNullable(parentPath)
        .map(path -> path + "." + field.getName())
        .orElse(field.getName());
  }

  private static Schema finalizeSchema(Schema targetSchema, InternalSchema inputSchema) {
    if (inputSchema.isNullable()) {
      return Schema.createUnion(Schema.create(Schema.Type.NULL), targetSchema);
    }
    return targetSchema;
  }
}
