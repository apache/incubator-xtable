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

package org.apache.xtable.parquet;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;

import org.apache.xtable.schema.SchemaUtils;
import org.apache.xtable.exception.SchemaExtractorException;

import java.util.Collections;
import java.util.Optional;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.xtable.hudi.idtracking.models.IdMapping;
import org.apache.avro.Schema;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.xtable.collectors.CustomCollectors;
import org.apache.xtable.exception.UnsupportedSchemaTypeException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.parquet.schema.Type.ID;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.apache.parquet.column.ColumnDescriptor;



/**
 * Class that converts parquet Schema {@link Schema} to Canonical Schema {@link InternalSchema} and
 * vice-versa. This conversion is fully reversible and there is a strict 1 to 1 mapping between
 * parquet data types and canonical data types.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ParquetSchemaExtractor {
    // parquet only supports string keys in maps
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
    private static final ParquetSchemaExtractor INSTANCE = new ParquetSchemaExtractor();
    private static final String ELEMENT = "element";
    private static final String KEY = "key";
    private static final String VALUE = "value";

    public static ParquetSchemaExtractor getInstance() {
        return INSTANCE;
    }

    private static boolean groupTypeIsNullable(Type schema) {
        return schema.getRepetition() == Repetition.REQUIRED ? false : true;
    }

    private static boolean groupTypeContainsNull(Type schema) {
        if (!schema.isPrimitive()) {
            for (Type field : schema.asGroupType().getFields()) {
                if (field/*.getLogicalTypeAnnotation().toOriginalType()*/ == null) {
                    return true;
                }
            }
        } else {
            if (schema.equals(null)) {
                return true;
            }
        }

        return false;
    }

    /*    private static LogicalTypeAnnotation finalizeSchema(LogicalTypeAnnotation targetSchema, InternalSchema inputSchema) {
            if (inputSchema.isNullable()) {
                return targetSchema.union(null); // LogicalTypeAnnotation.unknownType()
            }
            return targetSchema;
        }*/

    /**
     * Converts the parquet {@link Schema} to {@link InternalSchema}.
     *
     * @param schema     The schema being converted
     * @param parentPath If this schema is nested within another, this will be a dot separated string
     *                   representing the path from the top most field to the current schema.
     * @return a converted schema
     */
    public InternalSchema toInternalSchema(
            Type schema, String parentPath) {
        // TODO - Does not handle recursion in parquet schema
        InternalType newDataType = null;
        Type.Repetition currentRepetition = null;
        List<InternalField> subFields = new ArrayList<>();
        PrimitiveType primitiveType;
        LogicalTypeAnnotation logicalType;
        Map<InternalSchema.MetadataKey, Object> metadata = new HashMap<>();
        String elementName = schema.getName();
        if (schema.isPrimitive()) {
            primitiveType = schema.asPrimitiveType();
            switch (primitiveType.getPrimitiveTypeName()) {
                // PrimitiveTypes
                case INT64:
                    logicalType = schema.getLogicalTypeAnnotation();
                    if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                        LogicalTypeAnnotation.TimeUnit timeUnit =
                                ((LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) logicalType).getUnit();
                        if (timeUnit == LogicalTypeAnnotation.TimeUnit.MICROS) {
                            newDataType = InternalType.TIMESTAMP;
                            metadata.put(
                                    InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                                    InternalSchema.MetadataValue.MICROS);
                        } else if (timeUnit == LogicalTypeAnnotation.TimeUnit.MILLIS) {
                            newDataType = InternalType.TIMESTAMP_NTZ;
                            metadata.put(
                                    InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                                    InternalSchema.MetadataValue.MILLIS);
                        } else if (timeUnit == LogicalTypeAnnotation.TimeUnit.NANOS) {
                            newDataType = InternalType.TIMESTAMP_NTZ;
                            metadata.put(
                                    InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                                    InternalSchema.MetadataValue.NANOS);
                        }
                    } else if (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
                        newDataType = InternalType.INT;
                    } else if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
                        LogicalTypeAnnotation.TimeUnit timeUnit = ((LogicalTypeAnnotation.TimeLogicalTypeAnnotation) logicalType).getUnit();
                        if (timeUnit == LogicalTypeAnnotation.TimeUnit.MICROS || timeUnit == LogicalTypeAnnotation.TimeUnit.NANOS) {
                            // check if INT is the InternalType needed here
                            newDataType = InternalType.INT;
                        }
                    } else {
                        newDataType = InternalType.LONG;
                    }
                    break;
                case INT32:
                    logicalType = schema.getLogicalTypeAnnotation();
                    if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                        newDataType = InternalType.DATE;
                    } else if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
                        LogicalTypeAnnotation.TimeUnit timeUnit = ((LogicalTypeAnnotation.TimeLogicalTypeAnnotation) logicalType).getUnit();
                        if (timeUnit == LogicalTypeAnnotation.TimeUnit.MILLIS) {
                            // check if INT is the InternalType needed here
                            newDataType = InternalType.INT;
                        }
                    } else {
                        newDataType = InternalType.INT;
                    }
                    break;
    /*            case INT96:
                    newDataType = InternalType.INT;
                    break;*/
                case FLOAT:
                    logicalType = schema.getLogicalTypeAnnotation();
          /*  if (logicalType instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
              newDataType = InternalType.FLOAT;
          } else*/
                    if (logicalType
                            instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                        metadata.put(
                                InternalSchema.MetadataKey.DECIMAL_PRECISION,
                                ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType).getPrecision());
                        metadata.put(
                                InternalSchema.MetadataKey.DECIMAL_SCALE,
                                ((LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) logicalType).getScale());
                        newDataType = InternalType.DECIMAL;
                    } else {
                        newDataType = InternalType.FLOAT;
                    }
                    break;
                case FIXED_LEN_BYTE_ARRAY:
                    logicalType = schema.getLogicalTypeAnnotation();
                    if (logicalType instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
                        newDataType = InternalType.UUID;
                    } else if (logicalType instanceof LogicalTypeAnnotation.IntervalLogicalTypeAnnotation) {
                        metadata.put(InternalSchema.MetadataKey.FIXED_BYTES_SIZE, 12);
                        newDataType = InternalType.FIXED;
                    }
                    break;
                // TODO add other logicalTypes?
                case BINARY:
                    // ? Variant,GEOMETRY, GEOGRAPHY,
                    //logicalType = schemaField.getLogicalTypeAnnotation();
                    logicalType = schema.getLogicalTypeAnnotation();
                    if (logicalType instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
                        metadata.put(
                                InternalSchema.MetadataKey.ENUM_VALUES, logicalType.toOriginalType().values());
                        newDataType = InternalType.ENUM;
                    } else if (logicalType instanceof LogicalTypeAnnotation.JsonLogicalTypeAnnotation) {
                        newDataType = InternalType.BYTES;
                    } else if (logicalType instanceof LogicalTypeAnnotation.BsonLogicalTypeAnnotation) {
                        newDataType = InternalType.BYTES;
                    } else if (logicalType instanceof LogicalTypeAnnotation.StringLogicalTypeAnnotation) {
                        newDataType = InternalType.STRING;
                    } else {
                        newDataType = InternalType.BYTES;
                    }
                    break;
                case BOOLEAN:
                    newDataType = InternalType.BOOLEAN;
                    break;
             /*   case UNKNOWN:
                    newDataType = InternalType.NULL;
                    break;*/
                default:
                    /*if (logicalType instanceof LogicalTypeAnnotation.UnknownLogicalTypeAnnotation){
                        newDataType = InternalType.NULL;
                    }
                    else {*/
                    throw new UnsupportedSchemaTypeException(
                            String.format("Unsupported schema type %s", schema));
                    //}
                    //break;
            }
        } else {
            //GroupTypes
            //typeName = schema.asGroupType();
            logicalType = schema.getLogicalTypeAnnotation();
            if (logicalType instanceof LogicalTypeAnnotation.ListLogicalTypeAnnotation) {
                String schemaName = schema.asGroupType().getName();
                Type.ID schemaId = schema.getId();
                InternalSchema elementSchema =
                        toInternalSchema(
                                schema.asGroupType().getType(0),
                                SchemaUtils.getFullyQualifiedPath(
                                        parentPath, InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME));
                InternalField elementField =
                        InternalField.builder()
                                .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                .parentPath(parentPath)
                                .schema(elementSchema)
                                .fieldId(schemaId == null ? null : schemaId.intValue())
                                .build();
                return InternalSchema.builder()
                        .name(schema.getName())
                        .dataType(InternalType.LIST)
                        .comment(null)
                        .isNullable(groupTypeIsNullable(schema.asGroupType()))
                        .fields(Collections.singletonList(elementField))
                        .build();
            } else if (logicalType instanceof LogicalTypeAnnotation.MapLogicalTypeAnnotation) {
                String schemaName = schema.asGroupType().getName();
                Type.ID schemaId = schema.getId();
                InternalSchema valueSchema =
                        toInternalSchema(
                                schema.asGroupType().getType(0),
                                SchemaUtils.getFullyQualifiedPath(
                                        parentPath, InternalField.Constants.MAP_VALUE_FIELD_NAME));
                InternalField valueField =
                        InternalField.builder()
                                .name(InternalField.Constants.MAP_VALUE_FIELD_NAME)
                                .parentPath(parentPath)
                                .schema(valueSchema)
                                .fieldId(schemaId == null ? null : schemaId.intValue())
                                .build();
                return InternalSchema.builder()
                        .name(schemaName)
                        .dataType(InternalType.MAP)
                        .comment(null)
                        .isNullable(groupTypeIsNullable(schema.asGroupType()))
                        .fields(valueSchema.getFields()
                                /*Arrays.asList(
                                        MAP_KEY_FIELD.toBuilder()
                                                .parentPath(parentPath)
                                                .fieldId(schemaId == null ? null : schemaId.intValue())
                                                .build(),
                                        valueField)*/)
                        .build();
            } else {

                subFields = new ArrayList<>(schema.asGroupType().getFields().size());
                //if (currentRepetition == Repetition.REPEATED// && (schema.asGroupType().getName() == "list" || Arrays.asList("key_value", "map").contains(schema.asGroupType().getName())) /*&& schema.asGroupType().getFields().size() == 1*/) {
                //} else {
                //List<InternalField> subFields = new ArrayList<>(schema.asGroupType().getFields().size());
                for (Type parquetField : schema.asGroupType().getFields()) {
                    String fieldName = parquetField.getName();
                    Type.ID fieldId = parquetField.getId();
                    currentRepetition = parquetField.getRepetition();
                    InternalSchema subFieldSchema =
                            toInternalSchema(
                                    parquetField,
                                    SchemaUtils.getFullyQualifiedPath(parentPath, fieldName));

                    if (schema.asGroupType().getFields().size() == 1) {//todo Tuple (many subelements in a list)
                        newDataType = subFieldSchema.getDataType();
                        elementName = subFieldSchema.getName();
                        break;
                    }
                    subFields.add(
                            InternalField.builder()
                                    .parentPath(parentPath)
                                    .name(fieldName)
                                    .schema(subFieldSchema)
                                    .defaultValue(null)
                                    .fieldId(fieldId == null ? null : fieldId.intValue())
                                    .build());
                }
                if (currentRepetition != Repetition.REPEATED && schema.asGroupType().getName() != "list" && !Arrays.asList("key_value", "map").contains(schema.asGroupType().getName())) {
                    return InternalSchema.builder()
                            .name(schema.getName())
                            .comment(null)
                            .dataType(InternalType.RECORD)
                            .fields(subFields)
                            .isNullable(groupTypeIsNullable(schema.asGroupType()))
                            .build();
                }
            }
        }
        return InternalSchema.builder()
                .name(elementName)
                .dataType(newDataType)
                .fields(subFields.size() == 0 ? null : subFields)
                .comment(null)
                .isNullable(groupTypeIsNullable(schema)) // to check
                .metadata(metadata.isEmpty() ? null : metadata)
                .build();
    }


    /**
     * Internal method for converting the {@link InternalSchema} to parquet {@link Schema}.
     *
     * @param internalSchema internal schema representation
     * @param currentPath    If this schema is nested within another, this will be a dot separated
     *                       string. This is used for the parquet namespace to guarantee unique names for nested
     *                       records.
     * @return an parquet schema
     */
    private Type fromInternalSchema(InternalSchema internalSchema, String currentPath) {
        Type type = null;
        String fieldName = internalSchema.getName();
        InternalType internalType = internalSchema.getDataType();
        switch (internalType) {
              /*case BYTES:
                  return finalizeSchema(Schema.create(Schema.Type.BYTES), internalSchema);*/
            case BOOLEAN:
                type = Types
                        .required(PrimitiveTypeName.BOOLEAN).as(LogicalTypeAnnotation.intType(8, false))
                        .named(fieldName);
                break;
            case INT:
                type = Types
                        .required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(32, false))
                        .named(fieldName);
                break;
            case LONG:
                type = Types
                        .required(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.intType(64, false))
                        .named(fieldName);
                break;
            case STRING:
                type = Types
                        .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())
                        .named(fieldName);
                break;
            case FLOAT:
                int precision =
                        (int)
                                internalSchema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_PRECISION);
                int scale =
                        (int)
                                internalSchema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_SCALE);
                type = Types
                        .required(PrimitiveTypeName.FLOAT).as(LogicalTypeAnnotation.decimalType(scale, precision))
                        .named(fieldName);
                break;

            case ENUM:
                type = new org.apache.parquet.avro.AvroSchemaConverter().convert(Schema.createEnum(
                        fieldName,
                        internalSchema.getComment(),
                        null,
                        (List<String>)
                                internalSchema.getMetadata().get(InternalSchema.MetadataKey.ENUM_VALUES),
                        null)).getType(fieldName);
                break;
            case DATE:
                type = Types
                        .required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType())
                        .named(fieldName);
                break;
            case TIMESTAMP:
                if (internalSchema.getMetadata().get(InternalSchema.MetadataKey.TIMESTAMP_PRECISION) == InternalSchema.MetadataValue.MICROS) {
                    type = Types
                            .required(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                            .named(fieldName);
                }
                if (internalSchema.getMetadata().get(InternalSchema.MetadataKey.TIMESTAMP_PRECISION) == InternalSchema.MetadataValue.MILLIS) {
                    type = Types
                            .required(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                            .named(fieldName);
                } else if (internalSchema.getMetadata().get(InternalSchema.MetadataKey.TIMESTAMP_PRECISION) == InternalSchema.MetadataValue.NANOS) {
                    type = Types
                            .required(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                            .named(fieldName);
                }
                break;
            case TIMESTAMP_NTZ:
                if (internalSchema.getMetadata().get(InternalSchema.MetadataKey.TIMESTAMP_PRECISION)
                        == InternalSchema.MetadataValue.MICROS) {
                    type = Types
                            .required(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS))
                            .named(fieldName);

                } else {
                    type = Types
                            .required(PrimitiveTypeName.INT64).as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS))
                            .named(fieldName);
                }
                break;
            case LIST:
                InternalField elementField =
                        internalSchema.getFields().stream()
                                .filter(
                                        field ->
                                                InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME.equals(field.getName()))
                                .findFirst()
                                .orElseThrow(() -> new SchemaExtractorException("Invalid array schema"));
                type = fromInternalSchema(elementField.getSchema(), elementField.getPath());
                break;
            case MAP:
                InternalField valueField =
                        internalSchema.getFields().stream()
                                .filter(
                                        field -> InternalField.Constants.MAP_VALUE_FIELD_NAME.equals(field.getName()))
                                .findFirst()
                                .orElseThrow(() -> new SchemaExtractorException("Invalid map schema"));
                type = fromInternalSchema(valueField.getSchema(), valueField.getPath());
                break;
            case RECORD:
                List<Type> fields =
                        internalSchema.getFields().stream()
                                .map(
                                        field ->
                                                fromInternalSchema(
                                                        field.getSchema(),
                                                        SchemaUtils.getFullyQualifiedPath(field.getName(), currentPath))
                                )
                                .collect(CustomCollectors.toList(internalSchema.getFields().size()));
                type = Types.requiredGroup().addFields(fields.stream().toArray(Type[]::new)).named(fieldName);
                break;
            default:
                throw new UnsupportedSchemaTypeException("Encountered unhandled type during InternalSchema to parquet conversion:" + internalType);
        }
        return type;
    }
}
