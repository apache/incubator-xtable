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

import org.apache.parquet.schema.LogicalType;
//import org.apache.parquet.LogicalTypes;
//import org.apache.parquet.Schema;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.format.NullType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type.Repetition;



import org.apache.xtable.collectors.CustomCollectors;
import org.apache.xtable.exception.SchemaExtractorException;
import org.apache.xtable.exception.UnsupportedSchemaTypeException;
import org.apache.xtable.hudi.idtracking.IdTracker;
import org.apache.xtable.hudi.idtracking.models.IdMapping;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.schema.SchemaUtils;

import org.apache.xtable.avro.AvroSchemaConverter;
import org.apache.avro.Schema;
//import org.apache.parquet.avro.AvroSchemaConverter;

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

    private static Type finalizeSchema(MessageType targetSchema, InternalSchema inputSchema) {
        if (inputSchema.isNullable()) {
            return targetSchema.union(LogicalTypeAnnotation.unknownType());
        }
        return targetSchema;
    }

    private static boolean groupTypeContainsNull(Type schema) {
        for (Type field : schema.getFields()){
            if (field == null) {
                return True;
            }
        }
        return False;
    }

    public InternalSchema _toInternalSchema(Schema schema) {
        AvroSchemaConverter avroSchemaConverter =  AvroSchemaConverter.getInstance();
        Map<String, IdMapping> fieldNameToIdMapping =
                IdTracker.getInstance()
                        .getIdTracking(schema)
                        .map(
                                idTracking ->
                                        idTracking.getIdMappings().stream()
                                                .collect(Collectors.toMap(IdMapping::getName, Function.identity())))
                        .orElse(Collections.emptyMap());
        return avroSchemaConverter.toInternalSchema(schema,null,fieldNameToIdMapping);
    }

    // check which methods is best for the conversion
    private InternalSchema _toInternalSchema(
            MessageType schema, String parentPath, Map<String, IdMapping> fieldNameToIdMapping) {
        org.apache.parquet.avro.AvroSchemaConverter avroParquetSchemaConverter = new org.apache.parquet.avro.AvroSchemaConverter();
        Schema avroSchema = avroParquetSchemaConverter.convert(schema);
        AvroSchemaConverter avroSchemaConverter =  AvroSchemaConverter.getInstance();
        return avroSchemaConverter.toInternalSchema(avroSchema,parentPath,fieldNameToIdMapping);
    }

    /**
     * Converts the parquet {@link Schema} to {@link InternalSchema}.
     *
     * @param schema               The schema being converted
     * @param parentPath           If this schema is nested within another, this will be a dot separated string
     *                             representing the path from the top most field to the current schema.
     * @param fieldNameToIdMapping map of fieldName to IdMapping to track field IDs provided by the
     *                             source schema. If source schema does not contain IdMappings, map will be empty.
     * @return a converted schema
     */
    private InternalSchema toInternalSchema(
            MessageType schema, String parentPath, Map<String, IdMapping> fieldNameToIdMapping) {
        // TODO - Does not handle recursion in parquet schema
        InternalType newDataType;
        PrimitiveType typeName;
        LogicalTypeAnnotation logicalType;
        Map<InternalSchema.MetadataKey, Object> metadata = new HashMap<>();
        if (schema.isPrimitive()) {
            typeName = schema.asPrimitiveType();
            switch (typeName.getPrimitiveTypeName()) {
                //PrimitiveTypes
                case INT64:
                    logicalType = schema.getLogicalTypeAnnotation();
                    if (logicalType instanceof LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) {
                        LogicalTypeAnnotation.TimeUnit timeUnit = logicalType.getUnit();
                        if (timeUnit == LogicalTypeAnnotation.TimeUnit.MICROS) {
                            newDataType = InternalType.TIMESTAMP;
                            metadata.put(
                                    InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MICROS);
                        } else if (timeUnit == LogicalTypeAnnotation.TimeUnit.MILLIS) {
                            newDataType = InternalType.TIMESTAMP_NTZ;
                            metadata.put(
                                    InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MILLIS);
                        } else if (timeUnit == LogicalTypeAnnotation.TimeUnit.NANOS) {
                            newDataType = InternalType.TIMESTAMP_NTZ;
                            metadata.put(
                                    InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.NANOS);
                        }
                    } else if (logicalType instanceof LogicalTypeAnnotation.IntLogicalTypeAnnotation) {
                        newDataType = InternalType.INT;
                    } else if (logicalType instanceof LogicalTypeAnnotation.TimeLogicalTypeAnnotation) {
                        LogicalTypeAnnotation.TimeUnit timeUnit = logicalType.getUnit();
                        if (timeUnit == LogicalTypeAnnotation.TimeUnit.MICROS || timeUnit == LogicalTypeAnnotation.TimeUnit.NANOS) {
                            // check if INT is the InternalType needed here
                            newDataType = InternalType.INT;
                        }
                    } else {
                        newDataType = InternalType.INT;
                    }
                    break;
                case INT32:
                    logicalType = schema.getLogicalTypeAnnotation();
                    if (logicalType instanceof LogicalTypeAnnotation.DateLogicalTypeAnnotation) {
                        newDataType = InternalType.DATE;
                    } else if (logicalType instanceof TimeLogicalTypeAnnotation) {
                        LogicalTypeAnnotation.TimeUnit timeUnit = logicalType.getUnit();
                        if (timeUnit == LogicalTypeAnnotation.TimeUnit.MILLIS) {
                            // check if INT is the InternalType needed here
                            newDataType = InternalType.INT;
                        }
                    } else {
                        newDataType = InternalType.INT;
                    }
                    break;
                case INT96:
                    newDataType = InternalType.INT;
                    break;
                case FLOAT:
                    logicalType = schema.getLogicalTypeAnnotation();
                    if (logicalType instanceof LogicalTypeAnnotation.Float16LogicalTypeAnnotation) {
                        newDataType = InternalType.FLOAT;
                    } else if (logicalType instanceof LogicalTypeAnnotation.DecimalLogicalTypeAnnotation) {
                        metadata.put(
                                InternalSchema.MetadataKey.DECIMAL_PRECISION,
                                logicalType.getPrecision());
                        metadata.put(
                                InternalSchema.MetadataKey.DECIMAL_SCALE,
                                logicalType.getScale());
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
                //TODO add other logicalTypes?
                case BINARY:
                    //? Variant,GEOMETRY, GEOGRAPHY,
                    logicalType = schema.getLogicalTypeAnnotation();
                    if (logicalType instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
                        metadata.put(InternalSchema.MetadataKey.ENUM_VALUES, schema.toOriginalType().values());
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
                case UNKNOWN:
                    newDataType = InternalType.NULL;
                    break;
                default:
                    throw new UnsupportedSchemaTypeException(
                            String.format("Unsupported schema type %s", schema));
            }
        } else {
            //GroupTypes
            typeName = schema.asGroupType();
            switch (typeName.getOriginalType()) {
                case LIST:
                    IdMapping elementMapping = fieldNameToIdMapping.get(ELEMENT);
                    InternalSchema elementSchema =
                            toInternalSchema(
                                    schema.getName(),
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
                            .comment(schema.toString())
                            .isNullable(groupTypeContainsNull(schema))
                            .fields(Collections.singletonList(elementField))
                            .build();

                case MAP:
                    IdMapping keyMapping = fieldNameToIdMapping.get(KEY);
                    IdMapping valueMapping = fieldNameToIdMapping.get(VALUE);
                    InternalSchema valueSchema =
                            toInternalSchema(
                                    schema.getName(),
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
                            .comment(schema.toString())
                            .isNullable(groupTypeContainsNull(schema))
                            .fields(
                                    Arrays.asList(
                                            MAP_KEY_FIELD.toBuilder()
                                                    .parentPath(parentPath)
                                                    .fieldId(keyMapping == null ? null : keyMapping.getId())
                                                    .build(),
                                            valueField))
                            .build();
                default:
                    throw new UnsupportedSchemaTypeException(
                            String.format("Unsupported schema type %s", schema));
            }
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

    /**
     * Converts the {@link InternalSchema} to parquet {@link Schema}.
     *
     * @param internalSchema internal schema representation
     * @return an parquet schema
     */
    public Schema _fromInternalSchema(InternalSchema internalSchema) {
        return fromInternalSchema(internalSchema, null);
    }

    // check which methods is best for the conversion
    private MessageType fromInternalSchema(
            InternalSchema internalSchema, String currentPath) {
        org.apache.parquet.avro.AvroSchemaConverter avroParquetSchemaConverter = new org.apache.parquet.avro.AvroSchemaConverter();
        AvroSchemaConverter avroSchemaConverter =  AvroSchemaConverter.getInstance();
        Schema avroSchema = avroSchemaConverter.fromInternalSchema(internalSchema,currentPath);
        MessageType parquetSchema = avroParquetSchemaConverter.convert(avroSchema);
        return parquetSchema;
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
        switch (internalSchema.getDataType()) {
            /*case BYTES:
                return finalizeSchema(Schema.create(Schema.Type.BYTES), internalSchema);
            case BOOLEAN:
                return finalizeSchema(Schema.create(Schema.Type.BOOLEAN), internalSchema);*/
            case INT:
                return finalizeSchema(LogicalTypeAnnotation.intType(32), internalSchema);
            case LONG:
                return finalizeSchema(LogicalTypeAnnotation.intType(64), internalSchema);
            case STRING:
                return finalizeSchema(LogicalTypeAnnotation.stringType(), internalSchema);
            case FLOAT:
                return finalizeSchema(LogicalTypeAnnotation.float16Type(), internalSchema);
            case DOUBLE:
                int precision =
                        (int) internalSchema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_PRECISION);
                int scale =
                        (int) internalSchema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_SCALE);
                return finalizeSchema(LogicalTypeAnnotation.decimalType(scale, precision), internalSchema);
                // TODO check how to create ENUM
            case ENUM:
                return finalizeSchema(
                        new org.apache.parquet.avro.AvroSchemaConverter().convert(Schema.createEnum(
                                internalSchema.getName(),
                                internalSchema.getComment(),
                                null,
                                (List<String>)
                                        internalSchema.getMetadata().get(InternalSchema.MetadataKey.ENUM_VALUES),
                                null)),
                        internalSchema);
            case DATE:
                return finalizeSchema(
                        LogicalTypeAnnotation.dateType(), internalSchema);
            case TIMESTAMP:
                if (internalSchema.getMetadata().get(InternalSchema.MetadataKey.TIMESTAMP_PRECISION)
                        == InternalSchema.MetadataValue.MICROS) {
                    return finalizeSchema(
                             LogicalTypeAnnotation.timestampType(True, Repetition.MICROS),
                            internalSchema);
                } if (internalSchema.getMetadata().get(InternalSchema.MetadataKey.TIMESTAMP_PRECISION)
                    == InternalSchema.MetadataValue.MILLIS) {
                return finalizeSchema(
                         LogicalTypeAnnotation.timestampType(True, Repetition.MILLIS),
                        internalSchema);
            } else if (internalSchema.getMetadata().get(InternalSchema.MetadataKey.TIMESTAMP_PRECISION)
                    == InternalSchema.MetadataValue.NANOS) {
                return finalizeSchema(
                        LogicalTypeAnnotation.timestampType(True, Repetition.NANOS),
                        internalSchema);
            }
            case TIMESTAMP_NTZ:
                if (internalSchema.getMetadata().get(InternalSchema.MetadataKey.TIMESTAMP_PRECISION)
                        == InternalSchema.MetadataValue.MICROS) {
                    return finalizeSchema(
                            LogicalTypeAnnotation.timestampType(True, MICROS),
                            internalSchema);
                } else {
                    return finalizeSchema(
                            LogicalTypeAnnotation.timestampType(True, MILLIS),
                            internalSchema);
                }
                // TODO check from here FIXED, LIST and MAP types (still to todo)
            case LIST:
                InternalField elementField =
                        internalSchema.getFields().stream()
                                .filter(
                                        field ->
                                                InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME.equals(field.getName()))
                                .findFirst()
                                .orElseThrow(() -> new SchemaExtractorException("Invalid array schema"));
                return finalizeSchema(
                        new org.apache.parquet.avro.AvroSchemaConverter().convert(Schema.createArray(
                                fromInternalSchema(elementField.getSchema(), elementField.getPath()))),
                        internalSchema);
            case MAP:
                InternalField valueField =
                        internalSchema.getFields().stream()
                                .filter(
                                        field -> InternalField.Constants.MAP_VALUE_FIELD_NAME.equals(field.getName()))
                                .findFirst()
                                .orElseThrow(() -> new SchemaExtractorException("Invalid map schema"));
                return finalizeSchema(
                        new org.apache.parquet.avro.AvroSchemaConverter().convert(Schema.createMap(fromInternalSchema(valueField.getSchema(), valueField.getPath()))),
                        internalSchema);
            case DECIMAL:
                int precision =
                        (int) internalSchema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_PRECISION);
                int scale =
                        (int) internalSchema.getMetadata().get(InternalSchema.MetadataKey.DECIMAL_SCALE);
//                Integer size =
//                        (Integer) internalSchema.getMetadata().get(InternalSchema.MetadataKey.FIXED_BYTES_SIZE);
//                if (size == null) {
                return finalizeSchema(
                        LogicalTypeAnnotation.decimalType(scale, precision),
                        internalSchema);
//                } else {
//                    return finalizeSchema(
//                            LogicalTypes.decimal(precision, scale)
//                                    .addToSchema(
//                                            Schema.createFixed(
//                                                    internalSchema.getName(), internalSchema.getComment(), null, size)),
//                            internalSchema);
//                }
            case FIXED:
                Integer fixedSize =
                        (Integer) internalSchema.getMetadata().get(InternalSchema.MetadataKey.FIXED_BYTES_SIZE);
                return finalizeSchema(
                        new org.apache.parquet.avro.AvroSchemaConverter().convert(Schema.createFixed(
                                internalSchema.getName(), internalSchema.getComment(), null, fixedSize)),
                        internalSchema);
            case UUID:
                /*Schema uuidSchema =
                        Schema.createFixed(internalSchema.getName(), internalSchema.getComment(), null, 16);
                uuidSchema.addProp(InternalSchema.XTABLE_LOGICAL_TYPE, "uuid");*/
                return finalizeSchema(LogicalTypeAnnotation.uuidType(), internalSchema);
            default:
                throw new UnsupportedSchemaTypeException(
                        "Encountered unhandled type during InternalSchema to parquet conversion: "
                                + internalSchema.getDataType());
        }
    }

    private String buildCurrentPath(InternalField field, String parentPath) {
        return Optional.ofNullable(parentPath)
                .map(path -> path + "." + field.getName())
                .orElse(field.getName());
    }
}
