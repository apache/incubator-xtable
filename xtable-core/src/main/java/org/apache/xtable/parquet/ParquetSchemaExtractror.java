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

import org.apache.parquet.LogicalType;
import org.apache.parquet.LogicalTypes;
import org.apache.parquet.Schema;
import org.apache.parquet.Schema.Type;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.format.NullType;
import org.apache.parquet.schema.LogicalTypeAnnotation;


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

    private static Type finalizeSchema(Type targetSchema, InternalSchema inputSchema) {
        if (inputSchema.isNullable()) {
            return targetSchema.union(LogicalTypeAnnotation.unknownType())
        }
        return targetSchema;
    }

    private static boolean groupTypeContainsNull(Type schema) {
        for (Type field in schema.getFields()){
            if (field == null) {
                return True;
            }
        }
        return False;
    }

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
            Type schema, String parentPath, Map<String, IdMapping> fieldNameToIdMapping) {
        // TODO - Does not handle recursion in parquet schema
        InternalType newDataType;
        LogicalTypeAnnotation logicalType;
        Map<InternalSchema.MetadataKey, Object> metadata = new HashMap<>();
        switch (schema.getName()) {
            //PrimitiveTypes
            case "INT64":
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
            case "INT32":
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
                // is also a TIME
                break;
            case "INT96":
                newDataType = InternalType.INT;
                break;
            case "FLOAT":
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
            case "BYTES":
                newDataType = InternalType.BYTES;
                break;
            case FIXED_LEN_BYTE_ARRAY:
                logicalType = schema.getLogicalTypeAnnotation()
                if (logicalType instanceof LogicalTypeAnnotation.UUIDLogicalTypeAnnotation) {
                    newDataType = InternalType.UUID;
                } else {
                    newDataType = InternalType.BYTES;
                }
                break;
            //TODO add other logicalTypes ?
            case "BYTE_ARRAY":
                // includes metadata (?) for json,BSON, Variant,GEOMETRY, geography,
                logicalType = schema.getLogicalTypeAnnotation()
                if (logicalType instanceof LogicalTypeAnnotation.EnumLogicalTypeAnnotation) {
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
            case "BOOLEAN":
                newDataType = InternalType.BOOLEAN;
                break;
            case "UNKNOWN":
                newDataType = InternalType.NULL;
                break;
            //GroupTypes
            case "LIST":
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

            case "MAP":
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
    public Schema fromInternalSchema(InternalSchema internalSchema) {
        return fromInternalSchema(internalSchema, null);
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
    private Schema fromInternalSchema(InternalSchema internalSchema, String currentPath) {
        switch (internalSchema.getDataType()) {
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
            case UUID:
                Schema uuidSchema =
                        Schema.createFixed(internalSchema.getName(), internalSchema.getComment(), null, 16);
                uuidSchema.addProp(InternalSchema.XTABLE_LOGICAL_TYPE, "uuid");
                return finalizeSchema(uuidSchema, internalSchema);
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
