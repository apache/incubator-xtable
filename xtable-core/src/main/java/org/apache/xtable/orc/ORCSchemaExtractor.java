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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import lombok.AccessLevel;
import lombok.NoArgsConstructor;


import org.apache.xtable.exception.UnsupportedSchemaTypeException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.schema.SchemaUtils;
import org.apache.orc.TypeDescription;

/**
 * Class that converts Avro Schema {@link } to Canonical Schema {@link InternalSchema} and
 * vice-versa. This conversion is fully reversible and there is a strict 1 to 1 mapping between avro
 * data types and canonical data types.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ORCSchemaExtractor {
    private static final org.apache.xtable.orc.ORCSchemaExtractor INSTANCE = new org.apache.xtable.orc.ORCSchemaExtractor();

    public static org.apache.xtable.orc.ORCSchemaExtractor getInstance() {
        return INSTANCE;
    }

    private static boolean isNullable(TypeDescription schema) {
        List<TypeDescription> subFields = schema.getChildren();
        for (TypeDescription subField : subFields) {
            if (subField.equals(null)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Converts the ORC {@link } to {@link InternalSchema}.
     *
     * @param schema     The schema being converted
     * @param parentPath If this schema is nested within another, this will be a dot separated string
     *                   representing the path from the top most field to the current schema.
     * @return a converted schema
     */
    //TODO other types and precision and scale for decimal types
    private InternalSchema toInternalSchema(
            TypeDescription schema, String parentPath) {
        InternalType newDataType;
        Map<InternalSchema.MetadataKey, Object> metadata = new HashMap<>();
        switch (schema.
                getCategory()) {
            case INT:
                newDataType = InternalType.INT;
                break;
            case STRING:
                newDataType = InternalType.STRING;
                break;
            case BOOLEAN:
                newDataType = InternalType.BOOLEAN;
                break;
            case BYTE:
                newDataType = InternalType.BYTES;
                break;
            case DOUBLE:
                newDataType = InternalType.DOUBLE;
                break;
            case FLOAT:
                newDataType = InternalType.FLOAT;
                break;
            case LONG:
                newDataType = InternalType.LONG;
                break;
            case DECIMAL:
                newDataType = InternalType.DECIMAL;
                break;
            case LIST:
                int childId = schema.getId();
                InternalSchema elementSchema =
                        toInternalSchema(
                                schema,
                                SchemaUtils.getFullyQualifiedPath(
                                        parentPath, InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                        );
                InternalField elementField =
                        InternalField.builder()
                                .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                .parentPath(parentPath)
                                .schema(elementSchema)
                                .fieldId(childId)
                                .build();
                return InternalSchema.builder()
                        .name(schema.getFullFieldName())
                        .dataType(InternalType.LIST)
                        .comment(schema.toString())
                        .isNullable(isNullable(schema))
                        .fields(Collections.singletonList(elementField))
                        .build();
            case UNION:
            default:
                throw new UnsupportedSchemaTypeException(
                        String.format("Unsupported schema type %s", schema));
        }
        return InternalSchema.builder()
                .name(schema.getFullFieldName())
                .dataType(newDataType)
                .comment(schema.toString())
                .isNullable(isNullable(schema))
                .metadata(metadata.isEmpty() ? null : metadata)
                .build();
    }

    // TODO refine fromInternalSchema types
    public TypeDescription fromInternalSchema(InternalSchema internalSchema, String currentPath) {
        TypeDescription type = null;
        String fieldName = internalSchema.getName();
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
            case DECIMAL:
                type = TypeDescription.createDecimal();
                break;
            case DATE:
                type = TypeDescription.createDate();
                break;
            case TIMESTAMP:
                type = TypeDescription.createTimestamp();
                break;
            default:
                throw new UnsupportedSchemaTypeException(
                        "Encountered unhandled type during InternalSchema to ORC conversion:"
                                + internalType);
        }
        return type;
    }
}

