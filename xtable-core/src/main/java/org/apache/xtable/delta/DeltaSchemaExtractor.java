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
 
package org.apache.xtable.delta;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructType;

import org.apache.xtable.collectors.CustomCollectors;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.schema.SchemaUtils;

/**
 * Converts between Delta and InternalTable schemas. Some items to be aware of:
 *
 * <ul>
 *   <li>Delta schemas are represented as Spark StructTypes which do not have enums so the enum
 *       types are lost when converting from XTable to Delta Lake representations
 *   <li>Delta does not have a fixed length byte array option so {@link InternalType#FIXED} is
 *       simply translated to a {@link org.apache.spark.sql.types.BinaryType}
 *   <li>Similarly, {@link InternalType#TIMESTAMP_NTZ} is translated to a long in Delta Lake
 * </ul>
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaSchemaExtractor {
  private static final String DELTA_COLUMN_MAPPING_ID = "delta.columnMapping.id";
  private static final DeltaSchemaExtractor INSTANCE = new DeltaSchemaExtractor();
  // Timestamps in Delta are microsecond precision by default
  private static final Map<InternalSchema.MetadataKey, Object>
      DEFAULT_TIMESTAMP_PRECISION_METADATA =
          Collections.singletonMap(
              InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MICROS);

  public static DeltaSchemaExtractor getInstance() {
    return INSTANCE;
  }

  public InternalSchema toInternalSchema(StructType structType) {
    return toInternalSchema(structType, null, false, null, null);
  }

  private InternalSchema toInternalSchema(
      DataType dataType,
      String parentPath,
      boolean nullable,
      String comment,
      Metadata originalMetadata) {
    Map<InternalSchema.MetadataKey, Object> metadata = null;
    List<InternalField> fields = null;
    InternalType type;
    String typeName = dataType.typeName();
    // trims parameters to type name for matching
    int openParenIndex = typeName.indexOf("(");
    String trimmedTypeName = openParenIndex > 0 ? typeName.substring(0, openParenIndex) : typeName;
    switch (trimmedTypeName) {
      case "short":
        type = InternalType.INT;
        break;
      case "integer":
        type = InternalType.INT;
        break;
      case "string":
        type = InternalType.STRING;
        break;
      case "boolean":
        type = InternalType.BOOLEAN;
        break;
      case "float":
        type = InternalType.FLOAT;
        break;
      case "double":
        type = InternalType.DOUBLE;
        break;
      case "binary":
        if (originalMetadata.contains(InternalSchema.XTABLE_LOGICAL_TYPE)
            && "uuid".equals(originalMetadata.getString(InternalSchema.XTABLE_LOGICAL_TYPE))) {
          type = InternalType.UUID;
        } else {
          type = InternalType.BYTES;
        }
        break;
      case "long":
        type = InternalType.LONG;
        break;
      case "date":
        type = InternalType.DATE;
        break;
      case "timestamp":
        type = InternalType.TIMESTAMP;
        metadata = DEFAULT_TIMESTAMP_PRECISION_METADATA;
        break;
      case "timestamp_ntz":
        type = InternalType.TIMESTAMP_NTZ;
        metadata = DEFAULT_TIMESTAMP_PRECISION_METADATA;
        break;
      case "struct":
        StructType structType = (StructType) dataType;
        fields =
            Arrays.stream(structType.fields())
                .filter(
                    field ->
                        !field
                            .metadata()
                            .contains(DeltaPartitionExtractor.DELTA_GENERATION_EXPRESSION))
                .map(
                    field -> {
                      Integer fieldId =
                          field.metadata().contains(DELTA_COLUMN_MAPPING_ID)
                              ? (int) field.metadata().getLong(DELTA_COLUMN_MAPPING_ID)
                              : null;
                      String fieldComment =
                          field.getComment().isDefined() ? field.getComment().get() : null;
                      InternalSchema schema =
                          toInternalSchema(
                              field.dataType(),
                              SchemaUtils.getFullyQualifiedPath(parentPath, field.name()),
                              field.nullable(),
                              fieldComment,
                              field.metadata());
                      return InternalField.builder()
                          .name(field.name())
                          .fieldId(fieldId)
                          .parentPath(parentPath)
                          .schema(schema)
                          .defaultValue(
                              field.nullable() ? InternalField.Constants.NULL_DEFAULT_VALUE : null)
                          .build();
                    })
                .collect(CustomCollectors.toList(structType.fields().length));
        type = InternalType.RECORD;
        break;
      case "decimal":
        DecimalType decimalType = (DecimalType) dataType;
        metadata = new HashMap<>(2, 1.0f);
        metadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, decimalType.precision());
        metadata.put(InternalSchema.MetadataKey.DECIMAL_SCALE, decimalType.scale());
        type = InternalType.DECIMAL;
        break;
      case "array":
        ArrayType arrayType = (ArrayType) dataType;
        InternalSchema elementSchema =
            toInternalSchema(
                arrayType.elementType(),
                SchemaUtils.getFullyQualifiedPath(
                    parentPath, InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME),
                arrayType.containsNull(),
                null,
                null);
        InternalField elementField =
            InternalField.builder()
                .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                .parentPath(parentPath)
                .schema(elementSchema)
                .build();
        type = InternalType.LIST;
        fields = Collections.singletonList(elementField);
        break;
      case "map":
        MapType mapType = (MapType) dataType;
        InternalSchema keySchema =
            toInternalSchema(
                mapType.keyType(),
                SchemaUtils.getFullyQualifiedPath(
                    parentPath, InternalField.Constants.MAP_VALUE_FIELD_NAME),
                false,
                null,
                null);
        InternalField keyField =
            InternalField.builder()
                .name(InternalField.Constants.MAP_KEY_FIELD_NAME)
                .parentPath(parentPath)
                .schema(keySchema)
                .build();
        InternalSchema valueSchema =
            toInternalSchema(
                mapType.valueType(),
                SchemaUtils.getFullyQualifiedPath(
                    parentPath, InternalField.Constants.MAP_VALUE_FIELD_NAME),
                mapType.valueContainsNull(),
                null,
                null);
        InternalField valueField =
            InternalField.builder()
                .name(InternalField.Constants.MAP_VALUE_FIELD_NAME)
                .parentPath(parentPath)
                .schema(valueSchema)
                .build();
        type = InternalType.MAP;
        fields = Arrays.asList(keyField, valueField);
        break;
      default:
        throw new NotSupportedException("Unsupported type: " + dataType.typeName());
    }
    return InternalSchema.builder()
        .name(trimmedTypeName)
        .dataType(type)
        .comment(comment)
        .isNullable(nullable)
        .metadata(metadata)
        .fields(fields)
        .build();
  }
}
