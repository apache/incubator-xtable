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
 
package org.apache.xtable.kernel;

import java.util.*;

import io.delta.kernel.types.*;

import org.apache.xtable.collectors.CustomCollectors;
import org.apache.xtable.delta.DeltaPartitionExtractor;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.schema.SchemaUtils;

public class DeltaKernelSchemaExtractor {

  private static final String DELTA_COLUMN_MAPPING_ID = "delta.columnMapping.id";
  private static final DeltaKernelSchemaExtractor INSTANCE = new DeltaKernelSchemaExtractor();
  private static final Map<InternalSchema.MetadataKey, Object>
      DEFAULT_TIMESTAMP_PRECISION_METADATA =
          Collections.singletonMap(
              InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MICROS);
  static final String DELTA_GENERATION_EXPRESSION = "delta.generationExpression";

  public static DeltaKernelSchemaExtractor getInstance() {
    return INSTANCE;
  }

  public InternalSchema toInternalSchema(StructType structType) {
    return toInternalSchema(structType, null, false, null, null);
  }

  String trimmedTypeName = "";
  InternalType type = null;

  private InternalSchema toInternalSchema(
      DataType dataType,
      String parentPath,
      boolean nullable,
      String comment,
      FieldMetadata originalMetadata) {

    Map<InternalSchema.MetadataKey, Object> metadata = null;
    List<InternalField> fields = null;

    if (dataType instanceof IntegerType) {
      type = InternalType.INT;
      trimmedTypeName = "integer";
    } else if (dataType instanceof StringType) {
      type = InternalType.STRING;
      trimmedTypeName = "string";
    } else if (dataType instanceof BooleanType) {
      type = InternalType.BOOLEAN;
      trimmedTypeName = "boolean";
    } else if (dataType instanceof FloatType) {
      type = InternalType.FLOAT;
      trimmedTypeName = "float";
    } else if (dataType instanceof DoubleType) {
      type = InternalType.DOUBLE;
      trimmedTypeName = "double";
    } else if (dataType instanceof BinaryType) {
      if (originalMetadata.contains(InternalSchema.XTABLE_LOGICAL_TYPE)
          && "uuid".equals(originalMetadata.getString(InternalSchema.XTABLE_LOGICAL_TYPE))) {
        type = InternalType.UUID;
        trimmedTypeName = "binary";
      } else {
        type = InternalType.BYTES;
        trimmedTypeName = "binary";
      }
    } else if (dataType instanceof LongType) {
      type = InternalType.LONG;
      trimmedTypeName = "long";
    } else if (dataType instanceof DateType) {
      type = InternalType.DATE;
      trimmedTypeName = "date";
    } else if (dataType instanceof TimestampType) {
      type = InternalType.TIMESTAMP;
      metadata = DEFAULT_TIMESTAMP_PRECISION_METADATA;
      trimmedTypeName = "timestamp";
    } else if (dataType instanceof TimestampNTZType) {
      type = InternalType.TIMESTAMP_NTZ;
      metadata = DEFAULT_TIMESTAMP_PRECISION_METADATA;
      trimmedTypeName = "timestamp_ntz";
    } else if (dataType instanceof StructType) {
      // Handle StructType
      StructType structType = (StructType) dataType;
      // your logic here

      fields =
          structType.fields().stream()
              .filter(
                  field ->
                      !field
                          .getMetadata()
                          .contains(DeltaPartitionExtractor.DELTA_GENERATION_EXPRESSION))
              .map(
                  field -> {
                    Integer fieldId =
                        field.getMetadata().contains(DELTA_COLUMN_MAPPING_ID)
                            ? Long.valueOf(field.getMetadata().getLong(DELTA_COLUMN_MAPPING_ID))
                                .intValue()
                            : null;
                    String fieldComment =
                        field.getMetadata().contains("comment")
                            ? field.getMetadata().getString("comment")
                            : null;
                    InternalSchema schema =
                        toInternalSchema(
                            field.getDataType(),
                            SchemaUtils.getFullyQualifiedPath(parentPath, field.getName()),
                            field.isNullable(),
                            fieldComment,
                            field.getMetadata());
                    return InternalField.builder()
                        .name(field.getName())
                        .fieldId(fieldId)
                        .parentPath(parentPath)
                        .schema(schema)
                        .defaultValue(
                            field.isNullable() ? InternalField.Constants.NULL_DEFAULT_VALUE : null)
                        .build();
                  })
              .collect(CustomCollectors.toList(structType.fields().size()));
      type = InternalType.RECORD;
      trimmedTypeName = "struct";
    } else if (dataType instanceof DecimalType) {
      DecimalType decimalType = (DecimalType) dataType;
      metadata = new HashMap<>(2, 1.0f);
      metadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, decimalType.getPrecision());
      metadata.put(InternalSchema.MetadataKey.DECIMAL_SCALE, decimalType.getScale());
      type = InternalType.DECIMAL;
      trimmedTypeName = "decimal";

    } else if (dataType instanceof ArrayType) {
      ArrayType arrayType = (ArrayType) dataType;
      InternalSchema elementSchema =
          toInternalSchema(
              arrayType.getElementType(),
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
      trimmedTypeName = "array";
    } else if (dataType instanceof MapType) {
      MapType mapType = (MapType) dataType;
      InternalSchema keySchema =
          toInternalSchema(
              mapType.getKeyType(),
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
              mapType.getValueType(),
              SchemaUtils.getFullyQualifiedPath(
                  parentPath, InternalField.Constants.MAP_VALUE_FIELD_NAME),
              mapType.isValueContainsNull(),
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
      trimmedTypeName = "map";
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
