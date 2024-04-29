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
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.xtable.collectors.CustomCollectors;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.SchemaExtractorException;
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

  public static DeltaSchemaExtractor getInstance() {
    return INSTANCE;
  }

  public StructType fromInternalSchema(InternalSchema internalSchema) {
    StructField[] fields =
        internalSchema.getFields().stream()
            .map(
                field ->
                    new StructField(
                        field.getName(),
                        convertFieldType(field),
                        field.getSchema().isNullable(),
                        Metadata.empty()))
            .toArray(StructField[]::new);
    return new StructType(fields);
  }

  private DataType convertFieldType(InternalField field) {
    switch (field.getSchema().getDataType()) {
      case ENUM:
      case STRING:
        return DataTypes.StringType;
      case INT:
        return DataTypes.IntegerType;
      case LONG:
      case TIMESTAMP_NTZ:
        return DataTypes.LongType;
      case BYTES:
      case FIXED:
        return DataTypes.BinaryType;
      case BOOLEAN:
        return DataTypes.BooleanType;
      case FLOAT:
        return DataTypes.FloatType;
      case DATE:
        return DataTypes.DateType;
      case TIMESTAMP:
        return DataTypes.TimestampType;
      case DOUBLE:
        return DataTypes.DoubleType;
      case DECIMAL:
        int precision =
            (int) field.getSchema().getMetadata().get(InternalSchema.MetadataKey.DECIMAL_PRECISION);
        int scale =
            (int) field.getSchema().getMetadata().get(InternalSchema.MetadataKey.DECIMAL_SCALE);
        return DataTypes.createDecimalType(precision, scale);
      case RECORD:
        return fromInternalSchema(field.getSchema());
      case MAP:
        InternalField key =
            field.getSchema().getFields().stream()
                .filter(
                    mapField ->
                        InternalField.Constants.MAP_KEY_FIELD_NAME.equals(mapField.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid map schema"));
        InternalField value =
            field.getSchema().getFields().stream()
                .filter(
                    mapField ->
                        InternalField.Constants.MAP_VALUE_FIELD_NAME.equals(mapField.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid map schema"));
        return DataTypes.createMapType(
            convertFieldType(key), convertFieldType(value), value.getSchema().isNullable());
      case LIST:
        InternalField element =
            field.getSchema().getFields().stream()
                .filter(
                    arrayField ->
                        InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME.equals(
                            arrayField.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid array schema"));
        return DataTypes.createArrayType(
            convertFieldType(element), element.getSchema().isNullable());
      default:
        throw new NotSupportedException("Unsupported type: " + field.getSchema().getDataType());
    }
  }

  public InternalSchema toInternalSchema(StructType structType) {
    return toInternalSchema(structType, null, false, null);
  }

  private InternalSchema toInternalSchema(
      DataType dataType, String parentPath, boolean nullable, String comment) {
    Map<InternalSchema.MetadataKey, Object> metadata = null;
    List<InternalField> fields = null;
    InternalType type;
    String typeName = dataType.typeName();
    // trims parameters to type name for matching
    int openParenIndex = typeName.indexOf("(");
    String trimmedTypeName = openParenIndex > 0 ? typeName.substring(0, openParenIndex) : typeName;
    switch (trimmedTypeName) {
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
        type = InternalType.BYTES;
        break;
      case "long":
        type = InternalType.LONG;
        break;
      case "date":
        type = InternalType.DATE;
        break;
      case "timestamp":
        type = InternalType.TIMESTAMP;
        // Timestamps in Delta are microsecond precision by default
        metadata =
            Collections.singletonMap(
                InternalSchema.MetadataKey.TIMESTAMP_PRECISION,
                InternalSchema.MetadataValue.MICROS);
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
                              fieldComment);
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
