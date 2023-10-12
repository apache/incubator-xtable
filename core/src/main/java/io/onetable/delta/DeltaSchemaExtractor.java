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

package io.onetable.delta;

import io.onetable.exception.NotSupportedException;
import io.onetable.exception.SchemaExtractorException;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.OneType;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.avro.LogicalTypes;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import scala.Option;

/** Extracts the {@link StructType} schema of the target Delta table from {@link OneSchema}. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaSchemaExtractor {
  private static final DeltaSchemaExtractor INSTANCE = new DeltaSchemaExtractor();

  public static DeltaSchemaExtractor getInstance() {
    return INSTANCE;
  }

  public StructType fromOneSchema(OneSchema oneSchema) {
    StructField[] fields =
        oneSchema.getFields().stream()
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

  private DataType convertFieldType(OneField field) {
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
        return DataTypes.ByteType;
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
            (int) field.getSchema().getMetadata().get(OneSchema.MetadataKey.DECIMAL_PRECISION);
        int scale = (int) field.getSchema().getMetadata().get(OneSchema.MetadataKey.DECIMAL_SCALE);
        return DataTypes.createDecimalType(precision, scale);
      case RECORD:
        return fromOneSchema(field.getSchema());
      case MAP:
        OneField key =
            field.getSchema().getFields().stream()
                .filter(
                    mapField -> OneField.Constants.MAP_KEY_FIELD_NAME.equals(mapField.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid map schema"));
        OneField value =
            field.getSchema().getFields().stream()
                .filter(
                    mapField -> OneField.Constants.MAP_VALUE_FIELD_NAME.equals(mapField.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid map schema"));
        return DataTypes.createMapType(
            convertFieldType(key), convertFieldType(value), value.getSchema().isNullable());
      case ARRAY:
        OneField element =
            field.getSchema().getFields().stream()
                .filter(
                    arrayField ->
                        OneField.Constants.ARRAY_ELEMENT_FIELD_NAME.equals(arrayField.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid array schema"));
        return DataTypes.createArrayType(convertFieldType(element), element.getSchema().isNullable());
      default:
        throw new NotSupportedException("Unsupported type: " + field.getSchema().getDataType());
    }
  }

  public OneSchema toOneSchema(StructType structType) {
    return toOneSchema(structType, null, false);
  }

  private OneSchema toOneSchema(StructType structType, String parentPath, boolean nullable) {
    List<OneField> fields = Arrays.stream(structType.fields()).map(structField -> toOneField(structField, parentPath)).collect(Collectors.toList());
    return OneSchema.builder()
        .name(structType.typeName())
        .fields(fields)
        .dataType(OneType.RECORD)
        .isNullable(nullable)
        .build();
  }

  private OneField toOneField(StructField field, String parentPath) {
    OneSchema fieldSchema;
    String typeName = field.dataType().typeName();
    // trims parameters to type name for matching
    int openParenIndex = typeName.indexOf("(");
    String trimmedTypeName = openParenIndex > 0 ? typeName.substring(0, openParenIndex) : typeName;
    switch (trimmedTypeName) {
      case "integer":
        fieldSchema = getSingleFieldSchema(OneType.INT, field, null);
        break;
      case "string":
        fieldSchema = getSingleFieldSchema(OneType.STRING, field, null);
        break;
      case "boolean":
        fieldSchema = getSingleFieldSchema(OneType.BOOLEAN, field, null);
        break;
      case "float":
        fieldSchema = getSingleFieldSchema(OneType.FLOAT, field, null);
        break;
      case "double":
        fieldSchema = getSingleFieldSchema(OneType.DOUBLE, field, null);
        break;
      case "byte":
      case "fixed":
        fieldSchema = getSingleFieldSchema(OneType.BYTES, field, null);
        break;
      case "long":
        fieldSchema = getSingleFieldSchema(OneType.LONG, field, null);
        break;
      case "date":
        fieldSchema = getSingleFieldSchema(OneType.DATE, field, null);
        break;
      case "timestamp":
        fieldSchema = getSingleFieldSchema(OneType.TIMESTAMP, field, null);
        break;
      case "struct":
        fieldSchema = toOneSchema((StructType) field.dataType(), getFullyQualifiedPath(parentPath, field.name()), field.nullable());
        break;
      case "decimal":
        DecimalType decimalType = (DecimalType) field.dataType();
        Map<OneSchema.MetadataKey, Object> metadata = new HashMap<>(2, 1.0f);
        metadata.put(OneSchema.MetadataKey.DECIMAL_PRECISION, decimalType.precision());
        metadata.put(OneSchema.MetadataKey.DECIMAL_SCALE, decimalType.scale());
        fieldSchema = getSingleFieldSchema(OneType.DECIMAL, field, metadata);
        break;
      case "array":
      case "map":
//        MapType mapType = (MapType) field.dataType();
//        OneSchema valueSchema = mapType.valueType() instanceof StructType ?
//            toOneSchema((StructType) mapType.valueType(), getFullyQualifiedPath(parentPath, OneField.Constants.MAP_VALUE_FIELD_NAME), mapType.valueContainsNull()) :
//            OneSchema.builder()
//                .name(mapType.valueType().typeName())
//                .isNullable(mapType.valueContainsNull())
//                .dataType()
//                .build();
//        OneField valueField =
//            OneField.builder()
//                .name(OneField.Constants.MAP_VALUE_FIELD_NAME)
//                .parentPath(parentPath)
//                .schema(valueSchema)
//                .fieldId(valueMapping == null ? null : valueMapping.getId())
//                .build();
//        break;
      default:
        throw new NotSupportedException("Unsupported type: " + field.dataType().typeName());
    }

    return OneField
        .builder()
        .parentPath(parentPath)
        .name(field.name())
        .schema(fieldSchema)
        .defaultValue(field.nullable() ? OneField.Constants.NULL_DEFAULT_VALUE : null)
        .build();
  }

  private OneSchema getSingleFieldSchema(OneType type, StructField field, Map<OneSchema.MetadataKey, Object> metadata) {
    return OneSchema.builder()
        .name(field.dataType().typeName())
        .dataType(type)
        .isNullable(field.nullable())
        .comment(field.getComment().isDefined() ? field.getComment().get() : null)
        .metadata(metadata)
        .build();
  }

  private static String getFullyQualifiedPath(String path, String fieldName) {
    if (path == null || path.isEmpty()) {
      return fieldName;
    }
    return path + "." + fieldName;
  }
}