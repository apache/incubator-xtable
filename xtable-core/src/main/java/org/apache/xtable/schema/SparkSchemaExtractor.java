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
 
package org.apache.xtable.schema;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.SchemaExtractorException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;

public class SparkSchemaExtractor {

  private static final SparkSchemaExtractor INSTANCE = new SparkSchemaExtractor();
  private static final String COMMENT = "comment";

  public static SparkSchemaExtractor getInstance() {
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
                        getMetaData(field.getSchema())))
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
        return DataTypes.LongType;
      case BYTES:
      case FIXED:
      case UUID:
        return DataTypes.BinaryType;
      case BOOLEAN:
        return DataTypes.BooleanType;
      case FLOAT:
        return DataTypes.FloatType;
      case DATE:
        return DataTypes.DateType;
      case TIMESTAMP:
        return DataTypes.TimestampType;
      case TIMESTAMP_NTZ:
        return DataTypes.TimestampNTZType;
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

  private Metadata getMetaData(InternalSchema schema) {
    InternalType type = schema.getDataType();
    MetadataBuilder metadataBuilder = new MetadataBuilder();
    if (type == InternalType.UUID) {
      metadataBuilder.putString(InternalSchema.XTABLE_LOGICAL_TYPE, "uuid");
    }
    if (schema.getComment() != null) {
      metadataBuilder.putString(COMMENT, schema.getComment());
    }
    return metadataBuilder.build();
  }
}
