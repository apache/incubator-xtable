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
 
package io.onetable.iceberg;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import io.onetable.exception.NotSupportedException;
import io.onetable.exception.SchemaExtractorException;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;

/**
 * Schema extractor for Iceberg which converts canonical representation of the schema{@link
 * OneSchema} to Iceberg's schema representation {@link Schema}
 */
@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IcebergSchemaExtractor {
  private static final IcebergSchemaExtractor INSTANCE = new IcebergSchemaExtractor();
  private static final String MAP_KEY_FIELD_NAME = "key";
  private static final String MAP_VALUE_FIELD_NAME = "value";
  private static final String LIST_ELEMENT_FIELD_NAME = "element";

  public static IcebergSchemaExtractor getInstance() {
    return INSTANCE;
  }

  public Schema toIceberg(OneSchema oneSchema) {
    // if field IDs are not assigned in the source, just use an incrementing integer
    AtomicInteger fieldIdTracker = new AtomicInteger(0);
    List<Types.NestedField> nestedFields = convertFields(oneSchema, fieldIdTracker);
    List<OneField> recordKeyFields = oneSchema.getRecordKeyFields();
    if (recordKeyFields.isEmpty()) {
      return new Schema(nestedFields);
    }
    // Find field in iceberg schema that matches each of the record key path and collect ids.
    Schema partialSchema = new Schema(nestedFields);
    Set<Integer> recordKeyIds =
        recordKeyFields.stream()
            .map(keyField -> partialSchema.findField(convertFromOneTablePath(keyField.getPath())))
            .filter(Objects::nonNull)
            .map(Types.NestedField::fieldId)
            .collect(Collectors.toSet());
    if (recordKeyFields.size() != recordKeyIds.size()) {
      List<String> missingFieldPaths =
          recordKeyFields.stream()
              .map(OneField::getPath)
              .filter(path -> partialSchema.findField(convertFromOneTablePath(path)) == null)
              .collect(Collectors.toList());
      log.error("Missing field IDs for record key field paths: " + missingFieldPaths);
      throw new SchemaExtractorException("Mismatches in converting record key fields");
    }
    return new Schema(nestedFields, recordKeyIds);
  }

  static String convertFromOneTablePath(String path) {
    return path.replace(OneField.Constants.MAP_KEY_FIELD_NAME, MAP_KEY_FIELD_NAME)
        .replace(OneField.Constants.MAP_VALUE_FIELD_NAME, MAP_VALUE_FIELD_NAME)
        .replace(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME, LIST_ELEMENT_FIELD_NAME);
  }

  private List<Types.NestedField> convertFields(OneSchema schema, AtomicInteger fieldIdTracker) {
    // mirror iceberg pattern of assigning IDs for a level before recursing
    List<Integer> ids =
        schema.getFields().stream()
            .map(
                field ->
                    field.getFieldId() == null
                        ? fieldIdTracker.incrementAndGet()
                        : field.getFieldId())
            .collect(Collectors.toList());
    List<Types.NestedField> nestedFields = new ArrayList<>(schema.getFields().size());
    for (int i = 0; i < schema.getFields().size(); i++) {
      OneField field = schema.getFields().get(i);
      nestedFields.add(
          Types.NestedField.of(
              ids.get(i),
              field.getSchema().isNullable(),
              field.getName(),
              convertFieldType(field, fieldIdTracker),
              field.getSchema().getComment()));
    }
    return nestedFields;
  }

  private Type convertFieldType(OneField field, AtomicInteger fieldIdTracker) {
    switch (field.getSchema().getDataType()) {
      case ENUM:
      case STRING:
        return Types.StringType.get();
      case INT:
        return Types.IntegerType.get();
      case LONG:
      case TIMESTAMP_NTZ:
        return Types.LongType.get();
      case BYTES:
        return Types.BinaryType.get();
      case FIXED:
        int size =
            (int) field.getSchema().getMetadata().get(OneSchema.MetadataKey.FIXED_BYTES_SIZE);
        return Types.FixedType.ofLength(size);
      case BOOLEAN:
        return Types.BooleanType.get();
      case FLOAT:
        return Types.FloatType.get();
      case DATE:
        return Types.DateType.get();
      case TIMESTAMP:
        return Types.TimestampType.withZone();
      case DOUBLE:
        return Types.DoubleType.get();
      case DECIMAL:
        int precision =
            (int) field.getSchema().getMetadata().get(OneSchema.MetadataKey.DECIMAL_PRECISION);
        int scale = (int) field.getSchema().getMetadata().get(OneSchema.MetadataKey.DECIMAL_SCALE);
        return Types.DecimalType.of(precision, scale);
      case RECORD:
        return Types.StructType.of(convertFields(field.getSchema(), fieldIdTracker));
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
        int keyId = key.getFieldId() == null ? fieldIdTracker.incrementAndGet() : key.getFieldId();
        int valueId =
            value.getFieldId() == null ? fieldIdTracker.incrementAndGet() : value.getFieldId();
        if (field.getSchema().isNullable()) {
          return Types.MapType.ofOptional(
              keyId,
              valueId,
              convertFieldType(key, fieldIdTracker),
              convertFieldType(value, fieldIdTracker));
        } else {
          return Types.MapType.ofRequired(
              keyId,
              valueId,
              convertFieldType(key, fieldIdTracker),
              convertFieldType(value, fieldIdTracker));
        }
      case ARRAY:
        OneField element =
            field.getSchema().getFields().stream()
                .filter(
                    arrayField ->
                        OneField.Constants.ARRAY_ELEMENT_FIELD_NAME.equals(arrayField.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid array schema"));
        int elementId =
            element.getFieldId() == null ? fieldIdTracker.incrementAndGet() : element.getFieldId();
        if (field.getSchema().isNullable()) {
          return Types.ListType.ofOptional(elementId, convertFieldType(element, fieldIdTracker));
        } else {
          return Types.ListType.ofRequired(elementId, convertFieldType(element, fieldIdTracker));
        }
      default:
        throw new NotSupportedException("Unsupported type: " + field.getSchema().getDataType());
    }
  }
}
