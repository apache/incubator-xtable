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
 
package org.apache.xtable.glue;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.SchemaExtractorException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;

import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.Table;

@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GlueSchemaExtractor {
  private static final GlueSchemaExtractor INSTANCE = new GlueSchemaExtractor();
  private static final String FIELD_ID = "field.id";
  private static final String FIELD_OPTIONAL = "field.optional";
  private static final String FIELD_CURRENT = "field.current";
  private static final int MAX_COLUMN_COMMENT_LENGTH = 255;

  public static GlueSchemaExtractor getInstance() {
    return INSTANCE;
  }

  /**
   * Extract column list from InternalTable schema
   *
   * @param tableFormat tableFormat to handle format specific type conversion
   * @param tableSchema InternalTable schema
   * @return glue table column list
   */
  public List<Column> toColumns(String tableFormat, InternalSchema tableSchema) {
    return toColumns(tableFormat, tableSchema, null);
  }

  public List<Column> toColumns(
      String tableFormat, InternalSchema tableSchema, Table existingTable) {
    List<Column> columns = Lists.newArrayList();
    Set<String> addedNames = Sets.newHashSet();
    for (InternalField field : tableSchema.getFields()) {
      if (!addedNames.contains(field.getName())) {
        columns.add(toColumn(field, tableFormat));
        addedNames.add(field.getName());
      }
    }

    // if there are columns in existing glueTable that are not part of tableSchema,
    // include them by setting "field.current" property to false
    List<Column> existingColumns =
        existingTable != null && existingTable.storageDescriptor() != null
            ? existingTable.storageDescriptor().columns()
            : Collections.emptyList();
    for (Column column : existingColumns) {
      if (!addedNames.contains(column.name())) {
        Map<String, String> columnParams = new HashMap<>();
        if (column.hasParameters()) {
          columnParams.putAll(column.parameters());
        }
        columnParams.put(getColumnProperty(tableFormat, FIELD_CURRENT), "false");
        column = column.toBuilder().parameters(columnParams).build();
        columns.add(column);
        addedNames.add(column.name());
      }
    }
    return columns;
  }

  /** create Glue column from InternalField */
  @VisibleForTesting
  protected Column toColumn(InternalField field, String tableFormat) {
    int fieldId = field.getFieldId() != null ? field.getFieldId() : -1;
    Column.Builder builder =
        Column.builder()
            .name(field.getName())
            .type(toTypeString(field.getSchema(), tableFormat))
            .parameters(
                ImmutableMap.of(
                    getColumnProperty(tableFormat, FIELD_ID),
                    Integer.toString(fieldId),
                    getColumnProperty(tableFormat, FIELD_OPTIONAL),
                    Boolean.toString(field.getSchema().isNullable()),
                    getColumnProperty(tableFormat, FIELD_CURRENT),
                    "true"));

    String comment = field.getSchema().getComment();
    if (!StringUtils.isEmpty(comment)) {
      // Glue has restriction on column comment to not exceed 255 chars
      // https://docs.aws.amazon.com/glue/latest/webapi/API_Column.html
      if (comment.length() > MAX_COLUMN_COMMENT_LENGTH) {
        log.warn(
            "Column: {} comment has been truncated due to exceeding the maximum allowed length ({})",
            field.getName(),
            MAX_COLUMN_COMMENT_LENGTH);
        comment = comment.substring(0, MAX_COLUMN_COMMENT_LENGTH);
      }
      builder.comment(comment);
    }
    return builder.build();
  }

  /**
   * Get glue compatible column type from InternalTable field schema
   *
   * @param tableFormat tableFormat to handle format specific type conversion
   * @param fieldSchema InternalTable field schema
   * @return glue column type
   */
  protected String toTypeString(InternalSchema fieldSchema, String tableFormat) {
    switch (fieldSchema.getDataType()) {
      case BOOLEAN:
        return "boolean";
      case INT:
        return "int";
      case LONG:
        return "bigint";
      case FLOAT:
        return "float";
      case DOUBLE:
        return "double";
      case DATE:
        return "date";
      case ENUM:
      case STRING:
        return "string";
      case TIMESTAMP:
      case TIMESTAMP_NTZ:
        return "timestamp";
      case FIXED:
      case BYTES:
        return "binary";
      case DECIMAL:
        Map<InternalSchema.MetadataKey, Object> metadata = fieldSchema.getMetadata();
        if (metadata == null || metadata.isEmpty()) {
          throw new NotSupportedException("Invalid decimal type, precision and scale is missing");
        }
        int precision =
            (int)
                metadata.computeIfAbsent(
                    InternalSchema.MetadataKey.DECIMAL_PRECISION,
                    k -> {
                      throw new NotSupportedException("Invalid decimal type, precision is missing");
                    });
        int scale =
            (int)
                metadata.computeIfAbsent(
                    InternalSchema.MetadataKey.DECIMAL_SCALE,
                    k -> {
                      throw new NotSupportedException("Invalid decimal type, scale is missing");
                    });
        return String.format("decimal(%s,%s)", precision, scale);
      case RECORD:
        final String nameToType =
            fieldSchema.getFields().stream()
                .map(
                    f ->
                        String.format(
                            "%s:%s", f.getName(), toTypeString(f.getSchema(), tableFormat)))
                .collect(Collectors.joining(","));
        return String.format("struct<%s>", nameToType);
      case LIST:
        InternalField arrayElement =
            fieldSchema.getFields().stream()
                .filter(
                    arrayField ->
                        InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME.equals(
                            arrayField.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid array schema"));
        return String.format("array<%s>", toTypeString(arrayElement.getSchema(), tableFormat));
      case MAP:
        InternalField key =
            fieldSchema.getFields().stream()
                .filter(
                    mapField ->
                        InternalField.Constants.MAP_KEY_FIELD_NAME.equals(mapField.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid map schema"));
        InternalField value =
            fieldSchema.getFields().stream()
                .filter(
                    mapField ->
                        InternalField.Constants.MAP_VALUE_FIELD_NAME.equals(mapField.getName()))
                .findFirst()
                .orElseThrow(() -> new SchemaExtractorException("Invalid map schema"));
        return String.format(
            "map<%s,%s>",
            toTypeString(key.getSchema(), tableFormat),
            toTypeString(value.getSchema(), tableFormat));
      default:
        throw new NotSupportedException("Unsupported type: " + fieldSchema.getDataType());
    }
  }

  @VisibleForTesting
  protected static String getColumnProperty(String tableFormat, String property) {
    return String.format("%s.%s", tableFormat.toLowerCase(Locale.ENGLISH), property);
  }
}
