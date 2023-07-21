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
 
package io.onetable.model.schema;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import com.fasterxml.jackson.annotation.JsonCreator;

/**
 * OneSchema represents a schema which could be a composite containing multiple {@link OneField} or
 * one of the primitives. Any level of schema hierarchy can be represented in this model.
 *
 * @since 0.1
 */
@Getter
@Builder(toBuilder = true)
@EqualsAndHashCode
@ToString
public class OneSchema {
  private static final Set<String> METADATA_VALUES =
      Arrays.stream(MetadataValue.values()).map(MetadataValue::name).collect(Collectors.toSet());
  // The name of this schema
  private final String name;
  // The data type of this schema
  private final OneType dataType;
  // User readable comment for this field
  private final String comment;
  // Indicates if values of this field can be `null` values.
  private final Boolean isNullable;
  private final List<OneField> fields;
  private final Map<MetadataKey, Object> metadata;

  @JsonCreator
  OneSchema(
      String name,
      OneType dataType,
      String comment,
      Boolean isNullable,
      List<OneField> fields,
      Map<MetadataKey, Object> metadata) {
    this.name = name;
    this.dataType = dataType;
    this.comment = comment;
    this.isNullable = isNullable;
    this.fields = fields;
    // If a map value is one of the MetadataValue enums, then parse the object into the enum.
    // This is required to properly parse the value from json without adding the overhead of type
    // information in the json output.
    this.metadata = parseMetadataValues(metadata);
  }

  private Map<MetadataKey, Object> parseMetadataValues(Map<MetadataKey, Object> metadata) {
    if (metadata == null) {
      return Collections.emptyMap();
    }
    return metadata.entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                  if (METADATA_VALUES.contains(entry.getValue().toString())) {
                    return MetadataValue.valueOf(entry.getValue().toString());
                  } else {
                    return entry.getValue();
                  }
                }));
  }

  public static OneSchemaBuilder builderFrom(OneSchema field) {
    return field.toBuilder();
  }

  public enum MetadataKey {
    DECIMAL_SCALE,
    DECIMAL_PRECISION,
    ENUM_VALUES,
    FIXED_BYTES_SIZE,
    TIMESTAMP_PRECISION
  }

  public enum MetadataValue {
    MICROS,
    MILLIS
  }
}
