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
 
package org.apache.xtable.model.schema;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * InternalSchema represents a schema which could be a composite containing multiple {@link
 * InternalField} or one of the primitives. Any level of schema hierarchy can be represented in this
 * model.
 *
 * @since 0.1
 */
@Getter
@Builder(toBuilder = true)
@EqualsAndHashCode
@ToString
public class InternalSchema {
  // The name of this schema
  private final String name;
  // The data type of this schema
  private final InternalType dataType;
  // User readable comment for this field
  private final String comment;
  // Indicates if values of this field can be `null` values.
  private final boolean isNullable;
  private final List<InternalField> fields;
  // Record keys uniquely identify a record in a table.
  // Hudi Ref: https://hudi.apache.org/docs/key_generation/
  // Iceberg Ref:
  // https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/Schema.html#identifierFieldIds
  // Delta Ref: https://docs.databricks.com/en/tables/constraints.html
  // In formats like Hudi, ordering of fields is important, so we use a list to preserve
  // the order of record keys for the table, if they exist.
  @Builder.Default List<InternalField> recordKeyFields = Collections.emptyList();
  private final Map<MetadataKey, Object> metadata;

  public static InternalSchemaBuilder builderFrom(InternalSchema schema) {
    return schema.toBuilder();
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

  /**
   * Performs a level-order traversal of the schema and returns a list of all fields. Use this
   * method to get a list that includes nested fields. Use {@link InternalSchema#getFields()} when
   * fetching the top level fields.
   *
   * @return list of all fields in the schema
   */
  public List<InternalField> getAllFields() {
    List<InternalField> output = new ArrayList<>();
    Queue<InternalField> fieldQueue = new ArrayDeque<>(getFields());
    while (!fieldQueue.isEmpty()) {
      InternalField currentField = fieldQueue.poll();
      if (currentField.getSchema().getFields() != null) {
        fieldQueue.addAll(currentField.getSchema().getFields());
      }
      output.add(currentField);
    }
    return output;
  }
}
