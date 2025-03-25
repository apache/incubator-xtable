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

import lombok.Builder;
import lombok.Getter;
import lombok.Value;

/**
 * Represents logical information about an entry in the Schema.
 *
 * @since 0.1
 */
@Value
@Builder(toBuilder = true)
public class InternalField {
  private static final String PATH_DELIMITER = "\\.";

  // The name of this field
  String name;
  // A dot separated path to the parent of this field. Null if this is a top level field.
  String parentPath;
  // Schema for the field
  InternalSchema schema;
  // Default value for the field
  Object defaultValue;
  // The id field for the field. This is used to identify the field in the schema even after
  // renames.
  Integer fieldId;
  // represents the fully qualified path to the field (dot separated)
  @Getter(lazy = true)
  String path = createPath();
  // splits the dot separated path into parts
  @Getter(lazy = true)
  String[] pathParts = splitPath();

  private String createPath() {
    if (parentPath == null || parentPath.isEmpty()) {
      return name;
    }
    return parentPath + "." + name;
  }

  private String[] splitPath() {
    return getPath().split(PATH_DELIMITER);
  }

  public static class Constants {
    // just serves as a marker for conversion
    public static final NullDefault NULL_DEFAULT_VALUE = new NullDefault();
    // Field name for list elements
    public static final String ARRAY_ELEMENT_FIELD_NAME = "_one_field_element";
    // Field name for map keys
    public static final String MAP_KEY_FIELD_NAME = "_one_field_key";
    // Field name for map values
    public static final String MAP_VALUE_FIELD_NAME = "_one_field_value";
  }

  private static class NullDefault {}
}
