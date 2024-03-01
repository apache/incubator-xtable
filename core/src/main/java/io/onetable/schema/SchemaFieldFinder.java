/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.onetable.schema;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;

/**
 * SchemaFieldFinder finds the {@link OneField} in the given {@link OneSchema} identified by the
 * fully qualified path.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SchemaFieldFinder {
  private static final SchemaFieldFinder INSTANCE = new SchemaFieldFinder();

  public static SchemaFieldFinder getInstance() {
    return INSTANCE;
  }

  /**
   * Finds the field at the specified path in the provided schema. Currently, can only access nested
   * fields within Records and not Maps or Arrays.
   *
   * @param schema the schema to search
   * @param path dot separated path
   * @return the field if it exists, otherwise returns null
   */
  public OneField findFieldByPath(OneSchema schema, String path) {
    return findFieldByPath(schema, path.split("\\."), 0);
  }

  private OneField findFieldByPath(OneSchema schema, String[] pathParts, int startIndex) {
    if (pathParts.length == 0) {
      return null;
    }
    for (OneField field : schema.getFields()) {
      if (field.getName().equals(pathParts[startIndex])) {
        if (pathParts.length == startIndex + 1) {
          return field;
        }
        return findFieldByPath(field.getSchema(), pathParts, startIndex + 1);
      }
    }
    return null;
  }
}
