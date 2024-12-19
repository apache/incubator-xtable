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
 
package org.apache.xtable.catalog;

import java.util.Collections;
import java.util.Map;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;

public class TestSchemaExtractorBase {
  protected static InternalField getPrimitiveOneField(
      String fieldName, String schemaName, InternalType dataType, boolean isNullable, int fieldId) {
    return getPrimitiveOneField(
        fieldName, schemaName, dataType, isNullable, fieldId, Collections.emptyMap());
  }

  protected static InternalField getPrimitiveOneField(
      String fieldName,
      String schemaName,
      InternalType dataType,
      boolean isNullable,
      int fieldId,
      String parentPath) {
    return getPrimitiveOneField(
        fieldName, schemaName, dataType, isNullable, fieldId, parentPath, Collections.emptyMap());
  }

  protected static InternalField getPrimitiveOneField(
      String fieldName,
      String schemaName,
      InternalType dataType,
      boolean isNullable,
      int fieldId,
      Map<InternalSchema.MetadataKey, Object> metadata) {
    return getPrimitiveOneField(
        fieldName, schemaName, dataType, isNullable, fieldId, null, metadata);
  }

  protected static InternalField getPrimitiveOneField(
      String fieldName,
      String schemaName,
      InternalType dataType,
      boolean isNullable,
      int fieldId,
      String parentPath,
      Map<InternalSchema.MetadataKey, Object> metadata) {
    return InternalField.builder()
        .name(fieldName)
        .parentPath(parentPath)
        .schema(
            InternalSchema.builder()
                .name(schemaName)
                .dataType(dataType)
                .isNullable(isNullable)
                .metadata(metadata)
                .build())
        .fieldId(fieldId)
        .build();
  }
}
