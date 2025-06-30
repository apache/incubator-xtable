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

import java.util.*;

import io.delta.kernel.types.DataType;
import io.delta.kernel.types.IntegerType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructType;

import org.apache.xtable.collectors.CustomCollectors;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.schema.SchemaUtils;

public class DeltaKernelSchemaExtractor {

  private static final String DELTA_COLUMN_MAPPING_ID = "delta.columnMapping.id";
  private static final DeltaKernelSchemaExtractor INSTANCE = new DeltaKernelSchemaExtractor();
  private static final Map<InternalSchema.MetadataKey, Object>
      DEFAULT_TIMESTAMP_PRECISION_METADATA =
          Collections.singletonMap(
              InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MICROS);

  public static DeltaKernelSchemaExtractor getInstance() {
    return INSTANCE;
  }

  public InternalSchema toInternalSchema_v2(StructType structType) {
    return toInternalSchema_v2(structType, null, false, null);
  }

  String trimmedTypeName = "";

  private InternalSchema toInternalSchema_v2(
      DataType dataType, String parentPath, boolean nullable, String comment) {

    Map<InternalSchema.MetadataKey, Object> metadata = null;
    List<InternalField> fields = null;
    InternalType type = null;
    if (dataType instanceof IntegerType) {
      type = InternalType.INT;
      trimmedTypeName = "integer";
    }
    if (dataType instanceof StringType) {
      type = InternalType.STRING;
      trimmedTypeName = "string";
    }
    if (dataType instanceof StructType) {
      // Handle StructType
      StructType structType = (StructType) dataType;
      // your logic here

      fields =
          structType.fields().stream()
              .filter(
                  field ->
                      !field
                          .getMetadata()
                          .contains(DeltaPartitionExtractor.DELTA_GENERATION_EXPRESSION))
              .map(
                  field -> {
                    Integer fieldId =
                        field.getMetadata().contains(DELTA_COLUMN_MAPPING_ID)
                            ? Long.valueOf(field.getMetadata().getLong(DELTA_COLUMN_MAPPING_ID))
                                .intValue()
                            : null;
                    String fieldComment =
                        field.getMetadata().contains("comment")
                            ? field.getMetadata().getString("comment")
                            : null;
                    InternalSchema schema =
                        toInternalSchema_v2(
                            field.getDataType(),
                            SchemaUtils.getFullyQualifiedPath(parentPath, field.getName()),
                            field.isNullable(),
                            fieldComment);
                    return InternalField.builder()
                        .name(field.getName())
                        .fieldId(fieldId)
                        .parentPath(parentPath)
                        .schema(schema)
                        .defaultValue(
                            field.isNullable() ? InternalField.Constants.NULL_DEFAULT_VALUE : null)
                        .build();
                  })
              .collect(CustomCollectors.toList(structType.fields().size()));
      type = InternalType.RECORD;
      trimmedTypeName = "struct";
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
