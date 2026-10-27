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

package org.apache.xtable.hudi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.Types;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;

/**
 * Converts Hudi's InternalSchema (used by schema-on-read) into XTable's InternalSchema
 * while preserving field IDs. This allows Iceberg to treat renames correctly.
 */
public final class HudiInternalSchemaConverter {

  private HudiInternalSchemaConverter() {}

  public static InternalSchema convert(org.apache.hudi.internal.schema.InternalSchema hudiSchema) {
    if (hudiSchema == null || hudiSchema.isEmptySchema()) {
      return null;
    }
    return convertRecord(hudiSchema.getRecord(), null);
  }

  private static InternalSchema convertRecord(Types.RecordType record, String parentPath) {
    List<InternalField> fields = new ArrayList<>(record.fields().size());
    for (Types.Field f : record.fields()) {
      fields.add(convertField(f, parentPath));
    }
    return InternalSchema.builder()
        .name("record")
        .dataType(InternalType.RECORD)
        .fields(fields)
        .isNullable(false)
        .build();
  }

  private static InternalField convertField(Types.Field hudiField, String parentPath) {
    InternalSchema fieldSchema =
        convertType(
            hudiField.type(),
            parentPath == null ? hudiField.name() : parentPath + "." + hudiField.name());

    return InternalField.builder()
        .name(hudiField.name())
        .parentPath(parentPath)
        .schema(fieldSchema)
        .fieldId(hudiField.fieldId()) // preserve Hudi field ID
        .build();
  }

  private static InternalSchema convertType(Type type, String path) {
    switch (type.typeId()) {
      case BOOLEAN:
        return primitive(InternalType.BOOLEAN);
      case INT:
        return primitive(InternalType.INT);
      case LONG:
        return primitive(InternalType.LONG);
      case FLOAT:
        return primitive(InternalType.FLOAT);
      case DOUBLE:
        return primitive(InternalType.DOUBLE);
      case DATE:
        return primitive(InternalType.DATE);
      case TIME:
      case TIMESTAMP:
        return primitive(InternalType.TIMESTAMP); // best effort
      case STRING:
        return primitive(InternalType.STRING);
      case UUID:
        return primitive(InternalType.UUID);
      case FIXED:
      case BINARY:
        return primitive(InternalType.BYTES);
      case DECIMAL:
        return primitive(InternalType.DECIMAL);
      case RECORD:
        return convertRecord((Types.RecordType) type, path);
      case ARRAY:
        // Simplified – treat element as a field with conventional name
        Types.ArrayType array = (Types.ArrayType) type;
        InternalField element =
            InternalField.builder()
                .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                .parentPath(path)
                .schema(convertType(array.elementType(), path + ".element"))
                .fieldId(array.elementId())
                .build();
        return InternalSchema.builder()
            .dataType(InternalType.LIST)
            .fields(Collections.singletonList(element))
            .build();
      case MAP:
        // Simplified map handling
        Types.MapType map = (Types.MapType) type;
        InternalField keyField =
            InternalField.builder()
                .name(InternalField.Constants.MAP_KEY_FIELD_NAME)
                .parentPath(path)
                .schema(primitive(InternalType.STRING))
                .build();
        InternalField valueField =
            InternalField.builder()
                .name(InternalField.Constants.MAP_VALUE_FIELD_NAME)
                .parentPath(path)
                .schema(convertType(map.valueType(), path + ".value"))
                .build();
        return InternalSchema.builder()
            .dataType(InternalType.MAP)
            .fields(java.util.Arrays.asList(keyField, valueField))
            .build();
      default:
        throw new IllegalArgumentException("Unsupported Hudi InternalSchema type: " + type);
    }
  }

  private static InternalSchema primitive(InternalType t) {
    return InternalSchema.builder().dataType(t).isNullable(true).build();
  }
}