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
 
package org.apache.xtable.paimon;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.paimon.schema.TableSchema;

import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.SmallIntType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.TinyIntType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.schema.SchemaUtils;

/** Converts Paimon RowType to XTable InternalSchema. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PaimonSchemaExtractor {
  private static final PaimonSchemaExtractor INSTANCE = new PaimonSchemaExtractor();

  public static PaimonSchemaExtractor getInstance() {
    return INSTANCE;
  }

  public InternalSchema toInternalSchema(TableSchema paimonSchema) {
    RowType rowType = paimonSchema.logicalRowType();
    List<InternalField> fields = toInternalFields(rowType);
    return InternalSchema.builder()
        .name("record")
        .dataType(InternalType.RECORD)
        .fields(fields)
        .recordKeyFields(primaryKeyFields(paimonSchema, fields))
        .build();
  }

  private List<InternalField> primaryKeyFields(
      TableSchema paimonSchema, List<InternalField> internalFields) {
    List<String> keys = paimonSchema.primaryKeys();
    return internalFields.stream()
        .filter(f -> keys.contains(f.getName()))
        .collect(Collectors.toList());
  }

  private List<InternalField> toInternalFields(RowType rowType) {
    List<InternalField> fields = new ArrayList<>(rowType.getFieldCount());
    for (int i = 0; i < rowType.getFieldCount(); i++) {
      DataField dataField = rowType.getFields().get(i);
      InternalField internalField =
          InternalField.builder()
              .name(dataField.name())
              .fieldId(dataField.id())
              .parentPath(null)
              .schema(
                  fromPaimonType(dataField.type(), dataField.name(), dataField.type().isNullable()))
              .defaultValue(
                  dataField.type().isNullable() ? InternalField.Constants.NULL_DEFAULT_VALUE : null)
              .build();
      fields.add(internalField);
    }
    return fields;
  }

  private InternalSchema fromPaimonType(DataType type, String fieldPath, boolean nullable) {
    InternalType internalType;
    List<InternalField> fields = null;
    Map<InternalSchema.MetadataKey, Object> metadata = null;
    if (type instanceof CharType || type instanceof VarCharType) {
      internalType = InternalType.STRING;
    } else if (type instanceof BooleanType) {
      internalType = InternalType.BOOLEAN;
    } else if (type instanceof TinyIntType
        || type instanceof SmallIntType
        || type instanceof IntType) {
      internalType = InternalType.INT;
    } else if (type instanceof BigIntType) {
      internalType = InternalType.LONG;
    } else if (type instanceof FloatType) {
      internalType = InternalType.FLOAT;
    } else if (type instanceof DoubleType) {
      internalType = InternalType.DOUBLE;
    } else if (type instanceof BinaryType || type instanceof VarBinaryType) {
      internalType = InternalType.BYTES;
    } else if (type instanceof DateType) {
      internalType = InternalType.DATE;
    } else if (type instanceof TimestampType || type instanceof LocalZonedTimestampType) {
      internalType = InternalType.TIMESTAMP;
      metadata =
          Collections.singletonMap(
              InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MICROS);
    } else if (type instanceof DecimalType) {
      DecimalType d = (DecimalType) type;
      metadata = new HashMap<>(2, 1.0f);
      metadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, d.getPrecision());
      metadata.put(InternalSchema.MetadataKey.DECIMAL_SCALE, d.getScale());
      internalType = InternalType.DECIMAL;
    } else if (type instanceof RowType) {
      RowType rt = (RowType) type;
      List<InternalField> nested = new ArrayList<>(rt.getFieldCount());
      for (DataField df : rt.getFields()) {
        nested.add(
            InternalField.builder()
                .name(df.name())
                .fieldId(df.id())
                .parentPath(fieldPath)
                .schema(
                    fromPaimonType(
                        df.type(),
                        SchemaUtils.getFullyQualifiedPath(fieldPath, df.name()),
                        df.type().isNullable()))
                .defaultValue(
                    df.type().isNullable() ? InternalField.Constants.NULL_DEFAULT_VALUE : null)
                .build());
      }
      fields = nested;
      internalType = InternalType.RECORD;
    } else if (type instanceof ArrayType) {
      ArrayType at = (ArrayType) type;
      InternalSchema elementSchema =
          fromPaimonType(
              at.getElementType(),
              SchemaUtils.getFullyQualifiedPath(
                  fieldPath, InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME),
              at.getElementType().isNullable());
      InternalField elementField =
          InternalField.builder()
              .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
              .parentPath(fieldPath)
              .schema(elementSchema)
              .build();
      fields = Collections.singletonList(elementField);
      internalType = InternalType.LIST;
    } else if (type instanceof MapType) {
      MapType mt = (MapType) type;
      InternalSchema keySchema =
          fromPaimonType(
              mt.getKeyType(),
              SchemaUtils.getFullyQualifiedPath(
                  fieldPath, InternalField.Constants.MAP_KEY_FIELD_NAME),
              false);
      InternalField keyField =
          InternalField.builder()
              .name(InternalField.Constants.MAP_KEY_FIELD_NAME)
              .parentPath(fieldPath)
              .schema(keySchema)
              .build();
      InternalSchema valueSchema =
          fromPaimonType(
              mt.getValueType(),
              SchemaUtils.getFullyQualifiedPath(
                  fieldPath, InternalField.Constants.MAP_VALUE_FIELD_NAME),
              mt.getValueType().isNullable());
      InternalField valueField =
          InternalField.builder()
              .name(InternalField.Constants.MAP_VALUE_FIELD_NAME)
              .parentPath(fieldPath)
              .schema(valueSchema)
              .build();
      fields = Arrays.asList(keyField, valueField);
      internalType = InternalType.MAP;
    } else {
      throw new NotSupportedException("Unsupported Paimon type: " + type.asSQLString());
    }

    return InternalSchema.builder()
        .name(type.asSQLString())
        .dataType(internalType)
        .isNullable(nullable)
        .metadata(metadata)
        .fields(fields)
        .build();
  }
}
