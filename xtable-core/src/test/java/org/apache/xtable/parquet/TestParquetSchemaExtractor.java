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
 
package org.apache.xtable.parquet;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.parquet.schema.*;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;

public class TestParquetSchemaExtractor {
  private static final ParquetSchemaExtractor schemaExtractor =
      ParquetSchemaExtractor.getInstance();

  @Test
  public void testPrimitiveTypes() {

    InternalSchema primitive1 =
        InternalSchema.builder().name("integer").dataType(InternalType.INT).build();
    InternalSchema primitive2 =
        InternalSchema.builder().name("string").dataType(InternalType.STRING).build();

    Map<InternalSchema.MetadataKey, Object> fixedDecimalMetadata = new HashMap<>();
    fixedDecimalMetadata.put(InternalSchema.MetadataKey.DECIMAL_PRECISION, 6);
    fixedDecimalMetadata.put(InternalSchema.MetadataKey.DECIMAL_SCALE, 5);
    InternalSchema decimalType =
        InternalSchema.builder()
            .name("decimal")
            .dataType(InternalType.DECIMAL)
            .isNullable(false)
            .metadata(fixedDecimalMetadata)
            .build();

    Type stringPrimitiveType =
        Types.required(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("string");

    Type intPrimitiveType =
        Types.required(PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.intType(32, false))
            .named("integer");

    Type decimalPrimitive =
        Types.required(PrimitiveTypeName.INT32)
            .as(LogicalTypeAnnotation.decimalType(5, 6))
            .named("decimal");

    Assertions.assertEquals(primitive1, schemaExtractor.toInternalSchema(intPrimitiveType, null));

    Assertions.assertEquals(
        primitive2, schemaExtractor.toInternalSchema(stringPrimitiveType, null));

    Assertions.assertEquals(decimalType, schemaExtractor.toInternalSchema(decimalPrimitive, null));

    // tests for timestamp and date
    InternalSchema testDate =
        InternalSchema.builder().name("date").dataType(InternalType.DATE).isNullable(false).build();

    Map<InternalSchema.MetadataKey, Object> millisMetadata =
        Collections.singletonMap(
            InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MILLIS);
    Map<InternalSchema.MetadataKey, Object> microsMetadata =
        Collections.singletonMap(
            InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.MICROS);
    Map<InternalSchema.MetadataKey, Object> nanosMetadata =
        Collections.singletonMap(
            InternalSchema.MetadataKey.TIMESTAMP_PRECISION, InternalSchema.MetadataValue.NANOS);

    InternalSchema testTimestampMillis =
        InternalSchema.builder()
            .name("timestamp_millis")
            .dataType(InternalType.TIMESTAMP_NTZ)
            .isNullable(false)
            .metadata(millisMetadata)
            .build();

    InternalSchema testTimestampMicros =
        InternalSchema.builder()
            .name("timestamp_micros")
            .dataType(InternalType.TIMESTAMP)
            .isNullable(false)
            .metadata(microsMetadata)
            .build();

    InternalSchema testTimestampNanos =
        InternalSchema.builder()
            .name("timestamp_nanos")
            .dataType(InternalType.TIMESTAMP_NTZ)
            .isNullable(false)
            .metadata(nanosMetadata)
            .build();

    Type timestampMillisPrimitiveType =
        Types.required(PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.MILLIS))
            .named("timestamp_millis");
    Type timestampNanosPrimitiveType =
        Types.required(PrimitiveTypeName.INT64)
            .as(LogicalTypeAnnotation.timestampType(false, LogicalTypeAnnotation.TimeUnit.NANOS))
            .named("timestamp_nanos");
    Assertions.assertEquals(
        testTimestampMillis, schemaExtractor.toInternalSchema(timestampMillisPrimitiveType, null));
    Assertions.assertEquals(
        testTimestampNanos, schemaExtractor.toInternalSchema(timestampNanosPrimitiveType, null));

    // test date

    Type datePrimitiveType =
        Types.required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named("date");
    Assertions.assertEquals(testDate, schemaExtractor.toInternalSchema(datePrimitiveType, null));
  }

  @Test
  public void testGroupTypes() {

    // map

    InternalSchema internalMap =
        InternalSchema.builder()
            .name("map")
            .isNullable(false)
            .dataType(InternalType.MAP)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("key")
                        .parentPath("_one_field_value")
                        .schema(
                            InternalSchema.builder()
                                .name("key")
                                .dataType(InternalType.FLOAT)
                                .isNullable(false)
                                .build())
                        .defaultValue(null)
                        .build(),
                    InternalField.builder()
                        .name("value")
                        .parentPath("_one_field_value")
                        .schema(
                            InternalSchema.builder()
                                .name("value")
                                .dataType(InternalType.INT)
                                .isNullable(false)
                                .build())
                        .build()))
            .build();

    /* testing from fromInternalSchema()*/

    GroupType fromSimpleList =
        Types.requiredList()
            .element(
                Types.required(PrimitiveTypeName.INT32)
                    .as(LogicalTypeAnnotation.intType(32, false))
                    .named(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME))
            .named("my_list");

    InternalSchema fromInternalList =
        InternalSchema.builder()
            .name("my_list")
            .isNullable(false)
            .dataType(InternalType.LIST)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                        .parentPath(null)
                        .schema(
                            InternalSchema.builder()
                                .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                .dataType(InternalType.INT)
                                .isNullable(false)
                                .build())
                        .build()))
            .build();

    GroupType fromTestMap =
        Types.requiredMap()
            .key(Types.primitive(PrimitiveTypeName.FLOAT, Repetition.REQUIRED).named("key"))
            .value(
                Types.primitive(PrimitiveTypeName.INT32, Repetition.REQUIRED)
                    .as(LogicalTypeAnnotation.intType(32, false))
                    .named("value"))
            .named("map");
    InternalSchema fromInternalMap =
        InternalSchema.builder()
            .name("map")
            .isNullable(false)
            .dataType(InternalType.MAP)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("_one_field_key") // "key")
                        .parentPath("_one_field_value")
                        .schema(
                            InternalSchema.builder()
                                .name("key")
                                .dataType(InternalType.FLOAT)
                                .isNullable(false)
                                .build())
                        .defaultValue(null)
                        .build(),
                    InternalField.builder()
                        .name("_one_field_value") // "value")
                        .parentPath("_one_field_value")
                        .schema(
                            InternalSchema.builder()
                                .name("value")
                                .dataType(InternalType.INT)
                                .isNullable(false)
                                .build())
                        .build()))
            .build();

    InternalSchema recordListElementSchema =
        InternalSchema.builder()
            .name("my_group")
            .isNullable(false)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("id")
                        .parentPath("my_group")
                        .schema(
                            InternalSchema.builder()
                                .name("id")
                                .dataType(InternalType.LONG)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("name")
                        .parentPath("my_group")
                        .schema(
                            InternalSchema.builder()
                                .name("name")
                                .dataType(InternalType.STRING)
                                .isNullable(true)
                                .build())
                        .defaultValue(null)
                        .build()))
            .dataType(InternalType.RECORD)
            .build();
    InternalSchema internalSchema =
        InternalSchema.builder()
            .name("my_record")
            .dataType(InternalType.RECORD)
            .isNullable(true)
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("my_list")
                        .schema(
                            InternalSchema.builder()
                                .name("my_list")
                                .isNullable(false)
                                .dataType(InternalType.LIST)
                                .fields(
                                    Arrays.asList(
                                        InternalField.builder()
                                            .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                            .parentPath("my_list")
                                            .schema(
                                                InternalSchema.builder()
                                                    .name("element")
                                                    .dataType(InternalType.INT)
                                                    .isNullable(true)
                                                    .build())
                                            .build()))
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("my_group")
                        .schema(recordListElementSchema)
                        .defaultValue(null)
                        .build()))
            .build();

    Assertions.assertEquals(fromTestMap, schemaExtractor.fromInternalSchema(fromInternalMap, null));
    Assertions.assertEquals(
        fromSimpleList, schemaExtractor.fromInternalSchema(fromInternalList, null));

    GroupType testGroupType =
        Types.requiredGroup()
            .required(PrimitiveTypeName.INT64)
            .named("id")
            .optional(PrimitiveTypeName.BINARY)
            .as(LogicalTypeAnnotation.stringType())
            .named("name")
            .named("my_group");

    GroupType testMap =
        Types.requiredMap()
            .key(Types.primitive(PrimitiveTypeName.FLOAT, Repetition.REQUIRED).named("key"))
            .value(Types.primitive(PrimitiveTypeName.INT32, Repetition.REQUIRED).named("value"))
            .named("map");
    GroupType listType =
        Types.requiredList()
            .setElementType(
                Types.primitive(PrimitiveTypeName.INT32, Repetition.REQUIRED).named("element"))
            .named("my_list");
    MessageType messageType =
        Types.buildMessage()
            // .addField(testMap)
            .addField(listType)
            .addField(testGroupType)
            .named("my_record");

    Assertions.assertEquals(internalMap, schemaExtractor.toInternalSchema(testMap, null));
    Assertions.assertEquals(internalSchema, schemaExtractor.toInternalSchema(messageType, null));
  }
}
