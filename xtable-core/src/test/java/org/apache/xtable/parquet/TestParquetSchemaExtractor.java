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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.apache.parquet.schema.*;
import org.junit.jupiter.api.Assertions;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.GroupType;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.parquet.schema.OriginalType;

import java.util.Map;
import java.util.Arrays;


public class TestParquetSchemaExtractor {
    private static final ParquetSchemaExtractor schemaExtractor =
            ParquetSchemaExtractor.getInstance();

    @Test
    public void testPrimitiveTypes() {

        InternalSchema simpleList = InternalSchema.builder()
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
                                                        .name("element")
                                                        .dataType(InternalType.INT)
                                                        .isNullable(false)
                                                        .build())
                                        .build()))
                .build();
        InternalSchema primitive1 =
                InternalSchema.builder().name("integer").dataType(InternalType.INT).build();
        InternalSchema primitive2 =
                InternalSchema.builder().name("string").dataType(InternalType.STRING).build();
        InternalSchema group1 =
                InternalSchema.builder().name("list").dataType(InternalType.LIST).build();
        InternalSchema recordListElementSchema=
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
                                                                .isNullable(false)
                                                                .build())
                                                .defaultValue(null)
                                                .build()))
                        .dataType(InternalType.RECORD)
                        .build();
        InternalSchema internalSchema =
                InternalSchema.builder()
                        .name("my_record")
                        .dataType(InternalType.RECORD)
                        .isNullable(false)
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
                                                                                                        .isNullable(false)
                                                                                                        .build())
                                                                                        .build()))
                                                                .build())
                                                .build(),
                                        InternalField.builder()
                                                .name("my_group")
                                                .schema(recordListElementSchema)
                                                /*InternalSchema.builder()
                                                        .name("array")
                                                        .isNullable(true)
                                                        .dataType(InternalType.RECORD)
                                                        .fields(
                                                                Arrays.asList(
                                                                        InternalField.builder()
                                                                                .name(InternalField.Constants.ARRAY_ELEMENT_FIELD_NAME)
                                                                                .parentPath("my_group")
                                                                                .schema(recordListElementSchema)
                                                                                .build()))
                                                        .build())*/
                                                .defaultValue(null)
                                                .build()))
                        .build();
        Type stringPrimitiveType = Types
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())//.named("string")
                .named("string");

        Type intPrimitiveType = Types
                .required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(32, false))
                .named("integer");
 /*       Assertions.assertEquals(
                primitive1, schemaExtractor.toInternalSchema(intPrimitiveType, null));

        Assertions.assertEquals(
                primitive2, schemaExtractor.toInternalSchema(stringPrimitiveType, null));*/

        GroupType testGroupType = Types.requiredGroup()
                .required(PrimitiveTypeName.INT64).named("id")
                .optional(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType()).named("name")
                //.required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.dateType()).named("date")
                .named("my_group");

 /*       GroupType nestedGroupType = Types.requiredGroup()
                .required(INT64).named("id")
                .optional(BINARY).as(UTF8).named("email")
                .optionalGroup()
                .required(BINARY).as(UTF8).named("street")
                .required(INT32).named("zipcode")
                .named("address")
                .named("User");*/

        GroupType testMap = Types.requiredMap()
                .key(PrimitiveTypeName.FLOAT)
                .optionalValue(PrimitiveTypeName.INT32)
                .named("zipMap");
        GroupType listType = Types.requiredList().setElementType(Types.primitive(PrimitiveTypeName.INT32, Repetition.REQUIRED).named("element")).named("my_list");
        MessageType messageType = Types.buildMessage()
                //.addField(testMap)
                .addField(listType)
                .addField(testGroupType)
                .named("my_record");

        /*GroupType nestedList = Types.requiredList()
                .optionalList()
                .requiredElement(PrimitiveTypeNameINT32).named("integer")
                .named("nestedListInner1")
                .optionalList()
                .requiredElement(PrimitiveTypeNameINT32).named("integer")
                .named("nestedListInner2")
                .named("nestedListOuter");*/
        Assertions.assertEquals(
                internalSchema, schemaExtractor.toInternalSchema(messageType, null));

        Assertions.assertEquals(
                internalSchema, schemaExtractor.toInternalSchema(messageType, null));
    }

    @Test
    public void main(String[] args) {
        testPrimitiveTypes();
    }
}
