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


public class TestParquetSchemaExtractor {
    private static final ParquetSchemaExtractor schemaExtractor =
            ParquetSchemaExtractor.getInstance();

    @Test
    public void testPrimitiveTypes() {
        InternalSchema primitive1 =
                InternalSchema.builder().name("integer").dataType(InternalType.INT).build();
        InternalSchema primitive2 =
                InternalSchema.builder().name("string").dataType(InternalType.STRING).build();
        InternalSchema group1 =
                InternalSchema.builder().name("list").dataType(InternalType.LIST).build();
        Type stringPrimitiveType =Types
                .required(PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.stringType())//.named("string")
                .named("string");

        Type intPrimitiveType =Types
                .required(PrimitiveTypeName.INT32).as(LogicalTypeAnnotation.intType(32,false))
                .named("integer");
        Assertions.assertEquals(
                primitive1  , schemaExtractor.toInternalSchema(intPrimitiveType, null));

        Assertions.assertEquals(
                primitive2   , schemaExtractor.toInternalSchema(stringPrimitiveType, null));

        GroupType testGroupType = Types.requiredGroup()
                .required(INT64).named("id")
                .optional(BINARY).as(STRING).named("name")
                .optionalGroup()
                .required(DATE).as(INT32).named("date")
                .required(INT32).named("zipcode")
                .named("address")
                .named("User");
        // to do check how to create a LIST for testing
        Assertions.assertEquals(
                group1   , schemaExtractor.toInternalSchema(testGroupType, null));
    }

    @Test
    public void main() {
        testPrimitiveTypes();
    }
}