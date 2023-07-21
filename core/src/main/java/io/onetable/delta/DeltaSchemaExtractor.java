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
 
package io.onetable.delta;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.spark.sql.types.StructType;

import org.apache.hudi.AvroConversionUtils;

import io.onetable.avro.AvroSchemaConverter;
import io.onetable.model.schema.OneSchema;

/** Extracts the {@link StructType} schema of the target Delta table from {@link OneSchema}. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaSchemaExtractor {
  private static final DeltaSchemaExtractor INSTANCE = new DeltaSchemaExtractor();

  public static DeltaSchemaExtractor getInstance() {
    return INSTANCE;
  }

  public StructType schema(OneSchema oneSchema) {
    return AvroConversionUtils.convertAvroSchemaToStructType(
        AvroSchemaConverter.getInstance().fromOneSchema(oneSchema));
  }
}
