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
package io.onetable.hudi;

import org.apache.avro.Schema;

import com.google.common.annotations.VisibleForTesting;

import io.onetable.avro.AvroSchemaConverter;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OneSchema;
import io.onetable.spi.extractor.SchemaExtractor;

/**
 * Extracts the canonical {@link OneSchema} from {@link Schema} represented in Hudi. This uses logic
 * similar to how spark converts {@link Schema} to {@link
 * org.apache.spark.sql.avro.SchemaConverters.SchemaType} in {@link
 * org.apache.spark.sql.avro.SchemaConverters}
 *
 * @since 0.1
 */
public class HudiSchemaExtractor implements SchemaExtractor<Schema> {
  private static final String MAP_KEY_FIELD_NAME = "key_value.key";
  private static final String MAP_VALUE_FIELD_NAME = "key_value.value";
  private static final String LIST_ELEMENT_FIELD_NAME = "array";
  private final AvroSchemaConverter converter;

  public HudiSchemaExtractor() {
    this(AvroSchemaConverter.getInstance());
  }

  @VisibleForTesting
  HudiSchemaExtractor(final AvroSchemaConverter converter) {
    this.converter = converter;
  }

  @Override
  public OneSchema schema(Schema schema) {
    return converter.toOneSchema(schema);
  }

  static String convertFromOneTablePath(String path) {
    return path.replace(OneField.Constants.MAP_KEY_FIELD_NAME, MAP_KEY_FIELD_NAME)
        .replace(OneField.Constants.MAP_VALUE_FIELD_NAME, MAP_VALUE_FIELD_NAME)
        .replace(OneField.Constants.ARRAY_ELEMENT_FIELD_NAME, LIST_ELEMENT_FIELD_NAME);
  }
}
