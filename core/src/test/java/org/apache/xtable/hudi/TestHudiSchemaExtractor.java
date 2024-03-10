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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Test;

import org.apache.xtable.avro.AvroSchemaConverter;
import org.apache.xtable.model.schema.OneSchema;

public class TestHudiSchemaExtractor {

  @Test
  public void testHoodieTableSchema() {
    AvroSchemaConverter mockConverter = mock(AvroSchemaConverter.class);
    OneSchema mockOutput = OneSchema.builder().name("fake schema").build();
    Schema schema = SchemaBuilder.record("fake").fields().requiredString("foo").endRecord();
    when(mockConverter.toOneSchema(schema)).thenReturn(mockOutput);
    OneSchema canonicalSchema = new HudiSchemaExtractor(mockConverter).schema(schema);
    assertEquals(mockOutput, canonicalSchema);
    verify(mockConverter).toOneSchema(schema);
  }
}
