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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;

public class TestHudiTableExtractor {

  @Test
  void testOmitMetadataFieldsWhenEnabled() {
    InternalSchema schema =
        InternalSchema.builder()
            .name("test")
            .dataType(InternalType.RECORD)
            .fields(
                Arrays.asList(
                    field("_hoodie_commit_time"),
                    field("_hoodie_record_key"),
                    field("id"),
                    field("name")))
            .build();

    InternalSchema filtered = HudiTableExtractor.omitMetadataFieldsIfEnabled(schema, true);

    List<String> fieldNames =
        filtered.getFields().stream().map(InternalField::getName).collect(Collectors.toList());
    assertEquals(Arrays.asList("id", "name"), fieldNames);
  }

  @Test
  void testRetainsFieldsWhenOmitDisabled() {
    InternalSchema schema =
        InternalSchema.builder()
            .name("test")
            .dataType(InternalType.RECORD)
            .fields(Arrays.asList(field("_hoodie_commit_time"), field("id")))
            .build();

    InternalSchema filtered = HudiTableExtractor.omitMetadataFieldsIfEnabled(schema, false);

    assertEquals(schema, filtered);
    assertTrue(filtered.getFields().stream().anyMatch(f -> f.getName().startsWith("_hoodie_")));
  }

  private static InternalField field(String name) {
    return InternalField.builder()
        .name(name)
        .schema(InternalSchema.builder().dataType(InternalType.STRING).build())
        .build();
  }
}
