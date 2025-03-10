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
 
package org.apache.xtable.hudi.idtracking;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import org.apache.hudi.common.util.Option;

import org.apache.xtable.hudi.idtracking.models.IdMapping;
import org.apache.xtable.hudi.idtracking.models.IdTracking;

public class TestIdTracker {

  private static final String COMPLEX_SCHEMA_PRE_EVOLUTION =
      "{\"type\":\"record\",\"name\":\"Sample\",\"namespace\":\"test\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"},"
          + "{\"name\":\"ts\",\"type\":\"long\"},{\"name\":\"level\",\"type\":\"string\"},{\"name\":\"nested_record\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Nested\","
          + "\"fields\":[{\"name\":\"nested_int\",\"type\":\"int\",\"default\":0},{\"name\":\"double_nested\",\"type\":{\"type\":\"record\",\"name\":\"DoubleNested\","
          + "\"fields\":[{\"name\":\"double_nested_int\",\"type\":\"int\",\"default\":0}]}}]}],\"default\":null},{\"name\":\"nullable_map_field\","
          + "\"type\":[\"null\",{\"type\":\"map\",\"values\":\"Nested\"}],\"default\":null},{\"name\":\"primitive_map_field\",\"type\":{\"type\":\"map\",\"values\":\"string\"}}]}";
  private static final String COMPLEX_SCHEMA =
      "{\"type\":\"record\",\"name\":\"Sample\",\"namespace\":\"test\",\"fields\":[{\"name\":\"key\",\"type\":\"string\"},{\"name\":\"ts\",\"type\":\"long\"},"
          + "{\"name\":\"level\",\"type\":[\"string\", \"null\"]},{\"name\":\"nested_record\",\"type\":[\"null\",{\"type\":\"record\",\"name\":\"Nested\","
          + "\"fields\":[{\"name\":\"nested_int\",\"type\":\"int\",\"default\":0},{\"name\":\"double_nested\",\"type\":{\"type\":\"record\",\"name\":\"DoubleNested\","
          + "\"fields\":[{\"name\":\"double_nested_int\",\"type\":\"int\",\"default\":0}]}},{\"name\":\"level\",\"type\":\"string\"}]}],\"default\":null},"
          + "{\"name\":\"nullable_map_field\",\"type\":[\"null\",{\"type\":\"map\",\"values\":[\"Nested\", \"null\"]}],\"default\":null},"
          + "{\"name\":\"primitive_map_field\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"array_field\",\"type\":{\"type\":\"array\",\"items\":[\"null\", \"Nested\"]},\"default\":[]},"
          + "{\"name\":\"primitive_array_field\",\"type\":[{\"type\":\"array\",\"items\":\"string\"}, \"null\"]}]}";
  private final IdTracker idTracker = IdTracker.getInstance();

  @Test
  public void testIdTrackingBootstrapWithSchema() {
    Schema input = new Schema.Parser().parse(COMPLEX_SCHEMA);
    // initially schema does not have id tracking set so must bootstrap even though there is a
    // previous schema
    Schema schemaWithIdTracking = idTracker.addIdTracking(input, Option.of(input), true);
    IdTracking actual = idTracker.getIdTracking(schemaWithIdTracking).get();
    assertEquals(getExpectTrackingForComplexSchema(), actual);
    // validate that meta fields are not added to the schema itself
    assertEquals(input.getFields().size(), schemaWithIdTracking.getFields().size());
  }

  @Test
  public void testIdTrackingBootstrapWithoutSchema() {
    Schema input = new Schema.Parser().parse(COMPLEX_SCHEMA);
    Schema schemaWithIdTracking = idTracker.addIdTracking(input, Option.empty(), true);
    IdTracking actual = idTracker.getIdTracking(schemaWithIdTracking).get();
    assertEquals(getExpectTrackingForComplexSchema(), actual);
  }

  @Test
  public void testIdTrackingWithIdenticalSchemas() {
    Schema input = new Schema.Parser().parse(COMPLEX_SCHEMA);
    Schema inputWithIdTracking = idTracker.addIdTracking(input, Option.empty(), true);
    // Should be a no-op
    Schema schemaWithIdTracking =
        idTracker.addIdTracking(input, Option.of(inputWithIdTracking), true);
    IdTracking actual = idTracker.getIdTracking(schemaWithIdTracking).get();
    assertEquals(getExpectTrackingForComplexSchema(), actual);
  }

  @Test
  public void testIdTrackingWithPreviousSchema() {
    Schema initial =
        idTracker.addIdTracking(
            new Schema.Parser().parse(COMPLEX_SCHEMA_PRE_EVOLUTION), Option.empty(), true);
    Schema evolved = new Schema.Parser().parse(COMPLEX_SCHEMA);
    Schema schemaWithIdTracking = idTracker.addIdTracking(evolved, Option.of(initial), true);
    IdTracking actual = idTracker.getIdTracking(schemaWithIdTracking).get();
    assertEquals(getExpectTrackingForComplexSchemaEvolved(), actual);
  }

  @Test
  public void testIdTrackingWithPreviousSchemaWithoutMetaFields() {
    Schema initial =
        idTracker.addIdTracking(
            new Schema.Parser().parse(COMPLEX_SCHEMA_PRE_EVOLUTION), Option.empty(), false);
    Schema evolved = new Schema.Parser().parse(COMPLEX_SCHEMA);
    Schema schemaWithIdTracking = idTracker.addIdTracking(evolved, Option.of(initial), false);
    IdTracking actual = idTracker.getIdTracking(schemaWithIdTracking).get();
    assertEquals(getExpectTrackingForComplexSchemaEvolvedNoMetaFields(), actual);
  }

  @Test
  public void testIdTrackingWithFieldRemoval() {
    // create initial schema with 2 fields and assign IDs
    Schema initial =
        Schema.createRecord(
            "test1",
            null,
            "hudi",
            false,
            Arrays.asList(
                new Schema.Field("field1", Schema.create(Schema.Type.STRING)),
                new Schema.Field("field2", Schema.create(Schema.Type.STRING))));
    Schema initialWithIdTracking = idTracker.addIdTracking(initial, Option.empty(), false);
    // remove the second field
    Schema withFieldRemoved =
        Schema.createRecord(
            "test2",
            null,
            "hudi",
            false,
            Collections.singletonList(
                new Schema.Field("field1", Schema.create(Schema.Type.STRING))));
    Schema withFieldRemovedAndIdTracking =
        idTracker.addIdTracking(withFieldRemoved, Option.of(initialWithIdTracking), false);
    IdTracking actualWithFieldRemoved =
        idTracker.getIdTracking(withFieldRemovedAndIdTracking).get();
    IdTracking expectedWithFieldRemoved =
        new IdTracking(Collections.singletonList(new IdMapping("field1", 1)), 2);
    assertEquals(expectedWithFieldRemoved, actualWithFieldRemoved);
    // Adding a new field should be tracked with ID 3
    Schema withFieldAdded =
        Schema.createRecord(
            "test2",
            null,
            "hudi",
            false,
            Arrays.asList(
                new Schema.Field("field1", Schema.create(Schema.Type.STRING)),
                new Schema.Field("field3", Schema.create(Schema.Type.STRING))));
    Schema withFieldAddedAndIdTracking =
        idTracker.addIdTracking(withFieldAdded, Option.of(withFieldRemovedAndIdTracking), false);
    IdTracking actualWithFieldAdded = idTracker.getIdTracking(withFieldAddedAndIdTracking).get();
    IdTracking expectedWithFieldAdded =
        new IdTracking(Arrays.asList(new IdMapping("field1", 1), new IdMapping("field3", 3)), 3);
    assertEquals(expectedWithFieldAdded, actualWithFieldAdded);
  }

  @Test
  public void testIdTrackingAddMetaFields() {
    // create initial schema with a meta field manually specified
    Schema initial =
        Schema.createRecord(
            "test1",
            null,
            "hudi",
            false,
            Arrays.asList(
                new Schema.Field("_hoodie_commit_time", Schema.create(Schema.Type.STRING)),
                new Schema.Field("field1", Schema.create(Schema.Type.STRING))));
    Schema initialWithIdTracking = idTracker.addIdTracking(initial, Option.empty(), false);
    // add all meta fields and ensure IDs are properly assigned
    Schema withMetaFields =
        Schema.createRecord(
            "test2",
            null,
            "hudi",
            false,
            Arrays.asList(
                new Schema.Field("_hoodie_commit_time", Schema.create(Schema.Type.STRING)),
                new Schema.Field("field1", Schema.create(Schema.Type.STRING))));
    Schema withMetaFieldsAndIdTracking =
        idTracker.addIdTracking(withMetaFields, Option.of(initialWithIdTracking), true);
    IdTracking actual = idTracker.getIdTracking(withMetaFieldsAndIdTracking).get();
    IdTracking expected =
        new IdTracking(
            Arrays.asList(
                new IdMapping("_hoodie_commit_time", 1),
                new IdMapping("field1", 2),
                new IdMapping("_hoodie_commit_seqno", 3),
                new IdMapping("_hoodie_record_key", 4),
                new IdMapping("_hoodie_partition_path", 5),
                new IdMapping("_hoodie_file_name", 6)),
            6);
    assertEquals(expected, actual);
  }

  private static IdTracking getExpectTrackingForComplexSchema() {
    List<IdMapping> idMappings =
        Arrays.asList(
            new IdMapping("_hoodie_commit_time", 1),
            new IdMapping("_hoodie_commit_seqno", 2),
            new IdMapping("_hoodie_record_key", 3),
            new IdMapping("_hoodie_partition_path", 4),
            new IdMapping("_hoodie_file_name", 5),
            new IdMapping("key", 6),
            new IdMapping("ts", 7),
            new IdMapping("level", 8),
            new IdMapping(
                "nested_record",
                9,
                Arrays.asList(
                    new IdMapping("nested_int", 14),
                    new IdMapping(
                        "double_nested",
                        15,
                        Collections.singletonList(new IdMapping("double_nested_int", 17))),
                    new IdMapping("level", 16))),
            new IdMapping(
                "nullable_map_field",
                10,
                Arrays.asList(
                    new IdMapping("key", 18),
                    new IdMapping(
                        "value",
                        19,
                        Arrays.asList(
                            new IdMapping("nested_int", 20),
                            new IdMapping(
                                "double_nested",
                                21,
                                Collections.singletonList(new IdMapping("double_nested_int", 23))),
                            new IdMapping("level", 22))))),
            new IdMapping(
                "primitive_map_field",
                11,
                Arrays.asList(new IdMapping("key", 24), new IdMapping("value", 25))),
            new IdMapping(
                "array_field",
                12,
                Collections.singletonList(
                    new IdMapping(
                        "element",
                        26,
                        Arrays.asList(
                            new IdMapping("nested_int", 27),
                            new IdMapping(
                                "double_nested",
                                28,
                                Collections.singletonList(new IdMapping("double_nested_int", 30))),
                            new IdMapping("level", 29))))),
            new IdMapping(
                "primitive_array_field",
                13,
                Collections.singletonList(new IdMapping("element", 31))));
    return new IdTracking(idMappings, 31);
  }

  private static IdTracking getExpectTrackingForComplexSchemaEvolved() {
    List<IdMapping> idMappings =
        Arrays.asList(
            new IdMapping("_hoodie_commit_time", 1),
            new IdMapping("_hoodie_commit_seqno", 2),
            new IdMapping("_hoodie_record_key", 3),
            new IdMapping("_hoodie_partition_path", 4),
            new IdMapping("_hoodie_file_name", 5),
            new IdMapping("key", 6),
            new IdMapping("ts", 7),
            new IdMapping("level", 8),
            new IdMapping(
                "nested_record",
                9,
                Arrays.asList(
                    new IdMapping("nested_int", 12),
                    new IdMapping(
                        "double_nested",
                        13,
                        Collections.singletonList(new IdMapping("double_nested_int", 14))),
                    new IdMapping("level", 24))),
            new IdMapping(
                "nullable_map_field",
                10,
                Arrays.asList(
                    new IdMapping("key", 15),
                    new IdMapping(
                        "value",
                        16,
                        Arrays.asList(
                            new IdMapping("nested_int", 17),
                            new IdMapping(
                                "double_nested",
                                18,
                                Collections.singletonList(new IdMapping("double_nested_int", 19))),
                            new IdMapping("level", 25))))),
            new IdMapping(
                "primitive_map_field",
                11,
                Arrays.asList(new IdMapping("key", 20), new IdMapping("value", 21))),
            new IdMapping(
                "array_field",
                22,
                Collections.singletonList(
                    new IdMapping(
                        "element",
                        26,
                        Arrays.asList(
                            new IdMapping("nested_int", 27),
                            new IdMapping(
                                "double_nested",
                                28,
                                Collections.singletonList(new IdMapping("double_nested_int", 30))),
                            new IdMapping("level", 29))))),
            new IdMapping(
                "primitive_array_field",
                23,
                Collections.singletonList(new IdMapping("element", 31))));
    return new IdTracking(idMappings, 31);
  }

  private static IdTracking getExpectTrackingForComplexSchemaEvolvedNoMetaFields() {
    List<IdMapping> idMappings =
        Arrays.asList(
            new IdMapping("key", 1),
            new IdMapping("ts", 2),
            new IdMapping("level", 3),
            new IdMapping(
                "nested_record",
                4,
                Arrays.asList(
                    new IdMapping("nested_int", 7),
                    new IdMapping(
                        "double_nested",
                        8,
                        Collections.singletonList(new IdMapping("double_nested_int", 9))),
                    new IdMapping("level", 19))),
            new IdMapping(
                "nullable_map_field",
                5,
                Arrays.asList(
                    new IdMapping("key", 10),
                    new IdMapping(
                        "value",
                        11,
                        Arrays.asList(
                            new IdMapping("nested_int", 12),
                            new IdMapping(
                                "double_nested",
                                13,
                                Collections.singletonList(new IdMapping("double_nested_int", 14))),
                            new IdMapping("level", 20))))),
            new IdMapping(
                "primitive_map_field",
                6,
                Arrays.asList(new IdMapping("key", 15), new IdMapping("value", 16))),
            new IdMapping(
                "array_field",
                17,
                Collections.singletonList(
                    new IdMapping(
                        "element",
                        21,
                        Arrays.asList(
                            new IdMapping("nested_int", 22),
                            new IdMapping(
                                "double_nested",
                                23,
                                Collections.singletonList(new IdMapping("double_nested_int", 25))),
                            new IdMapping("level", 24))))),
            new IdMapping(
                "primitive_array_field",
                18,
                Collections.singletonList(new IdMapping("element", 26))));
    return new IdTracking(idMappings, 26);
  }
}
