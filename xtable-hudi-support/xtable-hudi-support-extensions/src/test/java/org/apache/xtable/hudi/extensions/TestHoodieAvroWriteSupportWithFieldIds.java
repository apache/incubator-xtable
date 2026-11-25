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
 
package org.apache.xtable.hudi.extensions;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.xtable.hudi.idtracking.models.IdMapping;

/**
 * Field ids land where Iceberg's name mapping puts them: on mapped fields at each level, on the
 * element of both list layouts and on the key and value of a map, and nowhere else.
 */
class TestHoodieAvroWriteSupportWithFieldIds {

  private static IdMapping field(String name, int id, IdMapping... nested) {
    return new IdMapping(name, id, Arrays.asList(nested));
  }

  private static Stream<Arguments> schemas() {
    return Stream.of(
        Arguments.of(
            "message m { required int32 id; optional binary name (STRING); optional int64 unmapped; }",
            "message m { required int32 id = 1; optional binary name (STRING) = 2; optional int64 unmapped; }",
            Arrays.asList(field("id", 1), field("name", 2), field("extra", 9))),
        Arguments.of(
            "message m { optional group s { required int32 a; optional group t { optional double b; } } }",
            "message m { optional group s = 1 { required int32 a = 2; optional group t = 3 { optional double b = 4; } } }",
            Collections.singletonList(field("s", 1, field("a", 2), field("t", 3, field("b", 4))))),
        Arguments.of(
            "message m { optional group ints (LIST) { repeated group list { optional int32 element; } } optional group structs (LIST) { repeated group list { optional group element { optional int32 x; } } } }",
            "message m { optional group ints (LIST) = 1 { repeated group list { optional int32 element = 2; } } optional group structs (LIST) = 3 { repeated group list { optional group element = 4 { optional int32 x = 5; } } } }",
            Arrays.asList(
                field("ints", 1, field("element", 2)),
                field("structs", 3, field("element", 4, field("x", 5))))),
        Arguments.of(
            "message m { optional group ints (LIST) { repeated int32 array; } optional group structs (LIST) { repeated group array { optional int32 x; optional int32 y; } } }",
            "message m { optional group ints (LIST) = 1 { repeated int32 array = 2; } optional group structs (LIST) = 3 { repeated group array = 4 { optional int32 x = 5; optional int32 y = 6; } } }",
            Arrays.asList(
                field("ints", 1, field("element", 2)),
                field("structs", 3, field("element", 4, field("x", 5), field("y", 6))))),
        Arguments.of(
            "message m { optional group m (MAP) { repeated group key_value { required binary key (STRING); optional group value { optional int32 v; } } } }",
            "message m { optional group m (MAP) = 1 { repeated group key_value { required binary key (STRING) = 2; optional group value = 3 { optional int32 v = 4; } } } }",
            Collections.singletonList(
                field("m", 1, field("key", 2), field("value", 3, field("v", 4))))),
        Arguments.of(
            "message m { optional group ints (LIST) { repeated group list { optional int32 element; } } }",
            "message m { optional group ints (LIST) = 1 { repeated group list { optional int32 element; } } }",
            Collections.singletonList(field("ints", 1))));
  }

  @ParameterizedTest
  @MethodSource("schemas")
  void fieldIdsFollowTheMappings(String schema, String expected, List<IdMapping> mappings) {
    assertEquals(
        MessageTypeParser.parseMessageType(expected),
        HoodieAvroWriteSupportWithFieldIds.applyFieldIds(
            MessageTypeParser.parseMessageType(schema), mappings));
  }
}
