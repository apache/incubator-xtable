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

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.jupiter.api.Test;

import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;

/** {@link ParquetFieldIds} gives the same IDs as Iceberg's own name mapping application. */
public class TestParquetFieldIds {
  private static final MessageType SCHEMA =
      MessageTypeParser.parseMessageType(
          "message record {\n"
              + "  optional binary _hoodie_commit_time (STRING);\n"
              + "  required binary key (STRING);\n"
              + "  optional group location {\n"
              + "    optional double lat;\n"
              + "    optional double lon;\n"
              + "  }\n"
              + "  optional group tags (LIST) {\n"
              + "    repeated group list {\n"
              + "      optional binary element (STRING);\n"
              + "    }\n"
              + "  }\n"
              + "  optional group points (LIST) {\n"
              + "    repeated group array {\n"
              + "      required double x;\n"
              + "    }\n"
              + "  }\n"
              + "  optional group nums (LIST) {\n"
              + "    repeated int32 nums_element;\n"
              + "  }\n"
              + "  optional group attrs (MAP) {\n"
              + "    repeated group key_value {\n"
              + "      required binary key (STRING);\n"
              + "      optional group value {\n"
              + "        optional int32 n;\n"
              + "      }\n"
              + "    }\n"
              + "  }\n"
              + "  optional int32 ver;\n"
              + "}");

  private static final NameMapping MAPPING =
      NameMappingParser.fromJson(
          "["
              + "{\"field-id\": 1, \"names\": [\"key\"]},"
              + "{\"field-id\": 2, \"names\": [\"location\"], \"fields\": ["
              + "  {\"field-id\": 3, \"names\": [\"lat\"]}, {\"field-id\": 4, \"names\": [\"lon\"]}]},"
              + "{\"field-id\": 5, \"names\": [\"tags\"], \"fields\": ["
              + "  {\"field-id\": 6, \"names\": [\"element\"]}]},"
              + "{\"field-id\": 7, \"names\": [\"points\"], \"fields\": ["
              + "  {\"field-id\": 8, \"names\": [\"element\"], \"fields\": ["
              + "    {\"field-id\": 9, \"names\": [\"x\"]}]}]},"
              + "{\"field-id\": 10, \"names\": [\"nums\"], \"fields\": ["
              + "  {\"field-id\": 11, \"names\": [\"element\"]}]},"
              + "{\"field-id\": 12, \"names\": [\"attrs\"], \"fields\": ["
              + "  {\"field-id\": 13, \"names\": [\"key\"]},"
              + "  {\"field-id\": 14, \"names\": [\"value\"], \"fields\": ["
              + "    {\"field-id\": 15, \"names\": [\"n\"]}]}]},"
              + "{\"field-id\": 16, \"names\": [\"ver\"]}"
              + "]");

  // The IDs Iceberg's ParquetSchemaUtil.applyNameMapping gives for the same mapping. Iceberg's own
  // method cannot be called here: the Iceberg Spark runtime on the test classpath relocates
  // Parquet.
  private static final MessageType EXPECTED =
      MessageTypeParser.parseMessageType(
          "message record {\n"
              + "  optional binary _hoodie_commit_time (STRING);\n"
              + "  required binary key (STRING) = 1;\n"
              + "  optional group location = 2 {\n"
              + "    optional double lat = 3;\n"
              + "    optional double lon = 4;\n"
              + "  }\n"
              + "  optional group tags (LIST) = 5 {\n"
              + "    repeated group list {\n"
              + "      optional binary element (STRING) = 6;\n"
              + "    }\n"
              + "  }\n"
              + "  optional group points (LIST) = 7 {\n"
              + "    repeated group array = 8 {\n"
              + "      required double x = 9;\n"
              + "    }\n"
              + "  }\n"
              + "  optional group nums (LIST) = 10 {\n"
              + "    repeated int32 nums_element = 11;\n"
              + "  }\n"
              + "  optional group attrs (MAP) = 12 {\n"
              + "    repeated group key_value {\n"
              + "      required binary key (STRING) = 13;\n"
              + "      optional group value = 14 {\n"
              + "        optional int32 n = 15;\n"
              + "      }\n"
              + "    }\n"
              + "  }\n"
              + "  optional int32 ver = 16;\n"
              + "}");

  @Test
  void sameIdsAsIcebergsNameMapping() {
    assertEquals(EXPECTED, ParquetFieldIds.apply(SCHEMA, MAPPING));
  }

  @Test
  void keepsTheSchemaApartFromTheIds() {
    MessageType withIds = ParquetFieldIds.apply(SCHEMA, MAPPING);
    assertEquals(SCHEMA.toString(), withIds.toString().replaceAll(" = \\d+", ""));
  }

  @Test
  void leavesUnmappedFieldsWithoutIds() {
    MessageType withIds = ParquetFieldIds.apply(SCHEMA, MAPPING);
    assertEquals(null, withIds.getType("_hoodie_commit_time").getId());
    assertEquals(1, withIds.getType("key").getId().intValue());
    assertEquals(
        6,
        withIds
            .getType("tags")
            .asGroupType()
            .getType(0)
            .asGroupType()
            .getType("element")
            .getId()
            .intValue());
  }
}
