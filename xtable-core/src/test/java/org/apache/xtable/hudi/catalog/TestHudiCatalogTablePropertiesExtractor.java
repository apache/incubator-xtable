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
 
package org.apache.xtable.hudi.catalog;

import static org.apache.xtable.hudi.catalog.HudiCatalogTablePropertiesExtractor.HUDI_METADATA_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import org.junit.jupiter.api.Test;

import org.apache.hudi.common.util.ConfigUtils;

import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.storage.TableFormat;

public class TestHudiCatalogTablePropertiesExtractor {

  @Test
  void testGetSparkTableProperties() {

    InternalPartitionField p1 =
        InternalPartitionField.builder()
            .sourceField(InternalField.builder().name("region").build())
            .build();
    InternalPartitionField p2 =
        InternalPartitionField.builder()
            .sourceField(InternalField.builder().name("category").build())
            .build();
    int schemaLengthThreshold = 1000;
    InternalSchema schema =
        InternalSchema.builder()
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("id")
                        .schema(
                            InternalSchema.builder()
                                .dataType(InternalType.INT)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("name")
                        .schema(
                            InternalSchema.builder()
                                .dataType(InternalType.STRING)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("region")
                        .schema(
                            InternalSchema.builder()
                                .dataType(InternalType.STRING)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("category")
                        .schema(
                            InternalSchema.builder()
                                .dataType(InternalType.STRING)
                                .isNullable(false)
                                .build())
                        .build()))
            .dataType(InternalType.RECORD)
            .name("testSchema")
            .build();

    InternalTable table =
        InternalTable.builder()
            .name("test-table")
            .tableFormat(TableFormat.HUDI)
            .readSchema(schema)
            .partitioningFields(Arrays.asList(p1, p2))
            .build();

    Map<String, String> result =
        HudiCatalogTablePropertiesExtractor.getInstance()
            .getTableProperties(table, schemaLengthThreshold);

    // Validate results
    assertEquals("hudi", result.get("spark.sql.sources.provider"));
    assertEquals("1", result.get("spark.sql.sources.schema.numParts"));
    assertEquals(
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"region\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}},{\"name\":\"category\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}",
        result.get("spark.sql.sources.schema.part.0"));
    assertEquals("2", result.get("spark.sql.sources.schema.numPartCols"));
    assertEquals("region", result.get("spark.sql.sources.schema.partCol.0"));
    assertEquals("category", result.get("spark.sql.sources.schema.partCol.1"));
    assertEquals("true", result.get(HUDI_METADATA_CONFIG));
  }

  @Test
  void testGetSparkTablePropertiesEmptyPartitions() {
    // Setup input data with no partitions
    int schemaLengthThreshold = 50;
    InternalSchema schema =
        InternalSchema.builder()
            .fields(
                Arrays.asList(
                    InternalField.builder()
                        .name("id")
                        .schema(
                            InternalSchema.builder()
                                .dataType(InternalType.INT)
                                .isNullable(false)
                                .build())
                        .build(),
                    InternalField.builder()
                        .name("name")
                        .schema(
                            InternalSchema.builder()
                                .dataType(InternalType.STRING)
                                .isNullable(false)
                                .build())
                        .build()))
            .dataType(InternalType.RECORD)
            .name("testSchema")
            .build();

    InternalTable table =
        InternalTable.builder()
            .name("test-table")
            .tableFormat(TableFormat.HUDI)
            .readSchema(schema)
            .partitioningFields(Collections.emptyList())
            .build();
    // Call the method
    Map<String, String> result =
        HudiCatalogTablePropertiesExtractor.getInstance()
            .getTableProperties(table, schemaLengthThreshold);

    assertEquals("hudi", result.get("spark.sql.sources.provider"));
    assertNull(result.get("spark.sql.create.version"));
    assertEquals("4", result.get("spark.sql.sources.schema.numParts"));
    assertEquals(
        "{\"type\":\"struct\",\"fields\":[{\"name\":\"id\",\"type\":\"integer\",\"nullable\":false,\"metadata\":{}},{\"name\":\"name\",\"type\":\"string\",\"nullable\":false,\"metadata\":{}}]}",
        result.get("spark.sql.sources.schema.part.0")
            + result.get("spark.sql.sources.schema.part.1")
            + result.get("spark.sql.sources.schema.part.2")
            + result.get("spark.sql.sources.schema.part.3"));
    assertNull(result.get("spark.sql.sources.schema.numPartCols"));
    assertEquals("true", result.get(HUDI_METADATA_CONFIG));
  }

  @Test
  void testGetSerdeProperties() {
    String basePath = "/test/base/path";
    Map<String, String> serdeProperties =
        HudiCatalogTablePropertiesExtractor.getInstance().getSerdeProperties(basePath);

    assertNotNull(serdeProperties);
    assertEquals(basePath, serdeProperties.get(ConfigUtils.TABLE_SERDE_PATH));
    assertEquals("false", serdeProperties.get(ConfigUtils.IS_QUERY_AS_RO_TABLE));
  }
}
