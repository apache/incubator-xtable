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
 
package org.apache.xtable.paimon;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryRowWriter;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.table.FileStoreTable;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.xtable.TestPaimonTable;
import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;

public class TestPaimonPartitionExtractor {
  private static final PaimonPartitionExtractor extractor = PaimonPartitionExtractor.getInstance();

  @TempDir private Path tempDir;

  @Test
  void testToInternalPartitionFieldsWithEmptyKeys() {
    InternalSchema schema = createMockSchema();

    List<InternalPartitionField> result = extractor.toInternalPartitionFields(null, schema);
    assertEquals(Collections.emptyList(), result);

    result = extractor.toInternalPartitionFields(Collections.emptyList(), schema);
    assertEquals(Collections.emptyList(), result);
  }

  @Test
  void testToInternalPartitionFieldsWithSingleKey() {
    InternalSchema schema = createMockSchema();
    List<String> partitionKeys = Collections.singletonList("level");

    List<InternalPartitionField> result =
        extractor.toInternalPartitionFields(partitionKeys, schema);

    assertEquals(1, result.size());
    InternalPartitionField partitionField = result.get(0);
    assertEquals("level", partitionField.getSourceField().getName());
    assertEquals(PartitionTransformType.VALUE, partitionField.getTransformType());
  }

  @Test
  void testToInternalPartitionFieldsWithMultipleKeys() {
    InternalSchema schema = createMockSchema();
    List<String> partitionKeys = Arrays.asList("level", "status");

    List<InternalPartitionField> result =
        extractor.toInternalPartitionFields(partitionKeys, schema);

    assertEquals(2, result.size());
    assertEquals("level", result.get(0).getSourceField().getName());
    assertEquals("status", result.get(1).getSourceField().getName());
    assertEquals(PartitionTransformType.VALUE, result.get(0).getTransformType());
    assertEquals(PartitionTransformType.VALUE, result.get(1).getTransformType());
  }

  @Test
  void testToInternalPartitionFieldsWithMissingKey() {
    InternalSchema schema = createMockSchema();
    List<String> partitionKeys = Collections.singletonList("missing_key");

    ReadException exception =
        assertThrows(
            ReadException.class, () -> extractor.toInternalPartitionFields(partitionKeys, schema));

    assertTrue(exception.getMessage().contains("Partition key not found in schema: missing_key"));
  }

  @Test
  void testToPartitionValuesWithPartitionedTable() {
    TestPaimonTable testTable = createPartitionedTable();
    FileStoreTable paimonTable = testTable.getPaimonTable();

    testTable.insertRows(1);

    BinaryRow partition = BinaryRow.singleColumn("INFO");

    InternalSchema schema = createMockSchema();
    List<PartitionValue> result = extractor.toPartitionValues(paimonTable, partition, schema);

    assertEquals(1, result.size());
    PartitionValue partitionValue = result.get(0);
    assertEquals("level", partitionValue.getPartitionField().getSourceField().getName());
    assertEquals(Range.scalar("INFO"), partitionValue.getRange());
  }

  @Test
  @Disabled("TODO: make it easier to create multi-partitioned table in tests")
  void testToPartitionPathWithMultiplePartitionValues() {
    // TODO this table is fixed at single partition, need to create a multi-partitioned table
    TestPaimonTable testTable = createPartitionedTable();
    FileStoreTable paimonTable = testTable.getPaimonTable();

    BinaryRow partition = new BinaryRow(2);
    BinaryRowWriter writer = new BinaryRowWriter(partition);
    writer.writeString(0, BinaryString.fromString("INFO"));
    writer.writeString(1, BinaryString.fromString("active"));
    writer.complete();

    Optional<String> result = extractor.toPartitionPath(paimonTable, partition);

    assertTrue(result.isPresent());
    assertEquals("level=INFO/level=DEBUG", result.get());
  }

  @Test
  void testToPartitionPathWithEmptyPartitions() {
    TestPaimonTable testTable = createUnpartitionedTable();
    FileStoreTable paimonTable = testTable.getPaimonTable();

    BinaryRow emptyPartition = BinaryRow.EMPTY_ROW;

    Optional<String> result = extractor.toPartitionPath(paimonTable, emptyPartition);

    assertFalse(result.isPresent());
  }

  private InternalSchema createMockSchema() {
    InternalField levelField =
        InternalField.builder()
            .name("level")
            .schema(
                InternalSchema.builder()
                    .name("STRING")
                    .dataType(InternalType.STRING)
                    .isNullable(true)
                    .build())
            .build();

    InternalField statusField =
        InternalField.builder()
            .name("status")
            .schema(
                InternalSchema.builder()
                    .name("STRING")
                    .dataType(InternalType.STRING)
                    .isNullable(true)
                    .build())
            .build();

    return InternalSchema.builder()
        .name("test_schema")
        .dataType(InternalType.RECORD)
        .fields(Arrays.asList(levelField, statusField))
        .build();
  }

  private TestPaimonTable createPartitionedTable() {
    return (TestPaimonTable)
        TestPaimonTable.createTable("test_table", "level", tempDir, new Configuration(), false);
  }

  private TestPaimonTable createUnpartitionedTable() {
    return (TestPaimonTable)
        TestPaimonTable.createTable("test_table", null, tempDir, new Configuration(), false);
  }
}
