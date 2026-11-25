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
 
package org.apache.xtable.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Collections;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

/**
 * End-to-end test (real Iceberg table via {@link HadoopTables}, no mocks) for the identifier-field
 * reconciliation in {@link IcebergSchemaSync}. Before the fix, making an existing identifier field
 * optional during sync failed at commit with "Cannot add field ... as an identifier field: not a
 * required field" (apache/incubator-xtable#1034).
 */
public class TestIcebergSchemaSyncIdentifierReconcile {

  @TempDir Path tempDir;

  private final IcebergSchemaSync schemaSync = IcebergSchemaSync.getInstance();

  @Test
  void recordKeyBecomingNullableCommitsAndClearsIdentifierFields() {
    // Existing table: id is a required identifier (record key) field.
    Schema current =
        new Schema(
            java.util.Arrays.asList(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.optional(2, "name", Types.StringType.get())),
            Collections.singleton(1));
    // Source evolves id to nullable, so the latest schema has id optional and no identifier field.
    Schema latest =
        new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()));

    String location = tempDir.resolve("tbl-" + UUID.randomUUID()).toString();
    Table table =
        new HadoopTables(new Configuration())
            .create(current, PartitionSpec.unpartitioned(), location);
    assertEquals(Collections.singleton(1), table.schema().identifierFieldIds());

    Transaction txn = table.newTransaction();
    // This commit threw IllegalArgumentException before the fix.
    schemaSync.sync(table.schema(), latest, txn);
    txn.commitTransaction();

    table.refresh();
    assertTrue(table.schema().findField("id").isOptional(), "id must now be optional");
    assertFalse(
        table.schema().identifierFieldIds().contains(1),
        "stale identifier field must be cleared so the commit succeeds");
    assertTrue(table.schema().identifierFieldIds().isEmpty(), "no identifier fields expected");
  }
}
