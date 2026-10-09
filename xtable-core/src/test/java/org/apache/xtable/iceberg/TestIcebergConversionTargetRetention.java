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

import static org.apache.iceberg.types.Types.NestedField.required;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.model.storage.TableFormat;

/** Time-based snapshot expiry at the end of a sync, and how to turn it off. */
class TestIcebergConversionTargetRetention {

  @TempDir public static Path tempDir;

  private static final Schema SCHEMA = new Schema(required(1, "id", Types.IntegerType.get()));

  @Test
  void positiveRetentionExpiresOlderSnapshots() throws Exception {
    Table table = tableWithTwoSnapshots();
    completeSyncWithRetention(table, Duration.ofMillis(1));
    assertEquals(1, snapshotCount(table), "snapshots older than the retention must be expired");
  }

  @Test
  void noMetadataExpiryKeepsEverySnapshot() throws Exception {
    Table table = tableWithTwoSnapshots();
    completeSyncWithRetention(table, TargetTable.NO_METADATA_EXPIRY);
    assertEquals(2, snapshotCount(table), "expiry must be disabled entirely");
  }

  private Table tableWithTwoSnapshots() throws Exception {
    String location = tempDir.resolve("retention-" + UUID.randomUUID()).toString();
    Table table =
        new HadoopTables(new Configuration())
            .create(SCHEMA, PartitionSpec.unpartitioned(), location);
    table.newAppend().appendFile(dataFile(table, "a")).commit();
    // Ensure the second snapshot is measurably newer than the first.
    Thread.sleep(20);
    table.newAppend().appendFile(dataFile(table, "b")).commit();
    assertEquals(2, snapshotCount(table));
    return table;
  }

  private void completeSyncWithRetention(Table table, Duration retention) {
    IcebergConversionTarget target = new IcebergConversionTarget();
    target.init(
        TargetTable.builder()
            .name("retention")
            .formatName(TableFormat.ICEBERG)
            .basePath(table.location())
            .metadataRetention(retention)
            .build(),
        new Configuration());
    // The table already exists, so the sync only needs a transaction, not a table state.
    target.beginSync(null);
    target.completeSync();
    table.refresh();
  }

  private static DataFile dataFile(Table table, String name) {
    return DataFiles.builder(table.spec())
        .withPath(table.location() + "/data/" + name + ".parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(10)
        .withRecordCount(1)
        .build();
  }

  private static long snapshotCount(Table table) {
    return StreamSupport.stream(table.snapshots().spliterator(), false).count();
  }
}
