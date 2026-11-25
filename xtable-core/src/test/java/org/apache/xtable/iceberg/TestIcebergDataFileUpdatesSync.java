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

import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.iceberg.DataOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFilesDiff;

/**
 * Iceberg's own writer names a snapshot after the statement that wrote it, so the operation of a
 * published snapshot depends on whether the source commit was a delete as well as on its files.
 */
class TestIcebergDataFileUpdatesSync {
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  private static final List<InternalDataFile> NONE = Collections.emptyList();
  private static final Map<String, List<Long>> NO_POSITIONS = Collections.emptyMap();

  @TempDir static Path tempDir;

  private final IcebergDataFileUpdatesSync sync =
      IcebergDataFileUpdatesSync.of(
          IcebergColumnStatsConverter.getInstance(), IcebergPartitionValueConverter.getInstance());

  private static Table newTable() {
    return new HadoopTables(new Configuration())
        .create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            Collections.singletonMap(TableProperties.FORMAT_VERSION, "3"),
            tempDir.resolve("table_" + UUID.randomUUID()).toUri().toString());
  }

  private static InternalDataFile dataFile(Table table, String name) {
    return InternalDataFile.builder()
        .physicalPath(table.location() + "/" + name + ".parquet")
        .fileFormat(FileFormat.APACHE_PARQUET)
        .fileSizeBytes(100)
        .recordCount(10)
        .partitionValues(Collections.emptyList())
        .columnStats(Collections.emptyList())
        .build();
  }

  private static InternalFilesDiff diff(
      List<InternalDataFile> added, List<InternalDataFile> removed) {
    return InternalFilesDiff.builder().filesAdded(added).filesRemoved(removed).build();
  }

  private static TableSyncMetadata metadata(long second) {
    return TableSyncMetadata.of(
        Instant.ofEpochSecond(second), Collections.emptyList(), "HUDI", "source");
  }

  private void applyDiff(
      Table table, InternalFilesDiff diff, boolean deleteOperation, TableSyncMetadata metadata) {
    Transaction transaction = table.newTransaction();
    sync.applyDiff(
        transaction,
        diff,
        deleteOperation,
        transaction.table().schema(),
        transaction.table().spec(),
        metadata);
    transaction.commitTransaction();
    table.refresh();
  }

  private void applyRowDelta(
      Table table,
      InternalFilesDiff diff,
      Map<String, ? extends Collection<Long>> deletedPositions,
      boolean deleteOperation,
      TableSyncMetadata metadata) {
    Transaction transaction = table.newTransaction();
    sync.applyRowDelta(
        table,
        transaction,
        diff,
        deletedPositions,
        Collections.emptyList(),
        false,
        deleteOperation,
        null,
        transaction.table().schema(),
        transaction.table().spec(),
        metadata);
    transaction.commitTransaction();
    table.refresh();
  }

  private static List<String> operations(Table table) {
    return StreamSupport.stream(table.snapshots().spliterator(), false)
        .map(Snapshot::operation)
        .collect(Collectors.toList());
  }

  private static TableSyncMetadata currentMetadata(Table table) {
    return TableSyncMetadata.fromJson(
            table.currentSnapshot().summary().get(TableSyncMetadata.XTABLE_METADATA))
        .get();
  }

  @Test
  void anEmptyDiffOfADeleteIsPublishedAsADelete() {
    Table table = newTable();
    applyDiff(table, diff(Arrays.asList(dataFile(table, "a")), NONE), false, metadata(0));

    applyDiff(table, diff(NONE, NONE), true, metadata(1));

    assertEquals(Arrays.asList(DataOperations.APPEND, DataOperations.DELETE), operations(table));
    assertEquals(metadata(1), currentMetadata(table));
    assertEquals("1", table.currentSnapshot().summary().get("total-data-files"));
  }

  @Test
  void anEmptyDiffOfAnyOtherCommitIsPublishedAsAnOverwrite() {
    Table table = newTable();
    applyDiff(table, diff(Arrays.asList(dataFile(table, "a")), NONE), false, metadata(0));

    applyDiff(table, diff(NONE, NONE), false, metadata(1));

    assertEquals(Arrays.asList(DataOperations.APPEND, DataOperations.OVERWRITE), operations(table));
    assertEquals(metadata(1), currentMetadata(table));
  }

  @Test
  void aDeleteThatChangesFilesIsLabelledByItsFiles() {
    Table table = newTable();
    InternalDataFile a = dataFile(table, "a");
    InternalDataFile b = dataFile(table, "b");
    InternalDataFile rewrittenA = dataFile(table, "a2");
    applyDiff(table, diff(Arrays.asList(a, b), NONE), false, metadata(0));

    applyDiff(table, diff(Arrays.asList(rewrittenA), Arrays.asList(a)), true, metadata(1));
    applyDiff(table, diff(NONE, Arrays.asList(b)), true, metadata(2));

    assertEquals(
        Arrays.asList(DataOperations.APPEND, DataOperations.OVERWRITE, DataOperations.DELETE),
        operations(table));
    assertEquals(metadata(2), currentMetadata(table));
  }

  @Test
  void aRowDeltaOfADeleteThatMatchesNoRowsIsPublishedAsADelete() {
    Table table = newTable();
    InternalDataFile a = dataFile(table, "a");
    applyRowDelta(table, diff(Arrays.asList(a), NONE), NO_POSITIONS, false, metadata(0));

    // Delete blocks located by record key that match no row leave an empty set for their file.
    applyRowDelta(
        table,
        diff(NONE, NONE),
        Collections.singletonMap(a.getPhysicalPath(), Collections.<Long>emptyList()),
        true,
        metadata(1));

    assertEquals(Arrays.asList(DataOperations.APPEND, DataOperations.DELETE), operations(table));
    assertEquals(metadata(1), currentMetadata(table));
  }

  @Test
  void anEmptyRowDeltaOfADeleteIsPublishedAsADelete() {
    Table table = newTable();
    applyRowDelta(
        table, diff(Arrays.asList(dataFile(table, "a")), NONE), NO_POSITIONS, false, metadata(0));

    applyRowDelta(table, diff(NONE, NONE), NO_POSITIONS, true, metadata(1));

    assertEquals(Arrays.asList(DataOperations.APPEND, DataOperations.DELETE), operations(table));
    assertEquals(metadata(1), currentMetadata(table));
    assertEquals("1", table.currentSnapshot().summary().get("total-data-files"));
  }

  @Test
  void anEmptyRowDeltaOfAnyOtherCommitIsPublishedAsAnOverwrite() {
    Table table = newTable();
    applyRowDelta(
        table, diff(Arrays.asList(dataFile(table, "a")), NONE), NO_POSITIONS, false, metadata(0));

    applyRowDelta(table, diff(NONE, NONE), NO_POSITIONS, false, metadata(1));

    assertEquals(Arrays.asList(DataOperations.APPEND, DataOperations.OVERWRITE), operations(table));
  }

  @Test
  void aDeleteThatWritesDeletionVectorsIsLabelledByItsFiles() {
    Table table = newTable();
    InternalDataFile a = dataFile(table, "a");
    applyRowDelta(table, diff(Arrays.asList(a), NONE), NO_POSITIONS, false, metadata(0));

    applyRowDelta(
        table,
        diff(NONE, NONE),
        Collections.singletonMap(a.getPhysicalPath(), Arrays.asList(1L, 2L)),
        true,
        metadata(1));

    assertEquals(Arrays.asList(DataOperations.APPEND, DataOperations.DELETE), operations(table));
    assertEquals("1", table.currentSnapshot().summary().get("total-delete-files"));
    assertEquals(metadata(1), currentMetadata(table));
  }
}
