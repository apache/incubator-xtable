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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.hadoop.HadoopTableOperations;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.ScanMetricsResult;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.LockManagers;

import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.UpdateException;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.storage.TableFormat;

/**
 * A files diff with deleted positions is committed as one row delta that either lands whole or
 * fails: it never leaves a deletion vector on a file that is not live, and never ignores a file it
 * was asked to remove.
 */
class TestIcebergDeletionVectorSync {
  private static final Schema SCHEMA =
      new Schema(Types.NestedField.required(1, "id", Types.LongType.get()));
  private static final Schema PARTITIONED_SCHEMA =
      new Schema(
          Types.NestedField.required(1, "id", Types.LongType.get()),
          Types.NestedField.optional(2, "level", Types.StringType.get()),
          Types.NestedField.optional(3, "ts", Types.TimestampType.withoutZone()));
  private static final InternalSchema INTERNAL_PARTITIONED_SCHEMA =
      IcebergSchemaExtractor.getInstance().fromIceberg(PARTITIONED_SCHEMA);

  @TempDir static Path tempDir;

  private final IcebergDataFileUpdatesSync sync =
      IcebergDataFileUpdatesSync.of(
          IcebergColumnStatsConverter.getInstance(), IcebergPartitionValueConverter.getInstance());

  private static Table newTable(int formatVersion) {
    return new HadoopTables(new Configuration())
        .create(
            SCHEMA,
            PartitionSpec.unpartitioned(),
            Collections.singletonMap(TableProperties.FORMAT_VERSION, String.valueOf(formatVersion)),
            tempDir.resolve("table_" + UUID.randomUUID()).toUri().toString());
  }

  private static InternalDataFile dataFile(Table table, String name) {
    return dataFile(table, name, Collections.emptyList());
  }

  private static InternalDataFile dataFile(
      Table table, String name, List<PartitionValue> partitionValues) {
    return InternalDataFile.builder()
        .physicalPath(table.location() + "/" + name + ".parquet")
        .fileFormat(FileFormat.APACHE_PARQUET)
        .fileSizeBytes(100)
        .recordCount(10)
        .partitionValues(partitionValues)
        .columnStats(Collections.emptyList())
        .build();
  }

  private enum Partitioning {
    IDENTITY,
    DAY
  }

  private static Table newPartitionedTable(Partitioning partitioning) {
    PartitionSpec.Builder spec = PartitionSpec.builderFor(PARTITIONED_SCHEMA);
    return new HadoopTables(new Configuration())
        .create(
            PARTITIONED_SCHEMA,
            partitioning == Partitioning.IDENTITY
                ? spec.identity("level").build()
                : spec.day("ts").build(),
            Collections.singletonMap(TableProperties.FORMAT_VERSION, "3"),
            tempDir.resolve("table_" + UUID.randomUUID()).toUri().toString());
  }

  /** The i-th partition: level {@code level<i>}, or the i-th day of 2026. */
  private static List<PartitionValue> partition(Partitioning partitioning, int i) {
    boolean identity = partitioning == Partitioning.IDENTITY;
    InternalField sourceField =
        INTERNAL_PARTITIONED_SCHEMA.getFields().stream()
            .filter(field -> field.getName().equals(identity ? "level" : "ts"))
            .findFirst()
            .get();
    return Collections.singletonList(
        PartitionValue.builder()
            .partitionField(
                InternalPartitionField.builder()
                    .sourceField(sourceField)
                    .transformType(
                        identity ? PartitionTransformType.VALUE : PartitionTransformType.DAY)
                    .build())
            .range(
                Range.scalar(
                    identity
                        ? "level" + i
                        : Instant.parse("2026-01-01T00:00:00Z")
                            .plus(Duration.ofDays(i))
                            .toEpochMilli()))
            .build());
  }

  /**
   * A table with one data file and one deletion vector in each of the given number of partitions,
   * each added by its own commit, so every partition has its own data and delete manifest.
   */
  private List<InternalDataFile> filesWithDeletionVectors(
      Table table, Partitioning partitioning, int partitions) {
    List<InternalDataFile> files = new ArrayList<>();
    for (int i = 0; i < partitions; i++) {
      InternalDataFile file = dataFile(table, "f" + i, partition(partitioning, i));
      files.add(file);
      apply(
          table,
          Collections.singletonList(file),
          Collections.emptyList(),
          positions(file, 0L),
          Collections.singletonList(partition(partitioning, i)),
          false);
    }
    return files;
  }

  /** The same table, reporting the scans planned on it. */
  private static Table reportingScans(Table table, List<ScanReport> scans) {
    return new BaseTable(
        ((HasTableOperations) table).operations(),
        table.name(),
        report -> {
          if (report instanceof ScanReport) {
            scans.add((ScanReport) report);
          }
        });
  }

  private void apply(
      Table table,
      List<InternalDataFile> added,
      List<InternalDataFile> removed,
      Map<String, List<Long>> deletedPositions,
      boolean replaceDeletionVectors) {
    apply(table, added, removed, deletedPositions, Collections.emptyList(), replaceDeletionVectors);
  }

  private void apply(
      Table table,
      List<InternalDataFile> added,
      List<InternalDataFile> removed,
      Map<String, List<Long>> deletedPositions,
      List<List<PartitionValue>> partitionsOfDataFiles,
      boolean replaceDeletionVectors) {
    apply(
        table,
        added,
        removed,
        deletedPositions,
        partitionsOfDataFiles,
        replaceDeletionVectors,
        false,
        null);
  }

  /** A row delta of a source delete, or not, and which files the source holds no live rows of. */
  private void applyDelete(
      Table table,
      List<InternalDataFile> removed,
      Map<String, List<Long>> deletedPositions,
      boolean deleteOperation,
      Predicate<String> withoutLiveRows) {
    apply(
        table,
        Collections.emptyList(),
        removed,
        deletedPositions,
        Collections.emptyList(),
        false,
        deleteOperation,
        withoutLiveRows);
  }

  private void apply(
      Table table,
      List<InternalDataFile> added,
      List<InternalDataFile> removed,
      Map<String, List<Long>> deletedPositions,
      List<List<PartitionValue>> partitionsOfDataFiles,
      boolean replaceDeletionVectors,
      boolean deleteOperation,
      Predicate<String> withoutLiveRows) {
    Transaction transaction = table.newTransaction();
    sync.applyRowDelta(
        table,
        transaction,
        InternalFilesDiff.builder().filesAdded(added).filesRemoved(removed).build(),
        deletedPositions,
        partitionsOfDataFiles,
        replaceDeletionVectors,
        deleteOperation,
        withoutLiveRows,
        transaction.table().schema(),
        transaction.table().spec(),
        TableSyncMetadata.of(Instant.now(), Collections.emptyList()));
    transaction.commitTransaction();
    table.refresh();
  }

  /** The positions deleted from each live data file, by file name, as an Iceberg reader loads. */
  @SneakyThrows
  private static Map<String, Set<Long>> deletedPositions(Table table) {
    Map<String, Set<Long>> deleted = new TreeMap<>();
    BaseDeleteLoader loader =
        new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile.location()));
    int deletionVectors = 0;
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        String location = task.file().location();
        Set<Long> positions = new TreeSet<>();
        deletionVectors += task.deletes().size();
        if (!task.deletes().isEmpty()) {
          loader.loadPositionDeletes(task.deletes(), location).forEach(positions::add);
        }
        String fileName = location.substring(location.lastIndexOf('/') + 1);
        deleted.put(fileName.substring(0, fileName.indexOf('.')), positions);
      }
    }
    String recorded =
        table.currentSnapshot() == null
            ? "0"
            : table.currentSnapshot().summary().getOrDefault("total-delete-files", "0");
    assertEquals(String.valueOf(deletionVectors), recorded, "deletion vectors of files not live");
    return deleted;
  }

  private static Map<String, Set<Long>> expected(Object... fileThenPositions) {
    Map<String, Set<Long>> expected = new TreeMap<>();
    for (int i = 0; i < fileThenPositions.length; i += 2) {
      @SuppressWarnings("unchecked")
      List<Long> positions = (List<Long>) fileThenPositions[i + 1];
      expected.put((String) fileThenPositions[i], new TreeSet<>(positions));
    }
    return expected;
  }

  private static Map<String, List<Long>> positions(InternalDataFile file, Long... positions) {
    return Collections.singletonMap(file.getPhysicalPath(), Arrays.asList(positions));
  }

  @Test
  void positionsAreMergedIntoTheCurrentDeletionVector() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    InternalDataFile b = dataFile(table, "b");
    apply(table, Arrays.asList(a, b), Collections.emptyList(), Collections.emptyMap(), false);
    assertEquals(expected("a", Arrays.asList(), "b", Arrays.asList()), deletedPositions(table));

    apply(table, Collections.emptyList(), Collections.emptyList(), positions(a, 0L, 3L), false);
    assertEquals(
        expected("a", Arrays.asList(0L, 3L), "b", Arrays.asList()), deletedPositions(table));

    apply(table, Collections.emptyList(), Collections.emptyList(), positions(a, 3L, 7L), false);
    assertEquals(
        expected("a", Arrays.asList(0L, 3L, 7L), "b", Arrays.asList()), deletedPositions(table));
  }

  @Test
  void positionsReplaceTheCurrentDeletionVector() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    apply(table, Arrays.asList(a), Collections.emptyList(), positions(a, 0L, 3L), false);
    assertEquals(expected("a", Arrays.asList(0L, 3L)), deletedPositions(table));

    apply(table, Collections.emptyList(), Collections.emptyList(), positions(a, 5L), true);
    assertEquals(expected("a", Arrays.asList(5L)), deletedPositions(table));

    apply(table, Collections.emptyList(), Collections.emptyList(), positions(a), true);
    assertEquals(expected("a", Arrays.asList()), deletedPositions(table));
  }

  @Test
  void removingADataFileRemovesItsDeletionVector() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    InternalDataFile b = dataFile(table, "b");
    apply(table, Arrays.asList(a), Collections.emptyList(), positions(a, 1L), false);

    // The replacement carries its own deletes in the commit that adds it.
    apply(table, Arrays.asList(b), Arrays.asList(a), positions(b, 2L), true);
    assertEquals(expected("b", Arrays.asList(2L)), deletedPositions(table));
  }

  private static List<Long> range(long from, long to) {
    List<Long> positions = new ArrayList<>();
    for (long position = from; position < to; position++) {
      positions.add(position);
    }
    return positions;
  }

  private static Map<String, List<Long>> positions(InternalDataFile file, List<Long> positions) {
    return Collections.singletonMap(file.getPhysicalPath(), positions);
  }

  @Test
  void aDeleteOfEveryRowOfAFileRemovesTheFileAndItsDeletionVector() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    InternalDataFile b = dataFile(table, "b");
    apply(table, Arrays.asList(a, b), Collections.emptyList(), Collections.emptyMap(), false);
    applyDelete(table, Collections.emptyList(), positions(a, range(0, 4)), true, null);
    assertEquals(expected("a", range(0, 4), "b", Arrays.asList()), deletedPositions(table));

    // The rows left in a, with two positions the deletion vector already holds.
    applyDelete(table, Collections.emptyList(), positions(a, range(2, 10)), true, null);
    assertEquals(expected("b", Arrays.asList()), deletedPositions(table));
    Snapshot delete = table.currentSnapshot();
    assertEquals(DataOperations.DELETE, delete.operation());
    assertEquals("1", delete.summary().get(SnapshotSummary.DELETED_FILES_PROP));
    assertEquals("1", delete.summary().get(SnapshotSummary.REMOVED_DVS_PROP));
    assertNull(delete.summary().get(SnapshotSummary.ADDED_DVS_PROP));
    assertEquals("10", delete.summary().get(SnapshotSummary.TOTAL_RECORDS_PROP));
  }

  @Test
  void aDeleteOfEveryRowOfOneFileAndSomeOfAnotherIsOneRowDelta() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    InternalDataFile b = dataFile(table, "b");
    apply(table, Arrays.asList(a, b), Collections.emptyList(), Collections.emptyMap(), false);
    Map<String, List<Long>> deletes = new LinkedHashMap<>();
    deletes.put(a.getPhysicalPath(), range(0, 10));
    deletes.put(b.getPhysicalPath(), Arrays.asList(3L));
    applyDelete(table, Collections.emptyList(), deletes, true, null);

    assertEquals(expected("b", Arrays.asList(3L)), deletedPositions(table));
    Snapshot delete = table.currentSnapshot();
    assertEquals(DataOperations.DELETE, delete.operation());
    assertEquals("1", delete.summary().get(SnapshotSummary.DELETED_FILES_PROP));
    assertEquals("1", delete.summary().get(SnapshotSummary.ADDED_DVS_PROP));
  }

  @Test
  void anUpdateOfEveryRowOfAFileKeepsTheFileWithADeletionVector() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    apply(table, Arrays.asList(a), Collections.emptyList(), Collections.emptyMap(), false);
    applyDelete(table, Collections.emptyList(), positions(a, range(0, 10)), false, null);
    assertEquals(expected("a", range(0, 10)), deletedPositions(table));
  }

  @Test
  void aFileThatAWholeDeleteRemovedIsLeftOutOfLaterDiffsWhenItHoldsNoLiveRows() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    InternalDataFile b = dataFile(table, "b");
    apply(table, Arrays.asList(a, b), Collections.emptyList(), Collections.emptyMap(), false);
    applyDelete(table, Collections.emptyList(), positions(a, range(0, 10)), true, null);
    long snapshot = table.currentSnapshot().snapshotId();

    // Deleting rows of a again, or removing it, fails unless the source holds no live rows of it.
    Predicate<String> aHoldsNoLiveRows = a.getPhysicalPath()::equals;
    assertThrows(
        UpdateException.class,
        () -> applyDelete(table, Collections.emptyList(), positions(a, 1L), false, path -> false));
    assertThrows(
        ValidationException.class,
        () -> applyDelete(table, Arrays.asList(a), Collections.emptyMap(), false, path -> false));
    table.refresh();
    assertEquals(snapshot, table.currentSnapshot().snapshotId());

    applyDelete(table, Collections.emptyList(), positions(a, 1L), true, aHoldsNoLiveRows);
    assertEquals(DataOperations.DELETE, table.currentSnapshot().operation());
    Map<String, List<Long>> deletes = new LinkedHashMap<>();
    deletes.put(a.getPhysicalPath(), Arrays.asList(1L));
    deletes.put(b.getPhysicalPath(), Arrays.asList(2L));
    applyDelete(table, Collections.emptyList(), deletes, false, aHoldsNoLiveRows);
    assertEquals(expected("b", Arrays.asList(2L)), deletedPositions(table));
    InternalDataFile c = dataFile(table, "c");
    apply(
        table,
        Arrays.asList(c),
        Arrays.asList(a, b),
        Collections.emptyMap(),
        Collections.emptyList(),
        false,
        false,
        aHoldsNoLiveRows);
    assertEquals(expected("c", Arrays.asList()), deletedPositions(table));

    // A file that is not in the table for any other reason still fails the commit.
    InternalDataFile unknown = dataFile(table, "unknown");
    assertThrows(
        UpdateException.class,
        () ->
            applyDelete(
                table, Collections.emptyList(), positions(unknown, 1L), false, aHoldsNoLiveRows));
    assertThrows(
        ValidationException.class,
        () ->
            applyDelete(
                table, Arrays.asList(unknown), Collections.emptyMap(), false, aHoldsNoLiveRows));
  }

  /** Commits the rewrite unless the sync leaves it to the caller, returning what the sync did. */
  private Set<String> rewrite(
      Table table,
      List<InternalDataFile> added,
      List<InternalDataFile> removed,
      Map<String, List<Long>> deletedPositions,
      Predicate<String> withoutLiveRows) {
    Transaction transaction = table.newTransaction();
    Set<String> deletionVectorFiles =
        sync.applyRewrite(
            table,
            transaction,
            InternalFilesDiff.builder().filesAdded(added).filesRemoved(removed).build(),
            deletedPositions,
            withoutLiveRows,
            transaction.table().schema(),
            transaction.table().spec(),
            TableSyncMetadata.of(Instant.now(), Collections.emptyList()));
    if (deletionVectorFiles != null) {
      transaction.commitTransaction();
    }
    table.refresh();
    return deletionVectorFiles;
  }

  @Test
  void aRewriteIsAReplaceWhoseFilesKeepTheStartingSequenceNumber() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    InternalDataFile b = dataFile(table, "b");
    apply(table, Arrays.asList(a, b), Collections.emptyList(), positions(a, 1L, 2L), false);
    long startingSequenceNumber = table.currentSnapshot().sequenceNumber();

    InternalDataFile c = dataFile(table, "c");
    InternalDataFile d = dataFile(table, "d");
    assertEquals(
        1, rewrite(table, Arrays.asList(c, d), Arrays.asList(a, b), positions(c, 4L), null).size());
    assertEquals(expected("c", Arrays.asList(4L), "d", Arrays.asList()), deletedPositions(table));
    Snapshot replace = table.currentSnapshot();
    assertEquals(DataOperations.REPLACE, replace.operation());
    assertEquals("2", replace.summary().get(SnapshotSummary.DELETED_FILES_PROP));
    assertEquals("1", replace.summary().get(SnapshotSummary.REMOVED_DVS_PROP));
    assertEquals("1", replace.summary().get(SnapshotSummary.ADDED_DVS_PROP));
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        assertEquals(startingSequenceNumber, task.file().dataSequenceNumber());
        task.deletes()
            .forEach(
                deletionVector ->
                    assertEquals(replace.sequenceNumber(), deletionVector.dataSequenceNumber()));
      }
    } catch (java.io.IOException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  void aDiffIcebergCannotExpressAsARewriteIsLeftToTheCaller() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    InternalDataFile b = dataFile(table, "b");
    InternalDataFile c = dataFile(table, "c");
    InternalDataFile unknown = dataFile(table, "unknown");
    apply(table, Arrays.asList(a, b), Collections.emptyList(), Collections.emptyMap(), false);
    long snapshot = table.currentSnapshot().snapshotId();

    Map<String, List<Long>> none = Collections.emptyMap();
    List<InternalDataFile> addsC = Arrays.asList(c);
    assertNull(rewrite(table, addsC, Collections.emptyList(), none, null), "removes nothing");
    assertNull(rewrite(table, addsC, Arrays.asList(unknown), none, null), "removes a missing file");
    assertNull(
        rewrite(table, addsC, Arrays.asList(a), positions(b, 1L), null),
        "deletes rows of a file it does not add");
    assertNull(
        rewrite(table, addsC, Arrays.asList(a), positions(c, 1L), null),
        "adds a deletion vector without removing one");
    Predicate<String> unknownHoldsNoLiveRows = unknown.getPhysicalPath()::equals;
    assertNull(
        rewrite(table, addsC, Arrays.asList(unknown), none, unknownHoldsNoLiveRows),
        "removes only a file the table does not hold");
    assertEquals(snapshot, table.currentSnapshot().snapshotId());

    assertEquals(
        Collections.emptySet(),
        rewrite(table, addsC, Arrays.asList(a, unknown), none, unknownHoldsNoLiveRows));
    assertEquals(DataOperations.REPLACE, table.currentSnapshot().operation());
    assertEquals(expected("b", Arrays.asList(), "c", Arrays.asList()), deletedPositions(table));
  }

  @Test
  void positionsOfAFileThatIsNotLiveFailTheCommit() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    InternalDataFile unknown = dataFile(table, "unknown");
    apply(table, Arrays.asList(a), Collections.emptyList(), Collections.emptyMap(), false);
    long snapshot = table.currentSnapshot().snapshotId();

    UpdateException notLive =
        assertThrows(
            UpdateException.class,
            () ->
                apply(
                    table,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    positions(unknown, 1L),
                    false));
    assertTrue(notLive.getMessage().contains("unknown.parquet"), notLive.getMessage());
    assertThrows(
        UpdateException.class,
        () -> apply(table, Collections.emptyList(), Arrays.asList(a), positions(a, 1L), false));
    table.refresh();
    assertEquals(snapshot, table.currentSnapshot().snapshotId());
  }

  @Test
  void removingAFileTheTableDoesNotHoldFailsTheCommit() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    apply(table, Arrays.asList(a), Collections.emptyList(), Collections.emptyMap(), false);
    long snapshot = table.currentSnapshot().snapshotId();

    ValidationException missing =
        assertThrows(
            ValidationException.class,
            () ->
                apply(
                    table,
                    Collections.emptyList(),
                    Arrays.asList(dataFile(table, "unknown")),
                    Collections.emptyMap(),
                    false));
    assertTrue(missing.getMessage().contains("unknown.parquet"), missing.getMessage());
    table.refresh();
    assertEquals(snapshot, table.currentSnapshot().snapshotId());
  }

  @Test
  void aDataFileRemovedSinceTheDeletesWereReadFailsTheCommit() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    apply(table, Arrays.asList(a), Collections.emptyList(), Collections.emptyMap(), false);
    Table concurrent = new HadoopTables(new Configuration()).load(table.location());

    Transaction transaction = table.newTransaction();
    sync.applyRowDelta(
        table,
        transaction,
        InternalFilesDiff.builder()
            .filesAdded(Collections.emptyList())
            .filesRemoved(Collections.emptyList())
            .build(),
        positions(a, 1L),
        Collections.emptyList(),
        false,
        false,
        null,
        transaction.table().schema(),
        transaction.table().spec(),
        TableSyncMetadata.of(Instant.now(), Collections.emptyList()));
    apply(concurrent, Collections.emptyList(), Arrays.asList(a), Collections.emptyMap(), false);

    ValidationException missing =
        assertThrows(ValidationException.class, transaction::commitTransaction);
    assertTrue(missing.getMessage().contains("missing data files"), missing.getMessage());
    table.refresh();
    assertEquals(Collections.emptyMap(), deletedPositions(table));
  }

  @ParameterizedTest
  @EnumSource(Partitioning.class)
  void onlyTheManifestsOfTheTouchedPartitionAreScanned(Partitioning partitioning) {
    Table table = newPartitionedTable(partitioning);
    List<InternalDataFile> files = filesWithDeletionVectors(table, partitioning, 8);

    List<ScanReport> scans = new ArrayList<>();
    apply(
        reportingScans(table, scans),
        Collections.emptyList(),
        Collections.emptyList(),
        positions(files.get(3), 5L),
        Collections.singletonList(partition(partitioning, 3)),
        false);
    table.refresh();

    Map<String, Set<Long>> expected = new TreeMap<>();
    for (int i = 0; i < 8; i++) {
      expected.put("f" + i, new TreeSet<>(i == 3 ? Arrays.asList(0L, 5L) : Arrays.asList(0L)));
    }
    assertEquals(expected, deletedPositions(table));
    assertEquals(1, scans.size());
    ScanMetricsResult scan = scans.get(0).scanMetrics();
    assertEquals(8, scan.totalDataManifests().value());
    assertEquals(1, scan.scannedDataManifests().value());
    assertEquals(1, scan.scannedDeleteManifests().value());
    assertEquals(1, scan.resultDataFiles().value());
    assertEquals(1, scan.resultDeleteFiles().value());
  }

  @Test
  void aDataFileOutsideTheGivenPartitionsIsLookedUpInTheWholeTable() {
    Table table = newPartitionedTable(Partitioning.IDENTITY);
    List<InternalDataFile> files = filesWithDeletionVectors(table, Partitioning.IDENTITY, 3);

    List<ScanReport> scans = new ArrayList<>();
    apply(
        reportingScans(table, scans),
        Collections.emptyList(),
        Collections.emptyList(),
        positions(files.get(2), 5L),
        Collections.singletonList(partition(Partitioning.IDENTITY, 0)),
        false);
    table.refresh();

    assertEquals(
        expected("f0", Arrays.asList(0L), "f1", Arrays.asList(0L), "f2", Arrays.asList(0L, 5L)),
        deletedPositions(table));
    assertEquals(2, scans.size());
    assertEquals(1, scans.get(0).scanMetrics().scannedDataManifests().value());
    assertEquals(3, scans.get(1).scanMetrics().scannedDataManifests().value());
  }

  /** The deletion vector files under the table's location, committed or not. */
  @SneakyThrows
  private static Set<String> deletionVectorFiles(Table table) {
    try (Stream<Path> files = Files.walk(Paths.get(URI.create(table.location())))) {
      return files
          .map(Path::toString)
          .filter(name -> name.endsWith(".puffin"))
          .collect(Collectors.toCollection(TreeSet::new));
    }
  }

  /** A target that syncs to the given table, with no snapshot expiry at the end of a sync. */
  private IcebergConversionTarget target(Table table) {
    IcebergTableManager tableManager = mock(IcebergTableManager.class);
    when(tableManager.tableExists(any(), any(), any())).thenReturn(true);
    when(tableManager.getTable(any(), any(), any())).thenReturn(table);
    return new IcebergConversionTarget(
        TargetTable.builder()
            .name("deletion_vectors")
            .formatName(TableFormat.ICEBERG)
            .basePath(table.location())
            .metadataRetention(TargetTable.NO_METADATA_EXPIRY)
            .build(),
        new Configuration(),
        IcebergSchemaExtractor.getInstance(),
        IcebergSchemaSync.getInstance(),
        IcebergPartitionSpecExtractor.getInstance(),
        IcebergPartitionSpecSync.getInstance(),
        sync,
        tableManager);
  }

  /** Runs a sync of deleted positions through the target up to, not including, its commit. */
  private static void stageSync(IcebergConversionTarget target, Map<String, List<Long>> positions) {
    target.beginSync(null);
    target.syncMetadata(TableSyncMetadata.of(Instant.now(), Collections.emptyList()));
    target.stageDeletionVectors(positions, Collections.emptyList(), false);
    target.syncFilesForDiff(
        InternalFilesDiff.builder()
            .filesAdded(Collections.emptyList())
            .filesRemoved(Collections.emptyList())
            .build());
  }

  /** Table operations whose commits fail while a failure is set. */
  private static class FailingOperations extends HadoopTableOperations {
    private final boolean strictCleanup;
    private RuntimeException commitFailure;

    FailingOperations(String location, boolean strictCleanup) {
      super(
          new org.apache.hadoop.fs.Path(location),
          new HadoopFileIO(new Configuration()),
          new Configuration(),
          LockManagers.defaultLockManager());
      this.strictCleanup = strictCleanup;
    }

    @Override
    public void commit(TableMetadata base, TableMetadata metadata) {
      if (commitFailure != null) {
        throw commitFailure;
      }
      super.commit(base, metadata);
    }

    @Override
    public boolean requireStrictCleanup() {
      return strictCleanup;
    }
  }

  @Test
  void aRowDeltaThatFailsDeletesTheDeletionVectorFileItWrote() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    InternalDataFile unknown = dataFile(table, "unknown");
    apply(table, Arrays.asList(a), Collections.emptyList(), positions(a, 0L), false);
    Set<String> committed = deletionVectorFiles(table);
    assertEquals(1, committed.size());

    // The file to remove is missing, which fails the row delta once the vectors are written.
    assertThrows(
        ValidationException.class,
        () ->
            apply(table, Collections.emptyList(), Arrays.asList(unknown), positions(a, 1L), false));
    // A file that is not live fails it while the vectors are written, after one for a live file.
    Map<String, List<Long>> liveThenNotLive = new LinkedHashMap<>();
    liveThenNotLive.put(a.getPhysicalPath(), Arrays.asList(2L));
    liveThenNotLive.put(unknown.getPhysicalPath(), Arrays.asList(1L));
    assertThrows(
        UpdateException.class,
        () ->
            apply(table, Collections.emptyList(), Collections.emptyList(), liveThenNotLive, false));

    assertEquals(committed, deletionVectorFiles(table));
    assertEquals(expected("a", Arrays.asList(0L)), deletedPositions(table));
  }

  @Test
  void aCommitThatFailsValidationDeletesTheDeletionVectorFileOfItsSync() {
    Table table = newTable(3);
    InternalDataFile a = dataFile(table, "a");
    InternalDataFile b = dataFile(table, "b");
    apply(table, Arrays.asList(a, b), Collections.emptyList(), positions(b, 0L), false);
    Set<String> committed = deletionVectorFiles(table);

    IcebergConversionTarget target = target(table);
    stageSync(target, positions(a, 1L));
    assertEquals(committed.size() + 1, deletionVectorFiles(table).size());
    Table concurrent = new HadoopTables(new Configuration()).load(table.location());
    apply(concurrent, Collections.emptyList(), Arrays.asList(a), Collections.emptyMap(), false);
    long concurrentSnapshot = concurrent.currentSnapshot().snapshotId();

    ValidationException missing = assertThrows(ValidationException.class, target::completeSync);
    assertTrue(missing.getMessage().contains("missing data files"), missing.getMessage());
    assertEquals(committed, deletionVectorFiles(table));
    table.refresh();
    assertEquals(concurrentSnapshot, table.currentSnapshot().snapshotId());
    assertEquals(expected("b", Arrays.asList(0L)), deletedPositions(table));
  }

  static Stream<Arguments> commitFailures() {
    return Stream.of(
        Arguments.of(new CommitFailedException("conflict"), false, false),
        Arguments.of(new CommitFailedException("conflict"), true, false),
        Arguments.of(new RuntimeException("connection reset"), false, false),
        Arguments.of(new RuntimeException("connection reset"), true, true),
        Arguments.of(
            new CommitStateUnknownException(new RuntimeException("connection reset")),
            false,
            true));
  }

  @ParameterizedTest
  @MethodSource("commitFailures")
  void deletionVectorFilesAreKeptOnlyWhenTheFailedCommitMayHaveLanded(
      RuntimeException failure, boolean strictCleanup, boolean mayHaveLanded) {
    Table created = newTable(3);
    created.updateProperties().set(TableProperties.COMMIT_NUM_RETRIES, "0").commit();
    FailingOperations operations = new FailingOperations(created.location(), strictCleanup);
    Table table = new BaseTable(operations, created.location());
    InternalDataFile a = dataFile(table, "a");
    apply(table, Arrays.asList(a), Collections.emptyList(), positions(a, 0L), false);
    Set<String> committed = deletionVectorFiles(table);
    long snapshot = table.currentSnapshot().snapshotId();

    IcebergConversionTarget target = target(table);
    stageSync(target, positions(a, 1L));
    Set<String> written = deletionVectorFiles(table);
    written.removeAll(committed);
    assertEquals(1, written.size());
    operations.commitFailure = failure;

    assertSame(failure, assertThrows(RuntimeException.class, target::completeSync));
    Set<String> expectedFiles = new TreeSet<>(committed);
    if (mayHaveLanded) {
      expectedFiles.addAll(written);
    }
    assertEquals(expectedFiles, deletionVectorFiles(table));
    operations.commitFailure = null;
    table.refresh();
    assertEquals(snapshot, table.currentSnapshot().snapshotId());
    assertEquals(expected("a", Arrays.asList(0L)), deletedPositions(table));
  }

  @Test
  void deletionVectorsAreRefusedBelowFormatVersionThree() {
    Table table = newTable(2);
    InternalDataFile a = dataFile(table, "a");
    apply(table, Arrays.asList(a), Collections.emptyList(), Collections.emptyMap(), false);

    NotSupportedException refused =
        assertThrows(
            NotSupportedException.class,
            () ->
                apply(
                    table,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    positions(a, 1L),
                    false));
    assertTrue(refused.getMessage().contains("format version 3"), refused.getMessage());
  }
}
