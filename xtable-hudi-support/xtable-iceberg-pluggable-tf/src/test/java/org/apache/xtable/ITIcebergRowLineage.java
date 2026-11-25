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
 
package org.apache.xtable;

import static org.apache.xtable.IcebergTableAssertions.assertNothingPending;
import static org.apache.xtable.IcebergTableAssertions.assertPublishedAsARewrite;
import static org.apache.xtable.IcebergTableAssertions.createWithTableConfig;
import static org.apache.xtable.IcebergTableAssertions.icebergTable;
import static org.apache.xtable.IcebergTableAssertions.reconstructedTimeline;
import static org.apache.xtable.MergeOnReadAssertions.assertIcebergMatchesHudi;
import static org.apache.xtable.MergeOnReadAssertions.deletionVectorTable;
import static org.apache.xtable.MergeOnReadAssertions.icebergRows;
import static org.apache.xtable.RowLineageAssertions.assertRowLineage;
import static org.apache.xtable.RowLineageAssertions.parquetColumns;
import static org.apache.xtable.RowLineageAssertions.rowLineage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.BaseFileColumnResolver;
import org.apache.hudi.common.FormatMetaColumn;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieException;

import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;

import org.apache.xtable.RowLineageAssertions.Changes;
import org.apache.xtable.RowLineageAssertions.RowLineage;
import org.apache.xtable.model.metadata.TableSyncMetadata;

/**
 * Iceberg row lineage of a format version 3 table across the Hudi operations that rewrite base
 * files, read through Iceberg. A row that an operation carries over unchanged keeps its {@code
 * _row_id} and {@code _last_updated_sequence_number}; an updated row keeps its row id and gets the
 * sequence number of the operation, and a new row gets a new row id.
 */
class ITIcebergRowLineage {
  @TempDir public static Path tempDir;

  private static SparkSession spark;
  private static JavaSparkContext jsc;

  @BeforeAll
  static void setupOnce() {
    spark = MergeOnReadAssertions.sparkSession(tempDir);
    jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
  }

  @AfterAll
  static void teardown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static String tableName() {
    return "lineage_" + UUID.randomUUID().toString().replace('-', '_');
  }

  /** Reads the row lineage after each operation and checks it against the one before. */
  private static final class Tracker {
    private final String basePath;
    /** Null when the last updated sequence numbers are not checked against the commits. */
    private final Map<String, Long> sequenceNumberByCommit;

    private Map<String, RowLineage> current;

    private Tracker(String basePath) {
      this(basePath, true);
    }

    private Tracker(String basePath, boolean checkSequenceNumberOfCommit) {
      this.basePath = basePath;
      this.sequenceNumberByCommit = checkSequenceNumberOfCommit ? new HashMap<>() : null;
      recordSequenceNumbers();
      current = rowLineage(spark, basePath);
      assertRowLineage(new HashMap<>(), current, sequenceNumberByCommit);
    }

    private void recordSequenceNumbers() {
      if (sequenceNumberByCommit != null) {
        RowLineageAssertions.recordSequenceNumbers(basePath, sequenceNumberByCommit);
      }
    }

    private Changes next() {
      recordSequenceNumbers();
      Map<String, RowLineage> next = rowLineage(spark, basePath);
      Changes changes = assertRowLineage(current, next, sequenceNumberByCommit);
      current = next;
      return changes;
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void copyOnWriteRewritesKeepRowLineage(boolean partitioned) {
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName(),
            tempDir,
            partitioned ? "level:SIMPLE" : null,
            HoodieTableType.COPY_ON_WRITE,
            new Properties())) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(60, true);
      Tracker tracker = new Tracker(basePath);
      assertEquals(60, tracker.current.size());

      List<HoodieRecord<HoodieAvroPayload>> updates =
          oneRecordPerSharedFile(tracker.current, inserts, 10);
      table.upsertRecords(updates, true);
      assertRewritten(tracker.next(), 60 - updates.size(), updates.size(), updates.size());

      // Routed into the existing small base files, which are rewritten with the new records.
      String insert = table.startCommit();
      List<WriteStatus> written = table.getWriteClient().insert(table.generateRecords(6), insert);
      table.commitInstant(insert, written);
      assertRewritten(tracker.next(), 60, 6, 0);

      table.cluster();
      assertPublishedAsARewrite(basePath);
      Changes clustering = tracker.next();
      assertChanges(clustering, 66, 0, 0);
      assertEquals(66, clustering.unchangedMovedToAnotherFile);

      table.deleteRecords(inserts.subList(10, 15), true);
      assertRewritten(tracker.next(), 61, 0, 0);
      assertEquals(61, tracker.current.size());

      table.upsertRecords(inserts.subList(20, 30), true);
      assertRewritten(tracker.next(), 51, 10, 10);
      assertNothingPending(reconstructedTimeline(basePath));
      assertRowLineageColumnsNotInTable(basePath);
    }
  }

  /**
   * The Spark write client merges Spark records and clusters with its row writer, both through
   * engine-specific code.
   */
  @Test
  void sparkWriterRewritesKeepRowLineage() {
    try (TestSparkHudiTable table =
        TestSparkHudiTable.forStandardSchema(
            tableName(),
            tempDir,
            jsc,
            "level:SIMPLE",
            HoodieTableType.COPY_ON_WRITE,
            new Properties())) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(60, true);
      Tracker tracker = new Tracker(basePath);

      List<HoodieRecord<HoodieAvroPayload>> updates =
          oneRecordPerSharedFile(tracker.current, inserts, 10);
      table.upsertRecords(updates, true);
      assertRewritten(tracker.next(), 60 - updates.size(), updates.size(), updates.size());

      table.cluster();
      assertPublishedAsARewrite(basePath);
      Changes clustering = tracker.next();
      assertChanges(clustering, 60, 0, 0);
      assertEquals(60, clustering.unchangedMovedToAnotherFile);

      table.upsertRecords(inserts.subList(20, 30), true);
      assertRewritten(tracker.next(), 50, 10, 10);
      assertRowLineageColumnsNotInTable(basePath);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {9, 10})
  void mergeOnReadCompactionAndClusteringKeepRowLineage(int tableVersion) {
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName(),
            tempDir,
            null,
            HoodieTableType.MERGE_ON_READ,
            deletionVectorTable(tableVersion))) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(50, true);
      table.insertRecords(30, true);
      Tracker tracker = new Tracker(basePath);

      // An update is a delete from the base file and an insert into a new one, which keeps the row
      // id.
      table.upsertRecords(inserts.subList(0, 10), true);
      assertInPlace(tracker.next(), 70, 10, 10);
      table.deleteRecords(inserts.subList(10, 15), true);
      assertInPlace(tracker.next(), 75, 0, 0);
      assertIcebergMatchesHudi(spark, basePath);

      table.compact();
      assertPublishedAsARewrite(basePath);
      Changes compaction = tracker.next();
      assertRewritten(compaction, 75, 0, 0);
      assertIcebergMatchesHudi(spark, basePath);

      table.upsertRecords(inserts.subList(20, 25), true);
      assertInPlace(tracker.next(), 70, 5, 5);

      table.cluster();
      assertPublishedAsARewrite(basePath);
      Changes clustering = tracker.next();
      assertChanges(clustering, 75, 0, 0);
      assertEquals(75, clustering.unchangedMovedToAnotherFile);

      table.upsertRecords(inserts.subList(25, 30), true);
      assertInPlace(tracker.next(), 70, 5, 5);
      table.compact();
      assertRewritten(tracker.next(), 75, 0, 0);
      assertIcebergMatchesHudi(spark, basePath);
      assertNothingPending(reconstructedTimeline(basePath));
      assertRowLineageColumnsNotInTable(basePath);
    }
  }

  /**
   * The Spark datasource writer, which Spark SQL UPDATE and MERGE INTO write through, upserts Spark
   * rows that the merge of each file group writes with the row ids of the rows they replace.
   */
  @Test
  void sparkDatasourceUpsertKeepsTheRowIdsOfTheRowsItUpdates() {
    String tableName = tableName();
    String basePath = tempDir.resolve(tableName).toUri().toString();
    writeThroughDatasource(tableName, basePath, "insert", "1:a", "2:b", "3:c", "4:d");
    Tracker tracker = new Tracker(basePath);

    writeThroughDatasource(tableName, basePath, "upsert", "1:x");
    assertRewritten(tracker.next(), 3, 1, 1);

    writeThroughDatasource(tableName, basePath, "upsert", "2:y", "4:z", "5:e");
    assertRewritten(tracker.next(), 2, 3, 2);
    assertEquals(5, tracker.current.size());
  }

  private static void writeThroughDatasource(
      String tableName, String basePath, String operation, String... rows) {
    StructType schema =
        new StructType().add("key", DataTypes.StringType).add("data", DataTypes.StringType);
    List<Row> data =
        Arrays.stream(rows)
            .map(row -> row.split(":"))
            .map(values -> RowFactory.create(values[0], values[1]))
            .collect(Collectors.toList());
    spark
        .createDataFrame(data, schema)
        .write()
        .format("hudi")
        .option(HoodieTableConfig.NAME.key(), tableName)
        .option(HoodieTableConfig.TABLE_FORMAT.key(), "ICEBERG")
        .option(IcebergFormatConfig.FORMAT_VERSION, "3")
        .option("hoodie.datasource.write.table.type", HoodieTableType.COPY_ON_WRITE.name())
        .option("hoodie.datasource.write.recordkey.field", "key")
        .option(
            "hoodie.datasource.write.keygenerator.class",
            "org.apache.hudi.keygen.NonpartitionedKeyGenerator")
        .option("hoodie.datasource.write.operation", operation)
        .mode(SaveMode.Append)
        .save(basePath);
  }

  /**
   * A merge-on-read update written as a delete and an insert puts the new version of a row into
   * another data file, which stores the row id of the row it replaces, so the row keeps its id and
   * gets the sequence number of the update; updating it again keeps the id once more.
   */
  @Test
  void mergeOnReadUpdateWrittenAsDeleteAndInsertKeepsTheRowIds() {
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName(), tempDir, null, HoodieTableType.MERGE_ON_READ, deletionVectorTable(10))) {
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(20, true);
      Tracker tracker = new Tracker(table.getBasePath());
      table.upsertRecords(inserts.subList(0, 5), true);
      assertInPlace(tracker.next(), 15, 5, 5);
      table.upsertRecords(inserts.subList(3, 8), true);
      assertInPlace(tracker.next(), 15, 5, 5);
    }
  }

  /**
   * A rollback adds the restored base files to Iceberg again with new first row ids, so the rows
   * they do not store lineage for get new row ids and the rollback's sequence number; the rows and
   * the uniqueness of the ids are what it keeps. A later rewrite of a restored file keeps the ids
   * the rows have since the rollback.
   */
  @Test
  void rollbackRestoresTheRowsWithUniqueRowIds() {
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName(), tempDir, null, HoodieTableType.COPY_ON_WRITE, new Properties())) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(40, true);
      Tracker tracker = new Tracker(basePath);
      table.upsertRecords(inserts.subList(0, 10), true);
      assertRewritten(tracker.next(), 30, 10, 10);
      List<String> rowsBefore = icebergRows(spark, basePath);
      table.upsertRecords(inserts.subList(5, 15), true);
      assertRewritten(tracker.next(), 30, 10, 10);
      String upsert =
          table.getMetaClient().reloadActiveTimeline().lastInstant().get().requestedTime();

      table.rollback(upsert);
      assertEquals(rowsBefore, icebergRows(spark, basePath));
      Tracker restored = new Tracker(basePath, false);
      assertEquals(40, restored.current.size());

      table.upsertRecords(inserts.subList(30, 35), true);
      assertRewritten(restored.next(), 35, 5, 5);
    }
  }

  /**
   * A format version 2 table declares no row lineage, so its rewritten base files stay as they are.
   */
  @Test
  void formatVersionTwoKeepsBaseFilesAsTheyAre() {
    Properties properties = new Properties();
    properties.setProperty(IcebergFormatConfig.FORMAT_VERSION, "2");
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName(), tempDir, null, HoodieTableType.COPY_ON_WRITE, properties)) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(40, true);
      List<String> insertedFileColumns =
          parquetColumns(table.getMetaClient(), table.getAllLatestBaseFilePaths().get(0));
      table.upsertRecords(inserts.subList(0, 10), true);
      table.cluster();
      table.upsertRecords(inserts.subList(10, 20), true);

      HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(table.getMetaClient());
      assertTrue(metaClient.getTableFormat().getFormatMetaColumns().isEmpty());
      for (String baseFile : table.getAllLatestBaseFilePaths()) {
        assertEquals(insertedFileColumns, parquetColumns(metaClient, baseFile));
      }
      Table icebergTable = icebergTable(basePath);
      assertEquals(2, TableUtil.formatVersion(icebergTable));
      NameMapping mapping =
          NameMappingParser.fromJson(
              icebergTable.properties().get(TableProperties.DEFAULT_NAME_MAPPING));
      assertNull(mapping.find(MetadataColumns.ROW_ID.name()));
      assertNull(mapping.find(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()));
      assertEquals(40, icebergRows(spark, basePath).size());
    }
  }

  /**
   * The table config decides whether Hudi stores the row lineage columns, while an existing Iceberg
   * table keeps its format version. Over an Iceberg table at version 2 the columns stay null and a
   * rewrite publishes; over one at version 3, a table config that asks for version 2 makes a
   * rewrite fail to publish rather than give the rows it carries over new row ids.
   */
  @Test
  void tableConfigAndIcebergTableAtDifferentFormatVersions() {
    String versionTwoTable = tableName();
    Properties settings = new Properties();
    settings.setProperty(IcebergFormatConfig.FORMAT_VERSION, "2");
    List<HoodieRecord<HoodieAvroPayload>> versionTwoInserts;
    try (TestJavaHudiTable table = createWithTableConfig(versionTwoTable, tempDir, settings)) {
      versionTwoInserts = table.insertRecords(20, true);
      settings.setProperty(IcebergFormatConfig.FORMAT_VERSION, "3");
      updateTableConfig(table.getMetaClient(), settings);
    }
    try (TestJavaHudiTable table = reopen(versionTwoTable)) {
      table.upsertRecords(versionTwoInserts.subList(0, 5), true);
      assertEquals(2, TableUtil.formatVersion(icebergTable(table.getBasePath())));
      assertEquals(20, icebergRows(spark, table.getBasePath()).size());
      assertTrue(
          table.getAllLatestBaseFilePaths().stream()
              .anyMatch(
                  baseFile ->
                      parquetColumns(table.getMetaClient(), baseFile)
                          .contains(MetadataColumns.ROW_ID.name())),
          "no rewritten base file stores the row lineage columns");
    }

    String versionThreeTable = tableName();
    List<HoodieRecord<HoodieAvroPayload>> versionThreeInserts;
    try (TestJavaHudiTable table = reopen(versionThreeTable)) {
      versionThreeInserts = table.insertRecords(20, true);
      settings.setProperty(IcebergFormatConfig.FORMAT_VERSION, "2");
      updateTableConfig(table.getMetaClient(), settings);
    }
    try (TestJavaHudiTable table = reopen(versionThreeTable)) {
      long snapshot = icebergTable(table.getBasePath()).currentSnapshot().snapshotId();
      Throwable failure =
          assertThrows(
              Throwable.class, () -> table.upsertRecords(versionThreeInserts.subList(0, 5), true));
      causeWithMessage(failure, "its table config sets Iceberg format version 2");
      assertEquals(snapshot, icebergTable(table.getBasePath()).currentSnapshot().snapshotId());
    }
  }

  private static TestJavaHudiTable reopen(String tableName) {
    return TestJavaHudiTable.forStandardSchema(
        tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE);
  }

  private static void updateTableConfig(HoodieTableMetaClient metaClient, Properties settings) {
    HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), settings);
  }

  /**
   * A base file that the current Iceberg snapshot does not have cannot give the rows carried over
   * from it their row lineage, so a rewrite of it fails rather than giving them new row ids.
   */
  @Test
  void rewriteOfABaseFileMissingFromIcebergFails() throws IOException {
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName(), tempDir, null, HoodieTableType.COPY_ON_WRITE, new Properties())) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(20, true);
      HoodieTableMetaClient metaClient = HoodieTableMetaClient.reload(table.getMetaClient());
      List<FormatMetaColumn> columns = metaClient.getTableFormat().getFormatMetaColumns();
      assertEquals(
          Arrays.asList(
              MetadataColumns.ROW_ID.name(), MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()),
          columns.stream().map(column -> column.field().name()).collect(Collectors.toList()));
      BaseFileColumnResolver rowIds =
          columns
              .get(0)
              .resolverFor(
                  metaClient, new HoodieBaseFile(table.getAllLatestBaseFilePaths().get(0)));
      assertEquals(7L, rowIds.valueAt(3, 7L));

      // Removes the base files from Iceberg in a snapshot that records the same Hudi instant, so
      // the Hudi timeline and file system view still have them.
      Table icebergTable = icebergTable(basePath);
      DeleteFiles delete =
          icebergTable
              .newDelete()
              .set(
                  TableSyncMetadata.XTABLE_METADATA,
                  icebergTable.currentSnapshot().summary().get(TableSyncMetadata.XTABLE_METADATA));
      List<String> dataFiles = new ArrayList<>();
      try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().planFiles()) {
        tasks.forEach(task -> dataFiles.add(task.file().location()));
      }
      dataFiles.forEach(delete::deleteFile);
      delete.commit();
      long snapshot = icebergTable(basePath).currentSnapshot().snapshotId();

      Throwable failure =
          assertThrows(Throwable.class, () -> table.upsertRecords(inserts.subList(0, 5), true));
      HoodieException cause = causeWithMessage(failure, "is not in the current Iceberg snapshot");
      assertTrue(
          dataFiles.stream().anyMatch(dataFile -> cause.getMessage().contains(dataFile)),
          cause.getMessage());
      assertEquals(snapshot, icebergTable(basePath).currentSnapshot().snapshotId());
    }
  }

  /**
   * A Hudi writer that does not store the row lineage columns, as one without the format meta
   * column support would, rewrites base files whose carried-over rows Iceberg would give new row
   * ids. Publishing the rewrite fails, and Iceberg keeps the table as it was.
   */
  @Test
  void publishOfARewriteWithoutRowLineageColumnsFails() {
    Properties properties = new Properties();
    properties.setProperty(
        HoodieTableConfig.TABLE_FORMAT.key(), IcebergTableFormatWithoutRowLineageColumns.NAME);
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName(), tempDir, null, HoodieTableType.COPY_ON_WRITE, properties)) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(20, true);
      long snapshot = icebergTable(basePath).currentSnapshot().snapshotId();
      List<String> rows = icebergRows(spark, basePath);

      Throwable failure =
          assertThrows(Throwable.class, () -> table.upsertRecords(inserts.subList(0, 5), true));
      HoodieException cause = causeWithMessage(failure, "without the Iceberg row lineage");
      assertTrue(cause.getMessage().contains("no _row_id column of type long"), cause.getMessage());
      assertEquals(snapshot, icebergTable(basePath).currentSnapshot().snapshotId());
      assertEquals(rows, icebergRows(spark, basePath));
    }
  }

  /**
   * Up to {@code max} of the records, at most one per data file and only from data files with other
   * rows, so that the rewrite of each of their data files carries rows over.
   */
  private static List<HoodieRecord<HoodieAvroPayload>> oneRecordPerSharedFile(
      Map<String, RowLineage> lineage, List<HoodieRecord<HoodieAvroPayload>> records, int max) {
    Map<String, List<String>> keysByDataFile = new TreeMap<>();
    lineage.forEach(
        (key, row) ->
            keysByDataFile.computeIfAbsent(row.dataFile, file -> new ArrayList<>()).add(key));
    Set<String> keys =
        keysByDataFile.values().stream()
            .filter(keysOfFile -> keysOfFile.size() > 1)
            .map(keysOfFile -> keysOfFile.get(0))
            .limit(max)
            .collect(Collectors.toSet());
    List<HoodieRecord<HoodieAvroPayload>> chosen =
        records.stream()
            .filter(record -> keys.contains(record.getRecordKey()))
            .collect(Collectors.toList());
    assertFalse(chosen.isEmpty(), "no data file holds more than one row");
    return chosen;
  }

  private static HoodieException causeWithMessage(Throwable failure, String message) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof HoodieException
          && String.valueOf(cause.getMessage()).contains(message)) {
        return (HoodieException) cause;
      }
    }
    throw new AssertionError("no HoodieException with \"" + message + "\" in " + failure, failure);
  }

  private static void assertChanges(
      Changes changes, int unchanged, int changedOrNew, int updatedKeepingRowId) {
    assertEquals(unchanged, changes.unchanged, "rows carried over unchanged");
    assertEquals(changedOrNew, changes.changedOrNew, "rows changed or new");
    assertEquals(
        updatedKeepingRowId, changes.updatedKeepingRowId, "updated rows that kept their row id");
  }

  /** An operation that rewrote base files, moving some of the unchanged rows to new files. */
  private static void assertRewritten(
      Changes changes, int unchanged, int changedOrNew, int updatedKeepingRowId) {
    assertChanges(changes, unchanged, changedOrNew, updatedKeepingRowId);
    assertTrue(changes.unchangedMovedToAnotherFile > 0, "no unchanged row was rewritten");
  }

  /** An operation that left every unchanged row in its file. */
  private static void assertInPlace(
      Changes changes, int unchanged, int changedOrNew, int updatedKeepingRowId) {
    assertChanges(changes, unchanged, changedOrNew, updatedKeepingRowId);
    assertEquals(0, changes.unchangedMovedToAnotherFile, "unchanged rows moved");
  }

  /** Neither the Iceberg schema nor the Hudi schema has the row lineage columns as columns. */
  private static void assertRowLineageColumnsNotInTable(String basePath) {
    Table table = icebergTable(basePath);
    for (String column :
        Arrays.asList(
            MetadataColumns.ROW_ID.name(), MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name())) {
      assertNull(table.schema().findField(column), "Iceberg schema has " + column);
      assertFalse(
          spark.read().format("hudi").load(basePath).schema().getFieldIndex(column).isDefined(),
          "Hudi schema has " + column);
    }
    NameMapping mapping =
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));
    assertEquals(
        MetadataColumns.ROW_ID.fieldId(),
        mapping.find(MetadataColumns.ROW_ID.name()).id().intValue());
    assertEquals(
        MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(),
        mapping.find(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()).id().intValue());
  }
}
