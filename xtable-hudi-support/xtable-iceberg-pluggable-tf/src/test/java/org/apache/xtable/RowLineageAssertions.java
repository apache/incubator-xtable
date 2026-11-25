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

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf;
import static org.apache.xtable.IcebergTableAssertions.icebergTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.Type;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.storage.StoragePath;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.BaseDeleteLoader;
import org.apache.iceberg.data.IdentityPartitionConverters;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.PartitionUtil;

import org.apache.xtable.timeline.IcebergSnapshotInstants;

/**
 * Reads the row lineage of every live row of the Iceberg table the way Iceberg engines do, and
 * checks it across a Hudi operation against what the Hudi meta fields say the operation did to each
 * row.
 */
final class RowLineageAssertions {
  /** The column of the test tables that holds the record key. */
  private static final String RECORD_KEY_COLUMN = "key";

  private RowLineageAssertions() {}

  /**
   * The row lineage of one row as Iceberg reads it, with the Hudi meta fields of the row and the
   * Iceberg data file it is read from.
   */
  static final class RowLineage {
    final String commitTime;
    final String commitSeqno;
    final String dataFile;
    final long rowId;
    final long lastUpdatedSequenceNumber;

    RowLineage(
        String commitTime,
        String commitSeqno,
        String dataFile,
        long rowId,
        long lastUpdatedSequenceNumber) {
      this.commitTime = commitTime;
      this.commitSeqno = commitSeqno;
      this.dataFile = dataFile;
      this.rowId = rowId;
      this.lastUpdatedSequenceNumber = lastUpdatedSequenceNumber;
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof RowLineage)) {
        return false;
      }
      RowLineage that = (RowLineage) other;
      return commitTime.equals(that.commitTime)
          && commitSeqno.equals(that.commitSeqno)
          && dataFile.equals(that.dataFile)
          && rowId == that.rowId
          && lastUpdatedSequenceNumber == that.lastUpdatedSequenceNumber;
    }

    @Override
    public int hashCode() {
      return Objects.hash(commitTime, commitSeqno, dataFile, rowId, lastUpdatedSequenceNumber);
    }

    @Override
    public String toString() {
      return String.format(
          "%s/%s in %s: _row_id=%d, _last_updated_sequence_number=%d",
          commitTime, commitSeqno, dataFile, rowId, lastUpdatedSequenceNumber);
    }
  }

  /** How the rows of the table changed across an operation. */
  static final class Changes {
    int unchanged;
    int unchangedMovedToAnotherFile;
    int changedOrNew;
    int updatedKeepingRowId;
  }

  /**
   * The row lineage of every live row by record key, through Spark on Iceberg and through Iceberg's
   * Parquet reader with the table's name mapping, which have to agree.
   */
  static Map<String, RowLineage> rowLineage(SparkSession spark, String basePath) {
    Map<String, RowLineage> throughSpark = rowLineageThroughSpark(spark, basePath);
    assertEquals(
        throughSpark,
        rowLineageThroughIcebergReader(basePath),
        "Spark and Iceberg's Parquet reader read different row lineage");
    return throughSpark;
  }

  /**
   * Hudi's meta columns are not part of the Iceberg schema, so they are read through Hudi and
   * matched to the rows Iceberg reads by record key, which the tables of these tests keep in the
   * {@value #RECORD_KEY_COLUMN} column.
   */
  private static Map<String, RowLineage> rowLineageThroughSpark(
      SparkSession spark, String basePath) {
    Map<String, Row> hudiMetaFieldsByKey = new HashMap<>();
    for (Row row :
        spark
            .read()
            .format("hudi")
            .load(basePath)
            .selectExpr(
                HoodieRecord.RECORD_KEY_METADATA_FIELD,
                HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                HoodieRecord.COMMIT_SEQNO_METADATA_FIELD)
            .collectAsList()) {
      Row previous = hudiMetaFieldsByKey.put(row.getString(0), row);
      assertEquals(null, previous, "duplicate record key " + row.getString(0) + " read by Hudi");
    }
    List<Row> rows =
        spark
            .read()
            .format("iceberg")
            .load(basePath)
            .selectExpr(
                RECORD_KEY_COLUMN,
                MetadataColumns.FILE_PATH.name(),
                MetadataColumns.ROW_ID.name(),
                MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name())
            .collectAsList();
    Map<String, RowLineage> lineage = new TreeMap<>();
    for (Row row : rows) {
      Row hudiMetaFields = hudiMetaFieldsByKey.get(row.getString(0));
      assertNotNull(hudiMetaFields, "Hudi reads no row with the record key " + row.getString(0));
      RowLineage previous =
          lineage.put(
              row.getString(0),
              new RowLineage(
                  hudiMetaFields.getString(1),
                  hudiMetaFields.getString(2),
                  row.getString(1),
                  row.getLong(2),
                  row.getLong(3)));
      assertEquals(null, previous, "duplicate record key " + row.getString(0));
    }
    assertEquals(hudiMetaFieldsByKey.keySet(), lineage.keySet(), "record keys Hudi reads");
    return lineage;
  }

  /**
   * Reads every planned data file with Iceberg's generic Parquet readers, applying the table's name
   * mapping and the file's deletion vector. Iceberg's {@code IcebergGenerics} does not apply the
   * name mapping, so it cannot see the values a file without field ids stores.
   *
   * <p>The base files store Hudi's meta columns, which the Iceberg schema and so the name mapping
   * leave out. They are read here under ids no table column has, mapped to their names.
   */
  @SneakyThrows
  private static Map<String, RowLineage> rowLineageThroughIcebergReader(String basePath) {
    Table table = icebergTable(basePath);
    NameMapping nameMapping =
        NameMappingParser.fromJson(table.properties().get(TableProperties.DEFAULT_NAME_MAPPING));
    List<MappedField> mappedFields = new ArrayList<>(nameMapping.asMappedFields().fields());
    List<Types.NestedField> columns = new ArrayList<>();
    columns.add(table.schema().findField(RECORD_KEY_COLUMN));
    int id = table.schema().highestFieldId();
    for (String metaColumn :
        Arrays.asList(
            HoodieRecord.RECORD_KEY_METADATA_FIELD,
            HoodieRecord.COMMIT_TIME_METADATA_FIELD,
            HoodieRecord.COMMIT_SEQNO_METADATA_FIELD)) {
      assertNull(table.schema().findField(metaColumn), "Iceberg schema has " + metaColumn);
      assertNull(nameMapping.find(metaColumn), "the name mapping has " + metaColumn);
      id++;
      columns.add(Types.NestedField.optional(id, metaColumn, Types.StringType.get()));
      mappedFields.add(MappedField.of(id, metaColumn));
    }
    NameMapping nameMappingWithMetaColumns = NameMapping.of(mappedFields);
    Schema projection =
        TypeUtil.join(
            MetadataColumns.schemaWithRowLineage(new Schema(columns)),
            new Schema(MetadataColumns.ROW_POSITION));
    BaseDeleteLoader deleteLoader =
        new BaseDeleteLoader(deleteFile -> table.io().newInputFile(deleteFile.location()));
    Map<String, RowLineage> lineage = new TreeMap<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        String dataFile = task.file().location();
        PositionDeleteIndex deleted =
            task.deletes().isEmpty()
                ? null
                : deleteLoader.loadPositionDeletes(task.deletes(), dataFile);
        Map<Integer, ?> constants =
            PartitionUtil.constantsMap(task, IdentityPartitionConverters::convertConstant);
        try (CloseableIterable<Record> records =
            Parquet.read(table.io().newInputFile(dataFile))
                .project(projection)
                .withNameMapping(nameMappingWithMetaColumns)
                .createReaderFunc(
                    fileSchema ->
                        GenericParquetReaders.buildReader(projection, fileSchema, constants))
                .build()) {
          for (Record record : records) {
            long position = (Long) record.getField(MetadataColumns.ROW_POSITION.name());
            if (deleted != null && deleted.isDeleted(position)) {
              continue;
            }
            String key = (String) record.getField(HoodieRecord.RECORD_KEY_METADATA_FIELD);
            assertEquals(
                key,
                record.getField(RECORD_KEY_COLUMN),
                "the record key column, read through the name mapping, of a row of " + dataFile);
            RowLineage previous =
                lineage.put(
                    key,
                    new RowLineage(
                        (String) record.getField(HoodieRecord.COMMIT_TIME_METADATA_FIELD),
                        (String) record.getField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD),
                        dataFile,
                        (Long) record.getField(MetadataColumns.ROW_ID.name()),
                        (Long)
                            record.getField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name())));
            assertEquals(null, previous, "duplicate record key " + key);
          }
        }
      }
    }
    return lineage;
  }

  /**
   * Adds the sequence number of the snapshot that recorded each Hudi commit still in the Iceberg
   * table's history. Called after every operation, so a commit stays known after archival expires
   * its snapshot.
   */
  static void recordSequenceNumbers(String basePath, Map<String, Long> sequenceNumberByCommit) {
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder()
            .setConf(getStorageConf(new Configuration()))
            .setBasePath(basePath)
            .build();
    for (Snapshot snapshot : icebergTable(basePath).snapshots()) {
      HoodieInstant instant =
          IcebergSnapshotInstants.recordedInstant(snapshot, metaClient.getInstantGenerator());
      if (!HoodieTimeline.SAVEPOINT_ACTION.equals(instant.getAction())) {
        sequenceNumberByCommit.putIfAbsent(instant.requestedTime(), snapshot.sequenceNumber());
      }
    }
  }

  /**
   * Checks the row lineage after an operation against the row lineage before it and the Hudi meta
   * fields. Row ids are unique. A row that keeps its {@code _hoodie_commit_seqno} keeps its row id
   * and its last updated sequence number. A row that an update changed either keeps its row id or
   * has a row id no row had before, and a new row has a row id no row had before. Unless {@code
   * sequenceNumberByCommit} is null, every row's last updated sequence number is the sequence
   * number of the snapshot that recorded its {@code _hoodie_commit_time}.
   */
  static Changes assertRowLineage(
      Map<String, RowLineage> before,
      Map<String, RowLineage> after,
      Map<String, Long> sequenceNumberByCommit) {
    assertUniqueRowIds(after);
    Set<Long> rowIdsBefore =
        before.values().stream().map(lineage -> lineage.rowId).collect(Collectors.toSet());
    Changes changes = new Changes();
    after.forEach(
        (key, lineage) -> {
          if (sequenceNumberByCommit != null) {
            Long recordedBy = sequenceNumberByCommit.get(lineage.commitTime);
            assertNotNull(recordedBy, "no snapshot recorded commit " + lineage.commitTime);
            assertEquals(
                recordedBy.longValue(),
                lineage.lastUpdatedSequenceNumber,
                "last updated sequence number of " + key + ": " + lineage);
          }
          RowLineage previous = before.get(key);
          if (previous != null && previous.commitSeqno.equals(lineage.commitSeqno)) {
            assertEquals(previous.rowId, lineage.rowId, "row id of unchanged row " + key);
            assertEquals(
                previous.lastUpdatedSequenceNumber,
                lineage.lastUpdatedSequenceNumber,
                "last updated sequence number of unchanged row " + key);
            changes.unchanged++;
            if (!previous.dataFile.equals(lineage.dataFile)) {
              changes.unchangedMovedToAnotherFile++;
            }
          } else if (previous != null && previous.rowId == lineage.rowId) {
            changes.changedOrNew++;
            changes.updatedKeepingRowId++;
          } else {
            assertFalse(
                rowIdsBefore.contains(lineage.rowId),
                "changed or new row " + key + " reuses a row id: " + lineage);
            changes.changedOrNew++;
          }
        });
    return changes;
  }

  static void assertUniqueRowIds(Map<String, RowLineage> lineage) {
    Map<Long, String> keyByRowId = new HashMap<>();
    lineage.forEach(
        (key, rowLineage) -> {
          String other = keyByRowId.put(rowLineage.rowId, key);
          assertEquals(
              null, other, "row id " + rowLineage.rowId + " of both " + other + " and " + key);
        });
  }

  /** The top-level column names of a Parquet file, from its footer. */
  static List<String> parquetColumns(HoodieTableMetaClient metaClient, String path) {
    return ParquetUtils.readMetadata(metaClient.getStorage(), new StoragePath(path))
        .getFileMetaData()
        .getSchema()
        .getFields()
        .stream()
        .map(Type::getName)
        .collect(Collectors.toList());
  }
}
