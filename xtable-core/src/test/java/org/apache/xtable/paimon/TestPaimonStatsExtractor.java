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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.xtable.GenericTable;
import org.apache.xtable.TestPaimonTable;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.InternalDataFile;

@Log4j2
public class TestPaimonStatsExtractor {
  private static final PaimonDataFileExtractor extractor = PaimonDataFileExtractor.getInstance();
  private static final PaimonSchemaExtractor schemaExtractor = PaimonSchemaExtractor.getInstance();

  @TempDir private Path tempDir;
  private TestPaimonTable testTable;
  private FileStoreTable paimonTable;

  @Test
  void testColumnStatsUnpartitioned() {
    createUnpartitionedTable();
    InternalSchema schema = schemaExtractor.toInternalSchema(testTable.getPaimonTable().schema());

    List<GenericRow> rows = testTable.insertRows(10);

    List<InternalDataFile> result =
        extractor.toInternalDataFiles(
            paimonTable, paimonTable.snapshotManager().latestSnapshot(), schema);

    assertFalse(result.isEmpty());
    InternalDataFile dataFile = result.get(0);
    List<ColumnStat> stats = dataFile.getColumnStats();
    assertFalse(stats.isEmpty());

    // Verify "id" stats (INT)
    int minId = rows.stream().map(r -> r.getInt(0)).min(Integer::compareTo).get();
    int maxId = rows.stream().map(r -> r.getInt(0)).max(Integer::compareTo).get();
    ColumnStat idStat = getColumnStat(stats, "id");
    assertEquals(Range.vector(minId, maxId), idStat.getRange());
    assertEquals(0, idStat.getNumNulls());

    // Verify "name" stats (STRING)
    String minName = rows.stream().map(r -> r.getString(1).toString()).min(String::compareTo).get();
    String maxName = rows.stream().map(r -> r.getString(1).toString()).max(String::compareTo).get();
    ColumnStat nameStat = getColumnStat(stats, "name");
    assertEquals(Range.vector(minName, maxName), nameStat.getRange());
    assertEquals(0, nameStat.getNumNulls());

    // Verify "value" stats (DOUBLE)
    double minValue = rows.stream().map(r -> r.getDouble(2)).min(Double::compareTo).get();
    double maxValue = rows.stream().map(r -> r.getDouble(2)).max(Double::compareTo).get();
    ColumnStat valueStat = getColumnStat(stats, "value");
    assertEquals(Range.vector(minValue, maxValue), valueStat.getRange());
    assertEquals(0, valueStat.getNumNulls());

    // Verify "created_at" stats (TIMESTAMP)
    Timestamp minCreatedAt =
        rows.stream().map(r -> r.getTimestamp(3, 9)).min(Timestamp::compareTo).get();
    Timestamp maxCreatedAt =
        rows.stream().map(r -> r.getTimestamp(3, 9)).max(Timestamp::compareTo).get();
    ColumnStat createdAtStat = getColumnStat(stats, "created_at");
    assertEquals(
        Range.vector(minCreatedAt.toMicros(), maxCreatedAt.toMicros()), createdAtStat.getRange());
    assertEquals(0, createdAtStat.getNumNulls());

    // Verify "updated_at" stats (TIMESTAMP)
    Timestamp minUpdatedAt =
        rows.stream().map(r -> r.getTimestamp(4, 9)).min(Timestamp::compareTo).get();
    Timestamp maxUpdatedAt =
        rows.stream().map(r -> r.getTimestamp(4, 9)).max(Timestamp::compareTo).get();
    ColumnStat updatedAtStat = getColumnStat(stats, "updated_at");
    assertEquals(
        Range.vector(minUpdatedAt.toMicros(), maxUpdatedAt.toMicros()), updatedAtStat.getRange());
    assertEquals(0, updatedAtStat.getNumNulls());

    // Verify "is_active" stats (BOOLEAN)
    ColumnStat isActiveStat = getColumnStat(stats, "is_active");
    assertEquals(Range.vector(false, true), isActiveStat.getRange());
    assertEquals(0, isActiveStat.getNumNulls());

    // Verify "description" stats (VARCHAR(255))
    String minDescription =
        rows.stream().map(r -> r.getString(6).toString()).min(String::compareTo).get();
    String maxDescription =
        rows.stream().map(r -> r.getString(6).toString()).max(String::compareTo).get();
    ColumnStat descriptionStat = getColumnStat(stats, "description");
    assertEquals(Range.vector(minDescription, maxDescription), descriptionStat.getRange());
    assertEquals(0, descriptionStat.getNumNulls());
  }

  @Test
  void testColumnStatsPartitionedTable() {
    createPartitionedTable();
    InternalSchema schema = schemaExtractor.toInternalSchema(testTable.getPaimonTable().schema());

    testTable.insertRows(10);

    List<InternalDataFile> result =
        extractor.toInternalDataFiles(
            paimonTable, paimonTable.snapshotManager().latestSnapshot(), schema);

    assertFalse(result.isEmpty());
    InternalDataFile dataFile = result.get(0);
    List<ColumnStat> stats = dataFile.getColumnStats();
    assertFalse(stats.isEmpty());

    // check that extracted stats' column orders are still the same
    // no need to check stats range, these are covered in testColumnStatsUnpartitioned
    assertEquals("id", stats.get(0).getField().getName());
    assertEquals("name", stats.get(1).getField().getName());
    assertEquals("value", stats.get(2).getField().getName());
    assertEquals("created_at", stats.get(3).getField().getName());
    assertEquals("updated_at", stats.get(4).getField().getName());
    assertEquals("is_active", stats.get(5).getField().getName());
    assertEquals("description", stats.get(6).getField().getName());
    assertEquals("level", stats.get(7).getField().getName());

    // check stats range for the partition column (level)
    assertEquals(Range.scalar(GenericTable.LEVEL_VALUES.get(0)), stats.get(7).getRange());
  }

  @Test
  void testTimestampPrecisionStats() {
    Schema schema =
        Schema.newBuilder()
            .primaryKey("id")
            .column("id", DataTypes.INT())
            .column("ts_millis", DataTypes.TIMESTAMP(3))
            .column("ts_micros", DataTypes.TIMESTAMP(6))
            .column("ts_nanos", DataTypes.TIMESTAMP(9))
            .option("bucket", "1")
            .option("bucket-key", "id")
            .option("full-compaction.delta-commits", "1")
            .build();

    FileStoreTable table =
        ((TestPaimonTable)
                TestPaimonTable.createTable(
                    "ts_precision", null, tempDir, new Configuration(), false, schema))
            .getPaimonTable();

    Timestamp millisOne =
        Timestamp.fromLocalDateTime(LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123_000_000));
    Timestamp microsOne =
        Timestamp.fromLocalDateTime(LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123_456_000));
    Timestamp nanosOne =
        Timestamp.fromLocalDateTime(LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123_456_789));

    Timestamp millisTwo =
        Timestamp.fromLocalDateTime(LocalDateTime.of(2024, 1, 1, 0, 0, 1, 987_000_000));
    Timestamp microsTwo =
        Timestamp.fromLocalDateTime(LocalDateTime.of(2024, 1, 1, 0, 0, 1, 987_654_000));
    Timestamp nanosTwo =
        Timestamp.fromLocalDateTime(LocalDateTime.of(2024, 1, 1, 0, 0, 1, 987_654_321));

    TestPaimonTable.writeRows(
        table,
        Arrays.asList(
            GenericRow.of(1, millisOne, microsOne, nanosOne),
            GenericRow.of(2, millisTwo, microsTwo, nanosTwo)));

    InternalSchema internalSchema = schemaExtractor.toInternalSchema(table.schema());
    List<ColumnStat> stats =
        extractor
            .toInternalDataFiles(table, table.snapshotManager().latestSnapshot(), internalSchema)
            .get(0)
            .getColumnStats();

    Range millisRange = getColumnStat(stats, "ts_millis").getRange();
    assertEquals(Range.vector(millisOne.getMillisecond(), millisTwo.getMillisecond()), millisRange);

    Range microsRange = getColumnStat(stats, "ts_micros").getRange();
    assertEquals(Range.vector(microsOne.toMicros(), microsTwo.toMicros()), microsRange);

    Range nanosRange = getColumnStat(stats, "ts_nanos").getRange();
    // TODO: Paimon does not fully support stats at nanos precision - this is null for parquet
    // format in 1.3.1
    assertNull(nanosRange);
  }

  @Test
  void testDecimalDateAndNullStatsWithLongStrings() {
    Schema schema =
        Schema.newBuilder()
            .primaryKey("id")
            .column("id", DataTypes.INT())
            .column("price", DataTypes.DECIMAL(10, 2))
            .column("event_date", DataTypes.DATE())
            .column("notes", DataTypes.STRING())
            .option("bucket", "1")
            .option("bucket-key", "id")
            .option("full-compaction.delta-commits", "1")
            .option("metadata.stats-mode", "truncate(16)")
            .build();

    FileStoreTable table =
        ((TestPaimonTable)
                TestPaimonTable.createTable(
                    "decimal_date", null, tempDir, new Configuration(), false, schema))
            .getPaimonTable();

    String longA = repeatChar('a', 32);
    String longB = repeatChar('b', 32);
    String longC = repeatChar('c', 32);
    int jan1 = (int) LocalDate.of(2024, 1, 1).toEpochDay();
    int jan2 = (int) LocalDate.of(2024, 1, 2).toEpochDay();

    GenericRow row1 =
        GenericRow.of(
            1,
            Decimal.fromBigDecimal(new BigDecimal("12.34"), 10, 2),
            jan1,
            BinaryString.fromString(longA));
    GenericRow row2 = GenericRow.of(2, null, jan2, BinaryString.fromString(longB));
    GenericRow row3 =
        GenericRow.of(
            3,
            Decimal.fromBigDecimal(new BigDecimal("-5.50"), 10, 2),
            null,
            BinaryString.fromString(longC));

    TestPaimonTable.writeRows(table, Arrays.asList(row1, row2, row3));

    InternalSchema internalSchema = schemaExtractor.toInternalSchema(table.schema());
    List<ColumnStat> stats =
        extractor
            .toInternalDataFiles(table, table.snapshotManager().latestSnapshot(), internalSchema)
            .get(0)
            .getColumnStats();

    ColumnStat priceStat = getColumnStat(stats, "price");
    assertEquals(
        Range.vector(new BigDecimal("-5.50"), new BigDecimal("12.34")), priceStat.getRange());
    assertEquals(1, priceStat.getNumNulls());
    assertEquals(3, priceStat.getNumValues());

    ColumnStat dateStat = getColumnStat(stats, "event_date");
    assertEquals(Range.vector(jan1, jan2), dateStat.getRange());
    assertEquals(1, dateStat.getNumNulls());
    assertEquals(3, dateStat.getNumValues());

    ColumnStat notesStat = getColumnStat(stats, "notes");
    String minNotes = (String) notesStat.getRange().getMinValue();
    String maxNotes = (String) notesStat.getRange().getMaxValue();
    assertEquals(repeatChar('a', 16), minNotes);
    assertEquals(repeatChar('c', 15) + 'd', maxNotes);
    assertEquals(16, minNotes.length());
    assertEquals(16, maxNotes.length());
    assertEquals(0, notesStat.getNumNulls());
    assertEquals(3, notesStat.getNumValues());
  }

  @Test
  void testFieldLevelStats() {
    Schema schema =
        Schema.newBuilder()
            .primaryKey("id")
            .column("id", DataTypes.INT())
            .column("foo", DataTypes.STRING())
            .column("bar", DataTypes.STRING())
            .column("boo", DataTypes.STRING())
            .option("bucket", "1")
            .option("bucket-key", "id")
            .option("full-compaction.delta-commits", "1")
            .option("metadata.stats-mode", "none")
            .option("fields.id.stats-mode", "truncate(16)")
            .option("fields.foo.stats-mode", "truncate(16)")
            .build();

    FileStoreTable table =
        ((TestPaimonTable)
                TestPaimonTable.createTable(
                    "field_level_stats", null, tempDir, new Configuration(), false, schema))
            .getPaimonTable();

    GenericRow row1 =
        GenericRow.of(
            1,
            BinaryString.fromString("foo1"),
            BinaryString.fromString("bar1"),
            BinaryString.fromString("boo1"));
    GenericRow row2 =
        GenericRow.of(
            2,
            BinaryString.fromString("foo2"),
            BinaryString.fromString("bar2"),
            BinaryString.fromString("boo2"));
    GenericRow row3 =
        GenericRow.of(
            3,
            BinaryString.fromString("foo3"),
            BinaryString.fromString("bar3"),
            BinaryString.fromString("boo3"));

    TestPaimonTable.writeRows(table, Arrays.asList(row1, row2, row3));

    InternalSchema internalSchema = schemaExtractor.toInternalSchema(table.schema());
    List<ColumnStat> stats =
        extractor
            .toInternalDataFiles(table, table.snapshotManager().latestSnapshot(), internalSchema)
            .get(0)
            .getColumnStats();

    ColumnStat idStat = getColumnStat(stats, "id");
    assertEquals(Range.vector(1, 3), idStat.getRange());
  }

  @Test
  void testPaimonNoStats() {
    Schema schema =
        Schema.newBuilder()
            .primaryKey("id")
            .column("id", DataTypes.INT())
            .column("foo", DataTypes.STRING())
            .column("bar", DataTypes.STRING())
            .column("boo", DataTypes.STRING())
            .option("bucket", "1")
            .option("bucket-key", "id")
            .option("full-compaction.delta-commits", "1")
            .option("metadata.stats-mode", "none")
            .build();

    FileStoreTable table =
        ((TestPaimonTable)
                TestPaimonTable.createTable(
                    "field_level_stats", null, tempDir, new Configuration(), false, schema))
            .getPaimonTable();

    GenericRow row1 =
        GenericRow.of(
            1,
            BinaryString.fromString("foo1"),
            BinaryString.fromString("bar1"),
            BinaryString.fromString("boo1"));
    GenericRow row2 =
        GenericRow.of(
            2,
            BinaryString.fromString("foo2"),
            BinaryString.fromString("bar2"),
            BinaryString.fromString("boo2"));
    GenericRow row3 =
        GenericRow.of(
            3,
            BinaryString.fromString("foo3"),
            BinaryString.fromString("bar3"),
            BinaryString.fromString("boo3"));

    TestPaimonTable.writeRows(table, Arrays.asList(row1, row2, row3));

    InternalSchema internalSchema = schemaExtractor.toInternalSchema(table.schema());
    List<ColumnStat> stats =
        extractor
            .toInternalDataFiles(table, table.snapshotManager().latestSnapshot(), internalSchema)
            .get(0)
            .getColumnStats();

    assertEquals(0, stats.size());
  }

  @Test
  void testStatsWithoutNullCount() {
    Schema schema =
        Schema.newBuilder()
            .primaryKey("id")
            .column("id", DataTypes.INT())
            .column("a", DataTypes.INT())
            .option("bucket", "1")
            .option("bucket-key", "id")
            .option("full-compaction.delta-commits", "1")
            .option("metadata.stats-dense-store", "false")
            .option("fields.a.stats-mode", "none")
            .build();

    FileStoreTable table =
        ((TestPaimonTable)
                TestPaimonTable.createTable(
                    "no_null_count", null, tempDir, new Configuration(), false, schema))
            .getPaimonTable();

    TestPaimonTable.writeRows(table, Arrays.asList(GenericRow.of(1, null), GenericRow.of(2, 20)));

    InternalSchema internalSchema = schemaExtractor.toInternalSchema(table.schema());
    List<ColumnStat> stats =
        extractor
            .toInternalDataFiles(table, table.snapshotManager().latestSnapshot(), internalSchema)
            .get(0)
            .getColumnStats();

    // Paimon stores no null count for a, so the number of nulls is unknown, not zero
    ColumnStat idStat = getColumnStat(stats, "id");
    assertEquals(Range.vector(1, 2), idStat.getRange());
    assertEquals(0, idStat.getNumNulls());
    ColumnStat aStat = getColumnStat(stats, "a");
    assertNull(aStat.getRange());
    assertNull(aStat.getNumNulls());
  }

  @Test
  void testPaimonDropStats() {
    Schema schema =
        Schema.newBuilder()
            .primaryKey("id")
            .column("id", DataTypes.INT())
            .column("foo", DataTypes.STRING())
            .column("bar", DataTypes.STRING())
            .column("boo", DataTypes.STRING())
            .option("bucket", "1")
            .option("bucket-key", "id")
            .option("full-compaction.delta-commits", "1")
            .option("metadata.stats-mode", "full")
            .option("manifest.delete-file-drop-stats", "true")
            .build();

    FileStoreTable table =
        ((TestPaimonTable)
                TestPaimonTable.createTable(
                    "field_level_stats", null, tempDir, new Configuration(), false, schema))
            .getPaimonTable();

    GenericRow row1 =
        GenericRow.of(
            1,
            BinaryString.fromString("foo1"),
            BinaryString.fromString("bar1"),
            BinaryString.fromString("boo1"));
    GenericRow row2 =
        GenericRow.of(
            2,
            BinaryString.fromString("foo2"),
            BinaryString.fromString("bar2"),
            BinaryString.fromString("boo2"));
    GenericRow row3 =
        GenericRow.of(
            3,
            BinaryString.fromString("foo3"),
            BinaryString.fromString("bar3"),
            BinaryString.fromString("boo3"));

    TestPaimonTable.writeRows(table, Arrays.asList(row1, row2, row3));

    InternalSchema internalSchema = schemaExtractor.toInternalSchema(table.schema());
    List<ColumnStat> stats =
        extractor
            .toInternalDataFiles(table, table.snapshotManager().latestSnapshot(), internalSchema)
            .get(0)
            .getColumnStats();

    // compaction create commits that are DELETE and ADD on the same file
    // with `manifest.delete-file-drop-stats` enabled, this means stats are empty after compaction
    // this is a smoke test to ensure exceptions aren't raised for this scenario
    // See also: https://github.com/apache/paimon/issues/7026
    assertEquals(0, stats.size());
  }

  @Test
  void testComplexFieldStats() {
    // Paimon does not collect stats on nested fields or array fields
    Schema schema =
        Schema.newBuilder()
            .primaryKey("id")
            .column("id", DataTypes.INT())
            .column(
                "nested",
                DataTypes.ROW(
                    DataTypes.FIELD(1, "f1", DataTypes.STRING()),
                    DataTypes.FIELD(2, "f2", DataTypes.INT())))
            .column("array", DataTypes.ARRAY(DataTypes.INT()))
            .option("bucket", "1")
            .option("bucket-key", "id")
            .option("full-compaction.delta-commits", "1")
            .option("metadata.stats-mode", "full")
            .build();

    FileStoreTable table =
        ((TestPaimonTable)
                TestPaimonTable.createTable(
                    "nested_field_stats", null, tempDir, new Configuration(), false, schema))
            .getPaimonTable();

    GenericRow row1 =
        GenericRow.of(
            1, GenericRow.of(BinaryString.fromString("a"), 10), new GenericArray(new int[] {1, 2}));
    GenericRow row2 =
        GenericRow.of(
            2, GenericRow.of(BinaryString.fromString("b"), 20), new GenericArray(new int[] {3, 4}));
    GenericRow row3 =
        GenericRow.of(
            3, GenericRow.of(BinaryString.fromString("c"), 30), new GenericArray(new int[] {}));

    TestPaimonTable.writeRows(table, Arrays.asList(row1, row2, row3));

    InternalSchema internalSchema = schemaExtractor.toInternalSchema(table.schema());
    List<ColumnStat> stats =
        extractor
            .toInternalDataFiles(table, table.snapshotManager().latestSnapshot(), internalSchema)
            .get(0)
            .getColumnStats();

    // only the id column has stats, nested fields and array fields do not have stats
    assertEquals(1, stats.size());
    ColumnStat idStat = getColumnStat(stats, "id");
    assertEquals(Range.vector(1, 3), idStat.getRange());
    assertEquals(0, idStat.getNumNulls());
    assertEquals(3, idStat.getNumValues());
  }

  @Test
  void testStatsOfTinyIntAndSmallIntColumns() {
    Schema schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("t", DataTypes.TINYINT())
            .column("s", DataTypes.SMALLINT())
            .build();
    FileStoreTable table = TestPaimonTable.createTable(createCatalog(), "tinyint_smallint", schema);
    String file =
        writeDataFile(
            table,
            Arrays.asList(
                GenericRow.of(1, (byte) -5, (short) -300), GenericRow.of(2, (byte) 7, (short) 9)));

    // The stats store t as a byte and s as a short, so reading them as an int gets negative values
    // wrong.
    List<ColumnStat> stats = getStatsByFileName(table).get(file);
    assertEquals(Range.vector(-5, 7), getColumnStat(stats, "t").getRange());
    assertEquals(Range.vector(-300, 9), getColumnStat(stats, "s").getRange());
  }

  @Test
  void testStatsAfterAddColumn() {
    Schema schema =
        Schema.newBuilder().column("id", DataTypes.INT()).column("a", DataTypes.INT()).build();
    Catalog catalog = createCatalog();
    FileStoreTable table = TestPaimonTable.createTable(catalog, "add_column", schema);
    String oldFile =
        writeDataFile(table, Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));
    table =
        TestPaimonTable.alterTable(catalog, table, SchemaChange.addColumn("b", DataTypes.INT()));
    String newFile = writeDataFile(table, Collections.singletonList(GenericRow.of(3, 30, 300)));

    Map<String, List<ColumnStat>> statsByFileName = getStatsByFileName(table);

    // The file written before b was added has no stats for b.
    List<ColumnStat> oldFileStats = statsByFileName.get(oldFile);
    assertEquals(Arrays.asList("id", "a"), getColumnNames(oldFileStats));
    assertEquals(Range.vector(1, 2), getColumnStat(oldFileStats, "id").getRange());
    assertEquals(Range.vector(10, 20), getColumnStat(oldFileStats, "a").getRange());
    List<ColumnStat> newFileStats = statsByFileName.get(newFile);
    assertEquals(Arrays.asList("id", "a", "b"), getColumnNames(newFileStats));
    assertEquals(Range.vector(300, 300), getColumnStat(newFileStats, "b").getRange());
  }

  @Test
  void testStatsAfterAddColumnPrimaryKeyTable() {
    Schema schema =
        Schema.newBuilder()
            .primaryKey("id")
            .column("id", DataTypes.INT())
            .column("a", DataTypes.INT())
            .option("bucket", "1")
            .option("bucket-key", "id")
            .option("full-compaction.delta-commits", "1")
            .build();
    Catalog catalog = createCatalog();
    FileStoreTable table = TestPaimonTable.createTable(catalog, "pk_add_column", schema);
    TestPaimonTable.writeRows(table, Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));
    table =
        TestPaimonTable.alterTable(catalog, table, SchemaChange.addColumn("b", DataTypes.INT()));

    // The compacted file keeps the schema it was written with until it is rewritten.
    Map<String, List<ColumnStat>> statsByFileName = getStatsByFileName(table);
    assertEquals(1, statsByFileName.size());
    List<ColumnStat> stats = statsByFileName.values().iterator().next();
    assertEquals(Arrays.asList("id", "a"), getColumnNames(stats));
    assertEquals(Range.vector(10, 20), getColumnStat(stats, "a").getRange());
  }

  @Test
  void testStatsAfterDropAndAddColumn() {
    Schema schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("a", DataTypes.INT())
            .column("b", DataTypes.INT())
            .build();
    Catalog catalog = createCatalog();
    FileStoreTable table = TestPaimonTable.createTable(catalog, "drop_and_add_column", schema);
    String oldFile =
        writeDataFile(table, Arrays.asList(GenericRow.of(1, 10, 100), GenericRow.of(2, 20, 200)));
    table = TestPaimonTable.alterTable(catalog, table, SchemaChange.dropColumn("b"));
    table =
        TestPaimonTable.alterTable(catalog, table, SchemaChange.addColumn("c", DataTypes.INT()));
    String newFile = writeDataFile(table, Collections.singletonList(GenericRow.of(3, 30, 3000)));

    Map<String, List<ColumnStat>> statsByFileName = getStatsByFileName(table);

    // The stats of the dropped column b must not be reported for c, which is null in this file.
    assertEquals(Arrays.asList("id", "a"), getColumnNames(statsByFileName.get(oldFile)));
    List<ColumnStat> newFileStats = statsByFileName.get(newFile);
    assertEquals(Arrays.asList("id", "a", "c"), getColumnNames(newFileStats));
    assertEquals(Range.vector(3000, 3000), getColumnStat(newFileStats, "c").getRange());
  }

  @Test
  void testStatsAfterDropAndAddColumnWithSameName() {
    Schema schema =
        Schema.newBuilder().column("id", DataTypes.INT()).column("a", DataTypes.INT()).build();
    Catalog catalog = createCatalog();
    FileStoreTable table =
        TestPaimonTable.createTable(catalog, "drop_and_add_column_with_same_name", schema);
    String oldFile =
        writeDataFile(table, Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));
    table = TestPaimonTable.alterTable(catalog, table, SchemaChange.dropColumn("a"));
    table =
        TestPaimonTable.alterTable(catalog, table, SchemaChange.addColumn("a", DataTypes.INT()));
    String newFile = writeDataFile(table, Collections.singletonList(GenericRow.of(3, 30)));

    Map<String, List<ColumnStat>> statsByFileName = getStatsByFileName(table);

    // Both columns are named a, but the new one has another field id and is null in the old file.
    assertEquals(Collections.singletonList("id"), getColumnNames(statsByFileName.get(oldFile)));
    List<ColumnStat> newFileStats = statsByFileName.get(newFile);
    assertEquals(Arrays.asList("id", "a"), getColumnNames(newFileStats));
    assertEquals(Range.vector(30, 30), getColumnStat(newFileStats, "a").getRange());
  }

  @Test
  void testStatsAfterRenameAndAddColumn() {
    Schema schema =
        Schema.newBuilder().column("id", DataTypes.INT()).column("a", DataTypes.INT()).build();
    Catalog catalog = createCatalog();
    FileStoreTable table = TestPaimonTable.createTable(catalog, "rename_column", schema);
    String oldFile =
        writeDataFile(table, Arrays.asList(GenericRow.of(1, 10), GenericRow.of(2, 20)));
    table = TestPaimonTable.alterTable(catalog, table, SchemaChange.renameColumn("a", "a_old"));
    table =
        TestPaimonTable.alterTable(catalog, table, SchemaChange.addColumn("a", DataTypes.INT()));
    String newFile = writeDataFile(table, Collections.singletonList(GenericRow.of(3, 30, 300)));

    Map<String, List<ColumnStat>> statsByFileName = getStatsByFileName(table);

    // Stats follow the field id, so the renamed column keeps its stats in the old file.
    List<ColumnStat> oldFileStats = statsByFileName.get(oldFile);
    assertEquals(Arrays.asList("id", "a_old"), getColumnNames(oldFileStats));
    assertEquals(Range.vector(10, 20), getColumnStat(oldFileStats, "a_old").getRange());
    List<ColumnStat> newFileStats = statsByFileName.get(newFile);
    assertEquals(Arrays.asList("id", "a_old", "a"), getColumnNames(newFileStats));
    assertEquals(Range.vector(30, 30), getColumnStat(newFileStats, "a_old").getRange());
    assertEquals(Range.vector(300, 300), getColumnStat(newFileStats, "a").getRange());
  }

  @Test
  void testStatsAfterColumnTypeWidening() {
    Schema schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("t", DataTypes.TINYINT())
            .column("s", DataTypes.SMALLINT())
            .column("i", DataTypes.INT())
            .column("f", DataTypes.FLOAT())
            .column("d", DataTypes.DECIMAL(5, 2))
            .column("d_scale", DataTypes.DECIMAL(5, 2))
            .column("ts", DataTypes.TIMESTAMP(3))
            .build();
    Catalog catalog = createCatalog();
    FileStoreTable table = TestPaimonTable.createTable(catalog, "type_widening", schema);
    String oldFile =
        writeDataFile(
            table,
            Arrays.asList(
                GenericRow.of(
                    1,
                    (byte) -5,
                    (short) -300,
                    -7,
                    -1.5f,
                    toDecimal("-1.50", 5, 2),
                    toDecimal("-1.50", 5, 2),
                    Timestamp.fromEpochMillis(1000L)),
                GenericRow.of(
                    2,
                    (byte) 7,
                    (short) 9,
                    8,
                    2.5f,
                    toDecimal("123.45", 5, 2),
                    toDecimal("123.45", 5, 2),
                    Timestamp.fromEpochMillis(2000L))));
    table = updateColumnType(catalog, table, "t", DataTypes.INT());
    table = updateColumnType(catalog, table, "s", DataTypes.BIGINT());
    table = updateColumnType(catalog, table, "i", DataTypes.BIGINT());
    table = updateColumnType(catalog, table, "f", DataTypes.DOUBLE());
    table = updateColumnType(catalog, table, "d", DataTypes.DECIMAL(10, 2));
    table = updateColumnType(catalog, table, "d_scale", DataTypes.DECIMAL(20, 4));
    table = updateColumnType(catalog, table, "ts", DataTypes.TIMESTAMP(6));

    // The old file stores each column in its old type, and the new type holds every value of it.
    List<ColumnStat> stats = getStatsByFileName(table).get(oldFile);
    assertEquals(
        Arrays.asList("id", "t", "s", "i", "f", "d", "d_scale", "ts"), getColumnNames(stats));
    assertEquals(Range.vector(-5, 7), getColumnStat(stats, "t").getRange());
    assertEquals(Range.vector(-300L, 9L), getColumnStat(stats, "s").getRange());
    assertEquals(Range.vector(-7L, 8L), getColumnStat(stats, "i").getRange());
    assertEquals(Range.vector(-1.5d, 2.5d), getColumnStat(stats, "f").getRange());
    assertEquals(
        Range.vector(new BigDecimal("-1.50"), new BigDecimal("123.45")),
        getColumnStat(stats, "d").getRange());
    assertEquals(
        Range.vector(new BigDecimal("-1.5000"), new BigDecimal("123.4500")),
        getColumnStat(stats, "d_scale").getRange());
    assertEquals(Range.vector(1_000_000L, 2_000_000L), getColumnStat(stats, "ts").getRange());
  }

  @Test
  void testStatsAfterColumnTypeNarrowing() {
    Schema schema =
        Schema.newBuilder()
            .column("id", DataTypes.INT())
            .column("l", DataTypes.BIGINT())
            .column("db", DataTypes.DOUBLE())
            .column("d", DataTypes.DECIMAL(10, 2))
            .column("d_scale", DataTypes.DECIMAL(10, 2))
            .column("s", DataTypes.INT())
            .build();
    Catalog catalog = createCatalog();
    FileStoreTable table = TestPaimonTable.createTable(catalog, "type_narrowing", schema);
    String oldFile =
        writeDataFile(
            table,
            Arrays.asList(
                GenericRow.of(1, 10L, 1.5d, toDecimal("1.25", 10, 2), toDecimal("1.25", 10, 2), 9),
                GenericRow.of(
                    2, 20L, 2.5d, toDecimal("2.25", 10, 2), toDecimal("2.25", 10, 2), 10)));
    table = updateColumnType(catalog, table, "l", DataTypes.INT());
    table = updateColumnType(catalog, table, "db", DataTypes.FLOAT());
    table = updateColumnType(catalog, table, "d", DataTypes.DECIMAL(5, 2));
    table = updateColumnType(catalog, table, "d_scale", DataTypes.DECIMAL(10, 1));
    table = updateColumnType(catalog, table, "s", DataTypes.STRING());

    // The new types cannot hold every value of the old ones, and strings order 9 and 10 the other
    // way, so the old file's stats for these columns are left out.
    assertEquals(
        Collections.singletonList("id"), getColumnNames(getStatsByFileName(table).get(oldFile)));
  }

  private void createUnpartitionedTable() {
    testTable =
        (TestPaimonTable)
            TestPaimonTable.createTable("test_table", null, tempDir, new Configuration(), false);
    paimonTable = testTable.getPaimonTable();
  }

  private void createPartitionedTable() {
    testTable =
        (TestPaimonTable)
            TestPaimonTable.createTable("test_table", "level", tempDir, new Configuration(), false);
    paimonTable = testTable.getPaimonTable();
  }

  private Catalog createCatalog() {
    return TestPaimonTable.createFilesystemCatalog(tempDir.toString(), new Configuration());
  }

  private static FileStoreTable updateColumnType(
      Catalog catalog, FileStoreTable table, String columnName, DataType newType) {
    return TestPaimonTable.alterTable(
        catalog, table, SchemaChange.updateColumnType(columnName, newType));
  }

  private static Decimal toDecimal(String value, int precision, int scale) {
    return Decimal.fromBigDecimal(new BigDecimal(value), precision, scale);
  }

  /** Writes the rows as one data file and returns its name. */
  private static String writeDataFile(FileStoreTable table, List<GenericRow> rows) {
    List<DataFileMeta> files = TestPaimonTable.writeRowsWithoutCompaction(table, rows);
    assertEquals(1, files.size());
    return files.get(0).fileName();
  }

  /** Extracts the column stats of the data files in the latest snapshot, by file name. */
  private Map<String, List<ColumnStat>> getStatsByFileName(FileStoreTable table) {
    InternalSchema internalSchema = schemaExtractor.toInternalSchema(table.schema());
    return extractor
        .toInternalDataFiles(table, table.snapshotManager().latestSnapshot(), internalSchema)
        .stream()
        .collect(
            Collectors.toMap(
                file -> new org.apache.paimon.fs.Path(file.getPhysicalPath()).getName(),
                InternalDataFile::getColumnStats));
  }

  private static List<String> getColumnNames(List<ColumnStat> stats) {
    return stats.stream().map(stat -> stat.getField().getName()).collect(Collectors.toList());
  }

  private ColumnStat getColumnStat(List<ColumnStat> stats, String columnName) {
    return stats.stream()
        .filter(stat -> stat.getField().getName().equals(columnName))
        .findFirst()
        .orElseThrow(
            () -> new IllegalArgumentException("Column stat not found for column: " + columnName));
  }

  private String repeatChar(char ch, int count) {
    char[] chars = new char[count];
    Arrays.fill(chars, ch);
    return new String(chars);
  }
}
