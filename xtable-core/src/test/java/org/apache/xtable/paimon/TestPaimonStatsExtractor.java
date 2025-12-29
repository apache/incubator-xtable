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
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.xtable.GenericTable;
import org.apache.xtable.TestPaimonTable;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.InternalDataFile;

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
    assertNull(nanosRange.getMinValue());
    assertNull(nanosRange.getMaxValue());
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
