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

import static org.apache.xtable.IcebergTableAssertions.assertIcebergReferencesExactly;
import static org.apache.xtable.IcebergTableAssertions.assertNothingPending;
import static org.apache.xtable.IcebergTableAssertions.icebergTable;
import static org.apache.xtable.IcebergTableAssertions.reconstructedTimeline;
import static org.apache.xtable.MergeOnReadAssertions.assertIcebergMatchesHudi;
import static org.apache.xtable.MergeOnReadAssertions.deletedKeysByDataFileInIceberg;
import static org.apache.xtable.MergeOnReadAssertions.deletedRowCount;
import static org.apache.xtable.MergeOnReadAssertions.deletionVectorTable;
import static org.apache.xtable.RowLineageAssertions.assertRowLineage;
import static org.apache.xtable.RowLineageAssertions.parquetColumns;
import static org.apache.xtable.RowLineageAssertions.rowLineage;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.avro.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableUtil;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import org.apache.xtable.RowLineageAssertions.Changes;
import org.apache.xtable.RowLineageAssertions.RowLineage;

/**
 * Hudi's meta columns, such as {@code _hoodie_commit_time}, are not part of the Iceberg schema,
 * while the base files keep them. Hudi's base files carry no field ids, so an Iceberg reader finds
 * the columns of the schema in them through the table's default name mapping, which the format sets
 * and maintains without any setting.
 */
class ITIcebergMetaColumns {
  @TempDir public static Path tempDir;

  /** Top-level and nested columns of the standard schema whose values Spark reads alike. */
  private static final String[] COMPARED_COLUMNS = {
    "key", "ts", "level", "severity", "long_field", "string_field", "nested_record.nested_int"
  };

  private static final List<String> ROW_LINEAGE_COLUMNS =
      Arrays.asList(
          MetadataColumns.ROW_ID.name(), MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name());

  private static final String ALLOW_COLUMN_DROP =
      "hoodie.datasource.write.schema.allow.auto.evolution.column.drop";

  private static SparkSession spark;

  @BeforeAll
  static void setupOnce() {
    spark = MergeOnReadAssertions.sparkSession(tempDir);
  }

  @AfterAll
  static void teardown() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static String tableName() {
    return "meta_columns_" + UUID.randomUUID().toString().replace('-', '_');
  }

  private static TestJavaHudiTable copyOnWriteTable(String tableName, Schema schema) {
    return table(tableName, schema, HoodieTableType.COPY_ON_WRITE);
  }

  /** A merge-on-read table is written the way the format publishes it, with deletion vectors. */
  private static TestJavaHudiTable table(
      String tableName, Schema schema, HoodieTableType tableType) {
    Properties properties =
        tableType == HoodieTableType.MERGE_ON_READ ? deletionVectorTable(8) : new Properties();
    // Lets a writer schema leave out columns of the table schema.
    properties.setProperty(ALLOW_COLUMN_DROP, "true");
    return TestJavaHudiTable.withSchema(tableName, tempDir, null, tableType, schema, properties);
  }

  private static Schema withoutColumns(Schema schema, String... columns) {
    Set<String> dropped = new HashSet<>(Arrays.asList(columns));
    List<Schema.Field> fields =
        schema.getFields().stream()
            .filter(field -> !dropped.contains(field.name()))
            .map(field -> new Schema.Field(field, field.schema()))
            .collect(Collectors.toList());
    assertEquals(schema.getFields().size() - columns.length, fields.size());
    return Schema.createRecord(
        schema.getName(), schema.getDoc(), schema.getNamespace(), false, fields);
  }

  private static List<String> columnNames(org.apache.iceberg.Schema schema) {
    return schema.columns().stream().map(Types.NestedField::name).collect(Collectors.toList());
  }

  private static Dataset<Row> icebergRead(String basePath) {
    return spark.read().format("iceberg").load(basePath);
  }

  private static List<String> rows(String format, String basePath, String... columns) {
    return spark.read().format(format).load(basePath).selectExpr(columns).collectAsList().stream()
        .map(row -> row.mkString("|"))
        .sorted()
        .collect(Collectors.toList());
  }

  private static void assertIcebergRowsMatchHudi(String basePath, String... columns) {
    List<String> hudiRows = rows("hudi", basePath, columns);
    assertFalse(hudiRows.isEmpty());
    assertEquals(hudiRows, rows("iceberg", basePath, columns), "rows read through Iceberg");
  }

  private static void assertNoMetaColumns(org.apache.iceberg.Schema schema) {
    for (String metaColumn : HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION) {
      assertNull(schema.findField(metaColumn), "Iceberg schema has " + metaColumn);
    }
  }

  /** The base files hold the meta columns ahead of the table's columns, as Hudi writes them. */
  private static void assertBaseFilesKeepTheMetaColumns(TestJavaHudiTable table) {
    List<String> baseFiles = table.getAllLatestBaseFilePaths();
    assertFalse(baseFiles.isEmpty());
    for (String baseFile : baseFiles) {
      assertEquals(
          HoodieRecord.HOODIE_META_COLUMNS,
          parquetColumns(table.getMetaClient(), baseFile).subList(0, 5),
          "first columns of " + baseFile);
    }
  }

  private static NameMapping nameMapping(Table table) {
    String json = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    assertNotNull(json, "the table has no " + TableProperties.DEFAULT_NAME_MAPPING);
    return NameMappingParser.fromJson(json);
  }

  /**
   * The name mapping gives every column of the current schema, nested ones included, the field id
   * the schema gives it, and maps none of Hudi's meta columns. At format version 3 it also maps the
   * row lineage columns to their reserved ids.
   *
   * @param retiredColumns dropped columns the mapping still names, under ids the schema no longer
   *     has
   */
  private static NameMapping assertNameMappingMatchesTheSchema(
      Table table, String... retiredColumns) {
    NameMapping mapping = nameMapping(table);
    org.apache.iceberg.Schema schema = table.schema();
    TypeUtil.indexNameById(schema.asStruct())
        .forEach(
            (id, name) -> {
              MappedField mapped = mapping.find(name);
              assertNotNull(mapped, "the name mapping lacks " + name);
              assertEquals(id, mapped.id(), "field id the name mapping gives " + name);
            });
    Set<String> expected = new TreeSet<>(columnNames(schema));
    expected.addAll(Arrays.asList(retiredColumns));
    if (TableUtil.formatVersion(table) >= 3) {
      expected.addAll(ROW_LINEAGE_COLUMNS);
      assertEquals(
          MetadataColumns.ROW_ID.fieldId(),
          mapping.find(MetadataColumns.ROW_ID.name()).id().intValue());
      assertEquals(
          MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId(),
          mapping.find(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name()).id().intValue());
    }
    assertEquals(
        expected,
        mapping.asMappedFields().fields().stream()
            .flatMap(field -> field.names().stream())
            .collect(Collectors.toCollection(TreeSet::new)),
        "top-level names of the name mapping");
    for (String retired : retiredColumns) {
      assertNull(schema.findField(mapping.find(retired).id()), "id of dropped column " + retired);
    }
    for (String metaColumn : HoodieRecord.HOODIE_META_COLUMNS_WITH_OPERATION) {
      assertNull(mapping.find(metaColumn), "the name mapping has " + metaColumn);
    }
    return mapping;
  }

  /** No data file has column stats under an id the schema does not have. */
  private static void assertNoStatsOutsideTheSchema(Table table) {
    Set<Integer> schemaIds = TypeUtil.indexById(table.schema().asStruct()).keySet();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile file = task.file();
        for (Map<Integer, ?> stats :
            Arrays.asList(
                file.valueCounts(),
                file.nullValueCounts(),
                file.lowerBounds(),
                file.upperBounds())) {
          if (stats != null) {
            assertTrue(
                schemaIds.containsAll(stats.keySet()),
                "column stats of " + file.location() + " for ids outside the schema");
          }
        }
        assertNotNull(file.valueCounts(), "value counts of " + file.location());
        assertTrue(file.valueCounts().containsKey(table.schema().findField("key").fieldId()));
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static String latestInstant(TestJavaHudiTable table) {
    return table.getMetaClient().reloadActiveTimeline().lastInstant().get().requestedTime();
  }

  @Test
  void copyOnWriteTableHasNoMetaColumnsInTheIcebergSchema() {
    try (TestJavaHudiTable table =
        copyOnWriteTable(tableName(), TestAbstractHudiTable.BASIC_SCHEMA)) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(20, true);
      assertNameMappingMatchesTheSchema(icebergTable(basePath));
      table.upsertRecords(inserts.subList(0, 5), true);
      table.deleteRecords(inserts.subList(5, 8), true);
      // A rollback is described from the timeline rather than from the commit's own metadata.
      table.upsertRecords(inserts.subList(8, 10), true);
      table.rollback(latestInstant(table));

      Table iceberg = icebergTable(basePath);
      org.apache.iceberg.Schema schema = iceberg.schema();
      assertNoMetaColumns(schema);
      assertEquals(
          TestAbstractHudiTable.BASIC_SCHEMA.getFields().stream()
              .map(Schema.Field::name)
              .collect(Collectors.toList()),
          columnNames(schema),
          "the Iceberg schema has the columns of the writer schema, in its order");
      assertEquals(1, schema.findField("key").fieldId(), "field ids start at the first column");
      assertEquals(
          1,
          StreamSupport.stream(iceberg.snapshots().spliterator(), false)
              .map(Snapshot::schemaId)
              .distinct()
              .count(),
          "every snapshot, the rollback's included, is taken with the same schema");
      assertEquals(columnNames(schema), Arrays.asList(icebergRead(basePath).columns()));
      assertEquals(17, icebergRead(basePath).count());
      assertIcebergRowsMatchHudi(basePath, COMPARED_COLUMNS);
      assertNameMappingMatchesTheSchema(iceberg);
      assertNoStatsOutsideTheSchema(iceberg);
      assertIcebergReferencesExactly(basePath, table.getAllLatestBaseFilePaths());
      assertBaseFilesKeepTheMetaColumns(table);
      assertNothingPending(reconstructedTimeline(basePath));
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {8, 10})
  void mergeOnReadTableWithDeletionVectorsHasNoMetaColumnsInTheIcebergSchema(int tableVersion) {
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName(),
            tempDir,
            null,
            HoodieTableType.MERGE_ON_READ,
            deletionVectorTable(tableVersion))) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(40, true);
      table.upsertRecords(inserts.subList(0, 10), true);
      table.deleteRecords(inserts.subList(10, 15), true);

      Table iceberg = icebergTable(basePath);
      assertNoMetaColumns(iceberg.schema());
      assertNameMappingMatchesTheSchema(iceberg);
      // The deletion vectors mark positions, which are read back as the record keys at them.
      assertIcebergMatchesHudi(spark, basePath);
      assertEquals(15, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      assertEquals(35, icebergRead(basePath).count());
      assertIcebergRowsMatchHudi(basePath, COMPARED_COLUMNS);
      assertBaseFilesKeepTheMetaColumns(table);

      table.compact();
      assertIcebergMatchesHudi(spark, basePath);
      assertIcebergRowsMatchHudi(basePath, COMPARED_COLUMNS);
      assertNoMetaColumns(icebergTable(basePath).schema());
      assertNameMappingMatchesTheSchema(icebergTable(basePath));
      assertNoStatsOutsideTheSchema(icebergTable(basePath));
      assertBaseFilesKeepTheMetaColumns(table);
    }
  }

  /**
   * The first commit of a table sets the name mapping although nothing asks for it, at either
   * format version, and a commit that finds the table without it sets it again.
   */
  @ParameterizedTest
  @ValueSource(ints = {2, 3})
  void nameMappingIsSetByTheFirstCommitAndRestoredWhenRemoved(int formatVersion) {
    Properties properties = new Properties();
    properties.setProperty(IcebergFormatConfig.FORMAT_VERSION, String.valueOf(formatVersion));
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName(), tempDir, null, HoodieTableType.COPY_ON_WRITE, properties)) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(10, true);
      Table iceberg = icebergTable(basePath);
      assertEquals(formatVersion, TableUtil.formatVersion(iceberg));
      NameMapping first = assertNameMappingMatchesTheSchema(iceberg);
      assertIcebergRowsMatchHudi(basePath, COMPARED_COLUMNS);

      iceberg.updateProperties().remove(TableProperties.DEFAULT_NAME_MAPPING).commit();
      assertNull(icebergTable(basePath).properties().get(TableProperties.DEFAULT_NAME_MAPPING));

      table.upsertRecords(inserts.subList(0, 3), true);
      NameMapping restored = assertNameMappingMatchesTheSchema(icebergTable(basePath));
      assertEquals(
          NameMappingParser.toJson(first), NameMappingParser.toJson(restored), "restored mapping");
      assertIcebergRowsMatchHudi(basePath, COMPARED_COLUMNS);
    }
  }

  /**
   * A commit with an added column adds the column to the Iceberg schema and, as Iceberg's schema
   * update maintains the mapping of the table it changes, to the name mapping. The base files
   * written before lack the column and read as null for it.
   */
  @ParameterizedTest
  @EnumSource(HoodieTableType.class)
  void addedColumnsAreAddedToTheNameMapping(HoodieTableType tableType) {
    String tableName = tableName();
    String basePath;
    List<HoodieRecord<HoodieAvroPayload>> inserts;
    Map<String, Integer> idsBefore = new HashMap<>();
    try (TestJavaHudiTable table =
        table(tableName, TestAbstractHudiTable.BASIC_SCHEMA, tableType)) {
      basePath = table.getBasePath();
      inserts = table.insertRecords(20, true);
      table.upsertRecords(inserts.subList(0, 4), true);
      org.apache.iceberg.Schema schema = icebergTable(basePath).schema();
      TypeUtil.indexNameById(schema.asStruct()).forEach((id, name) -> idsBefore.put(name, id));
    }
    // A top-level column, and a field of the nested record, of the map values and of the list
    // elements.
    Schema evolved =
        TestAbstractHudiTable.addTopLevelField(
            TestAbstractHudiTable.addSchemaEvolutionFieldsToBase(
                TestAbstractHudiTable.BASIC_SCHEMA));
    try (TestJavaHudiTable table = table(tableName, evolved, tableType)) {
      List<HoodieRecord<HoodieAvroPayload>> insertsWithTheColumns = table.insertRecords(20, true);
      table.upsertRecords(insertsWithTheColumns.subList(0, 4), true);
      // Deletes rows of the base file written before the columns were added: a copy-on-write table
      // rewrites it with the columns, a merge-on-read table marks the rows in a deletion vector.
      table.deleteRecords(inserts.subList(4, 8), true);

      Table iceberg = icebergTable(basePath);
      org.apache.iceberg.Schema schema = iceberg.schema();
      assertNoMetaColumns(schema);
      NameMapping mapping = assertNameMappingMatchesTheSchema(iceberg);
      Map<String, Integer> idsAfter = new HashMap<>();
      TypeUtil.indexNameById(schema.asStruct()).forEach((id, name) -> idsAfter.put(name, id));
      idsBefore.forEach(
          (name, id) -> assertEquals(id, idsAfter.get(name), "id of the earlier column " + name));
      int highestBefore = idsBefore.values().stream().mapToInt(Integer::intValue).max().getAsInt();
      for (String added :
          Arrays.asList(
              "new_top_level_field",
              "nested_record.new_nested_field",
              "nullable_map_field.value.new_nested_field",
              "array_field.element.new_nested_field")) {
        assertNotNull(schema.findField(added), "Iceberg schema lacks " + added);
        assertTrue(schema.findField(added).fieldId() > highestBefore, "id of " + added);
        assertEquals(schema.findField(added).fieldId(), mapping.find(added).id().intValue());
      }

      String[] columns =
          Arrays.copyOf(COMPARED_COLUMNS, COMPARED_COLUMNS.length + 2, String[].class);
      columns[COMPARED_COLUMNS.length] = "new_top_level_field";
      columns[COMPARED_COLUMNS.length + 1] = "nested_record.new_nested_field";
      assertIcebergRowsMatchHudi(basePath, columns);
      assertEquals(36, icebergRead(basePath).count());
      assertTrue(
          icebergRead(basePath).where("new_top_level_field IS NOT NULL").count() > 0,
          "values of the added column are read");
      assertTrue(
          icebergRead(basePath).where("nested_record.new_nested_field IS NOT NULL").count() > 0,
          "values of the added nested field are read");
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        assertIcebergMatchesHudi(spark, basePath);
        assertEquals(12, deletedRowCount(deletedKeysByDataFileInIceberg(spark, basePath)));
      }
      assertBaseFilesKeepTheMetaColumns(table);
    }
  }

  /**
   * A commit that drops several columns drops them all from the Iceberg schema. Iceberg keeps the
   * dropped names in the name mapping under their retired ids, so the base files that still hold
   * the columns are read without them. A name mapping set again after the schema changed has to use
   * the field ids of the table, which a column added after a drop makes differ from the ids a new
   * table with the same columns would get.
   */
  @Test
  void droppedColumnsAndARestoredNameMappingKeepTheFieldIdsOfTheTable() {
    String tableName = tableName();
    String basePath;
    List<HoodieRecord<HoodieAvroPayload>> inserts;
    Map<String, Integer> idsBefore = new HashMap<>();
    try (TestJavaHudiTable table =
        copyOnWriteTable(tableName, TestAbstractHudiTable.BASIC_SCHEMA)) {
      basePath = table.getBasePath();
      inserts = table.insertRecords(20, true);
      org.apache.iceberg.Schema schema = icebergTable(basePath).schema();
      TypeUtil.indexNameById(schema.asStruct()).forEach((id, name) -> idsBefore.put(name, id));
    }

    Schema twoColumnsDropped =
        withoutColumns(TestAbstractHudiTable.BASIC_SCHEMA, "double_field", "float_field");
    try (TestJavaHudiTable table = copyOnWriteTable(tableName, twoColumnsDropped)) {
      table.insertRecords(10, true);
      Table iceberg = icebergTable(basePath);
      org.apache.iceberg.Schema schema = iceberg.schema();
      assertNull(schema.findField("double_field"));
      assertNull(schema.findField("float_field"));
      assertEquals(
          twoColumnsDropped.getFields().stream()
              .map(Schema.Field::name)
              .collect(Collectors.toList()),
          columnNames(schema));
      TypeUtil.indexNameById(schema.asStruct())
          .forEach((id, name) -> assertEquals(idsBefore.get(name), id, "id of column " + name));
      assertNameMappingMatchesTheSchema(iceberg, "double_field", "float_field");
      assertIcebergRowsMatchHudi(basePath, COMPARED_COLUMNS);
      assertEquals(30, icebergRead(basePath).count());
    }

    Schema columnAddedAfterTheDrop = TestAbstractHudiTable.addTopLevelField(twoColumnsDropped);
    String[] columns = Arrays.copyOf(COMPARED_COLUMNS, COMPARED_COLUMNS.length + 1, String[].class);
    columns[COMPARED_COLUMNS.length] = "new_top_level_field";
    try (TestJavaHudiTable table = copyOnWriteTable(tableName, columnAddedAfterTheDrop)) {
      table.insertRecords(10, true);
      Table iceberg = icebergTable(basePath);
      int highestBefore = idsBefore.values().stream().mapToInt(Integer::intValue).max().getAsInt();
      assertEquals(
          highestBefore + 1,
          iceberg.schema().findField("new_top_level_field").fieldId(),
          "the added column gets an id no column had");
      assertNameMappingMatchesTheSchema(iceberg, "double_field", "float_field");
      assertIcebergRowsMatchHudi(basePath, columns);

      iceberg.updateProperties().remove(TableProperties.DEFAULT_NAME_MAPPING).commit();
      List<HoodieRecord<HoodieAvroPayload>> latestInserts = table.insertRecords(10, true);
      // Set again from the schema, which no longer has the dropped columns.
      assertNameMappingMatchesTheSchema(icebergTable(basePath));
      assertIcebergRowsMatchHudi(basePath, columns);

      table.upsertRecords(latestInserts.subList(0, 5), true);
      // Rewrites the base file that holds the dropped columns and lacks the added one.
      table.deleteRecords(inserts.subList(0, 5), true);
      assertNameMappingMatchesTheSchema(icebergTable(basePath));
      assertNoMetaColumns(icebergTable(basePath).schema());
      assertIcebergRowsMatchHudi(basePath, columns);
      assertEquals(45, icebergRead(basePath).count());
      assertTrue(
          icebergRead(basePath).where("new_top_level_field IS NOT NULL").count() > 0,
          "values of the added column are read");
      assertBaseFilesKeepTheMetaColumns(table);
    }
  }

  /**
   * At format version 3 a copy-on-write rewrite stores {@code _row_id} and {@code
   * _last_updated_sequence_number} in the base file it writes, next to Hudi's meta columns. Like
   * those, the two columns are not part of the Iceberg schema; unlike those, the name mapping maps
   * them, to Iceberg's reserved ids, so that Iceberg reads the stored values as its metadata
   * columns.
   */
  @Test
  void rowLineageColumnsAreStoredAndMappedButNotPartOfTheIcebergSchema() {
    try (TestJavaHudiTable table =
        copyOnWriteTable(tableName(), TestAbstractHudiTable.BASIC_SCHEMA)) {
      String basePath = table.getBasePath();
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(20, true);
      assertEquals(3, TableUtil.formatVersion(icebergTable(basePath)));
      String insertedBaseFile = table.getAllLatestBaseFilePaths().get(0);
      List<String> insertedColumns = parquetColumns(table.getMetaClient(), insertedBaseFile);
      assertTrue(
          insertedColumns.containsAll(ROW_LINEAGE_COLUMNS),
          "an insert stores the row lineage columns, null for its new rows: " + insertedColumns);
      Map<String, RowLineage> before = rowLineage(spark, basePath);
      assertEquals(20, before.size());

      table.upsertRecords(inserts.subList(0, 5), true);

      List<String> rewrittenBaseFiles = table.getAllLatestBaseFilePaths();
      assertEquals(1, rewrittenBaseFiles.size());
      assertNotEquals(insertedBaseFile, rewrittenBaseFiles.get(0));
      List<String> storedColumns = parquetColumns(table.getMetaClient(), rewrittenBaseFiles.get(0));
      assertTrue(storedColumns.containsAll(ROW_LINEAGE_COLUMNS), "stored columns " + storedColumns);
      assertTrue(storedColumns.containsAll(HoodieRecord.HOODIE_META_COLUMNS));

      Table iceberg = icebergTable(basePath);
      assertNoMetaColumns(iceberg.schema());
      for (String column : ROW_LINEAGE_COLUMNS) {
        assertNull(iceberg.schema().findField(column), "Iceberg schema has " + column);
        assertFalse(Arrays.asList(icebergRead(basePath).columns()).contains(column));
      }
      assertNameMappingMatchesTheSchema(iceberg);

      // Read as metadata columns through Spark and through Iceberg's Parquet reader with the name
      // mapping. The rows carried over keep the values they had in the file the rewrite replaced,
      // which only the stored columns can give them, as the new file has another first row id and
      // sequence number.
      Map<String, RowLineage> after = rowLineage(spark, basePath);
      Changes changes = assertRowLineage(before, after, null);
      assertEquals(15, changes.unchanged);
      assertEquals(15, changes.unchangedMovedToAnotherFile);
      assertEquals(5, changes.changedOrNew);
      long rewriteSequenceNumber = iceberg.currentSnapshot().sequenceNumber();
      after.forEach(
          (key, lineage) ->
              assertEquals(
                  before.get(key).commitSeqno.equals(lineage.commitSeqno),
                  lineage.lastUpdatedSequenceNumber < rewriteSequenceNumber,
                  "last updated sequence number of " + key + ": " + lineage));
      assertIcebergRowsMatchHudi(basePath, COMPARED_COLUMNS);
    }
  }

  /**
   * The limit of leaving the meta columns out: a reader has to apply the table's name mapping.
   * Iceberg's {@code IcebergGenerics} does not, and Iceberg then numbers the columns of a file
   * without field ids by position. The first column of the schema is thus read from the first
   * column of the base file, which is {@code _hoodie_commit_time}, without any error.
   */
  @Test
  void icebergGenericsIgnoresTheNameMappingAndReadsTheMetaColumnsByPosition() {
    try (TestJavaHudiTable table =
        copyOnWriteTable(tableName(), TestAbstractHudiTable.BASIC_SCHEMA)) {
      String basePath = table.getBasePath();
      table.insertRecords(10, true);
      String commitTime = latestInstant(table);
      Table iceberg = icebergTable(basePath);
      assertEquals("key", iceberg.schema().columns().get(0).name());
      Set<String> keys =
          spark.read().format("hudi").load(basePath).select("key").collectAsList().stream()
              .map(row -> row.getString(0))
              .collect(Collectors.toSet());
      assertEquals(10, keys.size());

      List<String> readByPosition = new ArrayList<>();
      try (CloseableIterable<Record> records =
          IcebergGenerics.read(iceberg).select("key").build()) {
        records.forEach(record -> readByPosition.add(String.valueOf(record.getField("key"))));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
      assertEquals(10, readByPosition.size());
      assertEquals(
          Arrays.asList(commitTime),
          readByPosition.stream().distinct().collect(Collectors.toList()),
          "IcebergGenerics reads _hoodie_commit_time as the key column");

      // The same files, read with the name mapping the way Iceberg's engines read them.
      assertEquals(keys, keysThroughTheNameMapping(iceberg));
      assertEquals(
          keys,
          icebergRead(basePath).select("key").collectAsList().stream()
              .map(row -> row.getString(0))
              .collect(Collectors.toSet()));

      // Read in full, the table fails to read: a column of the schema lands on a column of the
      // file with another type, here a struct on a string.
      assertThrows(
          IllegalArgumentException.class,
          () -> {
            try (CloseableIterable<Record> records = IcebergGenerics.read(iceberg).build()) {
              records.forEach(record -> {});
            }
          });
    }
  }

  private static Set<String> keysThroughTheNameMapping(Table table) {
    org.apache.iceberg.Schema projection = table.schema().select("key");
    Set<String> keys = new HashSet<>();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        try (CloseableIterable<Record> records =
            Parquet.read(table.io().newInputFile(task.file().location()))
                .project(projection)
                .withNameMapping(nameMapping(table))
                .createReaderFunc(
                    fileSchema -> GenericParquetReaders.buildReader(projection, fileSchema))
                .build()) {
          records.forEach(record -> keys.add((String) record.getField("key")));
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return keys;
  }
}
