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
 
package org.apache.xtable.index;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.client.HoodieReadClient;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import org.apache.xtable.TestIcebergTable;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.hudi.HudiTargetConfig;
import org.apache.xtable.hudi.HudiTestUtil;
import org.apache.xtable.iceberg.IcebergCatalogConfig;
import org.apache.xtable.model.storage.TableFormat;

/**
 * Builds a Hudi backed secondary index for an Iceberg table and checks that lookups resolve to the
 * same file and row position as Iceberg's {@code _file} and {@code _pos} metadata columns.
 */
public class ITHudiBackedIcebergSecondaryIndex {
  private static final String INDEXED_COLUMN = "id";
  private static final String PARTITION_COLUMN = "level";
  private static final String MISSING_COLUMN = "missing_column";
  // values of this column repeat across rows and files
  private static final String SECOND_INDEXED_COLUMN = "string_field";

  @TempDir public static java.nio.file.Path tempDir;

  private static JavaSparkContext jsc;
  private static SparkSession sparkSession;

  @BeforeAll
  public static void setupOnce() {
    SparkConf sparkConf = HudiTestUtil.getSparkConf(tempDir);
    sparkSession =
        SparkSession.builder().config(HoodieReadClient.addHoodieSupport(sparkConf)).getOrCreate();
    sparkSession
        .sparkContext()
        .hadoopConfiguration()
        .set("parquet.avro.write-old-list-structure", "false");
    jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
  }

  @AfterAll
  public static void teardown() {
    if (jsc != null) {
      jsc.close();
    }
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = PARTITION_COLUMN)
  void syncAndLookup(String partitionField) {
    String tableName = "test_table_" + UUID.randomUUID().toString().replace("-", "_");
    try (TestIcebergTable table =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, partitionField, tempDir, jsc.hadoopConfiguration())) {
      List<Record> records = new ArrayList<>(table.insertRows(100));
      Table icebergTable = table.getIcebergTable();
      HudiBackedIcebergSecondaryIndex index = newIndexFromLocation(icebergTable, INDEXED_COLUMN);
      assertFalse(index.doesIndexExist(INDEXED_COLUMN));

      index.syncIndex(icebergTable);
      assertTrue(index.doesIndexExist(INDEXED_COLUMN));
      assertLookupMatchesIceberg(table.getBasePath(), icebergTable, index, Collections.emptyList());
      assertNoLookupResults(icebergTable, index, Collections.emptyList());
      assertNoLookupResults(icebergTable, index, Arrays.asList(null, null));

      // a sync without a new snapshot has nothing to commit and leaves the index as it is
      index.syncIndex(icebergTable);
      assertLookupMatchesIceberg(table.getBasePath(), icebergTable, index, Collections.emptyList());

      // a second batch of files is added to the index by an incremental sync
      records.addAll(table.insertRows(50));
      icebergTable.refresh();
      index.syncIndex(icebergTable);
      assertLookupMatchesIceberg(table.getBasePath(), icebergTable, index, Collections.emptyList());

      // updating rows rewrites the files that hold them, so the index must resolve the updated keys
      // to their new file and row position instead of the ones the previous sync recorded
      table.upsertRows(records.subList(0, 30));
      icebergTable.refresh();
      index.syncIndex(icebergTable);
      assertLookupMatchesIceberg(table.getBasePath(), icebergTable, index, Collections.emptyList());

      // deleting rows must drop their keys from the index, and must not strand the rows that are
      // rewritten alongside them
      List<Record> deletedRecords = new ArrayList<>(records.subList(30, 50));
      List<String> deletedKeys =
          deletedRecords.stream()
              .map(record -> record.getField(INDEXED_COLUMN).toString())
              .collect(Collectors.toList());
      table.deleteRows(deletedRecords);
      records.removeAll(deletedRecords);
      icebergTable.refresh();
      index.syncIndex(icebergTable);
      assertLookupMatchesIceberg(table.getBasePath(), icebergTable, index, deletedKeys);
    }
  }

  /**
   * A data file that Hudi cannot read fails the commit after Hudi created the metadata table, which
   * marks its partitions complete before the commit. The empty index must not count as built.
   */
  @Test
  void failedInitialSyncThrows() throws Exception {
    String tableName = "test_table_" + UUID.randomUUID().toString().replace("-", "_");
    try (TestIcebergTable table =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, null, tempDir, jsc.hadoopConfiguration())) {
      table.insertRows(20);
      Table icebergTable = table.getIcebergTable();
      Path corruptFile = new Path(table.getDataPath(), UUID.randomUUID() + ".parquet");
      try (OutputStream stream =
          corruptFile.getFileSystem(jsc.hadoopConfiguration()).create(corruptFile)) {
        stream.write("not a parquet file".getBytes(StandardCharsets.UTF_8));
      }
      appendDataFile(icebergTable, corruptFile);
      HudiBackedIcebergSecondaryIndex index = newIndexFromLocation(icebergTable, INDEXED_COLUMN);
      IllegalStateException exception =
          assertThrows(IllegalStateException.class, () -> index.syncIndex(icebergTable));
      assertTrue(exception.getMessage().contains("Failed to sync"));
      assertFalse(index.doesIndexExist(INDEXED_COLUMN));
    }
  }

  /** Hudi needs every file under its base path, the data location. */
  @Test
  void dataFileOutsideDataLocationThrows() throws Exception {
    String tableName = "test_table_" + UUID.randomUUID().toString().replace("-", "_");
    try (TestIcebergTable table =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, null, tempDir, jsc.hadoopConfiguration())) {
      table.insertRows(20);
      Table icebergTable = table.getIcebergTable();
      Path source =
          new Path(
              icebergTable
                  .currentSnapshot()
                  .addedDataFiles(icebergTable.io())
                  .iterator()
                  .next()
                  .location());
      Path outside = new Path(icebergTable.location(), "outside/" + UUID.randomUUID() + ".parquet");
      FileSystem fs = source.getFileSystem(jsc.hadoopConfiguration());
      FileUtil.copy(fs, source, fs, outside, false, jsc.hadoopConfiguration());
      appendDataFile(icebergTable, outside);
      HudiBackedIcebergSecondaryIndex index = newIndexFromLocation(icebergTable, INDEXED_COLUMN);
      IllegalStateException exception =
          assertThrows(IllegalStateException.class, () -> index.syncIndex(icebergTable));
      assertTrue(exception.getMessage().contains("outside the Hudi table base path"));
      assertFalse(index.doesIndexExist(INDEXED_COLUMN));
    }
  }

  /** Registers a file as a data file of an unpartitioned table, without reading it. */
  private void appendDataFile(Table icebergTable, Path file) throws Exception {
    long length = file.getFileSystem(jsc.hadoopConfiguration()).getFileStatus(file).getLen();
    icebergTable
        .newAppend()
        .appendFile(
            DataFiles.builder(icebergTable.spec())
                .withPath(file.toString())
                .withFormat(FileFormat.PARQUET)
                .withFileSizeInBytes(length)
                .withRecordCount(20)
                .build())
        .commit();
  }

  /**
   * Changing the indexed column of an existing index is not supported yet. The sync fails before it
   * commits, so a sync with the original column brings the existing index up to date.
   */
  @Test
  void changingIndexedColumnThrows() {
    String tableName = "test_table_" + UUID.randomUUID().toString().replace("-", "_");
    try (TestIcebergTable table =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, null, tempDir, jsc.hadoopConfiguration())) {
      table.insertRows(20);
      Table icebergTable = table.getIcebergTable();
      HudiBackedIcebergSecondaryIndex index = newIndexFromLocation(icebergTable, INDEXED_COLUMN);
      index.syncIndex(icebergTable);

      table.insertRows(20);
      icebergTable.refresh();
      HudiBackedIcebergSecondaryIndex indexWithChangedColumn =
          newIndexFromLocation(icebergTable, SECOND_INDEXED_COLUMN);
      IllegalStateException exception =
          assertThrows(
              IllegalStateException.class, () -> indexWithChangedColumn.syncIndex(icebergTable));
      assertTrue(exception.getMessage().contains("not supported yet"));
      assertFalse(indexWithChangedColumn.doesIndexExist(SECOND_INDEXED_COLUMN));

      index.syncIndex(icebergTable);
      assertLookupMatchesIceberg(table.getBasePath(), icebergTable, index, Collections.emptyList());
    }
  }

  /**
   * Indexes columns of several types whose values repeat across rows and files, and checks every
   * row by value, file and row position, before and after later commits.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "string_field",
        "boolean_field",
        "int_field",
        "double_field",
        "float_field",
        "decimal_field",
        "date_nullable_field",
        "timestamp_micros_nullable_field",
        "timestamp_local_micros_nullable_field"
      })
  void syncAndLookupNonUniqueColumn(String column) {
    String tableName = "test_table_" + UUID.randomUUID().toString().replace("-", "_");
    try (TestIcebergTable table =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, PARTITION_COLUMN, tempDir, jsc.hadoopConfiguration())) {
      // several snapshots, so the rows of each value are spread over several files
      List<Record> records = new ArrayList<>(table.insertRows(100));
      records.addAll(table.insertRows(50));
      Table icebergTable = table.getIcebergTable();
      HudiBackedIcebergSecondaryIndex index = newIndexFromLocation(icebergTable, column);
      index.syncIndex(icebergTable);
      assertTrue(
          assertEveryRowIndexed(table.getBasePath(), icebergTable, index, column) > 1,
          "expected the indexed rows in several files");

      // later commits update the index, including updated and deleted rows
      records.addAll(table.insertRows(40));
      table.upsertRows(records.subList(0, 20));
      table.deleteRows(new ArrayList<>(records.subList(20, 30)));
      icebergTable.refresh();
      index.syncIndex(icebergTable);
      assertEveryRowIndexed(table.getBasePath(), icebergTable, index, column);
    }
  }

  /** Hudi renders a BINARY value as an object reference, so it cannot be indexed. */
  @ParameterizedTest
  @ValueSource(strings = {"bytes_field", MISSING_COLUMN})
  void unsupportedColumnThrows(String column) {
    String tableName = "test_table_" + UUID.randomUUID().toString().replace("-", "_");
    try (TestIcebergTable table =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, null, tempDir, jsc.hadoopConfiguration())) {
      table.insertRows(10);
      Table icebergTable = table.getIcebergTable();
      assertThrows(
          IllegalArgumentException.class, () -> newIndexFromLocation(icebergTable, column));
    }
  }

  /** A partition directory alone, as an in flight or failed build leaves, is not an index. */
  @Test
  void doesIndexExistIgnoresUnfinishedBuilds() throws Exception {
    String tableName = "test_table_" + UUID.randomUUID().toString().replace("-", "_");
    try (TestIcebergTable table =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, null, tempDir, jsc.hadoopConfiguration())) {
      table.insertRows(20);
      Table icebergTable = table.getIcebergTable();
      HudiBackedIcebergSecondaryIndex index = newIndexFromLocation(icebergTable, INDEXED_COLUMN);
      assertFalse(index.doesIndexExist(INDEXED_COLUMN));
      index.syncIndex(icebergTable);
      Path unfinishedPartition =
          new Path(
              table.getDataPath(), ".hoodie/metadata/secondary_index_" + SECOND_INDEXED_COLUMN);
      FileSystem fs = unfinishedPartition.getFileSystem(jsc.hadoopConfiguration());
      assertTrue(fs.mkdirs(unfinishedPartition));
      assertTrue(index.doesIndexExist(INDEXED_COLUMN));
      assertFalse(index.doesIndexExist(SECOND_INDEXED_COLUMN));
    }
  }

  /**
   * A table from a catalog that keeps no version hint cannot be loaded from its location, so the
   * sync must load it through the catalog in the source table config.
   */
  @Test
  void syncAndLookupTableFromCatalog() throws Exception {
    String catalogName = "test_metastore";
    Map<String, String> catalogOptions =
        Collections.singletonMap(
            CatalogProperties.WAREHOUSE_LOCATION, tempDir.resolve(catalogName).toString());
    TestMetastoreCatalog catalog = new TestMetastoreCatalog();
    catalog.initialize(catalogName, catalogOptions);
    TableIdentifier tableIdentifier = TableIdentifier.of("db", "catalog_table");
    Schema schema =
        new Schema(
            Types.NestedField.optional(1, INDEXED_COLUMN, Types.StringType.get()),
            Types.NestedField.optional(2, "value", Types.LongType.get()));
    Table icebergTable =
        catalog.createTable(tableIdentifier, schema, PartitionSpec.unpartitioned());
    SourceTable sourceTable =
        SourceTable.builder()
            .name(tableIdentifier.name())
            .namespace(tableIdentifier.namespace().levels())
            .basePath(icebergTable.location())
            .formatName(TableFormat.ICEBERG)
            .catalogConfig(
                IcebergCatalogConfig.builder()
                    .catalogName(catalogName)
                    .catalogImpl(TestMetastoreCatalog.class.getName())
                    .catalogOptions(catalogOptions)
                    .build())
            .build();
    try {
      appendRows(icebergTable, 0, 30);
      HudiBackedIcebergSecondaryIndex index =
          new HudiBackedIcebergSecondaryIndex(
              icebergTable, sourceTable, sparkSession, indexProperties(INDEXED_COLUMN));
      index.syncIndex(icebergTable);
      assertLookupMatchesIceberg(
          readIcebergRows(icebergTable), icebergTable, index, Collections.emptyList());

      // the incremental sync loads the new snapshot through the catalog as well
      appendRows(icebergTable, 30, 20);
      index.syncIndex(icebergTable);
      assertLookupMatchesIceberg(
          readIcebergRows(icebergTable), icebergTable, index, Collections.emptyList());
    } finally {
      catalog.dropTable(tableIdentifier, false);
    }
  }

  private static Properties indexProperties(String columns) {
    Properties properties = new Properties();
    properties.setProperty(HudiTargetConfig.SECONDARY_INDEX_COLUMN, columns);
    return properties;
  }

  /** Creates an index for a table that XTable loads from its location, as for a Hadoop catalog. */
  private static HudiBackedIcebergSecondaryIndex newIndexFromLocation(
      Table icebergTable, String columns) {
    SourceTable sourceTable =
        SourceTable.builder()
            .name(icebergTable.name())
            .basePath(icebergTable.location())
            .formatName(TableFormat.ICEBERG)
            .build();
    return new HudiBackedIcebergSecondaryIndex(
        icebergTable, sourceTable, sparkSession, indexProperties(columns));
  }

  /** Writes a data file with rows {@code start} to {@code start + count - 1} to the table. */
  private void appendRows(Table icebergTable, int start, int count) throws Exception {
    String filePath = icebergTable.location() + "/data/" + UUID.randomUUID() + ".parquet";
    DataWriter<Record> writer =
        Parquet.writeData(HadoopOutputFile.fromLocation(filePath, jsc.hadoopConfiguration()))
            .schema(icebergTable.schema())
            .createWriterFunc(GenericParquetWriter::create)
            .withSpec(icebergTable.spec())
            .overwrite()
            .build();
    try {
      for (int row = start; row < start + count; row++) {
        Record record = GenericRecord.create(icebergTable.schema());
        record.setField(INDEXED_COLUMN, "key-" + row);
        record.setField("value", (long) row);
        writer.write(record);
      }
    } finally {
      writer.close();
    }
    DataFile dataFile = writer.toDataFile();
    icebergTable.newAppend().appendFile(dataFile).commit();
  }

  /**
   * Reads the rows of a catalog table from its data files, because Spark cannot load the table by
   * its location. Each file's rows are in order, so a row's position is its index.
   */
  private Dataset<Row> readIcebergRows(Table icebergTable) {
    List<Row> rows = new ArrayList<>();
    icebergTable
        .newScan()
        .planFiles()
        .forEach(
            task -> {
              String file = task.file().location();
              List<Row> fileRows =
                  sparkSession.read().parquet(file).select(INDEXED_COLUMN).collectAsList();
              for (int position = 0; position < fileRows.size(); position++) {
                rows.add(
                    RowFactory.create(fileRows.get(position).getString(0), file, (long) position));
              }
            });
    StructType schema =
        new StructType()
            .add(INDEXED_COLUMN, DataTypes.StringType)
            .add(HudiBackedIcebergSecondaryIndex.FILE_COLUMN, DataTypes.StringType)
            .add(HudiBackedIcebergSecondaryIndex.POSITION_COLUMN, DataTypes.LongType);
    return sparkSession.createDataFrame(rows, schema);
  }

  private void assertNoLookupResults(
      Table icebergTable, HudiBackedIcebergSecondaryIndex index, List<String> keys) {
    Dataset<Row> lookupResults =
        index.lookup(
            icebergTable,
            sparkSession.createDataset(keys, Encoders.STRING()).toDF(INDEXED_COLUMN),
            INDEXED_COLUMN);
    assertEquals(
        Arrays.asList(
            INDEXED_COLUMN,
            HudiBackedIcebergSecondaryIndex.FILE_COLUMN,
            HudiBackedIcebergSecondaryIndex.POSITION_COLUMN),
        Arrays.asList(lookupResults.schema().fieldNames()));
    assertEquals(0, lookupResults.count());
  }

  private void assertLookupMatchesIceberg(
      String basePath,
      Table icebergTable,
      HudiBackedIcebergSecondaryIndex index,
      List<String> keysThatMustNotResolve) {
    assertLookupMatchesIceberg(
        sparkSession.read().format("iceberg").load(basePath),
        icebergTable,
        index,
        keysThatMustNotResolve);
  }

  /**
   * Looks every key currently in the Iceberg table up in the index and checks the result against
   * Iceberg's own {@code _file} and {@code _pos}. Expectations are read from the table rather than
   * from the records the test wrote, so this stays correct after rows are updated or deleted.
   * {@code keysThatMustNotResolve} are looked up as well and must return nothing.
   */
  private void assertLookupMatchesIceberg(
      Dataset<Row> icebergTableRows,
      Table icebergTable,
      HudiBackedIcebergSecondaryIndex index,
      List<String> keysThatMustNotResolve) {
    Map<String, Pair<String, Long>> expectedLocations = new HashMap<>();
    icebergTableRows
        .selectExpr(
            INDEXED_COLUMN,
            HudiBackedIcebergSecondaryIndex.FILE_COLUMN,
            HudiBackedIcebergSecondaryIndex.POSITION_COLUMN)
        .collectAsList()
        .forEach(
            row ->
                expectedLocations.put(
                    row.getString(0),
                    Pair.of(new Path(row.getString(1)).toUri().getPath(), row.getLong(2))));

    List<String> keys = new ArrayList<>(expectedLocations.keySet());
    // keys that are not in the table must not produce a result
    keys.add("missing-key-1");
    keys.add("missing-key-2");
    keys.addAll(keysThatMustNotResolve);
    Dataset<Row> keysToLookUp =
        sparkSession.createDataset(keys, Encoders.STRING()).toDF(INDEXED_COLUMN).repartition(2);
    Dataset<Row> lookupResults = index.lookup(icebergTable, keysToLookUp, INDEXED_COLUMN);
    assertEquals(
        Arrays.asList(
            INDEXED_COLUMN,
            HudiBackedIcebergSecondaryIndex.FILE_COLUMN,
            HudiBackedIcebergSecondaryIndex.POSITION_COLUMN),
        Arrays.asList(lookupResults.schema().fieldNames()));
    List<Row> lookupRows = lookupResults.collectAsList();
    assertEquals(expectedLocations.size(), lookupRows.size());

    Set<String> resolvedKeys =
        lookupRows.stream()
            .map(row -> row.<String>getAs(INDEXED_COLUMN))
            .collect(Collectors.toSet());
    keysThatMustNotResolve.forEach(key -> assertFalse(resolvedKeys.contains(key)));

    for (Row lookupRow : lookupRows) {
      String key = lookupRow.getAs(INDEXED_COLUMN);
      Pair<String, Long> expected = expectedLocations.get(key);
      assertNotNull(expected, "index returned a key that is not in the table");
      assertEquals(
          expected.getLeft(),
          new Path(lookupRow.<String>getAs(HudiBackedIcebergSecondaryIndex.FILE_COLUMN))
              .toUri()
              .getPath());
      assertEquals(
          expected.getRight(),
          lookupRow.<Long>getAs(HudiBackedIcebergSecondaryIndex.POSITION_COLUMN));
    }
  }

  /**
   * Looks up every value of {@code column} in the table and checks that the index returns exactly
   * the rows Iceberg holds for those values, by file and row position. A value can repeat, so the
   * rows are compared as sorted lists. Returns the number of files that hold the rows.
   */
  private long assertEveryRowIndexed(
      String basePath, Table icebergTable, HudiBackedIcebergSecondaryIndex index, String column) {
    Dataset<Row> icebergRows =
        sparkSession
            .read()
            .format("iceberg")
            .load(basePath)
            .where(column + " IS NOT NULL")
            .selectExpr(
                column + " AS value",
                HudiBackedIcebergSecondaryIndex.FILE_COLUMN,
                HudiBackedIcebergSecondaryIndex.POSITION_COLUMN);
    List<String> expected = toSortedLocations(icebergRows);
    Dataset<Row> keys = icebergRows.select("value").distinct().toDF(column).repartition(2);
    List<String> actual =
        toSortedLocations(
            index
                .lookup(icebergTable, keys, column)
                .selectExpr(
                    column + " AS value",
                    HudiBackedIcebergSecondaryIndex.FILE_COLUMN,
                    HudiBackedIcebergSecondaryIndex.POSITION_COLUMN));
    assertEquals(expected, actual);
    return icebergRows.select(HudiBackedIcebergSecondaryIndex.FILE_COLUMN).distinct().count();
  }

  /** Renders each row as "value file position", with the value cast to a string, sorted. */
  private static List<String> toSortedLocations(Dataset<Row> rows) {
    return rows.selectExpr("CAST(value AS STRING)", "_file", "_pos").collectAsList().stream()
        .map(
            row ->
                row.get(0) + " " + new Path(row.getString(1)).toUri().getPath() + " " + row.get(2))
        .sorted()
        .collect(Collectors.toList());
  }
}
