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
      HudiBackedIcebergSecondaryIndex index = newIndexFromLocation(icebergTable);
      assertFalse(index.doesIndexExist(INDEXED_COLUMN));

      index.syncIndex(icebergTable, INDEXED_COLUMN);
      assertTrue(index.doesIndexExist(INDEXED_COLUMN));
      assertLookupMatchesIceberg(table.getBasePath(), icebergTable, index, Collections.emptyList());
      assertNoLookupResults(icebergTable, index, Collections.emptyList());
      assertNoLookupResults(icebergTable, index, Arrays.asList(null, null));

      // a sync without a new snapshot has nothing to commit and leaves the index as it is
      index.syncIndex(icebergTable, INDEXED_COLUMN);
      assertLookupMatchesIceberg(table.getBasePath(), icebergTable, index, Collections.emptyList());

      // a second batch of files is added to the index by an incremental sync
      records.addAll(table.insertRows(50));
      icebergTable.refresh();
      index.syncIndex(icebergTable, INDEXED_COLUMN);
      assertLookupMatchesIceberg(table.getBasePath(), icebergTable, index, Collections.emptyList());

      // updating rows rewrites the files that hold them, so the index must resolve the updated keys
      // to their new file and row position instead of the ones the previous sync recorded
      table.upsertRows(records.subList(0, 30));
      icebergTable.refresh();
      index.syncIndex(icebergTable, INDEXED_COLUMN);
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
      index.syncIndex(icebergTable, INDEXED_COLUMN);
      assertLookupMatchesIceberg(table.getBasePath(), icebergTable, index, deletedKeys);
    }
  }

  @Test
  void failedInitialSyncThrows() {
    String tableName = "test_table_" + UUID.randomUUID().toString().replace("-", "_");
    try (TestIcebergTable table =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, null, tempDir, jsc.hadoopConfiguration())) {
      table.insertRows(20);
      Table icebergTable = table.getIcebergTable();
      HudiBackedIcebergSecondaryIndex index = newIndexFromLocation(icebergTable);
      assertThrows(
          IllegalStateException.class, () -> index.syncIndex(icebergTable, MISSING_COLUMN));
    }
  }

  @Test
  void failedUpdateThrowsAndKeepsTheExistingIndex() {
    String tableName = "test_table_" + UUID.randomUUID().toString().replace("-", "_");
    try (TestIcebergTable table =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, null, tempDir, jsc.hadoopConfiguration())) {
      table.insertRows(20);
      Table icebergTable = table.getIcebergTable();
      HudiBackedIcebergSecondaryIndex index = newIndexFromLocation(icebergTable);
      index.syncIndex(icebergTable, INDEXED_COLUMN);

      // the next sync commits the new files, and fails to index a column the table does not have
      table.insertRows(20);
      icebergTable.refresh();
      assertThrows(
          IllegalStateException.class, () -> index.syncIndex(icebergTable, MISSING_COLUMN));
      assertTrue(index.doesIndexExist(INDEXED_COLUMN));
      assertFalse(index.doesIndexExist(MISSING_COLUMN));
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
              icebergTable, sourceTable, sparkSession, new Properties());
      index.syncIndex(icebergTable, INDEXED_COLUMN);
      assertLookupMatchesIceberg(
          readIcebergRows(icebergTable), icebergTable, index, Collections.emptyList());

      // the incremental sync loads the new snapshot through the catalog as well
      appendRows(icebergTable, 30, 20);
      index.syncIndex(icebergTable, INDEXED_COLUMN);
      assertLookupMatchesIceberg(
          readIcebergRows(icebergTable), icebergTable, index, Collections.emptyList());
    } finally {
      catalog.dropTable(tableIdentifier, false);
    }
  }

  /** Creates an index for a table that XTable loads from its location, as for a Hadoop catalog. */
  private static HudiBackedIcebergSecondaryIndex newIndexFromLocation(Table icebergTable) {
    SourceTable sourceTable =
        SourceTable.builder()
            .name(icebergTable.name())
            .basePath(icebergTable.location())
            .formatName(TableFormat.ICEBERG)
            .build();
    return new HudiBackedIcebergSecondaryIndex(
        icebergTable, sourceTable, sparkSession, new Properties());
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
            .add(Index.FILE_COLUMN, DataTypes.StringType)
            .add(Index.POSITION_COLUMN, DataTypes.LongType);
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
        Arrays.asList(INDEXED_COLUMN, Index.FILE_COLUMN, Index.POSITION_COLUMN),
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
        .selectExpr(INDEXED_COLUMN, Index.FILE_COLUMN, Index.POSITION_COLUMN)
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
        Arrays.asList(INDEXED_COLUMN, Index.FILE_COLUMN, Index.POSITION_COLUMN),
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
          new Path(lookupRow.<String>getAs(Index.FILE_COLUMN)).toUri().getPath());
      assertEquals(expected.getRight(), lookupRow.<Long>getAs(Index.POSITION_COLUMN));
    }
  }
}
