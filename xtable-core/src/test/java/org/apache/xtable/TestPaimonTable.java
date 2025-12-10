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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.manifest.BucketEntry;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.ParameterUtils;

public class TestPaimonTable implements GenericTable<GenericRow, String> {

  private final Random random = new Random();
  private final FileStoreTable paimonTable;
  private final String partitionField;

  public TestPaimonTable(FileStoreTable paimonTable, String partitionField) {
    this.paimonTable = paimonTable;
    this.partitionField = partitionField;
  }

  public static GenericTable<GenericRow, String> createTable(
      String tableName,
      String partitionField,
      Path tempDir,
      Configuration hadoopConf,
      boolean additionalColumns) {

    Schema schema = buildGenericSchema(partitionField, additionalColumns);
    return createTable(
        tableName, partitionField, tempDir, hadoopConf, additionalColumns, schema);
  }

  public static GenericTable<GenericRow, String> createTable(
      String tableName,
      String partitionField,
      Path tempDir,
      Configuration hadoopConf,
      boolean additionalColumns,
      Schema schema) {
    String basePath = initBasePath(tempDir, tableName);
    Catalog catalog = createFilesystemCatalog(basePath, hadoopConf);
    FileStoreTable paimonTable =
        createTable(catalog, tableName, schema);

    System.out.println(
        "Initialized Paimon test table at base path: "
            + basePath
            + " with partition field: "
            + partitionField
            + " and additional columns: "
            + additionalColumns);

    return new TestPaimonTable(paimonTable, partitionField);
  }

  public static Catalog createFilesystemCatalog(String basePath, Configuration hadoopConf) {
    CatalogContext context = CatalogContext.create(new org.apache.paimon.fs.Path(basePath));
    return CatalogFactory.createCatalog(context);
  }

  public static FileStoreTable createTable(
      Catalog catalog,
      String tableName,
      Schema schema) {
    try {
      catalog.createDatabase("test_db", true);
      Identifier identifier = Identifier.create("test_db", tableName);
      catalog.createTable(identifier, schema, true);
      return (FileStoreTable) catalog.getTable(identifier);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Schema buildGenericSchema(String partitionField, boolean additionalColumns) {
    Schema.Builder builder =
        Schema.newBuilder()
            .primaryKey("id")
            .column("id", DataTypes.INT())
            .column("name", DataTypes.STRING())
            .column("value", DataTypes.DOUBLE())
            .column("created_at", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
            .column("updated_at", DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE())
            .column("is_active", DataTypes.BOOLEAN())
            .column("description", DataTypes.VARCHAR(255))
            .option("bucket", "1")
            .option("bucket-key", "id")
            .option("full-compaction.delta-commits", "1")
            .option("metadata.stats-mode", "full");

    if (partitionField != null) {
      builder
          .primaryKey("id", partitionField)
          .column(partitionField, DataTypes.STRING())
          .partitionKeys(partitionField);
    }

    if (additionalColumns) {
      builder.column("extra_info", DataTypes.STRING()).column("extra_value", DataTypes.DOUBLE());
    }

    return builder.build();
  }

  private GenericRow buildGenericRow(int rowIdx, TableSchema schema, String partitionValue) {
    List<Object> rowValues = new ArrayList<>(schema.fields().size());
    for (int i = 0; i < schema.fields().size(); i++) {
      DataField field = schema.fields().get(i);
      if (field.name().equals(partitionField)) {
        rowValues.add(BinaryString.fromString(partitionValue));
      } else if (field.type() instanceof IntType) {
        rowValues.add(random.nextInt());
      } else if (field.type() instanceof DoubleType) {
        rowValues.add(random.nextDouble());
      } else if (field.type() instanceof VarCharType) {
        rowValues.add(BinaryString.fromString(field.name() + "_" + rowIdx + "_" + i));
      } else if (field.type() instanceof LocalZonedTimestampType) {
        rowValues.add(Timestamp.fromEpochMillis(System.currentTimeMillis()));
      } else if (field.type() instanceof BooleanType) {
        rowValues.add(random.nextBoolean());
      } else {
        throw new UnsupportedOperationException("Unsupported field type: " + field.type());
      }
    }

    return GenericRow.of(rowValues.toArray());
  }

  private static String initBasePath(Path tempDir, String tableName) {
    try {
      Path basePath = tempDir.resolve(tableName);
      Files.createDirectories(basePath);
      return basePath.toUri().toString();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<GenericRow> insertRows(int numRows) {
    String partitionValue = LEVEL_VALUES.get(0);
    return insertRecordsToPartition(numRows, partitionValue);
  }

  @Override
  public List<GenericRow> insertRecordsForSpecialPartition(int numRows) {
    return insertRecordsToPartition(numRows, SPECIAL_PARTITION_VALUE);
  }

  private List<GenericRow> insertRecordsToPartition(int numRows, String partitionValue) {
    List<GenericRow> rows = new ArrayList<>(numRows);
    for (int i = 0; i < numRows; i++) {
      rows.add(buildGenericRow(i, paimonTable.schema(), partitionValue));
    }
    writeRows(paimonTable, rows);
    return rows;
  }

  @Override
  public void upsertRows(List<GenericRow> rows) {
    BatchWriteBuilder batchWriteBuilder = paimonTable.newBatchWriteBuilder();
    try (BatchTableWrite writer = batchWriteBuilder.newWrite()) {
      for (GenericRow row : rows) {
        writer.write(row);
      }
      commitWrites(batchWriteBuilder, writer);
      compactTable();
    } catch (Exception e) {
      throw new RuntimeException("Failed to upsert rows into Paimon table", e);
    }
  }

  @Override
  public void deleteRows(List<GenericRow> rows) {
    BatchWriteBuilder batchWriteBuilder = paimonTable.newBatchWriteBuilder();
    try (BatchTableWrite writer = batchWriteBuilder.newWrite()) {
      for (GenericRow row : rows) {
        row.setRowKind(RowKind.DELETE);
        writer.write(row);
      }
      commitWrites(batchWriteBuilder, writer);
      compactTable();
    } catch (Exception e) {
      throw new RuntimeException("Failed to delete rows from Paimon table", e);
    }
  }

  private void compactTable() {
    compactTable(paimonTable);
  }

  public static void compactTable(FileStoreTable table) {
    BatchWriteBuilder batchWriteBuilder = table.newBatchWriteBuilder();
    SnapshotReader snapshotReader = table.newSnapshotReader();
    try (BatchTableWrite writer = batchWriteBuilder.newWrite()) {
      for (BucketEntry bucketEntry : snapshotReader.bucketEntries()) {
        writer.compact(bucketEntry.partition(), bucketEntry.bucket(), true);
      }
      commitWrites(batchWriteBuilder, writer);
    } catch (Exception e) {
      throw new RuntimeException("Failed to compact writes in Paimon table", e);
    }
  }

  public static void writeRows(FileStoreTable table, List<GenericRow> rows) {
    BatchWriteBuilder batchWriteBuilder = table.newBatchWriteBuilder();
    try (BatchTableWrite writer = batchWriteBuilder.newWrite()) {
      for (GenericRow row : rows) {
        writer.write(row);
      }
      commitWrites(batchWriteBuilder, writer);
      compactTable(table);
    } catch (Exception e) {
      throw new RuntimeException("Failed to write rows into Paimon table", e);
    }
  }

  private static void commitWrites(BatchWriteBuilder batchWriteBuilder, BatchTableWrite writer)
      throws Exception {
    BatchTableCommit commit = batchWriteBuilder.newCommit();
    List<CommitMessage> messages = writer.prepareCommit();
    try {
      commit.commit(messages);
    } catch (Exception e) {
      commit.abort(messages);
      throw new RuntimeException("Failed to commit writes to Paimon table", e);
    } finally {
      commit.close();
    }
  }

  @Override
  public void deletePartition(String partitionValue) {
    try (BatchTableCommit commit = paimonTable.newBatchWriteBuilder().newCommit()) {
      commit.truncatePartitions(
          ParameterUtils.getPartitions(partitionField + "=" + partitionValue));
    } catch (Exception e) {
      throw new RuntimeException("Failed to delete partition from Paimon table", e);
    }
  }

  @Override
  public void deleteSpecialPartition() {
    deletePartition(SPECIAL_PARTITION_VALUE);
  }

  @Override
  public String getBasePath() {
    return paimonTable.location().toString();
  }

  @Override
  public String getMetadataPath() {
    return paimonTable.snapshotManager().snapshotDirectory().toString();
  }

  @Override
  public String getOrderByColumn() {
    return "id";
  }

  @Override
  public void close() {}

  @Override
  public void reload() {}

  @Override
  public List<String> getColumnsToSelect() {
    return paimonTable.schema().fieldNames().stream()
        .filter(
            // TODO Hudi thinks that paimon buckets are partition values, not sure how to handle it
            // filtering out the partition field on the comparison for now
            field -> !field.equals(partitionField))
        .collect(Collectors.toList());
  }

  @Override
  public String getFilterQuery() {
    return "id % 2 = 0";
  }

  public FileStoreTable getPaimonTable() {
    return paimonTable;
  }
}
