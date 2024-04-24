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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import org.apache.xtable.conversion.PerTableConfig;
import org.apache.xtable.conversion.PerTableConfigImpl;
import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.storage.*;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;

class TestIcebergConversionTargetSource {

  private IcebergTableManager tableManager;
  private Schema csSchema;
  private PartitionSpec csPartitionSpec;
  private IcebergConversionSourceProvider sourceProvider;
  private Configuration hadoopConf;

  @BeforeEach
  void setUp() throws IOException {
    hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "file:///");

    sourceProvider = new IcebergConversionSourceProvider();
    sourceProvider.init(hadoopConf, null);

    tableManager = IcebergTableManager.of(hadoopConf);

    byte[] bytes = readResourceFile("schemas/catalog_sales.json");
    csSchema = SchemaParser.fromJson(new String(bytes));

    bytes = readResourceFile("partition_specs/catalog_sales.json");
    csPartitionSpec = PartitionSpecParser.fromJson(csSchema, new String(bytes));
  }

  @Test
  void getTableTest(@TempDir Path workingDir) throws IOException {
    Table catalogSales = createTestTableWithData(workingDir.toString());
    PerTableConfig sourceTableConfig = getPerTableConfig(catalogSales);

    IcebergConversionSource conversionSource =
        sourceProvider.getConversionSourceInstance(sourceTableConfig);

    Snapshot snapshot = catalogSales.currentSnapshot();
    InternalTable internalTable = conversionSource.getTable(snapshot);
    Assertions.assertNotNull(internalTable);
    assertEquals(TableFormat.ICEBERG, internalTable.getTableFormat());
    Assertions.assertTrue(internalTable.getName().endsWith("catalog_sales"));
    assertEquals(catalogSales.location(), internalTable.getBasePath());
    assertEquals(DataLayoutStrategy.HIVE_STYLE_PARTITION, internalTable.getLayoutStrategy());
    assertEquals(snapshot.timestampMillis(), internalTable.getLatestCommitTime().toEpochMilli());
    Assertions.assertNotNull(internalTable.getReadSchema());

    Assertions.assertEquals(7, internalTable.getReadSchema().getFields().size());
    validateSchema(internalTable.getReadSchema(), catalogSales.schema());

    assertEquals(1, internalTable.getPartitioningFields().size());
    InternalField partitionField = internalTable.getPartitioningFields().get(0).getSourceField();
    assertEquals("cs_sold_date_sk", partitionField.getName());
    assertEquals(7, partitionField.getFieldId());
    assertEquals(
        PartitionTransformType.VALUE,
        internalTable.getPartitioningFields().get(0).getTransformType());
  }

  @Test
  public void testGetCurrentSnapshot(@TempDir Path workingDir) throws IOException {
    Table catalogSales = createTestTableWithData(workingDir.toString());
    Snapshot iceCurrentSnapshot = catalogSales.currentSnapshot();

    PerTableConfig sourceTableConfig = getPerTableConfig(catalogSales);

    IcebergDataFileExtractor spyDataFileExtractor = spy(IcebergDataFileExtractor.builder().build());
    IcebergPartitionValueConverter spyPartitionConverter =
        spy(IcebergPartitionValueConverter.getInstance());

    IcebergConversionSource spyConversionSource =
        spy(
            IcebergConversionSource.builder()
                .hadoopConf(hadoopConf)
                .sourceTableConfig(sourceTableConfig)
                .dataFileExtractor(spyDataFileExtractor)
                .partitionConverter(spyPartitionConverter)
                .build());

    InternalSnapshot internalSnapshot = spyConversionSource.getCurrentSnapshot();
    Assertions.assertNotNull(internalSnapshot);
    assertEquals(String.valueOf(iceCurrentSnapshot.snapshotId()), internalSnapshot.getVersion());
    Assertions.assertNotNull(internalSnapshot.getTable());
    verify(spyConversionSource, times(1)).getTable(iceCurrentSnapshot);
    verify(spyPartitionConverter, times(5)).toXTable(any(), any(), any());
    verify(spyDataFileExtractor, times(5)).fromIceberg(any(), any(), any());

    Assertions.assertNotNull(internalSnapshot.getPartitionedDataFiles());
    List<PartitionFileGroup> dataFileChunks = internalSnapshot.getPartitionedDataFiles();
    assertEquals(5, dataFileChunks.size());
    for (PartitionFileGroup dataFilesChunk : dataFileChunks) {
      List<InternalDataFile> internalDataFiles = dataFilesChunk.getFiles();
      assertEquals(1, internalDataFiles.size());
      InternalDataFile internalDataFile = internalDataFiles.get(0);
      assertEquals(FileFormat.APACHE_PARQUET, internalDataFile.getFileFormat());
      assertEquals(1, internalDataFile.getRecordCount());
      Assertions.assertTrue(internalDataFile.getPhysicalPath().startsWith("file:" + workingDir));

      List<PartitionValue> partitionValues = internalDataFile.getPartitionValues();
      assertEquals(1, partitionValues.size());
      PartitionValue partitionEntry = partitionValues.iterator().next();
      assertEquals(
          "cs_sold_date_sk", partitionEntry.getPartitionField().getSourceField().getName());
      // TODO generate test with column stats
      assertEquals(0, internalDataFile.getColumnStats().size());
    }
  }

  @Test
  public void testGetTableChangeForCommit(@TempDir Path workingDir) throws IOException {
    Table catalogSales = createTestTableWithData(workingDir.toString());
    String tableLocation = catalogSales.location();
    assertEquals(5, getDataFileCount(catalogSales));
    Snapshot snapshot1 = catalogSales.currentSnapshot();

    catalogSales
        .newDelete()
        .deleteFromRowFilter(Expressions.lessThan("cs_sold_date_sk", 3))
        .commit();
    assertEquals(2, getDataFileCount(catalogSales));
    Snapshot snapshot2 = catalogSales.currentSnapshot();

    AppendFiles appendAction = catalogSales.newAppend();
    for (int partition = 2; partition < 7; partition++) {
      String dataFilePath = String.join("/", tableLocation, "data", UUID.randomUUID() + ".parquet");
      appendAction.appendFile(generateTestDataFile(partition, catalogSales, dataFilePath));
    }
    appendAction.commit();
    assertEquals(7, getDataFileCount(catalogSales));
    Snapshot snapshot3 = catalogSales.currentSnapshot();

    Transaction tx = catalogSales.newTransaction();
    tx.newDelete().deleteFromRowFilter(Expressions.lessThan("cs_sold_date_sk", 3)).commit();
    appendAction = tx.newAppend();
    for (int partition = 6; partition < 7; partition++) {
      String dataFilePath = String.join("/", tableLocation, "data", UUID.randomUUID() + ".parquet");
      appendAction.appendFile(generateTestDataFile(partition, catalogSales, dataFilePath));
    }
    appendAction.commit();
    tx.commitTransaction();
    assertEquals(7, getDataFileCount(catalogSales));
    // the transaction would result in 2 snapshots
    Snapshot snapshot5 = catalogSales.currentSnapshot();
    Snapshot snapshot4 = catalogSales.snapshot(snapshot5.parentId());

    validateTableChangeDiffSize(catalogSales, snapshot1, 5, 0);
    validateTableChangeDiffSize(catalogSales, snapshot2, 0, 3);
    validateTableChangeDiffSize(catalogSales, snapshot3, 5, 0);
    // transaction related snapshot verification
    validateTableChangeDiffSize(catalogSales, snapshot4, 0, 1);
    validateTableChangeDiffSize(catalogSales, snapshot5, 1, 0);

    assertEquals(4, catalogSales.history().size());
    catalogSales.expireSnapshots().expireSnapshotId(snapshot1.snapshotId()).commit();
    assertEquals(3, catalogSales.history().size());
    Assertions.assertNull(catalogSales.snapshot(snapshot1.snapshotId()));
    Snapshot snapshot6 = catalogSales.currentSnapshot();
    // expire does not generate a new snapshot
    assertEquals(snapshot6, snapshot5);

    TableScan scan =
        catalogSales.newScan().filter(Expressions.lessThanOrEqual("cs_sold_date_sk", 3));
    try (CloseableIterable<FileScanTask> files = scan.planFiles()) {
      List<DataFile> dataFiles =
          StreamSupport.stream(files.spliterator(), false)
              .map(ContentScanTask::file)
              .collect(Collectors.toList());
      assertEquals(2, dataFiles.size());

      String dataFilePath = String.join("/", tableLocation, "data", UUID.randomUUID() + ".parquet");
      DataFile newFile = generateTestDataFile(3, catalogSales, dataFilePath);
      catalogSales
          .newRewrite()
          .addFile(newFile)
          .validateFromSnapshot(snapshot2.snapshotId())
          // rewrite operation requires validation to snapshot1 as it was expired earlier
          .deleteFile(dataFiles.get(0))
          .deleteFile(dataFiles.get(1))
          .commit();
    }
    Snapshot snapshot7 = catalogSales.currentSnapshot();

    catalogSales.updateSpec().removeField("cs_sold_date_sk").commit();
    Snapshot snapshot8 = catalogSales.currentSnapshot();

    validateTableChangeDiffSize(catalogSales, snapshot7, 1, 2);
    assertEquals(snapshot7, snapshot8);
  }

  @Test
  public void testGetCurrentCommitState(@TempDir Path workingDir) throws IOException {
    Table catalogSales = createTestTableWithData(workingDir.toString());
    String tablePath = catalogSales.location();
    Snapshot snapshot1 = catalogSales.currentSnapshot();

    String dataFilePath = String.join("/", tablePath, "data", UUID.randomUUID() + ".parquet");
    catalogSales
        .newAppend()
        .appendFile(generateTestDataFile(10, catalogSales, dataFilePath))
        .commit();
    Snapshot snapshot2 = catalogSales.currentSnapshot();

    Transaction tx = catalogSales.newTransaction();
    tx.newDelete().deleteFromRowFilter(Expressions.lessThan("cs_sold_date_sk", 3)).commit();
    AppendFiles appendAction = tx.newAppend();
    for (int partition = 6; partition < 7; partition++) {
      dataFilePath = String.join("/", tablePath, "data", UUID.randomUUID() + ".parquet");
      appendAction.appendFile(generateTestDataFile(partition, catalogSales, dataFilePath));
    }
    appendAction.commit();
    tx.commitTransaction();
    // the transaction would result in 2 snapshots, although 3a will not be in the history as only
    // the last snapshot of a multi-snapshot transaction is tracked in history.
    Snapshot snapshot3b = catalogSales.currentSnapshot();
    Snapshot snapshot3a = catalogSales.snapshot(snapshot3b.parentId());

    dataFilePath = String.join("/", tablePath, "data", UUID.randomUUID() + ".parquet");
    catalogSales
        .newAppend()
        .appendFile(generateTestDataFile(11, catalogSales, dataFilePath))
        .commit();
    Snapshot snapshot4 = catalogSales.currentSnapshot();

    validatePendingCommits(catalogSales, snapshot1, snapshot2, snapshot3a, snapshot3b, snapshot4);
    validatePendingCommits(catalogSales, snapshot3a, snapshot3b, snapshot4);

    // TODO this use case is invalid. If a snapshot in the middle of a chain is expired, the chain
    // TODO in invalid. This should result in termination of incremental sync?
    catalogSales.expireSnapshots().expireSnapshotId(snapshot2.snapshotId()).commit();
    validatePendingCommits(catalogSales, snapshot1, snapshot3a, snapshot3b, snapshot4);
    // TODO invalid use case below
    // even though 3a, 3b belong to same transaction, one of the two can be expired
    // catalogSales.expireSnapshots().expireSnapshotId(snapshot3a.snapshotId()).commit();
    // validatePendingCommits(catalogSales, snapshot1, snapshot2, snapshot3b, snapshot4);
  }

  private void validatePendingCommits(Table table, Snapshot lastSync, Snapshot... snapshots) {
    InstantsForIncrementalSync instant =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(Instant.ofEpochMilli(lastSync.timestampMillis()))
            .build();
    IcebergConversionSource conversionSource = getIcebergConversionSource(table);
    CommitsBacklog<Snapshot> commitsBacklog = conversionSource.getCommitsBacklog(instant);
    assertEquals(0, commitsBacklog.getInFlightInstants().size());
    Assertions.assertArrayEquals(snapshots, commitsBacklog.getCommitsToProcess().toArray());
  }

  private static long getDataFileCount(Table catalogSales) throws IOException {
    try (CloseableIterable<FileScanTask> files = catalogSales.newScan().planFiles()) {
      return StreamSupport.stream(files.spliterator(), false).count();
    }
  }

  private void validateTableChangeDiffSize(
      Table table, Snapshot snapshot, int addedFiles, int removedFiles) {
    IcebergConversionSource conversionSource = getIcebergConversionSource(table);
    TableChange tableChange = conversionSource.getTableChangeForCommit(snapshot);
    assertEquals(addedFiles, tableChange.getFilesDiff().getFilesAdded().size());
    assertEquals(removedFiles, tableChange.getFilesDiff().getFilesRemoved().size());
  }

  private void validateSchema(InternalSchema readSchema, Schema expectedSchema) {
    IcebergSchemaExtractor schemaExtractor = IcebergSchemaExtractor.getInstance();
    Schema result = schemaExtractor.toIceberg(readSchema);

    assertEquals(result.columns().size(), expectedSchema.columns().size());

    Map<String, Types.NestedField> columnMap =
        result.columns().stream().collect(Collectors.toMap(Types.NestedField::name, f -> f));

    for (Types.NestedField expectedField : expectedSchema.columns()) {
      Types.NestedField column = columnMap.get(expectedField.name());
      Assertions.assertNotNull(column);
      assertEquals(expectedField.fieldId(), column.fieldId());
      assertEquals(expectedField.type(), column.type());
      assertEquals(expectedField.isOptional(), column.isOptional());
      assertEquals(expectedField.doc(), column.doc());
    }
  }

  private Table createTestTableWithData(String workingDir) throws IOException {
    Table catalogSales = createTestCatalogTable(workingDir);

    AppendFiles appendFiles = catalogSales.newAppend();
    for (int partition = 0; partition < 5; partition++) {
      // The test creates one file in each partition
      String dataFilePath =
          String.join("/", catalogSales.location(), "data", UUID.randomUUID() + ".parquet");
      DataFile dataFile = generateTestDataFile(partition, catalogSales, dataFilePath);
      appendFiles.appendFile(dataFile);
    }
    appendFiles.commit();

    return catalogSales;
  }

  private DataFile generateTestDataFile(int partition, Table table, String filePath)
      throws IOException {
    PartitionData partitionInfo = new PartitionData(csPartitionSpec.partitionType());
    partitionInfo.set(0, partition);
    DataWriter<GenericRecord> dataWriter =
        Parquet.writeData(table.io().newOutputFile(filePath))
            .schema(csSchema)
            .createWriterFunc(GenericParquetWriter::buildWriter)
            .overwrite()
            .withSpec(csPartitionSpec)
            .withPartition(partitionInfo)
            .build();

    try {
      GenericRecord record = GenericRecord.create(csSchema);
      record.setField("cs_sold_date_sk", partition);
      dataWriter.write(record);
    } finally {
      dataWriter.close();
    }
    return dataWriter.toDataFile();
  }

  private Table createTestCatalogTable(String workingDir) {
    String csPath = Paths.get(workingDir, "catalog_sales").toString();
    return tableManager.getOrCreateTable(null, null, csPath, csSchema, csPartitionSpec);
  }

  private IcebergConversionSource getIcebergConversionSource(Table catalogSales) {
    PerTableConfig tableConfig = getPerTableConfig(catalogSales);

    return IcebergConversionSource.builder()
        .hadoopConf(hadoopConf)
        .sourceTableConfig(tableConfig)
        .build();
  }

  private static PerTableConfig getPerTableConfig(Table catalogSales) {
    return PerTableConfigImpl.builder()
        .tableName(catalogSales.name())
        .tableBasePath(catalogSales.location())
        .targetTableFormats(Collections.singletonList(TableFormat.DELTA))
        .build();
  }

  private byte[] readResourceFile(String resourcePath) throws IOException {
    return Files.readAllBytes(
        Paths.get(getClass().getClassLoader().getResource(resourcePath).getPath()));
  }
}
