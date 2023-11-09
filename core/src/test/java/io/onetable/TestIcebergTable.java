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
 
package io.onetable;

import static org.apache.iceberg.SnapshotSummary.TOTAL_RECORDS_PROP;
import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import lombok.Getter;
import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetReaders;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import com.google.common.base.Preconditions;

import io.onetable.iceberg.TestIcebergDataHelper;

@Getter
public class TestIcebergTable implements GenericTable<Record, String> {
  private static final String DEFAULT_RECORD_KEY_FIELD = "id";
  private static final List<String> DEFAULT_PARTITION_FIELDS = Arrays.asList("level");

  private final String tableName;
  private final String basePath;
  private final org.apache.iceberg.Schema schema;
  private final TestIcebergDataHelper icebergDataHelper;
  private final Table icebergTable;
  private final Configuration hadoopConf;
  private final HadoopCatalog hadoopCatalog;

  public static TestIcebergTable forStandardSchemaAndPartitioning(
      String tableName, String partitionField, Path tempDir, Configuration hadoopConf) {
    return new TestIcebergTable(
        tableName,
        tempDir,
        hadoopConf,
        DEFAULT_RECORD_KEY_FIELD,
        Collections.singletonList(partitionField));
  }

  public TestIcebergTable(
      String tableName,
      Path tempDir,
      Configuration hadoopConf,
      String recordKeyField,
      List<String> partitionFields) {
    this.tableName = tableName;
    this.basePath = tempDir.toUri().toString();
    this.icebergDataHelper =
        TestIcebergDataHelper.builder()
            .recordKeyField(recordKeyField)
            .partitionFieldNames(filterNullFields(partitionFields))
            .build();
    this.schema = icebergDataHelper.getTableSchema();
    this.hadoopConf = hadoopConf;
    PartitionSpec partitionSpec = icebergDataHelper.getPartitionSpec();

    hadoopCatalog = new HadoopCatalog(hadoopConf, basePath);
    // No namespace specified.
    TableIdentifier tableIdentifier = TableIdentifier.of("", tableName);
    if (!hadoopCatalog.tableExists(tableIdentifier)) {
      icebergTable = hadoopCatalog.createTable(tableIdentifier, schema, partitionSpec);
    } else {
      icebergTable = hadoopCatalog.loadTable(tableIdentifier);
    }
  }

  @Override
  @SneakyThrows
  public List<Record> insertRows(int numRows) {
    List<Record> records = icebergDataHelper.generateInsertRecords(numRows);
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(icebergTable.schema());

    // Group records by partition
    Map<StructLike, List<Record>> recordsByPartition = groupRecordsForWritingByPartition(records);
    AppendFiles append = icebergTable.newAppend();
    List<DataFile> dataFiles = writeAllDataFiles(appenderFactory, recordsByPartition);
    dataFiles.forEach(append::appendFile);
    append.commit();
    return records;
  }

  @SneakyThrows
  public List<Record> insertRecordsForPartition(int numRows, String partitionValue) {
    List<Record> records =
        icebergDataHelper.generateInsertRecordForPartition(numRows, partitionValue);
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(icebergTable.schema());

    Map<StructLike, List<Record>> recordsByPartition = groupRecordsForWritingByPartition(records);
    AppendFiles append = icebergTable.newAppend();
    List<DataFile> dataFiles = writeAllDataFiles(appenderFactory, recordsByPartition);
    dataFiles.forEach(append::appendFile);
    append.commit();
    return records;
  }

  @Override
  @SneakyThrows
  public List<Record> insertRecordsForSpecialPartition(int numRows) {
    return insertRecordsForPartition(numRows, SPECIAL_PARTITION_VALUE);
  }

  @SneakyThrows
  @Override
  public void upsertRows(List<Record> recordsToUpdate) {
    Set<String> idsToUpdate =
        recordsToUpdate.stream()
            .map(r -> r.getField(DEFAULT_RECORD_KEY_FIELD).toString())
            .collect(Collectors.toSet());
    List<Record> allRecordsInTable = getAllRecordsInTable();
    List<Record> recordsWithNoUpdates =
        allRecordsInTable.stream()
            .filter(r -> !idsToUpdate.contains(r.getField(DEFAULT_RECORD_KEY_FIELD).toString()))
            .collect(Collectors.toList());
    List<Record> updatedRecords = icebergDataHelper.generateUpsertRecords(recordsToUpdate);
    List<Record> combinedRecords = new ArrayList<>(recordsWithNoUpdates);
    combinedRecords.addAll(updatedRecords);

    OverwriteFiles overwrite = icebergTable.newOverwrite();
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(icebergTable.schema());

    // Group records by partition
    Map<StructLike, List<Record>> recordsByPartition =
        groupRecordsForWritingByPartition(combinedRecords);

    // Delete existing files in table.
    getCurrentFilesInTable().forEach(dataFile -> overwrite.deleteFile(dataFile));

    // Write new files.
    List<DataFile> dataFiles = writeAllDataFiles(appenderFactory, recordsByPartition);
    dataFiles.forEach(overwrite::addFile);
    overwrite.commit();
  }

  @Override
  @SneakyThrows
  public void deleteRows(List<Record> recordsToDelete) {
    Set<String> idsToDelete =
        recordsToDelete.stream()
            .map(r -> r.getField(DEFAULT_RECORD_KEY_FIELD).toString())
            .collect(Collectors.toSet());
    List<Record> allRecordsInTable = getAllRecordsInTable();
    List<Record> recordsToRetain =
        allRecordsInTable.stream()
            .filter(r -> !idsToDelete.contains(r.getField(DEFAULT_RECORD_KEY_FIELD).toString()))
            .collect(Collectors.toList());

    OverwriteFiles overwrite = icebergTable.newOverwrite();
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(icebergTable.schema());

    // Group records by partition
    Map<StructLike, List<Record>> recordsByPartition =
        groupRecordsForWritingByPartition(recordsToRetain);

    // Delete existing files in table.
    getCurrentFilesInTable().forEach(dataFile -> overwrite.deleteFile(dataFile));

    // Write new files.
    List<DataFile> dataFiles = writeAllDataFiles(appenderFactory, recordsByPartition);
    dataFiles.forEach(overwrite::addFile);
    overwrite.commit();
  }

  @Override
  public void deletePartition(String partitionValue) {
    // Here it is assumed that the partition value is identity and for level field, need to extend
    // to be generic to be used for timestamp and date.
    icebergTable
        .newDelete()
        .deleteFromRowFilter(Expressions.equal("level", partitionValue))
        .commit();
  }

  @Override
  public void deleteSpecialPartition() {
    deletePartition(SPECIAL_PARTITION_VALUE);
  }

  @Override
  public String getBasePath() {
    return basePath + "/" + tableName;
  }

  @Override
  public String getOrderByColumn() {
    return DEFAULT_RECORD_KEY_FIELD;
  }

  public Map<String, List<Record>> groupRecordsByPartition(List<Record> records) {
    Preconditions.checkArgument(
        icebergDataHelper.getPartitionFieldNames().size() == 1,
        "Only single partition field is supported for grouping records by partition");
    Preconditions.checkArgument(
        icebergDataHelper.getPartitionFieldNames().get(0).equals("level"),
        "Only level partition field is supported for grouping records by partition");
    return records.stream()
        .collect(Collectors.groupingBy(record -> record.getField("level").toString()));
  }

  @Override
  @SneakyThrows
  public void close() {
    hadoopCatalog.close();
  }

  @Override
  public void reload() {
    icebergTable.refresh();
  }

  @Override
  public List<String> getColumnsToSelect() {
    return icebergDataHelper.getTableSchema().columns().stream()
        .map(Types.NestedField::name)
        .collect(Collectors.toList());
  }

  public Long getLastCommitTimestamp() {
    return icebergTable.currentSnapshot().timestampMillis();
  }

  public List<String> getAllActiveFiles() {
    List<String> filePaths = new ArrayList<>();
    Iterable<FileScanTask> fileScanTasks = icebergTable.newScan().planFiles();
    for (FileScanTask fileScanTask : fileScanTasks) {
      DataFile dataFile = fileScanTask.file();
      filePaths.add(dataFile.path().toString());
    }
    return filePaths;
  }

  private List<String> filterNullFields(List<String> partitionFields) {
    return partitionFields.stream().filter(Objects::nonNull).collect(Collectors.toList());
  }

  public long getNumRows() {
    Snapshot currentSnapshot = icebergTable.currentSnapshot();
    Long totalRecords =
        Long.parseLong(currentSnapshot.summary().getOrDefault(TOTAL_RECORDS_PROP, "0"));
    assertTrue(totalRecords > 0, "Total records is expected to be greater than 0");
    return totalRecords;
  }

  private DataFile writeAndGetDataFile(
      GenericAppenderFactory appenderFactory, List<Record> records, StructLike partitionKey)
      throws IOException {
    String fileName = "data/" + UUID.randomUUID() + ".parquet";
    OutputFile outputFile =
        HadoopOutputFile.fromPath(
            new org.apache.hadoop.fs.Path(icebergTable.location(), fileName), hadoopConf);

    try (FileAppender<Record> appender =
        appenderFactory.newAppender(outputFile, FileFormat.PARQUET)) {
      appender.addAll(records);
    }

    return DataFiles.builder(icebergTable.spec())
        .withInputFile(
            HadoopInputFile.fromPath(
                new org.apache.hadoop.fs.Path(outputFile.location()), hadoopConf))
        .withPartition(partitionKey)
        .withRecordCount(records.size())
        .build();
  }

  private List<DataFile> getCurrentFilesInTable() {
    List<DataFile> allFiles = new ArrayList<>();
    Iterable<FileScanTask> fileScanTasks = icebergTable.newScan().planFiles();
    fileScanTasks.forEach(fileScanTask -> allFiles.add(fileScanTask.file()));
    return allFiles;
  }

  private List<Record> getAllRecordsInTable() throws IOException {
    List<Record> allRecords = new ArrayList<>();
    Iterable<FileScanTask> fileScanTasks = icebergTable.newScan().planFiles();
    for (FileScanTask fileScanTask : fileScanTasks) {
      DataFile dataFile = fileScanTask.file();
      InputFile inputFile = icebergTable.io().newInputFile(dataFile.path().toString());

      try (CloseableIterable<Record> reader =
          Parquet.read(inputFile)
              .project(icebergTable.schema())
              .createReaderFunc(
                  fileSchema ->
                      GenericParquetReaders.buildReader(icebergTable.schema(), fileSchema))
              .build()) {
        for (Record record : reader) {
          allRecords.add(record);
        }
      }
    }
    return allRecords;
  }

  private Map<StructLike, List<Record>> groupRecordsForWritingByPartition(List<Record> records) {
    Map<StructLike, List<Record>> recordsByPartition = new HashMap<>();
    PartitionKey partitionKey = new PartitionKey(icebergTable.spec(), icebergTable.schema());
    for (Record record : records) {
      partitionKey.partition(record);
      recordsByPartition.computeIfAbsent(partitionKey.copy(), k -> new ArrayList<>()).add(record);
    }
    return recordsByPartition;
  }

  private List<DataFile> writeAllDataFiles(
      GenericAppenderFactory appenderFactory, Map<StructLike, List<Record>> recordsByPartition) {
    return recordsByPartition.entrySet().stream()
        .map(
            entry -> {
              try {
                return writeAndGetDataFile(appenderFactory, entry.getValue(), entry.getKey());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .collect(Collectors.toList());
  }
}
