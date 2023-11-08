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
import java.util.UUID;

import lombok.Getter;
import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.OutputFile;

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
            .partitionFieldNames(partitionFields)
            .build();
    this.schema = icebergDataHelper.getTableSchema();
    this.hadoopConf = hadoopConf;
    PartitionSpec partitionSpec = generatePartitionSpec(schema, partitionFields);

    HadoopCatalog catalog = new HadoopCatalog(hadoopConf, basePath);
    // No namespace specified.
    TableIdentifier tableIdentifier = TableIdentifier.of("", tableName);
    if (!catalog.tableExists(tableIdentifier)) {
      icebergTable = catalog.createTable(tableIdentifier, schema, partitionSpec);
    } else {
      icebergTable = catalog.loadTable(tableIdentifier);
    }
  }

  @Override
  @SneakyThrows
  public List<Record> insertRows(int numRows) {
    List<Record> records = icebergDataHelper.generateInsertRecords(numRows);
    GenericAppenderFactory appenderFactory = new GenericAppenderFactory(icebergTable.schema());
    PartitionSpec spec = icebergTable.spec();

    // Group records by partition
    Map<StructLike, List<Record>> recordsByPartition = new HashMap<>();
    PartitionKey partitionKey = new PartitionKey(spec, icebergTable.schema());
    for (Record record : records) {
      partitionKey.partition(record);
      recordsByPartition.computeIfAbsent(partitionKey.copy(), k -> new ArrayList<>()).add(record);
    }
    AppendFiles append = icebergTable.newAppend();
    for (Map.Entry<StructLike, List<Record>> entry : recordsByPartition.entrySet()) {
      DataFile dataFile = writeAndGetDataFile(appenderFactory, entry.getValue(), entry.getKey());
      append.appendFile(dataFile);
    }
    append.commit();
    return records;
  }

  @Override
  public List<Record> insertRecordsForSpecialPartition(int numRows) {
    return null;
  }

  @Override
  public void upsertRows(List<Record> rows) {}

  @Override
  public void deleteRows(List<Record> rows) {}

  @Override
  public void deletePartition(String partitionValue) {}

  @Override
  public void deleteSpecialPartition() {}

  @Override
  public String getBasePath() {
    return null;
  }

  @Override
  public String getOrderByColumn() {
    return null;
  }

  @Override
  public void close() {}

  @Override
  public void reload() {}

  @Override
  public List<String> getColumnsToSelect() {
    return null;
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

  private PartitionSpec generatePartitionSpec(
      org.apache.iceberg.Schema icebergSchema, List<String> partitionColumns) {
    if (partitionColumns.size() != 1) {
      throw new IllegalArgumentException(
          "Please modify the test to support multiple partition columns");
    }
    if (!partitionColumns.get(0).equals("level")) {
      throw new IllegalArgumentException(
          "Please modify the test to support partitioning on " + partitionColumns.get(0));
    }
    return PartitionSpec.builderFor(icebergSchema).identity("level").build();
  }
}
