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

import static org.apache.xtable.GenericTable.getTableName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;

import org.apache.xtable.TestIcebergTable;
import org.apache.xtable.conversion.ConversionTargetFactory;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.sync.ConversionTarget;

public class ITIcebergTargetClient {

  @TempDir public static Path tempDir;
  private static final Configuration hadoopConf = new Configuration();
  private static final String TABLE_NAME = "test_table";
  private static final long FILE_SIZE = 100L;
  private static final long RECORD_COUNT = 200L;
  private static final long LAST_MODIFIED = System.currentTimeMillis();

  private ConversionTargetFactory conversionTargetFactory;

  @BeforeEach
  void setupOnce() {
    conversionTargetFactory = ConversionTargetFactory.getInstance();
  }

  @SneakyThrows
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testIcebergSync_metadataCleaner(boolean isPartitioned) {
    String tableName = getTableName();
    String partitionField = isPartitioned ? "level" : "";
    String partitionValue = isPartitioned ? "ERROR" : "";
    try (TestIcebergTable testIcebergTable =
        TestIcebergTable.forStandardSchemaAndPartitioning(
            tableName, isPartitioned ? partitionField : null, tempDir, hadoopConf)) {

      // initially load table with some data
      testIcebergTable.insertRows(50);
      List<Record> records1 = testIcebergTable.insertRows(50);
      testIcebergTable.upsertRows(records1.subList(0, 20));
      testIcebergTable.insertRows(50);
      testIcebergTable.deleteRows(records1.subList(0, 20));
      testIcebergTable.insertRows(50);

      Instant lastCommittedTime =
          Instant.ofEpochMilli(testIcebergTable.getLatestSnapshot().timestampMillis());
      Schema icebergSchema = testIcebergTable.getSchema();
      InternalSchema internalSchema =
          IcebergSchemaExtractor.getInstance().fromIceberg(icebergSchema);
      String basePath = testIcebergTable.getDataPath();

      Properties properties = new Properties();
      properties.put(IcebergSyncConfig.USE_METADATA_CLEANER, "true");
      properties.put(IcebergSyncConfig.METADATA_CLEANER_THREAD_POOL_SIZE, "2");
      TargetTable targetTable =
          TargetTable.builder()
              .name(testIcebergTable.getTableName())
              .basePath(testIcebergTable.getBasePath())
              .formatName(TableFormat.ICEBERG)
              .metadataRetention(Duration.ZERO)
              .additionalProperties(properties)
              .build();
      ConversionTarget icebergTargetClient =
          conversionTargetFactory.createForFormat(targetTable, hadoopConf);

      Set<String> existingFiles =
          getDataFilesInPartitions(
              basePath, partitionField, Collections.singletonList(partitionValue));

      assertFalse(existingFiles.isEmpty());
      // remove one existing file from the snapshot
      InternalDataFile fileToRemove =
          getDataFile(basePath, existingFiles.iterator().next(), partitionField, partitionValue);
      String fileNameToAdd = "file_1.parquet";
      createDataFile(basePath, fileNameToAdd, partitionField, partitionValue);
      // add one new data file to the snapshot
      InternalDataFile fileToAdd =
          getDataFile(basePath, fileNameToAdd, partitionField, partitionValue);

      InternalFilesDiff filesDiff =
          InternalFilesDiff.builder().fileAdded(fileToAdd).fileRemoved(fileToRemove).build();

      InternalTable initialState =
          getState(
              lastCommittedTime,
              basePath,
              internalSchema,
              InternalField.builder().name(partitionField).build());

      // get all data files in the base path across all partitions before sync
      Set<String> allDataFilesBeforeExpiration =
          getDataFilesInPartitions(
              basePath, partitionField, Arrays.asList("ERROR", "INFO", "WARN"));

      icebergTargetClient.beginSync(initialState);
      TableSyncMetadata latestState =
          TableSyncMetadata.of(initialState.getLatestCommitTime(), Collections.emptyList());
      icebergTargetClient.syncMetadata(latestState);
      icebergTargetClient.syncFilesForDiff(filesDiff);
      icebergTargetClient.completeSync();

      // get all data files in the base path across all partitions after sync
      Set<String> allDataFilesAfterExpiration =
          getDataFilesInPartitions(
              basePath, partitionField, Arrays.asList("ERROR", "INFO", "WARN"));
      // assert all manifest files related to expired snapshots are cleaned
      assertManifestFilesAreCleaned(testIcebergTable);

      // verify no data file got deleted after sync is complete
      assertEquals(allDataFilesBeforeExpiration, allDataFilesAfterExpiration);
    }
  }

  @SneakyThrows
  private void assertManifestFilesAreCleaned(TestIcebergTable testIcebergTable) {
    testIcebergTable.reload();

    Table table = testIcebergTable.getIcebergTable();
    Iterable<Snapshot> snapshots = table.snapshots();

    Set<String> validFiles = new HashSet<>();

    for (Snapshot snapshot : snapshots) {
      List<ManifestFile> manifestFiles = snapshot.allManifests(table.io());
      manifestFiles.forEach(file -> validFiles.add(getPath(file.path())));
      validFiles.add(getPath(snapshot.manifestListLocation()));
    }

    Set<String> allAvroFiles = allFilesInMetadata(testIcebergTable);

    // verify all avro files in metadata dir is a valid manifest file
    assertEquals(validFiles, allAvroFiles);
  }

  @SneakyThrows
  private Set<String> allFilesInMetadata(TestIcebergTable testIcebergTable) {
    String metadataDirPath = testIcebergTable.getBasePath() + "/" + "metadata";

    File metadataDir = new File(getPath(metadataDirPath));
    Set<String> avroFiles = new HashSet<>();

    if (metadataDir.exists() && metadataDir.isDirectory()) {
      File[] files = metadataDir.listFiles();

      if (files != null) {

        for (File avroFile : files) {
          if (avroFile.isFile() && avroFile.getName().endsWith(".avro")) {
            avroFiles.add(avroFile.getPath());
          }
        }
      }
    }

    return avroFiles;
  }

  private Set<String> getDataFilesInPartitions(
      String basePath, String partitionField, List<String> partitionValues) {
    List<String> filePaths;
    if (partitionField.isEmpty()) {
      filePaths = Collections.singletonList(basePath);
    } else {
      filePaths = new ArrayList<>(partitionValues.size());
      for (String partitionValue : partitionValues) {
        filePaths.add(String.format("%s/%s=%s/", basePath, partitionField, partitionValue));
      }
    }

    Set<String> dataFiles = new HashSet<>();
    for (String filePath : filePaths) {
      File dataDir = new File(getPath(filePath));
      if (dataDir.exists() && dataDir.isDirectory()) {
        File[] files = dataDir.listFiles();

        if (files != null) {

          for (File dataFile : files) {
            if (dataFile.isFile() && dataFile.getName().endsWith(".parquet")) {
              dataFiles.add(dataFile.getPath());
            }
          }
        }
      }
    }

    return dataFiles;
  }

  @SneakyThrows
  private String getPath(String fullPath) {
    return new URI(fullPath).getPath();
  }

  private InternalTable getState(
      Instant latestCommitTime,
      String basePath,
      InternalSchema internalSchema,
      InternalField partitionField) {
    return InternalTable.builder()
        .basePath(basePath)
        .name(TABLE_NAME)
        .latestCommitTime(latestCommitTime)
        .tableFormat(TableFormat.ICEBERG)
        .layoutStrategy(DataLayoutStrategy.HIVE_STYLE_PARTITION)
        .readSchema(internalSchema)
        .partitioningFields(
            Collections.singletonList(
                InternalPartitionField.builder()
                    .sourceField(partitionField)
                    .transformType(PartitionTransformType.VALUE)
                    .build()))
        .build();
  }

  @SneakyThrows
  private void createDataFile(
      String basePath, String fileName, String partitionField, String partitionValue) {
    String filePath;
    if (!partitionField.isEmpty()) {
      String partitionPath = String.format("%s=%s", partitionField, partitionValue);
      filePath = String.format("%s/%s/%s", basePath, partitionPath, fileName);
    } else {
      filePath = String.format("%s/%s", basePath, fileName);
    }

    File file = new File(getPath(filePath));
    file.createNewFile();
  }

  @SneakyThrows
  private InternalDataFile getDataFile(
      String basePath, String fileName, String partitionField, String partitionValueStr) {
    List<PartitionValue> partitionValues;
    String filePath;
    if (!partitionField.isEmpty()) {
      String partitionPath = String.format("%s=%s", partitionField, partitionValueStr);
      InternalPartitionField onePartitionField =
          InternalPartitionField.builder()
              .sourceField(InternalField.builder().name(partitionField).build())
              .transformType(PartitionTransformType.VALUE)
              .build();

      filePath = String.format("%s/%s/%s", basePath, partitionPath, fileName);

      partitionValues =
          Collections.singletonList(
              PartitionValue.builder()
                  .partitionField(onePartitionField)
                  .range(Range.vector("ERROR", "ERROR"))
                  .build());
    } else {
      filePath = String.format("%s/%s", basePath, fileName);
      partitionValues = Collections.emptyList();
    }

    return InternalDataFile.builder()
        .physicalPath(filePath)
        .fileSizeBytes(FILE_SIZE)
        .fileFormat(FileFormat.APACHE_PARQUET)
        .lastModified(LAST_MODIFIED)
        .recordCount(RECORD_COUNT)
        .columnStats(Collections.emptyList())
        .partitionValues(partitionValues)
        .build();
  }
}
