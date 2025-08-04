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
 
package org.apache.xtable.parquet;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import lombok.Builder;
import lombok.NonNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import org.apache.xtable.hudi.*;
import org.apache.xtable.model.*;
import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.*;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.spi.extractor.ConversionSource;

@Builder
public class ParquetConversionSource implements ConversionSource<Long> {
  @Builder.Default
  private static final ParquetSchemaExtractor schemaExtractor =
      ParquetSchemaExtractor.getInstance();

  @Builder.Default
  private static final ParquetMetadataExtractor parquetMetadataExtractor =
      ParquetMetadataExtractor.getInstance();

  @Builder.Default
  private static final ParquetStatsExtractor parquetStatsExtractor =
      ParquetStatsExtractor.getInstance();

  private final ParquetPartitionValueExtractor partitionValueExtractor;
  private final ConfigurationBasedPartitionSpecExtractor partitionSpecExtractor;
  private final String tableName;
  private final String basePath;
  @NonNull private final Configuration hadoopConf;

  private InternalTable createInternalTableFromFile(LocatedFileStatus latestFile) {
    ParquetMetadata parquetMetadata =
        parquetMetadataExtractor.readParquetMetadata(hadoopConf, latestFile.getPath());
    MessageType parquetSchema = parquetMetadataExtractor.getSchema(parquetMetadata);
    InternalSchema schema =
        schemaExtractor.toInternalSchema(parquetSchema, latestFile.getPath().toString());
    List<InternalPartitionField> partitionFields = partitionSpecExtractor.spec(schema);

    DataLayoutStrategy dataLayoutStrategy =
        partitionFields.isEmpty()
            ? DataLayoutStrategy.FLAT
            : DataLayoutStrategy.HIVE_STYLE_PARTITION;
    return InternalTable.builder()
        .tableFormat(TableFormat.PARQUET)
        .basePath(basePath)
        .name(tableName)
        .layoutStrategy(dataLayoutStrategy)
        .partitioningFields(partitionFields)
        .readSchema(schema)
        .latestCommitTime(Instant.ofEpochMilli(latestFile.getModificationTime()))
        .build();
  }

  private InternalTable getMostRecentTable(Stream<LocatedFileStatus> parquetFiles) {
    LocatedFileStatus latestFile = getMostRecentParquetFile(parquetFiles);
    return createInternalTableFromFile(latestFile);
  }

  @Override
  public InternalTable getTable(Long modificationTime) {
    // get parquetFile at specific time modificationTime
    Stream<LocatedFileStatus> parquetFiles = getParquetFiles(hadoopConf, basePath);
    LocatedFileStatus file = getParquetFileAt(parquetFiles, modificationTime);
    return createInternalTableFromFile(file);
  }

  private List<InternalDataFile> getInternalDataFiles(Stream<LocatedFileStatus> parquetFiles) {
    return parquetFiles
        .map(
            file ->
                InternalDataFile.builder()
                    .physicalPath(file.getPath().toString())
                    .fileFormat(FileFormat.APACHE_PARQUET)
                    .fileSizeBytes(file.getLen())
                    .partitionValues(
                        partitionValueExtractor.extractPartitionValues(
                            partitionValueExtractor.extractParquetPartitions(
                                parquetMetadataExtractor.readParquetMetadata(
                                    hadoopConf, file.getPath()),
                                file.getPath().toString()),
                            basePath))
                    .lastModified(file.getModificationTime())
                    .columnStats(
                        parquetStatsExtractor.getColumnStatsForaFile(
                            parquetMetadataExtractor.readParquetMetadata(
                                hadoopConf, file.getPath())))
                    .build())
        .collect(Collectors.toList());
  }

  private InternalDataFile createInternalDataFileFromParquetFile(FileStatus parquetFile) {
    return InternalDataFile.builder()
        .physicalPath(parquetFile.getPath().toString())
        .partitionValues(
            partitionValueExtractor.extractPartitionValues(
                partitionValueExtractor.extractParquetPartitions(
                    parquetMetadataExtractor.readParquetMetadata(hadoopConf, parquetFile.getPath()),
                    parquetFile.getPath().toString()),
                basePath))
        .lastModified(parquetFile.getModificationTime())
        .fileSizeBytes(parquetFile.getLen())
        .columnStats(
            parquetStatsExtractor.getColumnStatsForaFile(
                parquetMetadataExtractor.readParquetMetadata(hadoopConf, parquetFile.getPath())))
        .build();
  }

  @Override
  public CommitsBacklog<Long> getCommitsBacklog(InstantsForIncrementalSync syncInstants) {
    // based on either table formats?
    List<Long> commitsToProcess =
        Collections.singletonList(syncInstants.getLastSyncInstant().toEpochMilli());
    return CommitsBacklog.<Long>builder().commitsToProcess(commitsToProcess).build();
  }

  @Override
  public TableChange getTableChangeForCommit(Long modificationTime) {
    Stream<LocatedFileStatus> parquetFiles = getParquetFiles(hadoopConf, basePath);
    Set<InternalDataFile> addedInternalDataFiles = new HashSet<>();

    List<FileStatus> tableChangesAfter =
        parquetFiles
            .filter(fileStatus -> fileStatus.getModificationTime() > modificationTime)
            .collect(Collectors.toList());
    InternalTable internalTable = getMostRecentTable(parquetFiles);
    for (FileStatus tableStatus : tableChangesAfter) {
      InternalDataFile currentDataFile = createInternalDataFileFromParquetFile(tableStatus);
      addedInternalDataFiles.add(currentDataFile);
    }

    return TableChange.builder()
        .tableAsOfChange(internalTable)
        .filesDiff(InternalFilesDiff.builder().filesAdded(addedInternalDataFiles).build())
        .build();
  }

  @Override
  public InternalTable getCurrentTable() {
    Stream<LocatedFileStatus> parquetFiles = getParquetFiles(hadoopConf, basePath);
    return getMostRecentTable(parquetFiles);
  }

  /**
   * Here to get current snapshot listing all files hence the -1 is being passed
   *
   * @return
   */
  @Override
  public InternalSnapshot getCurrentSnapshot() {
    Stream<LocatedFileStatus> parquetFiles = getParquetFiles(hadoopConf, basePath);
    List<InternalDataFile> internalDataFiles = getInternalDataFiles(parquetFiles);
    InternalTable table = getMostRecentTable(parquetFiles);
    return InternalSnapshot.builder()
        .table(table)
        .partitionedDataFiles(PartitionFileGroup.fromFiles(internalDataFiles))
        .build();
  }

  private LocatedFileStatus getMostRecentParquetFile(Stream<LocatedFileStatus> parquetFiles) {
    return parquetFiles
        .max(Comparator.comparing(FileStatus::getModificationTime))
        .orElseThrow(() -> new IllegalStateException("No files found"));
  }

  private LocatedFileStatus getParquetFileAt(
      Stream<LocatedFileStatus> parquetFiles, long modificationTime) {
    return parquetFiles
        .filter(fileStatus -> fileStatus.getModificationTime() == modificationTime)
        .findFirst()
        .orElseThrow(
            () -> new IllegalStateException("No file found at " + Long.valueOf(modificationTime)));
  }

  private List<LocatedFileStatus> getParquetFilesInRange(
      List<LocatedFileStatus> parquetFiles, Long minModificationDate, Long maxModicationDate) {
    return parquetFiles.stream()
        .filter(
            fileStatus ->
                fileStatus.getModificationTime() > minModificationDate
                    && fileStatus.getModificationTime() < maxModicationDate)
        .collect(Collectors.toList());
  }

  private Stream<LocatedFileStatus> getParquetFiles(Configuration hadoopConf, String basePath) {
    try {
      FileSystem fs = FileSystem.get(hadoopConf);
      RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(basePath), true);
      return ParquetFilesUtil.remoteIteratorToStream(iterator)
          .filter(file -> file.getPath().getName().endsWith("parquet"));
    } catch (IOException e) { //
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isIncrementalSyncSafeFrom(Instant instant) {
    long modficationTime = instant.getEpochSecond();
    Stream<LocatedFileStatus> parquetFiles = getParquetFiles(hadoopConf, basePath);
    LocatedFileStatus parquetFile = getMostRecentParquetFile(parquetFiles);
    Path parquetFilePath = parquetFile.getPath();
    // check if its predecessor in terms of modification time is within instant (as done in Hudi)
    while (parquetFile.isFile() && parquetFile.getModificationTime() > modficationTime) {
      // check the preceeding parquetFile
      parquetFiles.filter(file -> !file.getPath().equals(parquetFilePath));
      parquetFile = getMostRecentParquetFile(parquetFiles);
    }
    return parquetFile.isFile();
  }

  @Override
  public String getCommitIdentifier(Long aLong) {
    return String.valueOf(aLong);
  }

  @Override
  public void close() {}
}
