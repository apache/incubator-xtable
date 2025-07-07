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

import lombok.Builder;
import lombok.NonNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

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
// @NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ParquetConversionSource implements ConversionSource<Long> {
  @Builder.Default
  private static final ParquetSchemaExtractor schemaExtractor =
      ParquetSchemaExtractor.getInstance();

  @Builder.Default
  private static final ParquetMetadataExtractor parquetMetadataExtractor =
      ParquetMetadataExtractor.getInstance();

  @Builder.Default
  private static final ParquetPartitionValueExtractor partitionValueExtractor =
      ParquetPartitionValueExtractor.getInstance();

  @Builder.Default
  private static final ParquetStatsExtractor parquetStatsExtractor =
      ParquetStatsExtractor.getInstance();

  private final String tableName;
  private final String basePath;
  private final String configPath;
  @NonNull private final Configuration hadoopConf;

  private InternalTable getMostRecentTable(List<LocatedFileStatus> parquetFiles) {
    Optional<LocatedFileStatus> latestFile = getMostRecentParquetFile(parquetFiles);
    ParquetMetadata parquetMetadata =
        parquetMetadataExtractor.readParquetMetadata(hadoopConf, latestFile.get().getPath());

    List<InternalPartitionField> partitionFields =
        partitionValueExtractor.extractParquertPartitions(
            parquetMetadata, latestFile.get().getPath().toString());
    MessageType parquetSchema = parquetMetadataExtractor.getSchema(parquetMetadata);
    InternalSchema schema =
        schemaExtractor.toInternalSchema(parquetSchema, latestFile.get().getPath().toString());
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
        .latestCommitTime(Instant.ofEpochMilli(latestFile.get().getModificationTime()))
        .build();
  }

  @Override
  public InternalTable getTable(Long modificationTime) {
    return null;
  }

  private List<InternalDataFile> getInternalDataFiles(List<LocatedFileStatus> parquetFiles) {
    return parquetFiles.stream()
        .map(
            file ->
                InternalDataFile.builder()
                    .physicalPath(file.getPath().toString())
                    .fileFormat(FileFormat.APACHE_PARQUET)
                    .fileSizeBytes(file.getLen())
                    .partitionValues(
                        partitionValueExtractor.extractPartitionValues(
                            partitionValueExtractor.extractParquertPartitions(
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
                partitionValueExtractor.extractParquertPartitions(
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
    List<LocatedFileStatus> parquetFiles = getParquetFiles(hadoopConf, basePath);
    // if a file is found in tableChangesAfter and wasnt in the list of tableChangesBefore then it
    // was added
    // if a file wasnot found in tableChangesAfter and it was in the list of tableChangesBefore then
    // it was removed
    Set<InternalDataFile> removedInternalDataFiles = new HashSet<>();
    Set<InternalDataFile> addedInternalDataFiles = new HashSet<>();

    List<FileStatus> tableChangesAfter =
        parquetFiles.stream()
            .filter(fileStatus -> fileStatus.getModificationTime() > modificationTime)
            .collect(Collectors.toList());
    List<FileStatus> tableChangesBefore =
        parquetFiles.stream()
            .filter(fileStatus -> fileStatus.getModificationTime() < modificationTime)
            .collect(Collectors.toList());
    InternalTable internalTable = getMostRecentTable(parquetFiles);
    List<FileStatus> keptFiles = new ArrayList<>();
    List<FileStatus> removedFiles = new ArrayList<>(tableChangesBefore);

    for (FileStatus tableStatus : tableChangesAfter) {
      boolean isPresent = tableChangesBefore.contains(tableStatus);
      if (!isPresent) {
        InternalDataFile currentDataFile = createInternalDataFileFromParquetFile(tableStatus);
        addedInternalDataFiles.add(currentDataFile);
      } else {
        keptFiles.add(tableStatus);
      }
    }
    removedFiles.removeAll(keptFiles);
    for (FileStatus removedFile : removedFiles) {
      InternalDataFile currentDataFile = createInternalDataFileFromParquetFile(removedFile);
      removedInternalDataFiles.add(currentDataFile);
    }
    return TableChange.builder()
        .tableAsOfChange(internalTable)
        .filesDiff(
            InternalFilesDiff.builder()
                .filesAdded(addedInternalDataFiles)
                .filesRemoved(removedInternalDataFiles)
                .build())
        .build();
  }

  @Override
  public InternalTable getCurrentTable() {
    return null;
  }

  /**
   * Here to get current snapshot listing all files hence the -1 is being passed
   *
   * @return
   */
  @Override
  public InternalSnapshot getCurrentSnapshot() {
    List<LocatedFileStatus> parquetFiles = getParquetFiles(hadoopConf, basePath);
    List<InternalDataFile> internalDataFiles = getInternalDataFiles(parquetFiles);
    InternalTable table = getMostRecentTable(parquetFiles);
    return InternalSnapshot.builder()
        .table(table)
        .partitionedDataFiles(PartitionFileGroup.fromFiles(internalDataFiles))
        .build();
  }

  private Optional<LocatedFileStatus> getMostRecentParquetFile(
      List<LocatedFileStatus> parquetFiles) {
    return parquetFiles.stream().max(Comparator.comparing(FileStatus::getModificationTime));
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

  private List<LocatedFileStatus> getParquetFiles(Configuration hadoopConf, String basePath) {
    try {
      FileSystem fs = FileSystem.get(hadoopConf);
      RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(basePath), true);
      return RemoteIterators.toList(iterator).stream()
          .filter(file -> file.getPath().getName().endsWith("parquet"))
          .collect(Collectors.toList());
    } catch (IOException e) { //
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean isIncrementalSyncSafeFrom(Instant instant) {
    return false;
  }

  @Override
  public String getCommitIdentifier(Long aLong) {
    return String.valueOf(aLong);
  }

  @Override
  public void close() {}
}
