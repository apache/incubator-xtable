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

import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.NonNull;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import org.apache.xtable.hudi.*;
import org.apache.xtable.hudi.HudiPathUtils;
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

@Log4j2
public class ParquetConversionSource implements ConversionSource<Long> {

  private static final ParquetSchemaExtractor schemaExtractor =
      ParquetSchemaExtractor.getInstance();

  private static final ParquetMetadataExtractor parquetMetadataExtractor =
      ParquetMetadataExtractor.getInstance();

  private static final ParquetStatsExtractor parquetStatsExtractor =
      ParquetStatsExtractor.getInstance();

  private final ParquetPartitionValueExtractor partitionValueExtractor;
  private final PathBasedPartitionSpecExtractor partitionSpecExtractor;
  private final String tableName;
  private final String basePath;
  @NonNull private final Configuration hadoopConf;
  private final ParquetDataManager parquetDataManager;

  @Builder
  ParquetConversionSource(
      String tableName,
      String basePath,
      @NonNull Configuration hadoopConf,
      ParquetPartitionValueExtractor partitionValueExtractor,
      PathBasedPartitionSpecExtractor partitionSpecExtractor) {
    this.tableName = tableName;
    this.basePath = basePath;
    this.hadoopConf = hadoopConf;
    this.partitionValueExtractor = partitionValueExtractor;
    this.partitionSpecExtractor = partitionSpecExtractor;
    this.parquetDataManager = new ParquetDataManager(hadoopConf, basePath);
  }

  private InternalTable createInternalTableFromFile(ParquetFileInfo latestFile) {
    ParquetMetadata parquetMetadata =
        parquetMetadataExtractor.readParquetMetadata(hadoopConf, latestFile.getPath());
    MessageType parquetSchema = parquetMetadataExtractor.getSchema(parquetMetadata);
    InternalSchema schema = schemaExtractor.toInternalSchema(parquetSchema, "");
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

  @Override
  public InternalTable getTable(Long modificationTime) {
    // get parquetFile at specific time modificationTime
    ParquetFileInfo file = parquetDataManager.getParquetDataFileAt(modificationTime);
    return createInternalTableFromFile(file);
  }

  private Stream<InternalDataFile> getInternalDataFiles(Stream<ParquetFileInfo> parquetFiles) {
    return parquetFiles.map(
        file -> {
          ParquetMetadata metadata =
              parquetMetadataExtractor.readParquetMetadata(hadoopConf, file.getPath());
          return InternalDataFile.builder()
              .physicalPath(file.getPath().toString())
              .fileFormat(FileFormat.APACHE_PARQUET)
              .fileSizeBytes(file.getSize())
              .partitionValues(
                  partitionValueExtractor.extractPartitionValues(
                      partitionSpecExtractor.spec(
                          partitionValueExtractor.extractSchemaForParquetPartitions(
                              metadata, file.getPath().toString())),
                      HudiPathUtils.getPartitionPath(new Path(basePath), file.getPath())))
              .lastModified(file.getModificationTime())
              .columnStats(parquetStatsExtractor.getColumnStatsForaFile(metadata))
              .build();
        });
  }

  private InternalDataFile createInternalDataFileFromParquetFile(ParquetFileInfo parquetFile) {
    return InternalDataFile.builder()
        .physicalPath(parquetFile.getPath().toString())
        .partitionValues(
            partitionValueExtractor.extractPartitionValues(
                partitionSpecExtractor.spec(
                    partitionValueExtractor.extractSchemaForParquetPartitions(
                        parquetFile.getMetadata(), parquetFile.getPath().toString())),
                HudiPathUtils.getPartitionPath(new Path(basePath), parquetFile.getPath())))
        .lastModified(parquetFile.getModificationTime())
        .fileSizeBytes(parquetFile.getSize())
        .columnStats(parquetStatsExtractor.getColumnStatsForaFile(parquetFile.getMetadata()))
        .build();
  }

  @Override
  public CommitsBacklog<Long> getCommitsBacklog(InstantsForIncrementalSync syncInstants) {
    List<Long> commitsToProcess =
        Collections.singletonList(syncInstants.getLastSyncInstant().toEpochMilli());
    return CommitsBacklog.<Long>builder().commitsToProcess(commitsToProcess).build();
  }

  @Override
  public TableChange getTableChangeForCommit(Long modificationTime) {

    List<ParquetFileInfo> tableChangesAfterModificationTime =
        parquetDataManager.getParquetFilesMetadataAfterTime(modificationTime);
    InternalTable internalTable =
        getMostRecentTableConfig(tableChangesAfterModificationTime.stream());
    Set<InternalDataFile> addedInternalDataFiles =
        tableChangesAfterModificationTime.stream()
            .map(this::createInternalDataFileFromParquetFile)
            .collect(Collectors.toSet());

    return TableChange.builder()
        .sourceIdentifier(
            getCommitIdentifier(
                parquetDataManager.getMostRecentParquetFile().getModificationTime()))
        .tableAsOfChange(internalTable)
        .filesDiff(InternalFilesDiff.builder().filesAdded(addedInternalDataFiles).build())
        .build();
  }

  private InternalTable getMostRecentTable() {
    ParquetFileInfo latestFile = parquetDataManager.getMostRecentParquetFile();
    return createInternalTableFromFile(latestFile);
  }

  private InternalTable getMostRecentTableConfig(Stream<ParquetFileInfo> parquetFiles) {
    ParquetFileInfo latestFile =
        parquetFiles
            .max(Comparator.comparing(ParquetFileInfo::getModificationTime))
            .orElseThrow(() -> new IllegalStateException("No files found"));
    return createInternalTableFromFile(latestFile);
  }

  @Override
  public InternalTable getCurrentTable() {
    return getMostRecentTable();
  }

  @Override
  public InternalSnapshot getCurrentSnapshot() {
    Stream<InternalDataFile> internalDataFiles =
        getInternalDataFiles(parquetDataManager.getCurrentFileInfo());
    InternalTable table = getMostRecentTable();
    return InternalSnapshot.builder()
        .table(table)
        .sourceIdentifier(
            getCommitIdentifier(
                parquetDataManager.getMostRecentParquetFile().getModificationTime()))
        .partitionedDataFiles(PartitionFileGroup.fromFiles(internalDataFiles))
        .build();
  }

  @Override
  public boolean isIncrementalSyncSafeFrom(Instant timeInMillis) {
    Stream<ParquetFileInfo> parquetFilesMetadata = parquetDataManager.getCurrentFileInfo();
    OptionalLong earliestModTimeOpt =
        parquetFilesMetadata.mapToLong(ParquetFileInfo::getModificationTime).min();

    if (!earliestModTimeOpt.isPresent()) {
      log.warn("No parquet files found in table {}. Incremental sync is not possible.", tableName);
      return false;
    }

    long earliestModTime = earliestModTimeOpt.getAsLong();

    if (earliestModTime > timeInMillis.toEpochMilli()) {
      log.warn(
          "Incremental sync is not safe. Earliest available metadata (time={}) is newer "
              + "than requested instant {}.",
          Instant.ofEpochMilli(earliestModTime),
          timeInMillis.toEpochMilli());
      return false;
    }

    log.debug(
        "Incremental sync is safe from instant {} for table {}",
        timeInMillis.toEpochMilli(),
        tableName);
    return true;
  }

  @Override
  public String getCommitIdentifier(Long aLong) {
    return String.valueOf(aLong);
  }

  @Override
  public void close() {}
}
