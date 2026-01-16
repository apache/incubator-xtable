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
import java.util.*;
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

@Builder
@Log4j2
public class ParquetConversionSource implements ConversionSource<Long> {

  private static final ParquetSchemaExtractor schemaExtractor =
      ParquetSchemaExtractor.getInstance();
  private static final ParquetMetadataExtractor metadataExtractor =
      ParquetMetadataExtractor.getInstance();
  private static final ParquetDataManager parquetDataManagerExtractor =
      ParquetDataManager.getInstance();

  private static final ParquetMetadataExtractor parquetMetadataExtractor =
      ParquetMetadataExtractor.getInstance();

  private static final ParquetStatsExtractor parquetStatsExtractor =
      ParquetStatsExtractor.getInstance();

  private final ParquetPartitionValueExtractor partitionValueExtractor;
  private final PathBasedPartitionSpecExtractor partitionSpecExtractor;
  private final String tableName;
  private final String basePath;
  @NonNull private final Configuration hadoopConf;

  private InternalTable createInternalTableFromFile(ParquetFileConfig latestFile) {
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
        .latestCommitTime(Instant.ofEpochMilli(latestFile.getModifTime()))
        .build();
  }

  @Override
  public InternalTable getTable(Long modificationTime) {
    // get parquetFile at specific time modificationTime
    Stream<LocatedFileStatus> parquetFiles =
        parquetDataManagerExtractor.getParquetFiles(hadoopConf, basePath);
    Stream<ParquetFileConfig> parquetFilesMetadata =
        parquetDataManagerExtractor.getConfigsFromStream(parquetFiles, hadoopConf);
    ParquetFileConfig file =
        parquetDataManagerExtractor.getParquetFileAt(parquetFilesMetadata, modificationTime);
    return createInternalTableFromFile(file);
  }

  private Stream<InternalDataFile> getInternalDataFiles(Stream<ParquetFileConfig> parquetFiles) {
    return parquetFiles.map(
        file ->
            InternalDataFile.builder()
                .physicalPath(file.getPath().toString())
                .fileFormat(FileFormat.APACHE_PARQUET)
                .fileSizeBytes(file.getSize())
                .partitionValues(
                    partitionValueExtractor.extractPartitionValues(
                        partitionSpecExtractor.spec(
                            partitionValueExtractor.extractSchemaForParquetPartitions(
                                parquetMetadataExtractor.readParquetMetadata(
                                    hadoopConf, file.getPath()),
                                file.getPath().toString())),
                        HudiPathUtils.getPartitionPath(new Path(basePath), file.getPath())))
                .lastModified(file.getModifTime())
                .columnStats(
                    parquetStatsExtractor.getColumnStatsForaFile(
                        parquetMetadataExtractor.readParquetMetadata(hadoopConf, file.getPath())))
                .build());
  }

  private InternalDataFile createInternalDataFileFromParquetFile(ParquetFileConfig parquetFile) {
    return InternalDataFile.builder()
        .physicalPath(parquetFile.getPath().toString())
        .partitionValues(
            partitionValueExtractor.extractPartitionValues(
                partitionSpecExtractor.spec(
                    partitionValueExtractor.extractSchemaForParquetPartitions(
                        parquetFile.getMetadata(), parquetFile.getPath().toString())),
                HudiPathUtils.getPartitionPath(new Path(basePath), parquetFile.getPath())))
        .lastModified(parquetFile.getModifTime())
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
    Set<InternalDataFile> addedInternalDataFiles = new HashSet<>();

    List<ParquetFileConfig> filesMetadata =
        parquetDataManagerExtractor.getParquetFilesMetadataAfterTime(
            hadoopConf,
            parquetDataManagerExtractor.getParquetFiles(hadoopConf, basePath),
            modificationTime);
    List<ParquetFileConfig> tableChangesAfterMetadata =
        parquetDataManagerExtractor.getParquetFilesMetadataAfterTime(
            hadoopConf,
            parquetDataManagerExtractor.getParquetFiles(hadoopConf, basePath),
            modificationTime);
    InternalTable internalTable = getMostRecentTable(tableChangesAfterMetadata.stream());
    for (ParquetFileConfig fileMetadata : filesMetadata) {
      InternalDataFile currentDataFile = createInternalDataFileFromParquetFile(fileMetadata);
      addedInternalDataFiles.add(currentDataFile);
    }

    return TableChange.builder()
        .sourceIdentifier(
            getCommitIdentifier(
                parquetDataManagerExtractor
                    .getMostRecentParquetFile(
                        parquetDataManagerExtractor.getConfigsFromStream(
                            parquetDataManagerExtractor.getParquetFiles(hadoopConf, basePath),
                            hadoopConf))
                    .getModifTime()))
        .tableAsOfChange(internalTable)
        .filesDiff(InternalFilesDiff.builder().filesAdded(addedInternalDataFiles).build())
        .build();
  }

  private InternalTable getMostRecentTable(Stream<ParquetFileConfig> parquetFiles) {
    ParquetFileConfig latestFile =
        parquetDataManagerExtractor.getMostRecentParquetFile(parquetFiles);
    return createInternalTableFromFile(latestFile);
  }

  @Override
  public InternalTable getCurrentTable() {
    Stream<LocatedFileStatus> parquetFiles =
        parquetDataManagerExtractor.getParquetFiles(hadoopConf, basePath);
    return getMostRecentTable(
        parquetDataManagerExtractor.getConfigsFromStream(parquetFiles, hadoopConf));
  }

  @Override
  public InternalSnapshot getCurrentSnapshot() {
    Stream<InternalDataFile> internalDataFiles =
        getInternalDataFiles(
            parquetDataManagerExtractor.getConfigsFromStream(
                parquetDataManagerExtractor.getParquetFiles(hadoopConf, basePath), hadoopConf));
    InternalTable table =
        getMostRecentTable(
            parquetDataManagerExtractor.getConfigsFromStream(
                parquetDataManagerExtractor.getParquetFiles(hadoopConf, basePath), hadoopConf));
    return InternalSnapshot.builder()
        .table(table)
        .sourceIdentifier(
            getCommitIdentifier(
                parquetDataManagerExtractor
                    .getMostRecentParquetFile(
                        parquetDataManagerExtractor.getConfigsFromStream(
                            parquetDataManagerExtractor.getParquetFiles(hadoopConf, basePath),
                            hadoopConf))
                    .getModifTime()))
        .partitionedDataFiles(PartitionFileGroup.fromFiles(internalDataFiles))
        .build();
  }

  @Override
  public boolean isIncrementalSyncSafeFrom(Instant timeInMillis) {
    Stream<ParquetFileConfig> parquetFilesMetadata =
        parquetDataManagerExtractor.getConfigsFromStream(
            parquetDataManagerExtractor.getParquetFiles(hadoopConf, basePath), hadoopConf);
    LongSummaryStatistics stats =
        parquetFilesMetadata.mapToLong(ParquetFileConfig::getModifTime).summaryStatistics();

    if (stats.getCount() == 0) {
      log.warn("No parquet files found in table {}. Incremental sync is not possible.", tableName);
      return false;
    }

    long earliestModTime = stats.getMin();
    long latestModTime = stats.getMax();

    if (timeInMillis.toEpochMilli() > latestModTime) {
      log.warn(
          "Instant {} is in the future relative to the data. Latest file time: {}",
          timeInMillis.toEpochMilli(),
          Instant.ofEpochMilli(latestModTime));
      return false;
    }

    if (earliestModTime > timeInMillis.toEpochMilli()) {
      log.warn(
          "Incremental sync is not safe. Earliest available metadata (time={}) is newer "
              + "than requested instant {}. Data history has been truncated.",
          Instant.ofEpochMilli(earliestModTime),
          timeInMillis.toEpochMilli());
      return false;
    }

    log.info(
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
