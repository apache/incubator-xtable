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
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.NonNull;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import org.apache.xtable.exception.ReadException;
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

  private InternalTable createInternalTableFromFile(LocatedFileStatus latestFile) {
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
    Stream<LocatedFileStatus> parquetFiles = getParquetFiles(hadoopConf, basePath);
    LocatedFileStatus file = getParquetFileAt(parquetFiles, modificationTime);
    return createInternalTableFromFile(file);
  }

  private Stream<InternalDataFile> getInternalDataFiles(Stream<LocatedFileStatus> parquetFiles) {
    return parquetFiles.map(
        file ->
            InternalDataFile.builder()
                .physicalPath(file.getPath().toString())
                .fileFormat(FileFormat.APACHE_PARQUET)
                .fileSizeBytes(file.getLen())
                .partitionValues(
                    partitionValueExtractor.extractPartitionValues(
                        partitionSpecExtractor.spec(
                            partitionValueExtractor.extractSchemaForParquetPartitions(
                                parquetMetadataExtractor.readParquetMetadata(
                                    hadoopConf, file.getPath()),
                                file.getPath().toString())),
                        HudiPathUtils.getPartitionPath(new Path(basePath), file.getPath())))
                .lastModified(file.getModificationTime())
                .columnStats(
                    parquetStatsExtractor.getStatsForFile(
                        parquetMetadataExtractor.readParquetMetadata(hadoopConf, file.getPath())))
                .build());
  }

  private InternalDataFile createInternalDataFileFromParquetFile(FileStatus parquetFile) {
    return InternalDataFile.builder()
        .physicalPath(parquetFile.getPath().toString())
        .partitionValues(
            partitionValueExtractor.extractPartitionValues(
                partitionSpecExtractor.spec(
                    partitionValueExtractor.extractSchemaForParquetPartitions(
                        parquetMetadataExtractor.readParquetMetadata(
                            hadoopConf, parquetFile.getPath()),
                        parquetFile.getPath().toString())),
                basePath))
        .lastModified(parquetFile.getModificationTime())
        .fileSizeBytes(parquetFile.getLen())
        .columnStats(
            parquetStatsExtractor.getStatsForFile(
                parquetMetadataExtractor.readParquetMetadata(hadoopConf, parquetFile.getPath())))
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

  private InternalTable getMostRecentTable(Stream<LocatedFileStatus> parquetFiles) {
    LocatedFileStatus latestFile = getMostRecentParquetFile(parquetFiles);
    return createInternalTableFromFile(latestFile);
  }

  @Override
  public InternalTable getCurrentTable() {
    Stream<LocatedFileStatus> parquetFiles = getParquetFiles(hadoopConf, basePath);
    return getMostRecentTable(parquetFiles);
  }

  /**
   * get current snapshot
   *
   * @return
   */
  @Override
  public InternalSnapshot getCurrentSnapshot() {
    // to avoid consume the stream call the method twice to return the same stream of parquet files
    Stream<InternalDataFile> internalDataFiles =
        getInternalDataFiles(getParquetFiles(hadoopConf, basePath));
    InternalTable table = getMostRecentTable(getParquetFiles(hadoopConf, basePath));
    return InternalSnapshot.builder()
        .table(table)
        .sourceIdentifier(
            getCommitIdentifier(
                getMostRecentParquetFile(getParquetFiles(hadoopConf, basePath))
                    .getModificationTime()))
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
        .orElseThrow(() -> new IllegalStateException("No file found at " + modificationTime));
  }

  private Stream<LocatedFileStatus> getParquetFiles(Configuration hadoopConf, String basePath) {
    try {
      FileSystem fs = FileSystem.get(hadoopConf);
      URI uriBasePath = new URI(basePath);
      String parentPath = Paths.get(uriBasePath).toString();
      RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(parentPath), true);
      return RemoteIterators.toList(iterator).stream()
          .filter(file -> file.getPath().getName().endsWith("parquet"));
    } catch (IOException | URISyntaxException e) {
      throw new ReadException("Unable to read files from file system", e);
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
