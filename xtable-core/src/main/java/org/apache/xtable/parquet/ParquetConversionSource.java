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
import org.apache.parquet.Schema;
import org.apache.parquet.SchemaBuilder;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import org.apache.xtable.model.*;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.*;
import org.apache.xtable.spi.extractor.ConversionSource;

@Builder
public class ParquetConversionSource implements ConversionSource<Long> {

  private final String tableName;
  private final String basePath;
  @NonNull private final Configuration hadoopConf;

  @Builder.Default
  private static final ParquetSchemaExtractor schemaExtractor =
      ParquetSchemaExtractor.getInstance();

  @Builder.Default
  private static final ParquetMetadataExtractor parquetMetadataExtractor =
      ParquetMetadataExtractor.getInstance();

  @Builder.Default
  private static final ParquetPartitionExtractor parquetPartitionExtractor =
      ParquetPartitionExtractor.getInstance();

  @Builder.Default
  private static final ParquetStatsExtractor parquetStatsExtractor =
      ParquetStatsExtractor.getInstance();

  private Map<String, List<String>> initPartitionInfo() {
    return getPartitionFromDirectoryStructure(hadoopConf, basePath, Collections.emptyMap());
  }

  /**
   * To infer schema getting the latest file assumption is that latest file will have new fields
   *
   * @param modificationTime the commit to consider for reading the table state
   * @return
   */
  @Override
  public InternalTable getTable(Long modificationTime) {

    Optional<LocatedFileStatus> latestFile =
        getParquetFiles(hadoopConf, basePath)
            .max(Comparator.comparing(FileStatus::getModificationTime));

    ParquetMetadata parquetMetadata =
        parquetMetadataExtractor.readParquetMetadata(hadoopConf, latestFile.get().getPath());
    Schema tableSchema =
        new org.apache.parquet.parquet.ParquetSchemaConverter()
            .convert(parquetMetadataExtractor.getSchema(parquetMetadata));

    Set<String> partitionKeys = initPartitionInfo().keySet();

    // merge schema of partition into original as partition is not part of parquet fie
    if (!partitionKeys.isEmpty()) {
      tableSchema = mergeParquetSchema(tableSchema, partitionKeys);
    }
    InternalSchema schema = schemaExtractor.toInternalSchema(tableSchema);

    List<InternalPartitionField> partitionFields =
        partitionKeys.isEmpty()
            ? Collections.emptyList()
            : parquetPartitionExtractor.getInternalPartitionField(partitionKeys, schema);
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

  /**
   * Here to get current snapshot listing all files hence the -1 is being passed
   *
   * @return
   */
  @Override
  public InternalSnapshot getCurrentSnapshot() {

    List<LocatedFileStatus> latestFile =
        getParquetFiles(hadoopConf, basePath).collect(Collectors.toList());
    Map<String, List<String>> partitionInfo = initPartitionInfo();
    InternalTable table = getTable(-1L);
    List<InternalDataFile> internalDataFiles =
        latestFile.stream()
            .map(
                file ->
                    InternalDataFile.builder()
                        .physicalPath(file.getPath().toString())
                        .fileFormat(FileFormat.APACHE_PARQUET)
                        .fileSizeBytes(file.getLen())
                        .partitionValues(
                            parquetPartitionExtractor.getPartitionValue(
                                basePath,
                                file.getPath().toString(),
                                table.getReadSchema(),
                                partitionInfo))
                        .lastModified(file.getModificationTime())
                        .columnStats(
                            parquetStatsExtractor
                                .getColumnStatsForaFile(
                                    parquetMetadataExtractor.readParquetMetadata(
                                        hadoopConf, file.getPath().toString()))
                                .build())
                        .collect(Collectors.toList()));

    return InternalSnapshot.builder()
        .table(table)
        .partitionedDataFiles(PartitionFileGroup.fromFiles(internalDataFiles))
        .build();
  }

  /**
   * Whenever new file is added , condition to get new file is listing files whose modification time
   * is greater than previous ysnc
   *
   * @param modificationTime commit to capture table changes for.
   * @return
   */
  @Override
  public TableChange getTableChangeForCommit(Long modificationTime) {
    List<FileStatus> tableChanges =
        getParquetFiles(hadoopConf, basePath)
            .filter(fileStatus -> fileStatus.getModificationTime() > modificationTime)
            .collect(Collectors.toList());
    // TODO avoid doing full list of directory to get schema , just argument of modification time
    // needs to be tweaked
    InternalTable internalTable = getTable(-1L);
    Set<InternalDataFile> internalDataFiles = new HashSet<>();
    Map<String, List<String>> partitionInfo = initPartitionInfo();
    for (FileStatus tableStatus : tableChanges) {
      internalDataFiles.add(
          InternalDataFile.builder()
              .physicalPath(tableStatus.getPath().toString())
              .partitionValues(
                  parquetPartitionExtractor.getPartitionValue(
                      basePath,
                      tableStatus.getPath().toString(),
                      internalTable.getReadSchema(),
                      partitionInfo))
              .lastModified(tableStatus.getModificationTime())
              .fileSizeBytes(tableStatus.getLen())
              .columnStats(
                  parquetMetadataExtractor.getColumnStatsForaFile(
                      hadoopConf, tableStatus, internalTable))
              .build());
    }

    return TableChange.builder()
        .tableAsOfChange(internalTable)
        .filesDiff(DataFilesDiff.builder().filesAdded(internalDataFiles).build())
        .build();
  }

  @Override
  public CommitsBacklog<Long> getCommitsBacklog(
      InstantsForIncrementalSync instantsForIncrementalSync) {

    List<Long> commitsToProcess =
        Collections.singletonList(instantsForIncrementalSync.getLastSyncInstant().toEpochMilli());

    return CommitsBacklog.<Long>builder().commitsToProcess(commitsToProcess).build();
  }

  // TODO  Need to understnad how this needs to be implemented should _SUCCESS or .staging dir needs
  // to be checked
  @Override
  public boolean isIncrementalSyncSafeFrom(Instant instant) {
    return true;
  }

  @Override
  public void close() throws IOException {}

  private Schema mergeParquetSchema(Schema internalSchema, Set<String> parititonFields) {

    SchemaBuilder.FieldAssembler<Schema> fieldAssembler =
        SchemaBuilder.record(internalSchema.getName()).fields();
    for (Schema.Field field : internalSchema.getFields()) {
      fieldAssembler = fieldAssembler.name(field.name()).type(field.schema()).noDefault();
    }

    for (String paritionKey : parititonFields) {
      fieldAssembler = fieldAssembler.name(paritionKey).type().stringType().noDefault();
    }

    return fieldAssembler.endRecord();
  }

  public Stream<LocatedFileStatus> getParquetFiles(Configuration hadoopConf, String basePath) {
    try {
      FileSystem fs = FileSystem.get(hadoopConf);
      RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path(basePath), true);
      return remoteIteratorToStream(iterator)
          .filter(file -> file.getPath().getName().endsWith("parquet"));
    } catch (IOException | FileNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public Map<String, List<String>> getPartitionFromDirectoryStructure(
      Configuration hadoopConf, String basePath, Map<String, List<String>> partitionMap) {

    try {
      FileSystem fs = FileSystem.get(hadoopConf);
      FileStatus[] baseFileStatus = fs.listStatus(new Path(basePath));
      Map<String, List<String>> currentPartitionMap = new HashMap<>(partitionMap);

      for (FileStatus dirStatus : baseFileStatus) {
        if (dirStatus.isDirectory()) {
          String partitionPath = dirStatus.getPath().getName();
          if (partitionPath.contains("=")) {
            String[] partitionKeyValue = partitionPath.split("=");
            currentPartitionMap
                .computeIfAbsent(partitionKeyValue[0], k -> new ArrayList<>())
                .add(partitionKeyValue[1]);
            getPartitionFromDirectoryStructure(
                hadoopConf, dirStatus.getPath().toString(), partitionMap);
          }
        }
      }
      return currentPartitionMap;

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
