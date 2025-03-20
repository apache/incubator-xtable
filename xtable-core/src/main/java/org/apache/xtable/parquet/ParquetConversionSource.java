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
import java.io.FileNotFoundException;
import org.apache.xtable.model.CommitsBacklog;
import lombok.Builder;
import lombok.NonNull;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.*;
import org.apache.xtable.model.config.InputPartitionField;
import org.apache.xtable.model.config.InputPartitionFields;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.*;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.hadoop.util.functional.RemoteIterators;
import org.apache.xtable.spi.extractor.ConversionSource;
import org.apache.parquet.format.FileMetaData;

@Builder
// @NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ParquetConversionSource  implements ConversionSource<Long> {
    @Builder.Default
    private static final ParquetSchemaExtractor schemaExtractor =
            ParquetSchemaExtractor.getInstance();
    /*private static final ParquetConversionSource INSTANCE = new ParquetConversionSource();
    public static ParquetConversionSource getInstance() {
        return INSTANCE;
    }*/
  /*    private static final ParquetSchemaConverter parquetSchemaConverter =
  ParquetSchemaConverter.getInstance();*/
    @Builder.Default
    private static final ParquetMetadataExtractor parquetMetadataExtractor =
            ParquetMetadataExtractor.getInstance();
    @Builder.Default
    private static final ParquetPartitionValueExtractor partitionValueExtractor =
            ParquetPartitionValueExtractor.getInstance();
    @Builder.Default
    private static final ParquetStatsExtractor parquetStatsExtractor =
            ParquetStatsExtractor.getInstance();
    private final InputPartitionFields partitions;
    private final String tableName;
    private final String basePath;
    // user config path of the parquet file (partitions)
    private final String configPath;
    @NonNull
    private final Configuration hadoopConf;

    private InputPartitionFields initPartitionInfo() {
        // return parquetPartitionExtractor.getPartitionsFromUserConfiguration(configPath);
        return partitions;
    }


    /**
     * To infer schema getting the latest file assumption is that latest file will have new fields
     *
     * @param modificationTime the commit to consider for reading the table state
     * @return
     */
    @Override
    public InternalTable getTable(Long modificationTime) {

        List<LocatedFileStatus> parquetFiles =
                getParquetFiles(hadoopConf, basePath);
        // TODO last file in terms of modifcation time instead
        LocatedFileStatus latestFile = parquetFiles.get(parquetFiles.size()-1);

                        //.max(Comparator.comparing(FileStatus::getModificationTime));

        ParquetMetadata parquetMetadata =
                parquetMetadataExtractor.readParquetMetadata(hadoopConf, latestFile.getPath());
        MessageType tableSchema = parquetMetadataExtractor.getSchema(parquetMetadata);

        List<String> partitionKeys =
                initPartitionInfo().getPartitions().stream()
                        .map(InputPartitionField::getPartitionFieldName)
                        .collect(Collectors.toList());

        // merge schema of partition into original as partition is not part of parquet file
        if (!partitionKeys.isEmpty()) {
            // TODO compilation error
            //   tableSchema = mergeParquetSchema(tableSchema, partitionKeys);
        }
        InternalSchema schema = schemaExtractor.toInternalSchema(tableSchema, null, null);

        List<InternalPartitionField> partitionFields =
                partitionKeys.isEmpty()
                        ? Collections.emptyList()
                        : partitionValueExtractor.getInternalPartitionFields(partitions);
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

    public List<InternalDataFile> getInternalDataFiles() {
        List<InternalDataFile> internalDataFiles = null;
        List<LocatedFileStatus> parquetFiles =
                getParquetFiles(hadoopConf, basePath);
        List<PartitionValue> partitionValuesFromConfig = partitionValueExtractor.createPartitionValues(partitionValueExtractor.extractPartitionValues(partitions));
        InternalTable table = getTable(-1L);
        internalDataFiles =
                parquetFiles.stream()
                        .map(
                                file ->
                                        InternalDataFile.builder()
                                                .physicalPath(file.getPath().toString())
                                                .fileFormat(FileFormat.APACHE_PARQUET)
                                                .fileSizeBytes(file.getLen())
                                                .partitionValues(partitionValuesFromConfig)
                                                .lastModified(file.getModificationTime())
                                                .columnStats(
                                                        parquetStatsExtractor
                                                                .getColumnStatsForaFile(
                                                                        parquetMetadataExtractor.readParquetMetadata(
                                                                                hadoopConf, file.getPath())))
                                                .build())
                                                .collect(Collectors.toList());
        return internalDataFiles;
    }

      // since we are considering files instead of tables in parquet
      @Override
      public CommitsBacklog<java.lang.Long> getCommitsBacklog(InstantsForIncrementalSync lastSyncInstant){
          long epochMilli = lastSyncInstant.getLastSyncInstant().toEpochMilli();
          return null;
      }
    @Override
    public TableChange getTableChangeForCommit(java.lang.Long commit){
        return null;
    }
     @Override
     public InternalTable getCurrentTable(){
        return null;
     };

    /**
     * Here to get current snapshot listing all files hence the -1 is being passed
     *
     * @return
     */
    @Override
    public InternalSnapshot getCurrentSnapshot() {
    List<InternalDataFile> internalDataFiles = getInternalDataFiles();
    InternalTable table = getTable(-1L);
    return InternalSnapshot.builder()
            .table(table)
            .partitionedDataFiles(PartitionFileGroup.fromFiles(internalDataFiles))
            .build();
    }

  /* private Schema mergeAvroSchema(Schema internalSchema, Set<String> parititonFields) {

      SchemaBuilder.FieldAssembler<Schema> fieldAssembler =
              SchemaBuilder.record(internalSchema.getName()).fields();
      for (Schema.Field field : internalSchema.getFields()) {
          fieldAssembler = fieldAssembler.name(field.name()).type(field.schema()).noDefault();
      }

      for (String paritionKey : parititonFields) {
          fieldAssembler = fieldAssembler.name(paritionKey).type().stringType().noDefault();
      }

      return fieldAssembler.endRecord();
  }*/

  /* private Type mergeParquetSchema(MessageType internalSchema, List<String> parititonFields) {

    List<Type> listOfAllFields = internalSchema.getFields();
    Type fieldsToMerge = listOfAllFields.get(0);
    listOfAllFields.remove(0);
    // start the merge
    for (Type field : internalSchema.getFields()) {
        fieldsToMerge = fieldsToMerge.union(field,false);
    }
  */
  /*  for (String partition : parititonFields) {
      //create Type from partiton, TODO: check further...
      fieldsToMerge = fieldsToMerge.union(new Type(partition, Repetition.REQUIRED),false);
  }*/
  /*

      return fieldsToMerge;
  }*/
    // was returning Stream<LocatedFileStatus>
    public List<LocatedFileStatus> getParquetFiles(Configuration hadoopConf, String basePath) {
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
    public void close() {

    }
}
