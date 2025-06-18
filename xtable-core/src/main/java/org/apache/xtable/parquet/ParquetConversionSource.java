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
import org.apache.xtable.model.stat.PartitionValue;
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
    // user config path of the parquet file (partitions)
    private final String configPath;
    @NonNull
    private final Configuration hadoopConf;

    /**
     * To infer schema getting the latest file assumption is that latest file will have new fields
     *
     * @param modificationTime the commit to consider for reading the table state
     * @return
     */
    @Override
    public InternalTable getTable(Long modificationTime) {

        List<LocatedFileStatus> parquetFiles = getParquetFiles(hadoopConf, basePath);
        // TODO last file in terms of modifcation time instead
        LocatedFileStatus latestFile = parquetFiles.get(parquetFiles.size() - 1);

        ParquetMetadata parquetMetadata =
                parquetMetadataExtractor.readParquetMetadata(hadoopConf, latestFile.getPath());

        List<InternalPartitionField> partitionFields = partitionValueExtractor.extractParquertPartitions(parquetMetadata, latestFile.getPath().toString());
        MessageType parquetSchema = parquetMetadataExtractor.getSchema(parquetMetadata);
        InternalSchema schema = schemaExtractor.toInternalSchema(parquetSchema, latestFile.getPath().toString());
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
        List<LocatedFileStatus> parquetFiles = getParquetFiles(hadoopConf, basePath);
        InternalTable table = getTable(-1L);
        internalDataFiles =
                parquetFiles.stream()
                        .map(
                                file ->
                                        InternalDataFile.builder()
                                                .physicalPath(file.getPath().toString())
                                                .fileFormat(FileFormat.APACHE_PARQUET)
                                                .fileSizeBytes(file.getLen())
                                                .partitionValues(partitionValueExtractor.extractPartitionValues(
                                                        partitionValueExtractor.extractParquertPartitions(parquetMetadataExtractor.readParquetMetadata(
                                                                hadoopConf, file.getPath()), file.getPath().toString()),
                                                        basePath))
                                                .lastModified(file.getModificationTime())
                                                .columnStats(
                                                        parquetStatsExtractor.getColumnStatsForaFile(
                                                                parquetMetadataExtractor.readParquetMetadata(
                                                                        hadoopConf, file.getPath())))
                                                .build())
                        .collect(Collectors.toList());
        return internalDataFiles;
    }

    // since we are considering files instead of tables in parquet
    @Override
    public CommitsBacklog<java.lang.Long> getCommitsBacklog(
            InstantsForIncrementalSync lastSyncInstant) {
        long epochMilli = lastSyncInstant.getLastSyncInstant().toEpochMilli();
        return null;
    }

    @Override
    public TableChange getTableChangeForCommit(java.lang.Long commit) {
        return null;
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
        List<InternalDataFile> internalDataFiles = getInternalDataFiles();
        InternalTable table = getTable(-1L);
        return InternalSnapshot.builder()
                .table(table)
                .partitionedDataFiles(PartitionFileGroup.fromFiles(internalDataFiles))
                .build();
    }


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
    public String getCommitIdentifier(Long aLong) {
        return null;
    }

    @Override
    public void close() {
    }
}
