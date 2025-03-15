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
import org.apache.parquet.Type;
import org.apache.parquet.SchemaBuilder;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.avro.Schema;
import org.apache.xtable.model.*;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.*;
import org.apache.xtable.spi.extractor.ConversionSource;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.Type.Repetition;

@Builder
public class ParquetConversionSource implements ConversionSource<Long> {

    @Builder.Default
    private static final ParquetSchemaExtractor schemaExtractor =
            ParquetSchemaExtractor.getInstance();
/*    private static final ParquetSchemaConverter parquetSchemaConverter =
            ParquetSchemaConverter.getInstance();*/
    @Builder.Default
    private static final ParquetMetadataExtractor parquetMetadataExtractor =
            ParquetMetadataExtractor.getInstance();
    @Builder.Default
    private static final ParquetPartitionExtractor parquetPartitionExtractor =
            ParquetPartitionExtractor.getInstance();

    @Builder.Default
    private static final ParquetPartitionValueExtractor parquetPartitionValueExtractor =
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

    private InputPartitionFields initPartitionInfo() {
        return parquetPartitionExtractor.getPartitionsFromUserConfiguration(configPath);
    }

    public Map<String, List<String>> getPartitionFromConfiguration() {
        List<InputPartitionField> partitionFields = initPartitionInfo().getPartitions();
        Map<String, List<String>> partitionsMap = new HashMap<>();
        for (InputPartitionField partition : partitionFields) {
            partitionsMap
                    .computeIfAbsent(partition.getPartitionFieldName(), k -> new ArrayList<>())
                    .addAll(partition.partitionFieldValues());
        }
        return partitionsMap;
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
        //Schema tableSchema =
        //        new org.apache.parquet.avro.AvroSchemaConverter().convert(parquetMetadataExtractor.getSchema(parquetMetadata));
//        Type tableSchema =
//                parquetSchemaConverter.convert(parquetMetadataExtractor.getSchema(parquetMetadata));
        MessageType tableSchema = parquetMetadataExtractor.getSchema(parquetMetadata);

        List<String> partitionKeys = initPartitionInfo().getPartitions().stream()
                .map(InputPartitionField::getPartitionFieldName)
                .collect(Collectors.toList());

        // merge schema of partition into original as partition is not part of parquet fie
        if (!partitionKeys.isEmpty()) {
            tableSchema = mergeParquetSchema(tableSchema, partitionKeys);
            //tableSchema = mergeAvroSchema(tableSchema, partitionKeys);
        }
        InternalSchema schema = schemaExtractor.toInternalSchema(tableSchema);

        List<InternalPartitionField> partitionFields =
                partitionKeys.isEmpty()
                        ? Collections.emptyList()
                        : parquetPartitionExtractor.getPartitionsFromUserConfiguration(configPath);
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

    public List<InternalDataFile> getInternalDataFiles() {
        List<LocatedFileStatus> parquetFiles =
                getParquetFiles(hadoopConf, basePath).collect(Collectors.toList());
        List<PartitionValue> partitionValuesFromConfig = parquetPartitionValueExtractor.createPartitionValues(parquetPartitionValueExtractor.extractPartitionValues(initPartitionInfo())
                InternalTable table = getTable(-1L);
        List<InternalDataFile> internalDataFiles =
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
                                                                                hadoopConf, file.getPath().toString()))
                                                                .build())
                                                .collect(Collectors.toList()));
        return internalDataFiles;
    }

    /**
     * Here to get current snapshot listing all files hence the -1 is being passed
     *
     * @return
     */
    @Override
    public InternalSnapshot getCurrentSnapshot() {
        List<InternalDataFiles> internalDataFiles = getInternalDataFiles();
        return InternalSnapshot.builder()
                .table(table)
                .partitionedDataFiles(PartitionFileGroup.fromFiles(internalDataFiles))
                .build();
    }

    private Schema mergeAvroSchema(Schema internalSchema, Set<String> parititonFields) {

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

    private Type mergeParquetSchema(MessageType internalSchema, List<String> parititonFields) {

        List<Type> listOfAllFields = internalSchema.getFields();
        Type fieldsToMerge = listOfAllFields.get(0);
        listOfAllFields.remove(0);
        // start the merge
        for (Type field : internalSchema.getFields()) {
            fieldsToMerge = fieldsToMerge.union(field);
        }
        for (String parition : parititonFields) {
            //create Type from partiton, TODO: check further...
            fieldsToMerge = fieldsToMerge.union(Type(partition, Repetition.REQUIRED))
        }

        return fieldsToMerge;
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
