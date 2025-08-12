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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Value;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;

@Value
@Builder
public class ParquetStatsExtractor {

  private static final ParquetStatsExtractor INSTANCE = new ParquetStatsExtractor();

  private static final ParquetSchemaExtractor schemaExtractor =
      ParquetSchemaExtractor.getInstance();

  private static final ParquetMetadataExtractor parquetMetadataExtractor =
      ParquetMetadataExtractor.getInstance();

  // private static final InputPartitionFields partitions = null;

  public static ParquetStatsExtractor getInstance() {
    return INSTANCE;
  }

  private static final ParquetPartitionValueExtractor partitionValueExtractor =
      ParquetPartitionValueExtractor.getInstance();

  public static List<ColumnStat> getColumnStatsForaFile(ParquetMetadata footer) {
    return getStatsForFile(footer).values().stream()
        .flatMap(List::stream)
        .collect(Collectors.toList());
  }

  private static Optional<Long> getMaxFromColumnStats(List<ColumnStat> columnStats) {
    return columnStats.stream()
        .filter(entry -> entry.getField().getParentPath() == null)
        .map(ColumnStat::getNumValues)
        .filter(numValues -> numValues > 0)
        .max(Long::compareTo);
  }

  public static Map<ColumnDescriptor, List<ColumnStat>> getStatsForFile(ParquetMetadata footer) {
    Map<ColumnDescriptor, List<ColumnStat>> columnDescStats = new HashMap<>();
    MessageType schema = parquetMetadataExtractor.getSchema(footer);
    List<ColumnChunkMetaData> columns = new ArrayList<>();
    columns =
        footer.getBlocks().stream()
            .flatMap(blockMetaData -> blockMetaData.getColumns().stream())
            .collect(Collectors.toList());
    columnDescStats =
        columns.stream()
            .collect(
                Collectors.groupingBy(
                    columnMetaData ->
                        schema.getColumnDescription(columnMetaData.getPath().toArray()),
                    Collectors.mapping(
                        columnMetaData ->
                            ColumnStat.builder()
                                .field(
                                    InternalField.builder()
                                        .name(columnMetaData.getPrimitiveType().getName())
                                        .fieldId(
                                            columnMetaData.getPrimitiveType().getId() == null
                                                ? null
                                                : columnMetaData
                                                    .getPrimitiveType()
                                                    .getId()
                                                    .intValue())
                                        .parentPath(null)
                                        .schema(
                                            schemaExtractor.toInternalSchema(
                                                columnMetaData.getPrimitiveType(),
                                                columnMetaData.getPath().toDotString()))
                                        .build())
                                .numValues(columnMetaData.getValueCount())
                                .totalSize(columnMetaData.getTotalSize())
                                .range(
                                    Range.vector(
                                        columnMetaData.getStatistics().genericGetMin(),
                                        columnMetaData.getStatistics().genericGetMax()))
                                .build(),
                        Collectors.toList())));
    return columnDescStats;
  }

  public static InternalDataFile toInternalDataFile(Configuration hadoopConf, Path parentPath)
      throws IOException {
    FileStatus file = null;
    List<PartitionValue> partitionValues = null;
    ParquetMetadata footer = null;
    List<ColumnStat> columnStatsForAFile = null;
    try {
      FileSystem fs = FileSystem.get(hadoopConf);
      file = fs.getFileStatus(parentPath);
      footer = parquetMetadataExtractor.readParquetMetadata(hadoopConf, parentPath);
      MessageType schema = parquetMetadataExtractor.getSchema(footer);
      columnStatsForAFile = getColumnStatsForaFile(footer);
      partitionValues =
          partitionValueExtractor.extractPartitionValues(
              partitionValueExtractor.extractParquetPartitions(
                  parquetMetadataExtractor.readParquetMetadata(hadoopConf, file.getPath()),
                  file.getPath().toString()),
              parentPath.toString());
    } catch (java.io.IOException e) {

    }
    return InternalDataFile.builder()
        .physicalPath(parentPath.toString())
        .fileFormat(FileFormat.APACHE_PARQUET)
        .partitionValues(partitionValues)
        .fileSizeBytes(file.getLen())
        .recordCount(getMaxFromColumnStats(columnStatsForAFile).orElse(0L))
        .columnStats(columnStatsForAFile)
        .lastModified(file.getModificationTime())
        .build();
  }
}
