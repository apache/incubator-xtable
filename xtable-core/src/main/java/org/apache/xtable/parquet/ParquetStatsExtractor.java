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
import java.util.stream.Collectors;

import org.apache.hadoop.fs.*;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.xtable.model.storage.InternalDataFile;

import java.util.Map;
import java.util.Collection;
import java.util.Set;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

@Value
@Builder
public class ParquetStatsExtractor {
  @Builder.Default
  private static final ParquetMetadataExtractor parquetMetadataExtractor =
      ParquetMetadataExtractor.getInstance();

  @Builder.Default
  private static final ParquetPartitionExtractor partitionExtractor =
      ParquetPartitionExtractor.getInstance();

  private static Map<ColumnDescriptor, ColStats> stats =
      new LinkedHashMap<ColumnDescriptor, ColStats>();
  private static long recordCount = 0;

  private Map<String, List<String>> initPartitionInfo() {
    return getPartitionFromDirectoryStructure(hadoopConf, basePath, Collections.emptyMap());
  }

  private InternalDataFile toInternalDataFile(
      Configuration hadoopConf, String parentPath, Map<ColumnDescriptor, ColStats> stats) {
    FileSystem fs = FileSystem.get(hadoopConf);
    FileStatus file = fs.getFileStatus(new Path(parentPath));
    Map<String, List<String>> partitionInfo = initPartitionInfo();

    ParquetMetadata footer = parquetMetadataExtractor.readParquetMetadata(hadoopConf, parentPath);
    MessageType schema = parquetMetadataExtractor.getSchema(footer);
    InternalSchema schema = schemaExtractor.toInternalSchema(schema);
    List<PartitionValue> partitionValues =
        partitionExtractor.getPartitionValue(
            parentPath, file.getPath().toString(), schema, partitionInfo);
    return InternalDataFile.builder()
        .physicalPath(parentPath)
        .fileFormat(FileFormat.APACHE_PARQUET)
        .partitionValues(partitionValues)
        .fileSizeBytes(file.getLen())
        .recordCount(recordCount)
        .columnStats(stats.values().stream().collect(Collectors.toList()))
        .lastModified(file.getModificationTime())
        .build();
  }

  private static void getColumnStatsForaFile(ParquetMetadata footer) {
    for (BlockMetaData blockMetaData : footer.getBlocks()) {

      MessageType schema = parquetMetadataExtractor.getSchema(footer);
      recordCount += blockMetaData.getRowCount();
      List<ColumnChunkMetaData> columns = blockMetaData.getColumns();
      for (ColumnChunkMetaData columnMetaData : columns) {
        ColumnDescriptor desc = schema.getColumnDescription(columnMetaData.getPath().toArray());
        ColStats.add(
            desc,
            columnMetaData.getValueCount(),
            columnMetaData.getTotalSize(),
            columnMetaData.getTotalUncompressedSize(),
            columnMetaData.getEncodings(),
            columnMetaData.getStatistics());
      }
    }
  }

  private static class Stats {
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    long total = 0;

    public void add(long length) {
      min = Math.min(length, min);
      max = Math.max(length, max);
      total += length;
    }
  }

  private static class ColStats {

    Stats valueCountStats = new Stats();
    Stats allStats = new Stats();
    Stats uncStats = new Stats();
    Set<Encoding> encodings = new TreeSet<Encoding>();
    Statistics colValuesStats = null;
    int blocks = 0;

    public void add(
        long valueCount,
        long size,
        long uncSize,
        Collection<Encoding> encodings,
        Statistics colValuesStats) {
      ++blocks;
      valueCountStats.add(valueCount);
      allStats.add(size);
      uncStats.add(uncSize);
      this.encodings.addAll(encodings);
      this.colValuesStats = colValuesStats;
    }

    private static void add(
        ColumnDescriptor desc,
        long valueCount,
        long size,
        long uncSize,
        Collection<Encoding> encodings,
        Statistics colValuesStats) {
      ColStats colStats = stats.get(desc);
      if (colStats == null) {
        colStats = new ColStats();
        stats.put(desc, colStats);
      }
      colStats.add(valueCount, size, uncSize, encodings, colValuesStats);
    }
  }
}
