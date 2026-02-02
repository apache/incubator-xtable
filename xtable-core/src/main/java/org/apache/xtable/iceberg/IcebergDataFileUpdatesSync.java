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
 
package org.apache.xtable.iceberg;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.io.CloseableIterable;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.FilesDiff;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFile;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.parquet.ParquetMetadataExtractor;
import org.apache.xtable.parquet.ParquetStatsExtractor;

@Log4j2
@AllArgsConstructor(staticName = "of")
public class IcebergDataFileUpdatesSync {
  private final IcebergColumnStatsConverter columnStatsConverter;
  private final IcebergPartitionValueConverter partitionValueConverter;
  private final Configuration hadoopConf;

  public void applySnapshot(
      Table table,
      InternalTable internalTable,
      Transaction transaction,
      List<PartitionFileGroup> partitionedDataFiles,
      Schema schema,
      PartitionSpec partitionSpec,
      TableSyncMetadata metadata) {

    Map<String, DataFile> previousFiles = new HashMap<>();
    
    // Optimize: Check if table has a snapshot before scanning
    // For empty tables, this avoids expensive manifest file reads from cloud storage (GCS, S3, etc.)
    Snapshot currentSnapshot = table.currentSnapshot();
    if (currentSnapshot != null) {
      try (CloseableIterable<FileScanTask> iterator = table.newScan().planFiles()) {
        StreamSupport.stream(iterator.spliterator(), false)
            .map(FileScanTask::file)
            .forEach(file -> previousFiles.put(file.path().toString(), file));
      } catch (Exception e) {
        throw new ReadException("Failed to iterate through Iceberg data files", e);
      }
    } else {
      log.debug("Table has no snapshot, skipping file scan (table is empty)");
    }

    FilesDiff<InternalFile, DataFile> diff =
        InternalFilesDiff.findNewAndRemovedFiles(partitionedDataFiles, previousFiles);

    applyDiff(
        transaction, diff.getFilesAdded(), diff.getFilesRemoved(), schema, partitionSpec, metadata);
  }

  public void applyDiff(
      Table table,
      Transaction transaction,
      InternalFilesDiff internalFilesDiff,
      Schema schema,
      PartitionSpec partitionSpec,
      TableSyncMetadata metadata) {

    // Get existing files in Iceberg to filter out duplicates
    // This handles cases where source (e.g., Delta with blind append OPTIMIZE)
    // sends AddFile actions for files that are already synced
    // Note: Must scan the base table (not transaction.table()) as transaction tables don't support
    // scans
    Map<String, DataFile> existingFiles = new HashMap<>();
    
    // Optimize: Check if table has a snapshot before scanning
    // For empty tables, this avoids expensive manifest file reads from cloud storage (GCS, S3, etc.)
    Snapshot currentSnapshot = table.currentSnapshot();
    if (currentSnapshot != null) {
      try (CloseableIterable<FileScanTask> iterator = table.newScan().planFiles()) {
        StreamSupport.stream(iterator.spliterator(), false)
            .map(FileScanTask::file)
            .forEach(file -> existingFiles.put(file.path().toString(), file));
      } catch (Exception e) {
        throw new ReadException("Failed to read existing Iceberg files during incremental sync", e);
      }
    } else {
      log.debug("Table has no snapshot, skipping file scan (table is empty)");
    }

    // Filter out files that already exist in Iceberg
    Collection<? extends InternalFile> filesToAdd =
        internalFilesDiff.dataFilesAdded().stream()
            .filter(InternalDataFile.class::isInstance)
            .map(file -> (InternalDataFile) file)
            .filter(file -> !existingFiles.containsKey(file.getPhysicalPath()))
            .collect(Collectors.toList());

    Collection<DataFile> filesRemoved =
        internalFilesDiff.dataFilesRemoved().stream()
            .map(file -> getDataFile(partitionSpec, schema, file))
            .collect(Collectors.toList());

    applyDiff(transaction, filesToAdd, filesRemoved, schema, partitionSpec, metadata);
  }

  private void applyDiff(
      Transaction transaction,
      Collection<? extends InternalFile> filesAdded,
      Collection<DataFile> filesRemoved,
      Schema schema,
      PartitionSpec partitionSpec,
      TableSyncMetadata metadata) {
    OverwriteFiles overwriteFiles = transaction.newOverwrite();
    filesAdded.stream()
        .filter(InternalDataFile.class::isInstance)
        .map(file -> (InternalDataFile) file)
        .forEach(f -> overwriteFiles.addFile(getDataFile(partitionSpec, schema, f)));
    filesRemoved.forEach(overwriteFiles::deleteFile);
    overwriteFiles.set(TableSyncMetadata.XTABLE_METADATA, metadata.toJson());
    overwriteFiles.commit();
  }

  private DataFile getDataFile(
      PartitionSpec partitionSpec, Schema schema, InternalDataFile dataFile) {
    // Convert Iceberg schema to InternalSchema for stats extraction
    InternalSchema internalSchema = IcebergSchemaExtractor.getInstance().fromIceberg(schema);
    
    // Get existing stats and check if they are complete
    List<ColumnStat> existingStats = dataFile.getColumnStats();
    long recordCount = dataFile.getRecordCount();
    List<ColumnStat> columnStats;
    
    // For Parquet files, ALWAYS read from footer to match native Iceberg behavior
    // Native Iceberg always reads Parquet footer during insertion for accuracy and completeness.
    // This ensures:
    // 1. Statistics are always accurate (source of truth from Parquet file)
    // 2. All columns present in the file have statistics
    // 3. Statistics match what's actually in the file (not potentially stale existing stats)
    // 4. Query performance is optimal (complete and accurate stats enable better predicate pushdown)
    if (dataFile.getFileFormat() == org.apache.xtable.model.storage.FileFormat.APACHE_PARQUET) {
      log.debug(
          "Reading stats from Parquet footer for file: {} to ensure accuracy and completeness (matching native Iceberg behavior).",
          dataFile.getPhysicalPath());
      try {
        StatsFromParquet statsFromParquet =
            readStatsFromParquetFooter(dataFile, internalSchema);
        
        // Use Parquet footer stats as primary source (most accurate)
        // Merge with existing stats only for columns not in Parquet footer (rare edge case)
        List<ColumnStat> parquetStats = statsFromParquet.getColumnStats();
        columnStats = mergeStats(existingStats, parquetStats, internalSchema);
        recordCount = statsFromParquet.getRecordCount();
        
        log.debug(
            "Successfully extracted {} column stats from Parquet footer for file: {}. Stats will be stored in manifest files for optimal query performance.",
            columnStats.size(),
            dataFile.getPhysicalPath());
      } catch (Exception e) {
        log.warn(
            "Failed to read stats from Parquet footer for file: {}. Using existing stats as fallback. Error: {}",
            dataFile.getPhysicalPath(),
            e.getMessage());
        // Fallback to existing stats if Parquet footer read fails
        // This should be rare - Parquet footer is the source of truth
        columnStats = existingStats != null ? existingStats : Collections.emptyList();
      }
    } else {
      // For non-Parquet files, use existing stats
      log.debug(
          "Using existing stats for non-Parquet file: {} (format: {})",
          dataFile.getPhysicalPath(),
          dataFile.getFileFormat());
      columnStats = existingStats != null ? existingStats : Collections.emptyList();
    }

    // Get accurate file size from filesystem if available, matching Iceberg's behavior
    // Iceberg always reads actual file size during data insertion for accuracy
    long fileSizeBytes = dataFile.getFileSizeBytes();
    if (fileSizeBytes <= 0) {
      // If file size is missing or invalid, read from filesystem
      try {
        Path filePath = new Path(dataFile.getPhysicalPath());
        FileSystem fs = filePath.getFileSystem(hadoopConf);
        if (fs.exists(filePath)) {
          fileSizeBytes = fs.getFileStatus(filePath).getLen();
          log.debug(
              "Read file size {} from filesystem for file: {}",
              fileSizeBytes,
              dataFile.getPhysicalPath());
        }
      } catch (Exception e) {
        log.warn(
            "Failed to read file size from filesystem for file: {}. Using provided size: {}. Error: {}",
            dataFile.getPhysicalPath(),
            fileSizeBytes,
            e.getMessage());
      }
    }

    // Build DataFile matching Iceberg's native insertion behavior:
    // - Path: File location
    // - File size: Accurate size from filesystem
    // - Metrics: Complete column statistics (min/max, null counts, value counts, sizes)
    // - Format: File format (Parquet, ORC, Avro)
    // - Partition: Partition values if table is partitioned
    // - Content: DATA (default for data files, not delete files)
    // Note: Sequence numbers and sort order are handled by Iceberg automatically
    DataFiles.Builder builder =
        DataFiles.builder(partitionSpec)
            .withPath(dataFile.getPhysicalPath())
            .withFileSizeInBytes(fileSizeBytes)
            .withMetrics(
                columnStatsConverter.toIceberg(schema, recordCount, columnStats))
            .withFormat(convertFileFormat(dataFile.getFileFormat()));
    if (partitionSpec.isPartitioned()) {
      builder.withPartition(
          partitionValueConverter.toIceberg(partitionSpec, schema, dataFile.getPartitionValues()));
    }
    return builder.build();
  }


  /**
   * Reads column statistics from Parquet file footer for all columns in the schema. This method
   * ensures that manifest files always have complete statistics, similar to how Iceberg handles
   * stats during data insertion.
   *
   * <p>This operation reads the Parquet file footer which contains:
   * <ul>
   *   <li>Column chunk metadata with min/max values for each column
   *   <li>Row group metadata with record counts
   *   <li>Schema information
   * </ul>
   *
   * <p>Performance: Reading Parquet footers is efficient as it only reads metadata (typically a few
   * KB), not the entire file. However, for cloud storage, there is network latency per file.
   *
   * @param dataFile the data file to read stats from
   * @param schema the table schema to extract stats for all columns
   * @return StatsFromParquet containing column stats and record count
   * @throws ReadException if reading the Parquet file fails
   */
  private StatsFromParquet readStatsFromParquetFooter(
      InternalDataFile dataFile, InternalSchema schema) {
    try {
      Path parquetPath = new Path(dataFile.getPhysicalPath());
      org.apache.parquet.hadoop.metadata.ParquetMetadata footer =
          ParquetMetadataExtractor.readParquetMetadata(hadoopConf, parquetPath);

      // Extract stats for all columns from Parquet footer
      List<ColumnStat> parquetStats = ParquetStatsExtractor.getColumnStatsForaFile(footer);

      // Extract record count from Parquet row groups metadata
      // This is always reliable and doesn't depend on column statistics
      long numRecords =
          footer.getBlocks().stream().mapToLong(block -> block.getRowCount()).sum();

      // Group stats by column path - a Parquet file can have multiple row groups,
      // each with its own stats. We need to aggregate them like Iceberg does during insertion.
      Map<String, List<ColumnStat>> pathToStatsList =
          parquetStats.stream()
              .collect(Collectors.groupingBy(stat -> stat.getField().getPath()));

      // Aggregate stats across all row groups for each column, matching Iceberg's behavior:
      // - Min value = min of all row group mins
      // - Max value = max of all row group maxes
      // - Null count = sum of all row group null counts
      // - Value count = sum of all row group value counts
      // - Total size = sum of all row group sizes
      Map<String, ColumnStat> pathToAggregatedStat = new HashMap<>();
      pathToStatsList.forEach(
          (path, statsList) -> {
            if (statsList.isEmpty()) {
              return;
            }
            // Use the first stat as base (all stats for same column should have same field)
            ColumnStat firstStat = statsList.get(0);
            InternalField field = firstStat.getField();

            // Aggregate across row groups
            long totalValues = statsList.stream().mapToLong(ColumnStat::getNumValues).sum();
            long totalNulls = statsList.stream().mapToLong(ColumnStat::getNumNulls).sum();
            long totalSize = statsList.stream().mapToLong(ColumnStat::getTotalSize).sum();

            // Aggregate min/max across row groups
            Object minValue = null;
            Object maxValue = null;
            for (ColumnStat stat : statsList) {
              Range range = stat.getRange();
              if (range != null && range.getMinValue() != null && range.getMaxValue() != null) {
                if (minValue == null || compareValues(range.getMinValue(), minValue) < 0) {
                  minValue = range.getMinValue();
                }
                if (maxValue == null || compareValues(range.getMaxValue(), maxValue) > 0) {
                  maxValue = range.getMaxValue();
                }
              }
            }

            if (minValue != null && maxValue != null) {
              pathToAggregatedStat.put(
                  path,
                  ColumnStat.builder()
                      .field(field)
                      .numValues(totalValues)
                      .numNulls(totalNulls)
                      .totalSize(totalSize)
                      .range(Range.vector(minValue, maxValue))
                      .build());
            }
          });

      // Build stats list for all schema fields
      // Include all columns that have stats (even without min/max bounds)
      // Native Iceberg includes columns in Metrics even if they don't have min/max bounds
      // This is important for query performance - null counts and value counts are still useful
      List<ColumnStat> mappedStats =
          schema.getAllFields().stream()
              .filter(field -> pathToAggregatedStat.containsKey(field.getPath()))
              .map(
                  field -> {
                    ColumnStat aggregatedStat = pathToAggregatedStat.get(field.getPath());
                    // Rebuild ColumnStat with correct schema field reference
                    // while preserving aggregated Parquet statistics values
                    // Include stats even if min/max are null (native Iceberg behavior)
                    return ColumnStat.builder()
                        .field(field)
                        .numValues(aggregatedStat.getNumValues())
                        .numNulls(aggregatedStat.getNumNulls())
                        .totalSize(aggregatedStat.getTotalSize())
                        .range(aggregatedStat.getRange())
                        .build();
                  })
              // Include all stats - min/max bounds are optional in Iceberg Metrics
              // Columns with counts but no bounds are still valuable for query planning
              .collect(Collectors.toList());

      return new StatsFromParquet(mappedStats, numRecords);
    } catch (Exception e) {
      throw new ReadException(
          "Failed to read stats from Parquet footer for file: " + dataFile.getPhysicalPath(), e);
    }
  }

  /**
   * Checks if existing stats are complete for all columns in the schema.
   * Stats are considered complete if:
   * - All columns in the schema have stats
   * - Each stat has a valid range with min/max values
   * - Each stat has non-negative counts (numValues, numNulls)
   *
   * @param existingStats the existing column statistics
   * @param schema the table schema
   * @return true if stats are complete for all columns, false otherwise
   */
  private static boolean areStatsComplete(
      List<ColumnStat> existingStats, InternalSchema schema) {
    if (existingStats == null || existingStats.isEmpty()) {
      return false;
    }

    // Build a map of existing stats by field path for quick lookup
    Map<String, ColumnStat> statsByPath =
        existingStats.stream()
            .filter(stat -> stat != null && stat.getField() != null)
            .collect(
                Collectors.toMap(
                    stat -> stat.getField().getPath(), Function.identity(), (s1, s2) -> s1));

    // Check if all schema fields have complete stats
    for (InternalField field : schema.getAllFields()) {
      ColumnStat stat = statsByPath.get(field.getPath());
      if (stat == null) {
        // Missing stat for this column
        return false;
      }

      // Check if stat has valid range with min/max
      Range range = stat.getRange();
      if (range == null
          || range.getMinValue() == null
          || range.getMaxValue() == null) {
        // Incomplete stat (missing min/max)
        return false;
      }

      // Check if counts are valid (non-negative)
      if (stat.getNumValues() < 0 || stat.getNumNulls() < 0) {
        // Invalid counts
        return false;
      }
    }

    return true;
  }

  /**
   * Merges existing stats with Parquet footer stats to ensure completeness.
   * Parquet footer stats take precedence (most accurate), but existing stats are preserved
   * for columns that might not be in the Parquet footer.
   *
   * @param existingStats existing column statistics (may be null or incomplete)
   * @param parquetStats statistics from Parquet footer
   * @param schema the table schema
   * @return merged list of complete column statistics
   */
  private static List<ColumnStat> mergeStats(
      List<ColumnStat> existingStats,
      List<ColumnStat> parquetStats,
      InternalSchema schema) {
    // Build maps for efficient lookup
    Map<String, ColumnStat> existingByPath = new HashMap<>();
    if (existingStats != null) {
      existingStats.stream()
          .filter(stat -> stat != null && stat.getField() != null)
          .forEach(stat -> existingByPath.put(stat.getField().getPath(), stat));
    }

    Map<String, ColumnStat> parquetByPath = new HashMap<>();
    if (parquetStats != null) {
      parquetStats.stream()
          .filter(stat -> stat != null && stat.getField() != null)
          .forEach(stat -> parquetByPath.put(stat.getField().getPath(), stat));
    }

    // Merge: Parquet stats take precedence, but use existing stats for columns not in Parquet
    Map<String, ColumnStat> mergedByPath = new HashMap<>(existingByPath);
    mergedByPath.putAll(parquetByPath); // Parquet stats override existing stats

    // Build final list with all schema fields, preferring Parquet stats
    // Include all columns with stats - min/max bounds are optional in Iceberg Metrics
    // Native Iceberg includes columns even without min/max bounds (they just have null bounds)
    // This ensures maximum query performance - null counts and value counts are still useful
    return schema.getAllFields().stream()
        .filter(field -> mergedByPath.containsKey(field.getPath()))
        .map(
            field -> {
              ColumnStat stat = mergedByPath.get(field.getPath());
              // Rebuild ColumnStat with correct schema field reference
              // Include stats even if min/max are null (native Iceberg behavior)
              return ColumnStat.builder()
                  .field(field)
                  .numValues(stat.getNumValues())
                  .numNulls(stat.getNumNulls())
                  .totalSize(stat.getTotalSize())
                  .range(stat.getRange())
                  .build();
            })
        // Include all stats - don't filter out columns without min/max bounds
        // Iceberg Metrics allows null bounds, and counts are still valuable for query planning
        .collect(Collectors.toList());
  }

  /**
   * Compares two values for min/max aggregation. Handles Comparable types and nulls.
   *
   * @param v1 first value
   * @param v2 second value
   * @return negative if v1 < v2, positive if v1 > v2, 0 if equal
   */
  @SuppressWarnings("unchecked")
  private static int compareValues(Object v1, Object v2) {
    if (v1 == null && v2 == null) {
      return 0;
    }
    if (v1 == null) {
      return -1;
    }
    if (v2 == null) {
      return 1;
    }
    if (v1 instanceof Comparable && v2 instanceof Comparable) {
      try {
        return ((Comparable<Object>) v1).compareTo(v2);
      } catch (ClassCastException e) {
        // If types are incompatible, compare by string representation
        return v1.toString().compareTo(v2.toString());
      }
    }
    // Fallback to string comparison
    return v1.toString().compareTo(v2.toString());
  }

  /** Container for stats extracted from Parquet footer. */
  private static class StatsFromParquet {
    private final List<ColumnStat> columnStats;
    private final long recordCount;

    StatsFromParquet(List<ColumnStat> columnStats, long recordCount) {
      this.columnStats = columnStats;
      this.recordCount = recordCount;
    }

    List<ColumnStat> getColumnStats() {
      return columnStats;
    }

    long getRecordCount() {
      return recordCount;
    }
  }

  private static FileFormat convertFileFormat(
      org.apache.xtable.model.storage.FileFormat fileFormat) {
    switch (fileFormat) {
      case APACHE_PARQUET:
        return FileFormat.PARQUET;
      case APACHE_ORC:
        return FileFormat.ORC;
      case APACHE_AVRO:
        return FileFormat.AVRO;
      default:
        throw new NotSupportedException(
            "Conversion to Iceberg with file format: " + fileFormat.name() + " is not supported");
    }
  }
}
