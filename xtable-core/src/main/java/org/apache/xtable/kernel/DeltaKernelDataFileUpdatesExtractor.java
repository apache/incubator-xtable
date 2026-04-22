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
 
package org.apache.xtable.kernel;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import lombok.Builder;

import scala.collection.JavaConverters;
import scala.collection.Seq;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.MapValue;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.actions.RowBackedAction;
import io.delta.kernel.internal.util.VectorUtils;
import io.delta.kernel.statistics.DataFileStatistics;
import io.delta.kernel.types.StructType;

import org.apache.xtable.collectors.CustomCollectors;
import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.Range;
import org.apache.xtable.model.storage.FilesDiff;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFile;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.paths.PathUtils;
import org.apache.xtable.spi.extractor.DataFileIterator;

@Builder
public class DeltaKernelDataFileUpdatesExtractor {
  @Builder.Default
  private final DeltaKernelStatsExtractor deltaStatsExtractor =
      DeltaKernelStatsExtractor.getInstance();

  @Builder.Default
  private final DeltaKernelPartitionExtractor deltaKernelPartitionExtractor =
      DeltaKernelPartitionExtractor.getInstance();

  @Builder.Default
  private final DeltaKernelDataFileExtractor dataFileExtractor =
      DeltaKernelDataFileExtractor.builder().build();

  private final Engine engine;
  private final String basePath;
  private final boolean includeColumnStats;

  /**
   * Applies snapshot changes, loading the snapshot fresh.
   *
   * @param table the Delta table
   * @param partitionedDataFiles the new data files to sync
   * @param tableSchema the internal schema
   * @return sequence of Delta actions (AddFile/RemoveFile)
   */
  public Seq<RowBackedAction> applySnapshot(
      Table table, List<PartitionFileGroup> partitionedDataFiles, InternalSchema tableSchema) {
    return applySnapshot(table, partitionedDataFiles, tableSchema, null);
  }

  /**
   * Applies snapshot changes with an optional cached snapshot to avoid redundant loads.
   *
   * @param table the Delta table
   * @param partitionedDataFiles the new data files to sync
   * @param tableSchema the internal schema
   * @param cachedSnapshot optional pre-loaded snapshot (null to load fresh)
   * @return sequence of Delta actions (AddFile/RemoveFile)
   */
  public Seq<RowBackedAction> applySnapshot(
      Table table,
      List<PartitionFileGroup> partitionedDataFiles,
      InternalSchema tableSchema,
      Snapshot cachedSnapshot) {

    // all files in the current delta snapshot are potential candidates for remove actions, i.e. if
    // the file is not present in the new snapshot (addedFiles) then the file is considered removed
    Map<String, RowBackedAction> previousFiles = new HashMap<>();
    StructType physicalSchema;

    // Use cached snapshot if provided, otherwise load it
    boolean tableExists = cachedSnapshot != null || checkTableExists(table);

    if (tableExists) {
      Snapshot snapshot = cachedSnapshot != null ? cachedSnapshot : table.getLatestSnapshot(engine);

      // Reuse DeltaKernelDataFileExtractor to iterate through existing files
      // This avoids duplicating the scan logic for reading Delta files
      try (DataFileIterator fileIterator =
          dataFileExtractor.iterator(snapshot, table, engine, tableSchema)) {

        while (fileIterator.hasNext()) {
          InternalDataFile internalFile = fileIterator.next();

          // Convert InternalDataFile back to AddFile to create RemoveFile action
          AddFile addFile =
              createAddFileAction(internalFile, tableSchema, basePath, snapshot.getSchema());
          RemoveFile removeFile =
              new RemoveFile(addFile.toRemoveFileRow(false, Optional.of(snapshot.getVersion())));
          // Use optimized path construction with Engine (reuses existing engine, no new
          // Configuration)
          String fullPath =
              DeltaKernelActionsConverter.getFullPathToFile(removeFile.getPath(), engine, table);
          previousFiles.put(fullPath, removeFile);
        }
      } catch (Exception e) {
        throw new ReadException("Failed to scan existing Delta files", e);
      }

      physicalSchema = snapshot.getSchema();

    } else {

      // Table doesn't exist yet - no previous files to remove
      // Convert InternalSchema to StructType for physical schema
      DeltaKernelSchemaExtractor schemaExtractor = DeltaKernelSchemaExtractor.getInstance();
      physicalSchema = schemaExtractor.fromInternalSchema(tableSchema);
    }

    FilesDiff<InternalFile, RowBackedAction> diff =
        InternalFilesDiff.findNewAndRemovedFiles(partitionedDataFiles, previousFiles);

    return applyDiff(
        diff.getFilesAdded(), diff.getFilesRemoved(), tableSchema, basePath, physicalSchema);
  }

  private boolean checkTableExists(Table table) {
    return DeltaKernelUtils.tableExists(engine, table.getPath(engine));
  }

  public Seq<RowBackedAction> applyDiff(
      InternalFilesDiff internalFilesDiff,
      InternalSchema tableSchema,
      String tableBasePath,
      StructType physicalSchema) {
    List<RowBackedAction> removeActions =
        internalFilesDiff.dataFilesRemoved().stream()
            .map(dFile -> createAddFileAction(dFile, tableSchema, tableBasePath, physicalSchema))
            .map(addFile -> new RemoveFile(addFile.toRemoveFileRow(false, Optional.empty())))
            .collect(CustomCollectors.toList(internalFilesDiff.dataFilesRemoved().size()));
    return applyDiff(
        internalFilesDiff.dataFilesAdded(),
        removeActions,
        tableSchema,
        tableBasePath,
        physicalSchema);
  }

  private Seq<RowBackedAction> applyDiff(
      Set<? extends InternalFile> filesAdded,
      Collection<RowBackedAction> removeFileActions,
      InternalSchema tableSchema,
      String tableBasePath,
      StructType physicalSchema) {
    Stream<RowBackedAction> addActions =
        filesAdded.stream()
            .filter(InternalDataFile.class::isInstance)
            .map(file -> (InternalDataFile) file)
            .map(dFile -> createAddFileAction(dFile, tableSchema, tableBasePath, physicalSchema));
    int totalActions = filesAdded.size() + removeFileActions.size();
    List<RowBackedAction> allActions =
        Stream.concat(addActions, removeFileActions.stream())
            .collect(CustomCollectors.toList(totalActions));
    return JavaConverters.asScalaBuffer(allActions).toSeq();
  }

  private AddFile createAddFileAction(
      InternalDataFile dataFile,
      InternalSchema tableSchema,
      String tableBasePath,
      StructType physicalSchema) {
    // Convert partition values from Map<String, String> to MapValue
    Map<String, String> partitionValuesMap =
        deltaKernelPartitionExtractor.partitionValueSerialization(dataFile);
    MapValue partitionValues = convertToMapValue(partitionValuesMap);

    // Generate column stats if enabled
    Optional<DataFileStatistics> stats = Optional.empty();
    if (includeColumnStats) {
      stats =
          Optional.of(
              convertToDataFileStatistics(dataFile.getRecordCount(), dataFile.getColumnStats()));
    }

    Row addFileRow =
        AddFile.createAddFileRow(
            physicalSchema,
            // Delta Lake supports relative and absolute paths in theory but relative paths seem
            // more commonly supported by query engines in our testing
            PathUtils.getRelativePath(dataFile.getPhysicalPath(), tableBasePath),
            partitionValues,
            dataFile.getFileSizeBytes(),
            dataFile.getLastModified(),
            true, // dataChange
            Optional.empty(), // deletionVector
            Optional.empty(), // tags
            Optional.empty(), // baseRowId
            Optional.empty(), // defaultRowCommitVersion
            stats // stats - converted from InternalDataFile column stats
            );

    // Wrap the Row back into an AddFile object so we can use its methods
    return new AddFile(addFileRow);
  }

  /**
   * Converts XTable's internal column statistics to Delta Kernel's DataFileStatistics format.
   *
   * @param recordCount the number of records in the file
   * @param columnStats the list of column statistics from InternalDataFile
   * @return DataFileStatistics object for Delta Kernel
   */
  private DataFileStatistics convertToDataFileStatistics(
      long recordCount, List<ColumnStat> columnStats) {
    if (columnStats == null || columnStats.isEmpty()) {
      return new DataFileStatistics(recordCount, new HashMap<>(), new HashMap<>(), new HashMap<>());
    }

    Map<Column, Literal> minValues = new HashMap<>();
    Map<Column, Literal> maxValues = new HashMap<>();
    Map<Column, Long> nullCounts = new HashMap<>();

    for (ColumnStat columnStat : columnStats) {
      InternalField field = columnStat.getField();
      InternalType dataType = field.getSchema().getDataType();

      // Only process supported types for statistics
      if (!isSupportedStatsType(dataType)) {
        continue;
      }

      // Create Column reference from field path
      Column column = new Column(field.getPathParts());

      // Extract min/max from range
      Range range = columnStat.getRange();
      if (range != null) {
        Object minValue = range.getMinValue();
        Object maxValue = range.getMaxValue();

        if (minValue != null) {
          Literal minLiteral = convertToLiteral(minValue, field.getSchema());
          if (minLiteral != null) {
            minValues.put(column, minLiteral);
          }
        }

        if (maxValue != null) {
          Literal maxLiteral = convertToLiteral(maxValue, field.getSchema());
          if (maxLiteral != null) {
            maxValues.put(column, maxLiteral);
          }
        }
      }

      // Add null count
      nullCounts.put(column, columnStat.getNumNulls());
    }

    return new DataFileStatistics(recordCount, minValues, maxValues, nullCounts);
  }

  /** Checks if a data type is supported for statistics. */
  private boolean isSupportedStatsType(InternalType type) {
    return type == InternalType.BOOLEAN
        || type == InternalType.DATE
        || type == InternalType.DECIMAL
        || type == InternalType.DOUBLE
        || type == InternalType.INT
        || type == InternalType.LONG
        || type == InternalType.FLOAT
        || type == InternalType.STRING
        || type == InternalType.TIMESTAMP
        || type == InternalType.TIMESTAMP_NTZ;
  }

  /** Converts an XTable value to a Delta Kernel Literal based on the field schema. */
  private Literal convertToLiteral(Object value, InternalSchema fieldSchema) {
    InternalType dataType = fieldSchema.getDataType();

    switch (dataType) {
      case BOOLEAN:
        return Literal.ofBoolean((Boolean) value);
      case INT:
        return Literal.ofInt(((Number) value).intValue());
      case LONG:
        return Literal.ofLong(((Number) value).longValue());
      case FLOAT:
        return Literal.ofFloat(((Number) value).floatValue());
      case DOUBLE:
        return Literal.ofDouble(((Number) value).doubleValue());
      case STRING:
        return Literal.ofString((String) value);
      case DATE:
        // XTable stores dates as days since epoch (int)
        return Literal.ofDate(((Number) value).intValue());
      case TIMESTAMP:
        // XTable stores timestamps as microseconds since epoch
        return Literal.ofTimestamp(((Number) value).longValue());
      case TIMESTAMP_NTZ:
        // XTable stores timestamp_ntz as microseconds since epoch
        return Literal.ofTimestampNtz(((Number) value).longValue());
      case DECIMAL:
        // Extract precision and scale from schema metadata
        Map<InternalSchema.MetadataKey, Object> metadata = fieldSchema.getMetadata();
        int precision = 10; // default precision
        int scale = 0; // default scale
        if (metadata != null) {
          Object precisionObj = metadata.get(InternalSchema.MetadataKey.DECIMAL_PRECISION);
          Object scaleObj = metadata.get(InternalSchema.MetadataKey.DECIMAL_SCALE);
          if (precisionObj instanceof Number) {
            precision = ((Number) precisionObj).intValue();
          }
          if (scaleObj instanceof Number) {
            scale = ((Number) scaleObj).intValue();
          }
        }
        return Literal.ofDecimal((java.math.BigDecimal) value, precision, scale);
      default:
        // Unsupported type for stats
        return null;
    }
  }

  private MapValue convertToMapValue(Map<String, String> map) {
    return VectorUtils.stringStringMapValue(map);
  }
}
