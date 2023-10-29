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
 
package io.onetable.iceberg;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import lombok.Builder;

import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterator;

import io.onetable.exception.NotSupportedException;
import io.onetable.model.OneTable;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.stat.ColumnStat;
import io.onetable.model.stat.Range;
import io.onetable.model.storage.FileFormat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.spi.extractor.DataFileIterator;

/** Extractor of data files for Iceberg */
@Builder
public class IcebergDataFileExtractor {

  @Builder.Default
  private IcebergPartitionValueConverter partitionValueConverter =
      IcebergPartitionValueConverter.getInstance();

  /**
   * Initializes an iterator for Iceberg files.
   *
   * @return Iceberg table file iterator
   */
  public DataFileIterator iterator(Table iceTable, OneTable oneTable) {
    return new IcebergDataFileIterator(iceTable, oneTable);
  }

  public class IcebergDataFileIterator implements DataFileIterator {
    private final Table iceTable;
    private final OneTable oneTable;
    private final CloseableIterator<CombinedScanTask> iceScan;
    private Iterator<OneDataFile> currentScanTaskIterator;

    private IcebergDataFileIterator(Table iceTable, OneTable oneTable) {
      this.iceTable = iceTable;
      this.oneTable = oneTable;
      this.iceScan = iceTable.newScan().planTasks().iterator();
      this.currentScanTaskIterator =
          iceScan.hasNext() ? getCurrentScanTaskIterator(iceScan.next()) : null;
    }

    @Override
    public void close() throws Exception {
      iceScan.close();
    }

    @Override
    public boolean hasNext() {
      advanceScanTask();
      return currentScanTaskIterator != null && currentScanTaskIterator.hasNext();
    }

    @Override
    public OneDataFile next() {
      if (currentScanTaskIterator == null) {
        throw new IllegalStateException("Iterator is not initialized");
      }
      advanceScanTask();
      return currentScanTaskIterator.next();
    }

    private void advanceScanTask() {
      if (currentScanTaskIterator != null && currentScanTaskIterator.hasNext()) {
        return;
      }
      if (iceScan.hasNext()) {
        currentScanTaskIterator = getCurrentScanTaskIterator(iceScan.next());
      } else {
        currentScanTaskIterator = null;
      }
    }

    private Iterator<OneDataFile> getCurrentScanTaskIterator(CombinedScanTask scanTask) {
      return scanTask.files().stream()
          .map(
              fileScanTask -> {
                DataFile dataFile = fileScanTask.file();
                Map<OnePartitionField, Range> onePartitionFieldRangeMap =
                    partitionValueConverter.toOneTable(
                        oneTable, dataFile.partition(), iceTable.spec());
                return fromIcebergWithoutColumnStats(dataFile, onePartitionFieldRangeMap);
              })
          .iterator();
    }
  }

  /**
   * Builds {@link OneDataFile} representation from Iceberg {@link DataFile} without any column
   * statistics set. This can be used to reduce memory overhead when statistics are not required.
   *
   * @param dataFile Iceberg data file
   * @param partitionsInfo representation of partition fields and ranges
   * @return corresponding OneTable data file
   */
  OneDataFile fromIcebergWithoutColumnStats(
      DataFile dataFile, Map<OnePartitionField, Range> partitionsInfo) {
    return fromIceberg(dataFile, partitionsInfo, null, false);
  }

  /**
   * Builds {@link OneDataFile} representation from Iceberg {@link DataFile}.
   *
   * @param dataFile Iceberg data file
   * @param partitionsInfo representation of partition fields and ranges
   * @param schema current schema for the table, used for mapping field IDs to stats
   * @return corresponding OneTable data file
   */
  OneDataFile fromIceberg(
      DataFile dataFile, Map<OnePartitionField, Range> partitionsInfo, OneSchema schema) {
    return fromIceberg(dataFile, partitionsInfo, schema, true);
  }

  private OneDataFile fromIceberg(
      DataFile dataFile,
      Map<OnePartitionField, Range> partitionsInfo,
      OneSchema schema,
      boolean includeColumnStats) {
    Map<OneField, ColumnStat> columnStatMap =
        includeColumnStats
            ? IcebergColumnStatsConverter.getInstance()
                .fromIceberg(
                    schema.getAllFields(),
                    dataFile.valueCounts(),
                    dataFile.nullValueCounts(),
                    dataFile.columnSizes(),
                    dataFile.lowerBounds(),
                    dataFile.upperBounds())
            : Collections.emptyMap();
    return OneDataFile.builder()
        .physicalPath(dataFile.path().toString())
        .fileFormat(fromIcebergFileFormat(dataFile.format()))
        .fileSizeBytes(dataFile.fileSizeInBytes())
        .recordCount(dataFile.recordCount())
        .partitionValues(partitionsInfo)
        .partitionPath(dataFile.partition().toString())
        .columnStats(columnStatMap)
        .build();
  }

  /**
   * Maps Iceberg file format to OneTable file format
   *
   * @param format Iceberg file format
   * @return corresponding OneTable file format
   */
  FileFormat fromIcebergFileFormat(org.apache.iceberg.FileFormat format) {
    switch (format) {
      case PARQUET:
        return FileFormat.APACHE_PARQUET;
      case ORC:
        return FileFormat.APACHE_ORC;
      case AVRO:
        return FileFormat.APACHE_AVRO;
      default:
        throw new NotSupportedException("Unsupported file format: " + format);
    }
  }
}
