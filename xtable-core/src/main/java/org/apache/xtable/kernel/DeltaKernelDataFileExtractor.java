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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.Builder;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.ScanImpl;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;

import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.FileFormat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.spi.extractor.DataFileIterator;

/** DeltaDataFileExtractor lets the consumer iterate over partitions. */
@Builder
public class DeltaKernelDataFileExtractor {

  @Builder.Default
  private final DeltaKernelPartitionExtractor partitionExtractor =
      DeltaKernelPartitionExtractor.getInstance();

  @Builder.Default
  private final DeltaKernelStatsExtractor fileStatsExtractor =
      DeltaKernelStatsExtractor.getInstance();

  @Builder.Default
  private final DeltaKernelActionsConverter actionsConverter =
      DeltaKernelActionsConverter.getInstance();

  private final String basePath;

  /**
   * Initializes an iterator for Delta Lake files.
   *
   * @return Delta table file iterator
   */
  public DataFileIterator iterator(
      Snapshot deltaSnapshot, Table table, Engine engine, InternalSchema schema) {
    return new DeltaDataFileIterator(deltaSnapshot, table, engine, schema, true);
  }

  public class DeltaDataFileIterator implements DataFileIterator {
    private final CloseableIterator<FilteredColumnarBatch> scanFiles;
    private final FileFormat fileFormat;
    private final Table table;
    private final List<InternalField> fields;
    private final List<InternalPartitionField> partitionFields;
    private final boolean includeColumnStats;

    private CloseableIterator<Row> currentFileRows;
    private InternalDataFile nextFile;

    private DeltaDataFileIterator(
        Snapshot snapshot,
        Table table,
        Engine engine,
        InternalSchema schema,
        boolean includeColumnStats) {
      this.includeColumnStats = includeColumnStats;
      this.table = table;
      this.fields = schema.getFields();
      String provider = ((SnapshotImpl) snapshot).getMetadata().getFormat().getProvider();
      this.fileFormat = actionsConverter.convertToFileFormat(provider);

      StructType fullSchema = snapshot.getSchema(); // The full table schema
      List<String> partitionColumns = snapshot.getPartitionColumnNames();

      List<StructField> partitionFieldsStr =
          fullSchema.fields().stream()
              .filter(field -> partitionColumns.contains(field.getName()))
              .collect(Collectors.toList());

      StructType partitionSchema = new StructType(partitionFieldsStr);

      this.partitionFields =
          partitionExtractor.convertFromDeltaPartitionFormat(schema, partitionSchema);

      ScanImpl myScan = (ScanImpl) snapshot.getScanBuilder().build();
      this.scanFiles = myScan.getScanFiles(engine, includeColumnStats);

      // Initialize first element
      this.nextFile = computeNext();
    }

    @Override
    public void close() throws Exception {
      try {
        if (currentFileRows != null) {
          currentFileRows.close();
        }
      } finally {
        scanFiles.close();
      }
    }

    @Override
    public boolean hasNext() {
      return nextFile != null;
    }

    @Override
    public InternalDataFile next() {
      InternalDataFile current = nextFile;
      nextFile = computeNext();
      return current;
    }

    private InternalDataFile computeNext() {
      try {
        while (true) {
          // If we have a current file with rows, process the next row
          if (currentFileRows != null && currentFileRows.hasNext()) {
            Row scanFileRow = currentFileRows.next();
            AddFile addFile =
                new AddFile(scanFileRow.getStruct(scanFileRow.getSchema().indexOf("add")));
            Map<String, String> partitionValues =
                InternalScanFileUtils.getPartitionValues(scanFileRow);

            return actionsConverter.convertAddActionToInternalDataFile(
                addFile,
                table,
                fileFormat,
                partitionFields,
                fields,
                includeColumnStats,
                partitionExtractor,
                fileStatsExtractor,
                partitionValues);
          }

          // Close current file rows if any
          if (currentFileRows != null) {
            currentFileRows.close();
            currentFileRows = null;
          }

          // Get next batch of files if available
          if (!scanFiles.hasNext()) {
            return null; // No more files to process
          }

          // Get next batch of files
          FilteredColumnarBatch scanFileColumnarBatch = scanFiles.next();
          currentFileRows = scanFileColumnarBatch.getRows();
        }
      } catch (Exception e) {
        // Close resources in case of error
        try {
          close();
        } catch (Exception closeEx) {
          e.addSuppressed(closeEx);
        }
        throw new RuntimeException("Error while computing next data file", e);
      }
    }
  }
}
