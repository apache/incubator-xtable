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
 
package org.apache.xtable.delta;

// import scala.collection.Map;
import java.util.*;
import java.util.stream.Collectors;

import lombok.Builder;

import org.apache.hadoop.conf.Configuration;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.SnapshotImpl;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;

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
  public DataFileIterator iterator(Snapshot deltaSnapshot, InternalSchema schema) {
    return new DeltaDataFileIterator(deltaSnapshot, schema, true);
  }

  public class DeltaDataFileIterator implements DataFileIterator {
    private final FileFormat fileFormat;
    private final List<InternalField> fields;
    private final List<InternalPartitionField> partitionFields;
    private Iterator<InternalDataFile> dataFilesIterator = Collections.emptyIterator();

    private DeltaDataFileIterator(
        Snapshot snapshot, InternalSchema schema, boolean includeColumnStats) {
      String provider = ((SnapshotImpl) snapshot).getMetadata().getFormat().getProvider();
      this.fileFormat = actionsConverter.convertToFileFormat(provider);

      this.fields = schema.getFields();

      StructType fullSchema = snapshot.getSchema(); // The full table schema
      List<String> partitionColumns = snapshot.getPartitionColumnNames(); // List<String>

      List<StructField> partitionFields_strfld =
          fullSchema.fields().stream()
              .filter(field -> partitionColumns.contains(field.getName()))
              .collect(Collectors.toList());

      StructType partitionSchema = new StructType(partitionFields_strfld);

      this.partitionFields =
          partitionExtractor.convertFromDeltaPartitionFormat(schema, partitionSchema);
      Configuration hadoopConf = new Configuration();
      Engine engine = DefaultEngine.create(hadoopConf);

      Scan myScan = snapshot.getScanBuilder().build();
      CloseableIterator<FilteredColumnarBatch> scanFiles = myScan.getScanFiles(engine);
      this.dataFilesIterator =
          Collections
              .emptyIterator(); // Initialize the dataFilesIterator by iterating over the scan files
      while (scanFiles.hasNext()) {
        FilteredColumnarBatch scanFileColumnarBatch = scanFiles.next();
        CloseableIterator<Row> scanFileRows = scanFileColumnarBatch.getRows();
        while (scanFileRows.hasNext()) {
          Row scanFileRow = scanFileRows.next();

          // From the scan file row, extract the file path, size and modification time metadata
          // needed to read the file.
          FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
          Map<String, String> partitionValues =
              InternalScanFileUtils.getPartitionValues(scanFileRow);
          // Convert the FileStatus to InternalDataFile using the actionsConverter
          System.out.println("Calling the ActionToInternalDataFile");
          this.dataFilesIterator =
              Collections.singletonList(
                      actionsConverter.convertAddActionToInternalDataFile(
                          fileStatus,
                          snapshot,
                          fileFormat,
                          partitionFields,
                          fields,
                          includeColumnStats,
                          partitionExtractor,
                          fileStatsExtractor,
                          partitionValues))
                  .iterator();
        }
      }
    }

    @Override
    public void close() throws Exception {}

    @Override
    public boolean hasNext() {
      return this.dataFilesIterator.hasNext();
    }

    @Override
    public InternalDataFile next() {
      return dataFilesIterator.next();
    }
  }
}
