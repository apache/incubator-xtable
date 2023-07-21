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
import java.util.List;
import java.util.stream.Collectors;

import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterator;

import io.onetable.exception.NotSupportedException;
import io.onetable.model.storage.FileFormat;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.spi.extractor.PartitionedDataFileIterator;

/** Extractor of data files for Iceberg */
public class IcebergDataFileExtractor implements PartitionedDataFileIterator {
  private final CloseableIterator<CombinedScanTask> iceScan;
  private final IcebergPartitionValueConverter partitionValueConverter;
  private final PartitionSpec partitionSpec;

  public IcebergDataFileExtractor(
      Table iceTable, IcebergPartitionValueConverter partitionValueConverter) {
    this.partitionSpec = iceTable.spec();
    this.iceScan = iceTable.newScan().planTasks().iterator();
    this.partitionValueConverter = partitionValueConverter;
  }

  @Override
  public void close() throws Exception {
    iceScan.close();
  }

  @Override
  public boolean hasNext() {
    return iceScan.hasNext();
  }

  @Override
  public OneDataFiles next() {
    CombinedScanTask combinedScan = iceScan.next();
    List<OneDataFile> files =
        combinedScan.files().stream()
            .map(
                fileScanTask -> {
                  DataFile dataFile = fileScanTask.file();
                  return OneDataFile.builder()
                      .physicalPath(dataFile.path().toString())
                      .fileFormat(getFileFormat(dataFile.format()))
                      .fileSizeBytes(dataFile.fileSizeInBytes())
                      .recordCount(dataFile.recordCount())
                      .partitionValues(
                          partitionValueConverter.toOneTable(dataFile.partition(), partitionSpec))
                      .columnStats(Collections.emptyMap())
                      .build();
                })
            .collect(Collectors.toList());
    return OneDataFiles.collectionBuilder().files(files).build();
  }

  private FileFormat getFileFormat(org.apache.iceberg.FileFormat format) {
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
