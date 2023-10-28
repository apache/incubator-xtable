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

import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;

import io.onetable.exception.NotSupportedException;
import io.onetable.exception.OneIOException;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.model.storage.PartitionedDataFiles;
import io.onetable.spi.extractor.DataFileIterator;

@AllArgsConstructor(staticName = "of")
public class IcebergDataFileUpdatesSync {
  private final IcebergColumnStatsConverter columnStatsConverter;
  private final IcebergPartitionValueConverter partitionValueConverter;

  public void applySnapshot(
      Table table,
      Transaction transaction,
      PartitionedDataFiles partitionedDataFiles,
      Schema schema,
      PartitionSpec partitionSpec) {
    List<OneDataFile> currentDataFiles = new ArrayList<>();
    IcebergDataFileExtractor dataFileExtractor =
        IcebergDataFileExtractor.builder().partitionValueConverter(partitionValueConverter).build();
    try (DataFileIterator fileIterator = dataFileExtractor.iterator(table)) {
      fileIterator.forEachRemaining(currentDataFiles::add);
    } catch (Exception e) {
      throw new OneIOException("Failed to iterate through Iceberg data files", e);
    }

    // Sync the files diff
    OneDataFilesDiff filesDiff =
        OneDataFilesDiff.from(currentDataFiles, partitionedDataFiles.getAllFiles());
    applyDiff(transaction, filesDiff, schema, partitionSpec);
  }

  public void applyDiff(
      Transaction transaction,
      OneDataFilesDiff oneDataFilesDiff,
      Schema schema,
      PartitionSpec partitionSpec) {
    OverwriteFiles overwriteFiles = transaction.newOverwrite();
    oneDataFilesDiff
        .getFilesAdded()
        .forEach(f -> overwriteFiles.addFile(getDataFile(partitionSpec, schema, f)));
    oneDataFilesDiff
        .getFilesRemoved()
        .forEach(
            f ->
                overwriteFiles.deleteFile(
                    getDataFile(transaction.table().spec(), transaction.table().schema(), f)));
    overwriteFiles.commit();
  }

  private DataFile getDataFile(PartitionSpec partitionSpec, Schema schema, OneDataFile dataFile) {
    DataFiles.Builder builder =
        DataFiles.builder(partitionSpec)
            .withPath(dataFile.getPhysicalPath())
            .withFileSizeInBytes(dataFile.getFileSizeBytes())
            .withMetrics(
                columnStatsConverter.toIceberg(
                    schema, dataFile.getRecordCount(), dataFile.getColumnStats()))
            .withFormat(convertFileFormat(dataFile.getFileFormat()));
    if (partitionSpec.isPartitioned()) {
      builder.withPartition(
          partitionValueConverter.toIceberg(partitionSpec, schema, dataFile.getPartitionValues()));
    }
    return builder.build();
  }

  private static FileFormat convertFileFormat(io.onetable.model.storage.FileFormat fileFormat) {
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
