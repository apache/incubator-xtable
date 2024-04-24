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
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import lombok.AllArgsConstructor;

import org.apache.iceberg.*;
import org.apache.iceberg.io.CloseableIterable;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.storage.DataFilesDiff;
import org.apache.xtable.model.storage.FilesDiff;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.PartitionFileGroup;

@AllArgsConstructor(staticName = "of")
public class IcebergDataFileUpdatesSync {
  private final IcebergColumnStatsConverter columnStatsConverter;
  private final IcebergPartitionValueConverter partitionValueConverter;

  public void applySnapshot(
      Table table,
      InternalTable internalTable,
      Transaction transaction,
      List<PartitionFileGroup> partitionedDataFiles,
      Schema schema,
      PartitionSpec partitionSpec) {

    Map<String, DataFile> previousFiles = new HashMap<>();
    try (CloseableIterable<FileScanTask> iterator = table.newScan().planFiles()) {
      StreamSupport.stream(iterator.spliterator(), false)
          .map(FileScanTask::file)
          .forEach(file -> previousFiles.put(file.path().toString(), file));
    } catch (Exception e) {
      throw new ReadException("Failed to iterate through Iceberg data files", e);
    }

    FilesDiff<InternalDataFile, DataFile> diff =
        DataFilesDiff.findNewAndRemovedFiles(partitionedDataFiles, previousFiles);

    applyDiff(transaction, diff.getFilesAdded(), diff.getFilesRemoved(), schema, partitionSpec);
  }

  public void applyDiff(
      Transaction transaction,
      DataFilesDiff dataFilesDiff,
      Schema schema,
      PartitionSpec partitionSpec) {

    Collection<DataFile> filesRemoved =
        dataFilesDiff.getFilesRemoved().stream()
            .map(file -> getDataFile(partitionSpec, schema, file))
            .collect(Collectors.toList());

    applyDiff(transaction, dataFilesDiff.getFilesAdded(), filesRemoved, schema, partitionSpec);
  }

  private void applyDiff(
      Transaction transaction,
      Collection<InternalDataFile> filesAdded,
      Collection<DataFile> filesRemoved,
      Schema schema,
      PartitionSpec partitionSpec) {
    OverwriteFiles overwriteFiles = transaction.newOverwrite();
    filesAdded.forEach(f -> overwriteFiles.addFile(getDataFile(partitionSpec, schema, f)));
    filesRemoved.forEach(overwriteFiles::deleteFile);
    overwriteFiles.commit();
  }

  private DataFile getDataFile(
      PartitionSpec partitionSpec, Schema schema, InternalDataFile dataFile) {
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
