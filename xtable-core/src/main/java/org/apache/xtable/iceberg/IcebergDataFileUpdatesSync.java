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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import lombok.AllArgsConstructor;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.SeekableInputStream;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.ReadException;
import org.apache.xtable.exception.UpdateException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.storage.FilesDiff;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFile;
import org.apache.xtable.model.storage.InternalFilesDiff;
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
      PartitionSpec partitionSpec,
      TableSyncMetadata metadata) {

    Map<String, DataFile> previousFiles = new HashMap<>();
    try (CloseableIterable<FileScanTask> iterator = table.newScan().planFiles()) {
      StreamSupport.stream(iterator.spliterator(), false)
          .map(FileScanTask::file)
          .forEach(file -> previousFiles.put(file.path().toString(), file));
    } catch (Exception e) {
      throw new ReadException("Failed to iterate through Iceberg data files", e);
    }

    FilesDiff<InternalFile, DataFile> diff =
        InternalFilesDiff.findNewAndRemovedFiles(partitionedDataFiles, previousFiles);

    applyDiff(
        transaction, diff.getFilesAdded(), diff.getFilesRemoved(), schema, partitionSpec, metadata);
  }

  public void applyDiff(
      Transaction transaction,
      InternalFilesDiff internalFilesDiff,
      Schema schema,
      PartitionSpec partitionSpec,
      TableSyncMetadata metadata) {

    Collection<DataFile> filesRemoved =
        internalFilesDiff.dataFilesRemoved().stream()
            .map(file -> getDataFile(partitionSpec, schema, file))
            .collect(Collectors.toList());

    applyDiff(
        transaction,
        internalFilesDiff.dataFilesAdded(),
        filesRemoved,
        schema,
        partitionSpec,
        metadata);
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

  /**
   * Commits added data files and positional deletes as a single {@code RowDelta} snapshot. Each
   * entry of {@code positionsByDataFile} maps an existing Iceberg data file path to the row
   * positions deleted from it; the positions are written as a format-version 3 deletion vector that
   * supersedes the data file's current deletion vector, if any.
   */
  public void applyRowDelta(
      Table table,
      Transaction transaction,
      InternalFilesDiff internalFilesDiff,
      Map<String, List<Long>> positionsByDataFile,
      Schema schema,
      PartitionSpec partitionSpec,
      TableSyncMetadata metadata) {
    if (!internalFilesDiff.dataFilesRemoved().isEmpty()) {
      throw new NotSupportedException(
          "Row-delta sync does not support removing data files in the same commit");
    }
    RowDelta rowDelta = transaction.newRowDelta();
    if (table.currentSnapshot() != null) {
      // The deletion vectors below merge the data files' current deletion vectors, so the current
      // snapshot is the base state and prior deletion vectors must not count as concurrent
      rowDelta.validateFromSnapshot(table.currentSnapshot().snapshotId());
    }
    internalFilesDiff
        .dataFilesAdded()
        .forEach(f -> rowDelta.addRows(getDataFile(partitionSpec, schema, f)));
    if (!positionsByDataFile.isEmpty()) {
      Map<String, FileScanTask> existingFiles = new HashMap<>();
      try (CloseableIterable<FileScanTask> iterator = table.newScan().planFiles()) {
        StreamSupport.stream(iterator.spliterator(), false)
            .forEach(task -> existingFiles.put(task.file().path().toString(), task));
      } catch (Exception e) {
        throw new ReadException("Failed to iterate through Iceberg data files", e);
      }
      DeleteWriteResult deleteWriteResult =
          writeDeletionVectors(table, positionsByDataFile, existingFiles);
      deleteWriteResult.deleteFiles().forEach(rowDelta::addDeletes);
      deleteWriteResult.rewrittenDeleteFiles().forEach(rowDelta::removeDeletes);
    }
    rowDelta.set(TableSyncMetadata.XTABLE_METADATA, metadata.toJson());
    rowDelta.commit();
  }

  private DeleteWriteResult writeDeletionVectors(
      Table table,
      Map<String, List<Long>> positionsByDataFile,
      Map<String, FileScanTask> existingFiles) {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.PUFFIN).build();
    try (DVFileWriter writer =
        new BaseDVFileWriter(
            fileFactory, path -> loadPreviousDeletes(table, existingFiles, path))) {
      for (Map.Entry<String, List<Long>> entry : positionsByDataFile.entrySet()) {
        FileScanTask task = existingFiles.get(entry.getKey());
        if (task == null) {
          throw new NotSupportedException(
              "Positional deletes reference a data file unknown to the Iceberg table: "
                  + entry.getKey());
        }
        PartitionSpec fileSpec = task.spec();
        StructLike partition = task.file().partition();
        for (Long position : entry.getValue()) {
          writer.delete(entry.getKey(), position, fileSpec, partition);
        }
      }
      writer.close();
      return writer.result();
    } catch (IOException e) {
      throw new UpdateException("Failed to write Iceberg deletion vectors", e);
    }
  }

  private PositionDeleteIndex loadPreviousDeletes(
      Table table, Map<String, FileScanTask> existingFiles, String dataFilePath) {
    FileScanTask task = existingFiles.get(dataFilePath);
    if (task == null) {
      return null;
    }
    List<DeleteFile> deleteFiles =
        task.deletes().stream()
            .filter(deleteFile -> deleteFile.format() == FileFormat.PUFFIN)
            .collect(Collectors.toList());
    if (deleteFiles.isEmpty()) {
      return null;
    }
    if (deleteFiles.size() > 1) {
      throw new ReadException("Expected at most one deletion vector for data file " + dataFilePath);
    }
    DeleteFile dv = deleteFiles.get(0);
    try (SeekableInputStream in = table.io().newInputFile(dv.location()).newStream()) {
      in.seek(dv.contentOffset());
      byte[] bytes = new byte[Math.toIntExact(dv.contentSizeInBytes())];
      IOUtil.readFully(in, bytes, 0, bytes.length);
      return PositionDeleteIndex.deserialize(bytes, dv);
    } catch (IOException e) {
      throw new ReadException("Failed to read the deletion vector at " + dv.location(), e);
    }
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
