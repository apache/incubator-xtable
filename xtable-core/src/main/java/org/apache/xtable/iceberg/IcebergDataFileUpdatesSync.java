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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import lombok.AllArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeleteFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.deletes.BaseDVFileWriter;
import org.apache.iceberg.deletes.DVFileWriter;
import org.apache.iceberg.deletes.PositionDeleteIndex;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DeleteWriteResult;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.IOUtil;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.util.ContentFileUtil;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.exception.ReadException;
import org.apache.xtable.exception.UpdateException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.stat.PartitionValue;
import org.apache.xtable.model.storage.FilesDiff;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFile;
import org.apache.xtable.model.storage.InternalFilesDiff;
import org.apache.xtable.model.storage.PartitionFileGroup;

@Log4j2
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

  /**
   * @param deleteOperation whether the source commit is a delete. Iceberg names a snapshot after
   *     the statement that wrote it, so a delete that matched no rows is published as an empty
   *     delete; any other diff is labelled by the files it adds and removes.
   */
  public void applyDiff(
      Transaction transaction,
      InternalFilesDiff internalFilesDiff,
      boolean deleteOperation,
      Schema schema,
      PartitionSpec partitionSpec,
      TableSyncMetadata metadata) {
    if (deleteOperation && changesNoDataFiles(internalFilesDiff)) {
      commitEmptyDelete(transaction, metadata);
      return;
    }

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

  private static boolean changesNoDataFiles(InternalFilesDiff internalFilesDiff) {
    return internalFilesDiff.dataFilesAdded().isEmpty()
        && internalFilesDiff.dataFilesRemoved().isEmpty();
  }

  /**
   * An overwrite or row delta that changes nothing is labelled an overwrite, where Iceberg's own
   * writer labels a delete that matched no rows a delete.
   */
  private static void commitEmptyDelete(Transaction transaction, TableSyncMetadata metadata) {
    DeleteFiles deleteFiles = transaction.newDelete();
    deleteFiles.set(TableSyncMetadata.XTABLE_METADATA, metadata.toJson());
    deleteFiles.commit();
  }

  /**
   * Commits a files diff together with deletion vectors as one row delta, so that readers never see
   * the added rows without the deletes that belong to them.
   *
   * <p>The commit fails rather than merges when the table changed underneath it: it is validated
   * from the current snapshot, a file to remove that the table does not hold fails it, and so does
   * a deletion vector whose data file is neither live nor added by this commit. Iceberg drops the
   * deletion vector of every data file the diff removes.
   *
   * <p>A delete that leaves no row of a live data file removes the file instead of giving it a
   * deletion vector of all its rows, as Iceberg's own writer does for a delete that matches whole
   * files; when it only removes files it is committed as an Iceberg delete of them. Such a file is
   * no longer in the table while the source still has it, so a later diff that removes it or
   * deletes its rows again leaves it out, provided {@code withoutLiveRows} confirms that it holds
   * no live rows; any other file the table does not hold still fails the commit.
   *
   * @param deletedPositionsByDataFile row positions deleted from data files, keyed by the path the
   *     data file is recorded under
   * @param partitionsOfDataFiles the partitions holding those data files; the lookup of their
   *     current deletion vectors reads only the manifests of these partitions
   * @param replaceDeletionVectors when true the positions are everything deleted from the file and
   *     replace its current deletion vector, an empty collection leaving the file without one; when
   *     false they are merged into the file's current deletion vector
   * @param deleteOperation whether the source commit is a delete, see {@link #applyDiff}
   * @param withoutLiveRows whether the source holds no live rows of the data file at a path, or
   *     null when every file the diff references has to be in the table
   * @return the deletion vector files written for the row delta, which no snapshot references until
   *     the transaction commits; when the row delta fails they are deleted before it throws
   */
  public Set<String> applyRowDelta(
      Table table,
      Transaction transaction,
      InternalFilesDiff internalFilesDiff,
      Map<String, ? extends Collection<Long>> deletedPositionsByDataFile,
      Collection<List<PartitionValue>> partitionsOfDataFiles,
      boolean replaceDeletionVectors,
      boolean deleteOperation,
      Predicate<String> withoutLiveRows,
      Schema schema,
      PartitionSpec partitionSpec,
      TableSyncMetadata metadata) {
    // In merge mode an empty set of positions leaves the file's deletion vector as it is.
    if (deleteOperation
        && changesNoDataFiles(internalFilesDiff)
        && (deletedPositionsByDataFile.isEmpty()
            || (!replaceDeletionVectors
                && deletedPositionsByDataFile.values().stream().allMatch(Collection::isEmpty)))) {
      commitEmptyDelete(transaction, metadata);
      return Collections.emptySet();
    }
    if (!deletedPositionsByDataFile.isEmpty()) {
      requireDeletionVectors(table);
    }
    Map<String, DataFile> filesAdded =
        dataFiles(internalFilesDiff.dataFilesAdded(), schema, partitionSpec);
    Map<String, DataFile> filesRemoved =
        dataFiles(internalFilesDiff.dataFilesRemoved(), schema, partitionSpec);
    Set<String> lookedUp = new HashSet<>(deletedPositionsByDataFile.keySet());
    Set<List<PartitionValue>> partitions = new LinkedHashSet<>(partitionsOfDataFiles);
    if (withoutLiveRows != null) {
      lookedUp.addAll(filesRemoved.keySet());
      internalFilesDiff
          .dataFilesRemoved()
          .forEach(file -> partitions.add(file.getPartitionValues()));
    }
    Map<String, FileScanTask> liveFiles =
        lookedUp.isEmpty()
            ? Collections.emptyMap()
            : liveFiles(
                table,
                lookedUp,
                filesAdded.keySet(),
                partitionValueConverter.partitionFilter(partitionSpec, schema, partitions));

    Map<String, Collection<Long>> positionsToWrite = new LinkedHashMap<>();
    Map<String, PositionDeleteIndex> previousPositions = new HashMap<>();
    List<DataFile> filesDeletedWhole = new ArrayList<>();
    for (Map.Entry<String, ? extends Collection<Long>> entry :
        deletedPositionsByDataFile.entrySet()) {
      String path = entry.getKey();
      FileScanTask liveFile = liveFiles.get(path);
      if (filesRemoved.containsKey(path) || (liveFile == null && !filesAdded.containsKey(path))) {
        if (!filesRemoved.containsKey(path) && holdsNoLiveRows(withoutLiveRows, path)) {
          continue;
        }
        throw new UpdateException(
            "Deleted positions reference a data file that is not live in the Iceberg table: "
                + path);
      }
      PositionDeleteIndex previous =
          liveFile == null || replaceDeletionVectors
              ? null
              : currentDeletionVector(liveFile)
                  .map(deletionVector -> readPositions(table, deletionVector))
                  .orElse(null);
      if (deleteOperation
          && liveFile != null
          && deletesEveryRow(liveFile.file(), previous, entry.getValue())) {
        filesDeletedWhole.add(liveFile.file());
        continue;
      }
      if (previous != null) {
        previousPositions.put(path, previous);
      }
      positionsToWrite.put(path, entry.getValue());
    }
    if (withoutLiveRows != null) {
      filesRemoved
          .keySet()
          .removeIf(path -> !liveFiles.containsKey(path) && withoutLiveRows.test(path));
    }

    if (deleteOperation && filesAdded.isEmpty() && positionsToWrite.isEmpty()) {
      if (filesRemoved.isEmpty() && filesDeletedWhole.isEmpty()) {
        commitEmptyDelete(transaction, metadata);
        return Collections.emptySet();
      }
      if (!filesDeletedWhole.isEmpty()) {
        // A row delta that only removes files is labelled an overwrite.
        DeleteFiles deleteFiles = transaction.newDelete().validateFilesExist();
        filesDeletedWhole.forEach(deleteFiles::deleteFile);
        filesRemoved.values().forEach(deleteFiles::deleteFile);
        deleteFiles.set(TableSyncMetadata.XTABLE_METADATA, metadata.toJson());
        deleteFiles.commit();
        return Collections.emptySet();
      }
    }

    RowDelta rowDelta = transaction.newRowDelta();
    Snapshot currentSnapshot = transaction.table().currentSnapshot();
    if (currentSnapshot != null) {
      // Without a starting snapshot Iceberg validates against the whole history, in which the
      // deletion vector this commit supersedes counts as concurrently added.
      rowDelta.validateFromSnapshot(currentSnapshot.snapshotId());
    }
    rowDelta.validateDeletedFiles();
    filesAdded.values().forEach(rowDelta::addRows);
    filesRemoved.values().forEach(rowDelta::removeRows);
    filesDeletedWhole.forEach(rowDelta::removeRows);
    Set<String> deletionVectorFiles = Collections.emptySet();
    if (!positionsToWrite.isEmpty()) {
      DeleteWriteResult written =
          writeDeletionVectors(
              table, positionsToWrite, liveFiles, filesAdded, previousPositions, partitionSpec);
      List<String> liveFilesWithNewDeletionVectors = new ArrayList<>();
      for (String path : positionsToWrite.keySet()) {
        FileScanTask liveFile = liveFiles.get(path);
        if (liveFile != null) {
          liveFilesWithNewDeletionVectors.add(path);
          if (replaceDeletionVectors) {
            currentDeletionVector(liveFile).ifPresent(rowDelta::removeDeletes);
          }
        }
      }
      written.deleteFiles().forEach(rowDelta::addDeletes);
      written.rewrittenDeleteFiles().forEach(rowDelta::removeDeletes);
      rowDelta.validateDataFilesExist(liveFilesWithNewDeletionVectors);
      deletionVectorFiles =
          written.deleteFiles().stream()
              .map(DeleteFile::location)
              .collect(Collectors.toCollection(HashSet::new));
    }
    try {
      rowDelta.set(TableSyncMetadata.XTABLE_METADATA, metadata.toJson());
      rowDelta.commit();
    } catch (RuntimeException e) {
      deleteUncommittedFiles(table.io(), deletionVectorFiles);
      throw e;
    }
    return deletionVectorFiles;
  }

  /**
   * Commits a files diff of a source commit that rewrites data files and keeps every row, as a
   * compaction or a clustering does, as one Iceberg replace snapshot, the way Iceberg's own rewrite
   * of data files publishes it, which readers of the table's changes skip. The deletion vectors of
   * the files it removes go with them, and the positions still deleted from a file it adds become
   * that file's deletion vector. The added data files take the data sequence number of the snapshot
   * the rewrite starts from, as Iceberg's rewrite does by default, so that a delete committed after
   * that snapshot still applies to their rows, and so do the deletion vectors the commit adds,
   * which take its own.
   *
   * @param deletedPositionsByDataFile row positions deleted from the data files the diff adds,
   *     keyed by their paths
   * @param withoutLiveRows whether the source holds no live rows of the data file at a path, which
   *     the diff then may remove although the table does not hold it, see {@link #applyRowDelta};
   *     null when every file the diff removes has to be in the table
   * @return the deletion vector files written for the commit, or null without committing anything
   *     when Iceberg cannot express the diff as a rewrite: it removes no data file the table holds
   *     or one the table does not hold, deletes positions of a file it does not add, or adds
   *     deletion vectors without removing one
   */
  public Set<String> applyRewrite(
      Table table,
      Transaction transaction,
      InternalFilesDiff internalFilesDiff,
      Map<String, ? extends Collection<Long>> deletedPositionsByDataFile,
      Predicate<String> withoutLiveRows,
      Schema schema,
      PartitionSpec partitionSpec,
      TableSyncMetadata metadata) {
    Snapshot startingSnapshot = transaction.table().currentSnapshot();
    Map<String, DataFile> filesAdded =
        dataFiles(internalFilesDiff.dataFilesAdded(), schema, partitionSpec);
    Map<String, Collection<Long>> positions = new LinkedHashMap<>();
    deletedPositionsByDataFile.forEach(
        (path, deleted) -> {
          if (!deleted.isEmpty()) {
            positions.put(path, deleted);
          }
        });
    if (startingSnapshot == null
        || internalFilesDiff.dataFilesRemoved().isEmpty()
        || !filesAdded.keySet().containsAll(positions.keySet())) {
      return null;
    }
    Set<List<PartitionValue>> partitions = new LinkedHashSet<>();
    internalFilesDiff.dataFilesRemoved().forEach(file -> partitions.add(file.getPartitionValues()));
    Set<String> filesRemoved =
        dataFiles(internalFilesDiff.dataFilesRemoved(), schema, partitionSpec).keySet();
    Map<String, FileScanTask> liveFiles =
        liveFiles(
            table,
            filesRemoved,
            filesAdded.keySet(),
            partitionValueConverter.partitionFilter(partitionSpec, schema, partitions));
    List<FileScanTask> replaced = new ArrayList<>();
    for (String path : filesRemoved) {
      FileScanTask liveFile = liveFiles.get(path);
      if (liveFile != null) {
        replaced.add(liveFile);
      } else if (!holdsNoLiveRows(withoutLiveRows, path)) {
        return null;
      }
    }
    List<DeleteFile> replacedDeletionVectors =
        replaced.stream()
            .flatMap(liveFile -> liveFile.deletes().stream())
            .filter(ContentFileUtil::isDV)
            .collect(Collectors.toList());
    if (replaced.isEmpty() || (!positions.isEmpty() && replacedDeletionVectors.isEmpty())) {
      return null;
    }
    if (!positions.isEmpty()) {
      requireDeletionVectors(table);
    }
    RewriteFiles rewrite =
        transaction
            .newRewrite()
            .validateFromSnapshot(startingSnapshot.snapshotId())
            .dataSequenceNumber(startingSnapshot.sequenceNumber());
    replaced.forEach(liveFile -> rewrite.deleteFile(liveFile.file()));
    replacedDeletionVectors.forEach(rewrite::deleteFile);
    filesAdded.values().forEach(rewrite::addFile);
    Set<String> deletionVectorFiles = Collections.emptySet();
    if (!positions.isEmpty()) {
      DeleteWriteResult written =
          writeDeletionVectors(
              table,
              positions,
              Collections.emptyMap(),
              filesAdded,
              Collections.emptyMap(),
              partitionSpec);
      written.deleteFiles().forEach(rewrite::addFile);
      deletionVectorFiles =
          written.deleteFiles().stream()
              .map(DeleteFile::location)
              .collect(Collectors.toCollection(HashSet::new));
    }
    try {
      rewrite.set(TableSyncMetadata.XTABLE_METADATA, metadata.toJson());
      rewrite.commit();
    } catch (RuntimeException e) {
      deleteUncommittedFiles(table.io(), deletionVectorFiles);
      throw e;
    }
    return deletionVectorFiles;
  }

  private static boolean holdsNoLiveRows(Predicate<String> withoutLiveRows, String path) {
    return withoutLiveRows != null && withoutLiveRows.test(path);
  }

  /**
   * Whether the positions, with those the data file's current deletion vector already deletes,
   * cover every row of a data file that holds rows.
   */
  private static boolean deletesEveryRow(
      DataFile dataFile, PositionDeleteIndex previous, Collection<Long> positions) {
    long rows = dataFile.recordCount();
    if (rows <= 0) {
      return false;
    }
    long deleted = previous == null ? 0 : previous.cardinality();
    for (Long position : new HashSet<>(positions)) {
      if (position >= 0 && position < rows && (previous == null || !previous.isDeleted(position))) {
        deleted++;
      }
    }
    return deleted >= rows;
  }

  private Map<String, DataFile> dataFiles(
      Collection<InternalDataFile> files, Schema schema, PartitionSpec partitionSpec) {
    Map<String, DataFile> dataFiles = new LinkedHashMap<>();
    for (InternalDataFile file : files) {
      DataFile dataFile = getDataFile(partitionSpec, schema, file);
      dataFiles.put(dataFile.location(), dataFile);
    }
    return dataFiles;
  }

  /**
   * Deletes files written for a commit that failed. Each file is deleted if it exists, and a
   * failure to delete one is logged rather than thrown, so that it does not hide why the commit
   * failed.
   */
  static void deleteUncommittedFiles(FileIO io, Collection<String> files) {
    for (String file : files) {
      try {
        if (io.newInputFile(file).exists()) {
          io.deleteFile(file);
        }
      } catch (RuntimeException e) {
        log.warn("Failed to delete file {} of a failed commit", file, e);
      }
    }
  }

  private static void requireDeletionVectors(Table table) {
    int formatVersion = ((HasTableOperations) table).operations().current().formatVersion();
    if (formatVersion < 3) {
      throw new NotSupportedException(
          String.format(
              "Deletion vectors need Iceberg format version 3 or later, but table %s is at version"
                  + " %d",
              table.name(), formatVersion));
    }
  }

  /**
   * Writes one deletion vector per data file into a new file, merging each with the positions the
   * data file's current deletion vector deletes, when given.
   *
   * @param previousPositions the positions the current deletion vector of a data file deletes, by
   *     path; a file without an entry gets a deletion vector of the given positions only
   */
  private DeleteWriteResult writeDeletionVectors(
      Table table,
      Map<String, ? extends Collection<Long>> positionsByDataFile,
      Map<String, FileScanTask> liveFiles,
      Map<String, DataFile> filesAdded,
      Map<String, PositionDeleteIndex> previousPositions,
      PartitionSpec partitionSpec) {
    OutputFile deletionVectorFile =
        OutputFileFactory.builderFor(table, 1, 1)
            .format(FileFormat.PUFFIN)
            .build()
            .newOutputFile()
            .encryptingOutputFile();
    try (DVFileWriter writer =
        new BaseDVFileWriter(() -> deletionVectorFile, previousPositions::get)) {
      for (Map.Entry<String, ? extends Collection<Long>> entry : positionsByDataFile.entrySet()) {
        String path = entry.getKey();
        FileScanTask liveFile = liveFiles.get(path);
        PartitionSpec spec = liveFile != null ? liveFile.spec() : partitionSpec;
        StructLike partition =
            liveFile != null ? liveFile.file().partition() : filesAdded.get(path).partition();
        for (Long position : entry.getValue()) {
          writer.delete(path, position, spec, partition);
        }
      }
      writer.close();
      return writer.result();
    } catch (IOException e) {
      deleteUncommittedFiles(table.io(), Collections.singleton(deletionVectorFile.location()));
      throw new UpdateException("Failed to write Iceberg deletion vectors", e);
    } catch (RuntimeException e) {
      // Closing the writer on the way out writes the vectors collected so far.
      deleteUncommittedFiles(table.io(), Collections.singleton(deletionVectorFile.location()));
      throw e;
    }
  }

  /**
   * The live data files among the given paths, with the delete files Iceberg applies to them. The
   * scan is limited to the partition filter; a path that it does not find and that the commit does
   * not add is looked up in the whole table, so the partitions change the cost but never the
   * result.
   */
  private static Map<String, FileScanTask> liveFiles(
      Table table, Set<String> paths, Set<String> filesAdded, Expression partitionFilter) {
    Map<String, FileScanTask> liveFiles = scanLiveFiles(table, paths, partitionFilter);
    if (partitionFilter.op() != Expression.Operation.TRUE
        && paths.stream()
            .anyMatch(path -> !liveFiles.containsKey(path) && !filesAdded.contains(path))) {
      return scanLiveFiles(table, paths, Expressions.alwaysTrue());
    }
    return liveFiles;
  }

  private static Map<String, FileScanTask> scanLiveFiles(
      Table table, Set<String> paths, Expression partitionFilter) {
    Map<String, FileScanTask> liveFiles = new HashMap<>();
    try (CloseableIterable<FileScanTask> tasks =
        table.newScan().filter(partitionFilter).planFiles()) {
      for (FileScanTask task : tasks) {
        if (paths.contains(task.file().location())) {
          liveFiles.put(task.file().location(), task);
        }
      }
    } catch (IOException e) {
      throw new ReadException("Failed to iterate through Iceberg data files", e);
    }
    return liveFiles;
  }

  /**
   * The deletion vector Iceberg applies to a live data file, if it has one.
   *
   * @throws ReadException when the file has more than one delete file, or one that is not a
   *     deletion vector, since neither can be superseded by writing a deletion vector
   */
  private static Optional<DeleteFile> currentDeletionVector(FileScanTask liveFile) {
    if (liveFile == null || liveFile.deletes().isEmpty()) {
      return Optional.empty();
    }
    if (liveFile.deletes().size() > 1 || !ContentFileUtil.isDV(liveFile.deletes().get(0))) {
      throw new ReadException(
          "Expected at most one deletion vector for data file "
              + liveFile.file().location()
              + " but found "
              + liveFile.deletes());
    }
    return Optional.of(liveFile.deletes().get(0));
  }

  private static PositionDeleteIndex readPositions(Table table, DeleteFile deletionVector) {
    try (SeekableInputStream in = table.io().newInputFile(deletionVector.location()).newStream()) {
      in.seek(deletionVector.contentOffset());
      byte[] bytes = new byte[Math.toIntExact(deletionVector.contentSizeInBytes())];
      IOUtil.readFully(in, bytes, 0, bytes.length);
      return PositionDeleteIndex.deserialize(bytes, deletionVector);
    } catch (IOException e) {
      throw new ReadException(
          "Failed to read the deletion vector at " + deletionVector.location(), e);
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
