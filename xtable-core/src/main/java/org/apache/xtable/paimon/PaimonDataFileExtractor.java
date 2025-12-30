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
 
package org.apache.xtable.paimon;

import java.util.*;

import lombok.extern.log4j.Log4j2;

import org.apache.paimon.Snapshot;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;

import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.InternalFilesDiff;

@Log4j2
public class PaimonDataFileExtractor {

  private final PaimonPartitionExtractor partitionExtractor =
      PaimonPartitionExtractor.getInstance();

  private static final PaimonDataFileExtractor INSTANCE = new PaimonDataFileExtractor();

  public static PaimonDataFileExtractor getInstance() {
    return INSTANCE;
  }

  public List<InternalDataFile> toInternalDataFiles(
      FileStoreTable table, Snapshot snapshot, InternalSchema internalSchema) {
    List<InternalDataFile> result = new ArrayList<>();
    Iterator<ManifestEntry> manifestEntryIterator =
        newSnapshotReader(table, snapshot).readFileIterator();
    while (manifestEntryIterator.hasNext()) {
      result.add(toInternalDataFile(table, manifestEntryIterator.next(), internalSchema));
    }
    return result;
  }

  /**
   * Converts a Paimon ManifestEntry to an InternalDataFile. This method is used for both full
   * snapshot reads and incremental sync.
   *
   * @param table the Paimon table
   * @param entry the manifest entry representing a data file
   * @param internalSchema the internal schema for partition value extraction
   * @return InternalDataFile representation
   */
  public InternalDataFile toInternalDataFile(
      FileStoreTable table, ManifestEntry entry, InternalSchema internalSchema) {
    return InternalDataFile.builder()
        .physicalPath(toFullPhysicalPath(table, entry))
        .fileSizeBytes(entry.file().fileSize())
        .lastModified(entry.file().creationTimeEpochMillis())
        .recordCount(entry.file().rowCount())
        .partitionValues(
            partitionExtractor.toPartitionValues(table, entry.partition(), internalSchema))
        .columnStats(toColumnStats(entry.file()))
        .build();
  }

  private String toFullPhysicalPath(FileStoreTable table, ManifestEntry entry) {
    String basePath = table.location().toString();
    String bucketPath = "bucket-" + entry.bucket();
    String filePath = entry.file().fileName();

    Optional<String> partitionPath = partitionExtractor.toPartitionPath(table, entry.partition());
    if (partitionPath.isPresent()) {
      return String.join("/", basePath, partitionPath.get(), bucketPath, filePath);
    } else {
      return String.join("/", basePath, bucketPath, filePath);
    }
  }

  private List<ColumnStat> toColumnStats(DataFileMeta file) {
    // TODO: Implement logic to extract column stats from the file meta
    // https://github.com/apache/incubator-xtable/issues/755
    return Collections.emptyList();
  }

  /**
   * Extracts file changes (added and removed files) from delta manifests for a given snapshot. This
   * method reads only the delta manifests which contain the changes introduced in this specific
   * snapshot, making it efficient for incremental sync.
   *
   * @param table the Paimon table
   * @param snapshot the snapshot to extract changes from
   * @param internalSchema the internal schema for partition value extraction
   * @return InternalFilesDiff containing added and removed files
   */
  public InternalFilesDiff extractFilesDiff(
      FileStoreTable table, Snapshot snapshot, InternalSchema internalSchema) {

    ManifestList manifestList = table.store().manifestListFactory().create();
    ManifestFile manifestFile = table.store().manifestFileFactory().create();

    // Read delta manifests - these contain only the changes in this snapshot
    List<ManifestFileMeta> deltaManifests = manifestList.readDeltaManifests(snapshot);
    log.debug("Found {} delta manifests for snapshot {}", deltaManifests.size(), snapshot.id());

    Set<InternalDataFile> addedFiles = new HashSet<>();
    Set<InternalDataFile> removedFiles = new HashSet<>();

    // For primary key tables, only consider top-level files (fully compacted)
    int topLevel = table.coreOptions().numLevels() - 1;
    boolean hasPrimaryKeys = !table.schema().primaryKeys().isEmpty();

    for (ManifestFileMeta manifestMeta : deltaManifests) {
      List<ManifestEntry> entries = manifestFile.read(manifestMeta.fileName());
      log.debug("Processing {} manifest entries from {}", entries.size(), manifestMeta.fileName());

      for (ManifestEntry entry : entries) {
        if (hasPrimaryKeys && entry.file().level() != topLevel) {
          continue;
        }

        InternalDataFile dataFile = toInternalDataFile(table, entry, internalSchema);
        if (entry.kind() == FileKind.ADD) {
          addedFiles.add(dataFile);
        } else if (entry.kind() == FileKind.DELETE) {
          removedFiles.add(dataFile);
        }
      }
    }

    log.info(
        "Snapshot {} has {} files added and {} files removed",
        snapshot.id(),
        addedFiles.size(),
        removedFiles.size());

    return InternalFilesDiff.builder().filesAdded(addedFiles).filesRemoved(removedFiles).build();
  }

  private SnapshotReader newSnapshotReader(FileStoreTable table, Snapshot snapshot) {
    // If the table has primary keys, we read only the top level files
    // which means we can only consider fully compacted files.
    if (!table.schema().primaryKeys().isEmpty()) {
      return table
          .newSnapshotReader()
          .withLevel(table.coreOptions().numLevels() - 1)
          .withSnapshot(snapshot);
    } else {
      return table.newSnapshotReader().withSnapshot(snapshot);
    }
  }
}
