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

import org.apache.paimon.Snapshot;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.snapshot.SnapshotReader;

import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.storage.InternalDataFile;

public class PaimonDataFileExtractor {

  private final PaimonPartitionExtractor partitionExtractor =
      PaimonPartitionExtractor.getInstance();

  private static final PaimonDataFileExtractor INSTANCE = new PaimonDataFileExtractor();

  public static PaimonDataFileExtractor getInstance() {
    return INSTANCE;
  }

  public List<InternalDataFile> toInternalDataFiles(FileStoreTable table, Snapshot snapshot) {
    List<InternalDataFile> result = new ArrayList<>();
    Iterator<ManifestEntry> iterator = newSnapshotReader(table, snapshot).readFileIterator();
    while (iterator.hasNext()) {
      result.add(toInternalDataFile(table, iterator.next()));
    }
    return result;
  }

  private InternalDataFile toInternalDataFile(FileStoreTable table, ManifestEntry entry) {
    return InternalDataFile.builder()
        .physicalPath(toFullPhysicalPath(table, entry))
        .fileSizeBytes(entry.file().fileSize())
        .lastModified(entry.file().creationTimeEpochMillis())
        .recordCount(entry.file().rowCount())
        .partitionValues(partitionExtractor.toPartitionValues(table, entry.partition()))
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
    return Collections.emptyList();
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
