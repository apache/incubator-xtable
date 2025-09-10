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

import static org.apache.xtable.model.storage.DataLayoutStrategy.HIVE_STYLE_PARTITION;

import java.io.IOException;
import java.time.Instant;
import java.util.List;

import lombok.extern.log4j.Log4j2;

import org.apache.paimon.Snapshot;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.*;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.extractor.ConversionSource;

@Log4j2
public class PaimonConversionSource implements ConversionSource<Snapshot> {

  private final FileStoreTable paimonTable;
  private final SchemaManager schemaManager;
  private final SnapshotManager snapshotManager;

  private final PaimonDataFileExtractor dataFileExtractor = PaimonDataFileExtractor.getInstance();
  private final PaimonSchemaExtractor schemaExtractor = PaimonSchemaExtractor.getInstance();
  private final PaimonPartitionExtractor partitionSpecExtractor =
      PaimonPartitionExtractor.getInstance();

  public PaimonConversionSource(FileStoreTable paimonTable) {
    this.paimonTable = paimonTable;
    this.schemaManager = paimonTable.schemaManager();
    this.snapshotManager = paimonTable.snapshotManager();
  }

  @Override
  public InternalTable getTable(Snapshot snapshot) {
    TableSchema paimonSchema = schemaManager.schema(snapshot.schemaId());
    InternalSchema internalSchema = schemaExtractor.toInternalSchema(paimonSchema);

    List<String> partitionKeys = paimonTable.partitionKeys();
    List<InternalPartitionField> partitioningFields =
        partitionSpecExtractor.toInternalPartitionFields(partitionKeys, internalSchema);

    DataLayoutStrategy dataLayoutStrategy =
        partitioningFields.isEmpty() ? DataLayoutStrategy.FLAT : HIVE_STYLE_PARTITION;

    return InternalTable.builder()
        .name(paimonTable.name())
        .tableFormat(TableFormat.PAIMON)
        .readSchema(internalSchema)
        .layoutStrategy(dataLayoutStrategy)
        .basePath(paimonTable.location().toString())
        .partitioningFields(partitioningFields)
        .latestCommitTime(Instant.ofEpochMilli(snapshot.timeMillis()))
        .latestMetadataPath(snapshotManager.snapshotPath(snapshot.id()).toString())
        .build();
  }

  @Override
  public InternalTable getCurrentTable() {
    SnapshotManager snapshotManager = paimonTable.snapshotManager();
    Snapshot snapshot = snapshotManager.latestSnapshot();
    if (snapshot == null) {
      throw new ReadException("No snapshots found for table " + paimonTable.name());
    }
    return getTable(snapshot);
  }

  @Override
  public InternalSnapshot getCurrentSnapshot() {
    SnapshotManager snapshotManager = paimonTable.snapshotManager();
    Snapshot snapshot = snapshotManager.latestSnapshot();
    if (snapshot == null) {
      throw new ReadException("No snapshots found for table " + paimonTable.name());
    }

    InternalTable internalTable = getTable(snapshot);
    List<InternalDataFile> internalDataFiles =
        dataFileExtractor.toInternalDataFiles(paimonTable, snapshot);

    return InternalSnapshot.builder()
        .table(internalTable)
        .version(Long.toString(snapshot.timeMillis()))
        .partitionedDataFiles(PartitionFileGroup.fromFiles(internalDataFiles))
        // TODO : Implement pending commits extraction, required for incremental sync
        .sourceIdentifier(getCommitIdentifier(snapshot))
        .build();
  }

  @Override
  public TableChange getTableChangeForCommit(Snapshot snapshot) {
    throw new UnsupportedOperationException("Incremental Sync is not supported yet.");
  }

  @Override
  public CommitsBacklog<Snapshot> getCommitsBacklog(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    throw new UnsupportedOperationException("Incremental Sync is not supported yet.");
  }

  @Override
  public boolean isIncrementalSyncSafeFrom(Instant instant) {
    return false; // Incremental sync is not supported yet
  }

  @Override
  public String getCommitIdentifier(Snapshot snapshot) {
    return Long.toString(snapshot.commitIdentifier());
  }

  @Override
  public void close() throws IOException {}
}
