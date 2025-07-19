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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import lombok.Builder;

import org.apache.hadoop.conf.Configuration;

import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;

import org.apache.xtable.delta.*;
import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.*;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.InternalDataFile;
import org.apache.xtable.model.storage.PartitionFileGroup;
import org.apache.xtable.spi.extractor.ConversionSource;
import org.apache.xtable.spi.extractor.DataFileIterator;

@Builder
public class DeltaKernelConversionSource implements ConversionSource<Long> {

  @Builder.Default
  private final DeltaKernelDataFileExtractor dataFileExtractor =
      DeltaKernelDataFileExtractor.builder().build();

  private final String basePath;
  private final String tableName;
  private final Engine engine;

  //  private final DeltaKernelTableExtractor tableExtractor;

  @Builder.Default
  private final DeltaKernelTableExtractor tableExtractor =
      DeltaKernelTableExtractor.builder().build();

  @Override
  public InternalTable getTable(Long version) {
    Configuration hadoopConf = new Configuration();
    try {
      Engine engine = DefaultEngine.create(hadoopConf);
      Table table = Table.forPath(engine, basePath);
      Snapshot snapshot = table.getSnapshotAsOfVersion(engine, version);
      System.out.println("getTable: " + basePath);
      return tableExtractor.table(table, snapshot, engine, tableName, basePath);
    } catch (Exception e) {
      throw new ReadException("Failed to get table at version " + version, e);
    }
  }

  @Override
  public InternalTable getCurrentTable() {
    Configuration hadoopConf = new Configuration();
    Engine engine = DefaultEngine.create(hadoopConf);
    Table table = Table.forPath(engine, basePath);
    System.out.println("getCurrentTable: " + basePath);
    Snapshot snapshot = table.getLatestSnapshot(engine);
    return getTable(snapshot.getVersion());
  }

  @Override
  public InternalSnapshot getCurrentSnapshot() {
    Configuration hadoopConf = new Configuration();
    Engine engine = DefaultEngine.create(hadoopConf);
    System.out.println("getCurrentSnapshot12: " + basePath);
    Table table_snapshot = Table.forPath(engine, basePath);
    Snapshot snapshot = table_snapshot.getLatestSnapshot(engine);
    InternalTable table = getTable(snapshot.getVersion());
    return InternalSnapshot.builder()
        .table(table)
        .partitionedDataFiles(getInternalDataFiles(snapshot, table.getReadSchema()))
        .sourceIdentifier(getCommitIdentifier(snapshot.getVersion()))
        .build();
  }

  @Override
  public TableChange getTableChangeForCommit(Long aLong) {
    return null;
  }

  @Override
  public CommitsBacklog<Long> getCommitsBacklog(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    return null;
  }

  @Override
  public boolean isIncrementalSyncSafeFrom(Instant instant) {
    return false;
  }

  @Override
  public String getCommitIdentifier(Long aLong) {
    return "";
  }

  private List<PartitionFileGroup> getInternalDataFiles(
      io.delta.kernel.Snapshot snapshot, InternalSchema schema) {
    try (DataFileIterator fileIterator = dataFileExtractor.iterator(snapshot, schema)) {
      List<InternalDataFile> dataFiles = new ArrayList<>();
      fileIterator.forEachRemaining(dataFiles::add);
      return PartitionFileGroup.fromFiles(dataFiles);
    } catch (Exception e) {
      throw new ReadException("Failed to iterate through Delta data files", e);
    }
  }

  @Override
  public void close() throws IOException {}

  //
  //  @Override
  //  public InternalSnapshot getCurrentSnapshot() {
  //    throw new UnsupportedOperationException("Not implemented yet");
  //  }
  //
  //  @Override
  //  public TableChange getTableChangeForCommit(Long commit) {
  //    throw new UnsupportedOperationException("Not implemented yet");
  //  }
  //
  //  @Override
  //  public CommitsBacklog<Long> getCommitsBacklog(InstantsForIncrementalSync
  // instantsForIncrementalSync) {
  //    throw new UnsupportedOperationException("Not implemented yet");
  //  }
  //
  //  @Override
  //  public void close() {
  //    // No resources to close
  //  }
}
