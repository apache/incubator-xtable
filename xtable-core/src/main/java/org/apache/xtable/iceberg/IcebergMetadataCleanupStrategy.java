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

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import lombok.extern.log4j.Log4j2;

import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.exceptions.NotFoundException;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.Tasks;

@Log4j2
abstract class IcebergMetadataCleanupStrategy {
  protected final FileIO fileIO;
  private final Consumer<String> deleteFunc;
  protected final ExecutorService cleanExecutorService;

  protected IcebergMetadataCleanupStrategy(
      FileIO fileIO, ExecutorService cleanExecutorService, Consumer<String> deleteFunc) {
    this.fileIO = fileIO;
    this.deleteFunc = deleteFunc;
    this.cleanExecutorService = cleanExecutorService;
  }

  public abstract void cleanFiles(Table table, List<Snapshot> removedSnapshots);

  private static final Schema MANIFEST_PROJECTION =
      ManifestFile.schema()
          .select(
              "manifest_path",
              "manifest_length",
              "partition_spec_id",
              "added_snapshot_id",
              "deleted_data_files_count");

  protected CloseableIterable<ManifestFile> readManifests(Snapshot snapshot) {
    if (snapshot.manifestListLocation() == null) {
      return CloseableIterable.withNoopClose(snapshot.allManifests(fileIO));
    }
    return Avro.read(fileIO.newInputFile(snapshot.manifestListLocation()))
        .rename("manifest_file", GenericManifestFile.class.getName())
        .classLoader(GenericManifestFile.class.getClassLoader())
        .project(MANIFEST_PROJECTION)
        .reuseContainers(true)
        .build();
  }

  protected void deleteFiles(Set<String> pathsToDelete, String fileType) {
    Tasks.foreach(pathsToDelete)
        .executeWith(cleanExecutorService)
        .retry(3)
        .stopRetryOn(NotFoundException.class)
        .suppressFailureWhenFinished()
        .onFailure(
            (file, thrown) -> log.warn("Delete failed for {} file: {}", fileType, file, thrown))
        .run(deleteFunc::accept);
  }
}
