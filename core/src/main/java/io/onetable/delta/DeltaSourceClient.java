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
 
package io.onetable.delta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.extern.log4j.Log4j2;

import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;

import io.onetable.client.PerTableConfig;
import io.onetable.exception.OneIOException;
import io.onetable.model.CurrentCommitState;
import io.onetable.model.InstantsForIncrementalSync;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.TableChange;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.model.schema.SchemaVersion;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.spi.extractor.SourceClient;

@Log4j2
public class DeltaSourceClient implements SourceClient<Snapshot> {
  private final PerTableConfig sourceTableConfig;
  private final SparkSession sparkSession;

  public DeltaSourceClient(PerTableConfig sourceTableConfig, SparkSession sparkSession) {
    this.sourceTableConfig = sourceTableConfig;
    this.sparkSession = sparkSession;
  }

  @Override
  public OneTable getTable(Snapshot snapshot) {
    return new DeltaTableExtractor().table(sourceTableConfig.getTableName(), snapshot);
  }

  @Override
  public SchemaCatalog getSchemaCatalog(OneTable table, Snapshot snapshot) {
    // TODO: Does not support schema versions for now
    Map<SchemaVersion, OneSchema> schemas = new HashMap<>();
    SchemaVersion schemaVersion = new SchemaVersion(1, "");
    schemas.put(schemaVersion, table.getReadSchema());
    return SchemaCatalog.builder().schemas(schemas).build();
  }

  @Override
  public OneSnapshot getCurrentSnapshot() {
    DeltaLog deltaLog = DeltaLog.forTable(sparkSession, sourceTableConfig.getTableBasePath());
    Snapshot snapshot = deltaLog.snapshot();
    OneTable table = getTable(snapshot);
    return OneSnapshot.builder()
        .table(table)
        .schemaCatalog(getSchemaCatalog(table, snapshot))
        .dataFiles(getOneDataFiles(snapshot))
        .build();
  }

  private OneDataFiles getOneDataFiles(Snapshot snapshot) {
    OneDataFiles oneDataFiles;
    try (DeltaDataFileExtractor fileIterator = new DeltaDataFileExtractor(snapshot)) {
      List<OneDataFile> dataFiles = new ArrayList<>();
      fileIterator.forEachRemaining(dataFiles::add);
      oneDataFiles = OneDataFiles.collectionBuilder().files(dataFiles).build();
    } catch (Exception e) {
      throw new OneIOException("Failed to iterate through Delta data files", e);
    }
    return oneDataFiles;
  }

  @Override
  public TableChange getTableChangeForCommit(Snapshot snapshot) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CurrentCommitState<Snapshot> getCurrentCommitState(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    throw new UnsupportedOperationException();
  }
}
