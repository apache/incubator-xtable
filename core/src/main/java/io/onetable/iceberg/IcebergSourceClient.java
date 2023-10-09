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

import java.time.Instant;
import java.util.List;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import io.onetable.client.PerTableConfig;
import io.onetable.model.*;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.model.storage.TableFormat;
import io.onetable.spi.extractor.SourceClient;

@Log4j2
public class IcebergSourceClient implements SourceClient<Snapshot> {
  private final Configuration hadoopConf;
  private final PerTableConfig sourceTableConfig;
  private final Table sourceTable;

  public IcebergSourceClient(Configuration hadoopConf, PerTableConfig sourceTableConfig) {
    this.hadoopConf = hadoopConf;
    this.sourceTableConfig = sourceTableConfig;

    HadoopTables tables = new HadoopTables(hadoopConf);
    sourceTable = tables.load(sourceTableConfig.getTableBasePath());
  }

  @Override
  public OneTable getTable(Snapshot snapshot) {
    Schema iceSchema = sourceTable.schemas().get(snapshot.schemaId());
    IcebergSchemaExtractor schemaExtractor = IcebergSchemaExtractor.getInstance();
    OneSchema irSchema = schemaExtractor.fromIceberg(iceSchema);

    // TODO select snapshot specific partition spec
    IcebergPartitionSpecExtractor partitionExtractor = IcebergPartitionSpecExtractor.getInstance();
    List<OnePartitionField> irPartitionFields =
        partitionExtractor.fromIceberg(sourceTable.spec(), irSchema);

    return OneTable.builder()
        .tableFormat(TableFormat.ICEBERG)
        .basePath(sourceTable.location())
        .name(sourceTable.name())
        .partitioningFields(irPartitionFields)
        .latestCommitTime(Instant.ofEpochMilli(sourceTable.currentSnapshot().timestampMillis()))
        .readSchema(irSchema)
        // .layoutStrategy(dataLayoutStrategy)
        .build();
  }

  @Override
  public SchemaCatalog getSchemaCatalog(OneTable table, Snapshot snapshot) {
    return null;
  }

  @Override
  public OneSnapshot getCurrentSnapshot() {
    return null;
  }

  @Override
  public TableChange getTableChangeForCommit(Snapshot snapshot) {
    return null;
  }

  @Override
  public CurrentCommitState<Snapshot> getCurrentCommitState(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    return null;
  }
}
