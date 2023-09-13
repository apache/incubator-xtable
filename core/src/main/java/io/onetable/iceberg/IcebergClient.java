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
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.UpdateProperties;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;

import io.onetable.client.PerTableConfig;
import io.onetable.model.OneTable;
import io.onetable.model.OneTableMetadata;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.OneDataFilesDiff;
import io.onetable.spi.sync.TargetClient;

public class IcebergClient implements TargetClient {
  private static final String METADATA_DIR_PATH = "/metadata/";
  private final IcebergSchemaExtractor schemaExtractor;
  private final IcebergSchemaSync schemaSync;
  private final IcebergPartitionSpecExtractor partitionSpecExtractor;
  private final IcebergPartitionSpecSync partitionSpecSync;
  private final IcebergDataFileUpdatesSync dataFileUpdatesExtractor;
  private final String basePath;
  private final String tableName;
  private final Configuration configuration;
  private final int snapshotRetentionInHours;
  private Transaction transaction;
  private Table table;

  public IcebergClient(PerTableConfig perTableConfig, Configuration configuration) {
    this(
        perTableConfig.getTableBasePath(),
        perTableConfig.getTableName(),
        perTableConfig.getTargetMetadataRetentionInHours(),
        configuration,
        IcebergSchemaExtractor.getInstance(),
        IcebergSchemaSync.getInstance(),
        IcebergPartitionSpecExtractor.getInstance(),
        IcebergPartitionSpecSync.getInstance(),
        IcebergDataFileUpdatesSync.of(
            IcebergColumnStatsConverter.getInstance(),
            IcebergPartitionValueConverter.getInstance()));
  }

  IcebergClient(
      String basePath,
      String tableName,
      int snapshotRetentionInHours,
      Configuration configuration,
      IcebergSchemaExtractor schemaExtractor,
      IcebergSchemaSync schemaSync,
      IcebergPartitionSpecExtractor partitionSpecExtractor,
      IcebergPartitionSpecSync partitionSpecSync,
      IcebergDataFileUpdatesSync dataFileUpdatesExtractor) {
    this.schemaExtractor = schemaExtractor;
    this.schemaSync = schemaSync;
    this.partitionSpecExtractor = partitionSpecExtractor;
    this.partitionSpecSync = partitionSpecSync;
    this.dataFileUpdatesExtractor = dataFileUpdatesExtractor;
    this.tableName = tableName;
    this.basePath = basePath;
    this.configuration = configuration;
    this.snapshotRetentionInHours = snapshotRetentionInHours;

    HadoopTables tables = new HadoopTables(configuration);
    if (tables.exists(basePath)) {
      // Load the table state if it already exists
      this.table = tables.load(basePath);
    }
  }

  @Override
  public void beginSync(OneTable oneTable) {
    initializeTableIfRequired(oneTable);
    transaction = table.newTransaction();
  }

  private void initializeTableIfRequired(OneTable oneTable) {
    if (table == null) {
      HadoopTables tables = new HadoopTables(configuration);
      boolean doesIcebergTableExist = tables.exists(basePath);
      if (!doesIcebergTableExist) {
        Schema schema = schemaExtractor.toIceberg(oneTable.getReadSchema());
        PartitionSpec partitionSpec =
            partitionSpecExtractor.toPartitionSpec(oneTable.getPartitioningFields(), schema);
        Map<String, String> properties = new HashMap<>();
        properties.put(
            TableProperties.DEFAULT_NAME_MAPPING,
            NameMappingParser.toJson(MappingUtil.create(schema)));
        table = tables.create(schema, partitionSpec, SortOrder.unsorted(), properties, basePath);
      } else {
        table = tables.load(basePath);
      }
    }
  }

  @Override
  public void syncSchema(OneSchema schema) {
    Schema latestSchema = schemaExtractor.toIceberg(schema);
    schemaSync.sync(transaction.table().schema(), latestSchema, transaction);
  }

  @Override
  public void syncPartitionSpec(List<OnePartitionField> partitionSpec) {
    PartitionSpec latestPartitionSpec =
        partitionSpecExtractor.toPartitionSpec(partitionSpec, transaction.table().schema());
    partitionSpecSync.sync(table.spec(), latestPartitionSpec, transaction);
  }

  @Override
  public void syncMetadata(OneTableMetadata metadata) {
    UpdateProperties updateProperties = transaction.updateProperties();
    for (Map.Entry<String, String> stateProperty : metadata.asMap().entrySet()) {
      updateProperties.set(stateProperty.getKey(), stateProperty.getValue());
    }
    if (!Boolean.parseBoolean(
        table
            .properties()
            .getOrDefault(
                TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, Boolean.FALSE.toString()))) {
      // Helps control the number of metadata files for frequently updated tables
      updateProperties.set(
          TableProperties.METADATA_DELETE_AFTER_COMMIT_ENABLED, Boolean.TRUE.toString());
    }
    updateProperties.commit();
  }

  @Override
  public void syncFilesForSnapshot(OneDataFiles snapshotFiles) {
    dataFileUpdatesExtractor.applySnapshot(
        table,
        transaction,
        snapshotFiles,
        transaction.table().schema(),
        transaction.table().spec());
  }

  @Override
  public void syncFilesForDiff(OneDataFilesDiff oneDataFilesDiff) {
    dataFileUpdatesExtractor.applyDiff(
        transaction, oneDataFilesDiff, transaction.table().schema(), transaction.table().spec());
  }

  @Override
  public void completeSync() {
    transaction
        .expireSnapshots()
        .expireOlderThan(
            Instant.now().minus(snapshotRetentionInHours, ChronoUnit.HOURS).toEpochMilli())
        .deleteWith(this::safeDelete) // ensures that only metadata files are deleted
        .cleanExpiredFiles(true)
        .commit();
    transaction.commitTransaction();
    transaction = null;
  }

  private void safeDelete(String file) {
    if (file.startsWith(new Path(basePath) + METADATA_DIR_PATH)) {
      table.io().deleteFile(file);
    }
  }

  @Override
  public Optional<OneTableMetadata> getTableMetadata() {
    return OneTableMetadata.fromMap(table.properties());
  }
}
