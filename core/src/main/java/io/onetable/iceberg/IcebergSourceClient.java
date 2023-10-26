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

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.*;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;

import io.onetable.client.PerTableConfig;
import io.onetable.exception.OneIOException;
import io.onetable.model.*;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.model.schema.SchemaVersion;
import io.onetable.model.stat.Range;
import io.onetable.model.storage.OneDataFile;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.model.storage.TableFormat;
import io.onetable.spi.extractor.SourceClient;

@Log4j2
@Builder
public class IcebergSourceClient implements SourceClient<Snapshot> {
  @NonNull private final Configuration hadoopConf;
  @NonNull private final PerTableConfig sourceTableConfig;

  @Getter(lazy = true, value = AccessLevel.PACKAGE)
  private final Table sourceTable = initSourceTable();

  @Builder.Default
  private final IcebergPartitionValueConverter partitionConverter =
      IcebergPartitionValueConverter.getInstance();

  @Builder.Default
  private final IcebergDataFileExtractor dataFileExtractor =
      IcebergDataFileExtractor.builder().build();

  private Table initSourceTable() {
    IcebergTableManager tableManager = IcebergTableManager.of(hadoopConf);
    String[] namespace = sourceTableConfig.getNamespace();
    String tableName = sourceTableConfig.getTableName();
    TableIdentifier tableIdentifier =
        namespace == null
            ? TableIdentifier.of(tableName)
            : TableIdentifier.of(Namespace.of(namespace), tableName);
    return tableManager.getTable(
        sourceTableConfig.getIcebergCatalogConfig(),
        tableIdentifier,
        sourceTableConfig.getTableBasePath());
  }

  @Override
  public OneTable getTable(Snapshot snapshot) {
    Table iceTable = getSourceTable();
    Schema iceSchema = iceTable.schemas().get(snapshot.schemaId());
    IcebergSchemaExtractor schemaExtractor = IcebergSchemaExtractor.getInstance();
    OneSchema irSchema = schemaExtractor.fromIceberg(iceSchema);

    // TODO select snapshot specific partition spec
    IcebergPartitionSpecExtractor partitionExtractor = IcebergPartitionSpecExtractor.getInstance();
    List<OnePartitionField> irPartitionFields =
        partitionExtractor.fromIceberg(iceTable.spec(), iceSchema, irSchema);

    return OneTable.builder()
        .tableFormat(TableFormat.ICEBERG)
        .basePath(iceTable.location())
        .name(iceTable.name())
        .partitioningFields(irPartitionFields)
        .latestCommitTime(Instant.ofEpochMilli(iceTable.currentSnapshot().timestampMillis()))
        .readSchema(irSchema)
        // .layoutStrategy(dataLayoutStrategy)
        .build();
  }

  @Override
  public SchemaCatalog getSchemaCatalog(OneTable table, Snapshot snapshot) {
    Table iceTable = getSourceTable();
    Integer iceSchemaId = snapshot.schemaId();
    Schema iceSchema = iceTable.schemas().get(iceSchemaId);
    IcebergSchemaExtractor schemaExtractor = IcebergSchemaExtractor.getInstance();
    OneSchema irSchema = schemaExtractor.fromIceberg(iceSchema);
    Map<SchemaVersion, OneSchema> catalog =
        Collections.singletonMap(new SchemaVersion(iceSchemaId, ""), irSchema);
    return SchemaCatalog.builder().schemas(catalog).build();
  }

  @Override
  public OneSnapshot getCurrentSnapshot() {
    Table iceTable = getSourceTable();

    Snapshot currentSnapshot = iceTable.currentSnapshot();
    OneTable irTable = getTable(currentSnapshot);
    SchemaCatalog schemaCatalog = getSchemaCatalog(irTable, currentSnapshot);

    TableScan scan = iceTable.newScan().useSnapshot(currentSnapshot.snapshotId());
    PartitionSpec partitionSpec = iceTable.spec();
    OneDataFiles oneDataFiles;
    try (CloseableIterable<FileScanTask> files = scan.planFiles()) {
      List<OneDataFile> irFiles = new ArrayList<>();
      for (FileScanTask fileScanTask : files) {
        DataFile file = fileScanTask.file();
        Map<OnePartitionField, Range> onePartitionFieldRangeMap =
            partitionConverter.toOneTable(file.partition(), partitionSpec);
        OneDataFile irDataFile = dataFileExtractor.fromIceberg(file, onePartitionFieldRangeMap);
        irFiles.add(irDataFile);
      }
      oneDataFiles = clusterFilesByPartition(irFiles);
    } catch (IOException e) {
      throw new OneIOException("Failed to fetch current snapshot files from Iceberg source", e);
    }

    return OneSnapshot.builder()
        .version(String.valueOf(currentSnapshot.snapshotId()))
        .table(irTable)
        .schemaCatalog(schemaCatalog)
        .dataFiles(oneDataFiles)
        .build();
  }

  /**
   * Divides / groups a collection of {@link OneDataFile}s into {@link OneDataFiles} based on the
   * file's partition values.
   *
   * @param files a collection of files to be grouped by partition
   * @return a collection of {@link OneDataFiles}, each containing a collection of {@link
   *     OneDataFile} with the same partition values
   */
  private OneDataFiles clusterFilesByPartition(List<OneDataFile> files) {
    Map<String, List<OneDataFile>> fileClustersMap =
        files.stream().collect(Collectors.groupingBy(OneDataFile::getPartitionPath));
    List<OneDataFile> fileClustersList =
        fileClustersMap.entrySet().stream()
            .map(
                entry ->
                    OneDataFiles.collectionBuilder()
                        .partitionPath(entry.getKey())
                        .files(entry.getValue())
                        .build())
            .collect(Collectors.toList());
    return OneDataFiles.collectionBuilder().files(fileClustersList).build();
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
