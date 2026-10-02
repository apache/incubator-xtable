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
 
package org.apache.xtable.glue;

import static org.apache.hudi.common.util.CollectionUtils.isNullOrEmpty;
import static org.apache.xtable.catalog.CatalogUtils.toHierarchicalTableIdentifier;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hudi.common.util.CollectionUtils;

import org.apache.xtable.catalog.CatalogPartition;
import org.apache.xtable.catalog.CatalogPartitionSyncOperations;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchDeletePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequestEntry;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.PartitionValueList;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

@Log4j2
public class GlueCatalogPartitionSyncOperations implements CatalogPartitionSyncOperations {

  private final GlueClient glueClient;
  private final GlueCatalogConfig glueCatalogConfig;

  public GlueCatalogPartitionSyncOperations(
      GlueClient glueClient, GlueCatalogConfig glueCatalogConfig) {
    this.glueClient = glueClient;
    this.glueCatalogConfig = glueCatalogConfig;
  }

  @Override
  public List<CatalogPartition> getAllPartitions(CatalogTableIdentifier catalogTableIdentifier) {
    HierarchicalTableIdentifier tableIdentifier =
        toHierarchicalTableIdentifier(catalogTableIdentifier);
    try {
      List<CatalogPartition> partitions = new ArrayList<>();
      String nextToken = null;
      do {
        GetPartitionsResponse result =
            glueClient.getPartitions(
                GetPartitionsRequest.builder()
                    .databaseName(tableIdentifier.getDatabaseName())
                    .tableName(tableIdentifier.getTableName())
                    .nextToken(nextToken)
                    .build());
        partitions.addAll(
            result.partitions().stream()
                .map(p -> new CatalogPartition(p.values(), p.storageDescriptor().location()))
                .collect(Collectors.toList()));
        nextToken = result.nextToken();
      } while (nextToken != null);
      return partitions;
    } catch (Exception e) {
      throw new CatalogSyncException(
          "Failed to get all partitions for table " + tableIdentifier, e);
    }
  }

  @Override
  public void addPartitionsToTable(
      CatalogTableIdentifier catalogTableIdentifier, List<CatalogPartition> partitionsToAdd) {
    HierarchicalTableIdentifier tableIdentifier =
        toHierarchicalTableIdentifier(catalogTableIdentifier);
    if (partitionsToAdd.isEmpty()) {
      log.info("No partitions to add for {}", tableIdentifier);
      return;
    }
    log.info("Adding {} CatalogPartition(s) in table {}", partitionsToAdd.size(), tableIdentifier);
    try {
      Table table =
          GlueCatalogTableUtils.getTable(
              glueClient, glueCatalogConfig.getCatalogId(), catalogTableIdentifier);
      StorageDescriptor sd = table.storageDescriptor();
      List<PartitionInput> partitionInputs =
          partitionsToAdd.stream()
              .map(partition -> createPartitionInput(table, partition))
              .collect(Collectors.toList());

      List<BatchCreatePartitionResponse> responses = new ArrayList<>();

      CollectionUtils.batches(partitionInputs, glueCatalogConfig.getMaxPartitionsPerRequest())
          .forEach(
              batch -> {
                BatchCreatePartitionRequest request =
                    BatchCreatePartitionRequest.builder()
                        .databaseName(tableIdentifier.getDatabaseName())
                        .tableName(tableIdentifier.getTableName())
                        .partitionInputList(batch)
                        .build();
                responses.add(glueClient.batchCreatePartition(request));
              });

      responses.forEach(
          response -> {
            if (CollectionUtils.nonEmpty(response.errors())) {
              if (response.errors().stream()
                  .allMatch(
                      (error) ->
                          "AlreadyExistsException".equals(error.errorDetail().errorCode()))) {
                log.warn("Partitions already exist in glue: {}", response.errors());
              } else {
                throw new CatalogSyncException(
                    "Fail to add partitions to "
                        + tableIdentifier
                        + " with error(s): "
                        + response.errors());
              }
            }
          });
    } catch (Exception e) {
      throw new CatalogSyncException("Fail to add partitions to " + tableIdentifier, e);
    }
  }

  @Override
  public void updatePartitionsToTable(
      CatalogTableIdentifier catalogTableIdentifier, List<CatalogPartition> changedPartitions) {
    HierarchicalTableIdentifier tableIdentifier =
        toHierarchicalTableIdentifier(catalogTableIdentifier);
    if (changedPartitions.isEmpty()) {
      log.info("No partitions to change for {}", tableIdentifier.getTableName());
      return;
    }
    log.info("Updating {} partition(s) in table {}", changedPartitions.size(), tableIdentifier);
    try {
      Table table =
          GlueCatalogTableUtils.getTable(
              glueClient, glueCatalogConfig.getCatalogId(), catalogTableIdentifier);
      StorageDescriptor sd = table.storageDescriptor();
      List<BatchUpdatePartitionRequestEntry> updatePartitionEntries =
          changedPartitions.stream()
              .map(
                  partition ->
                      BatchUpdatePartitionRequestEntry.builder()
                          .partitionInput(createPartitionInput(table, partition))
                          .partitionValueList(partition.getValues())
                          .build())
              .collect(Collectors.toList());

      List<BatchUpdatePartitionResponse> responses = new ArrayList<>();

      CollectionUtils.batches(
              updatePartitionEntries, glueCatalogConfig.getMaxPartitionsPerRequest())
          .forEach(
              batch -> {
                BatchUpdatePartitionRequest request =
                    BatchUpdatePartitionRequest.builder()
                        .databaseName(tableIdentifier.getDatabaseName())
                        .tableName(tableIdentifier.getTableName())
                        .entries(batch)
                        .build();
                responses.add(glueClient.batchUpdatePartition(request));
              });

      responses.forEach(
          response -> {
            if (CollectionUtils.nonEmpty(response.errors())) {
              throw new CatalogSyncException(
                  "Fail to update partitions to "
                      + tableIdentifier
                      + " with error(s): "
                      + response.errors());
            }
          });
    } catch (Exception e) {
      throw new CatalogSyncException("Fail to update partitions to " + tableIdentifier, e);
    }
  }

  @Override
  public void dropPartitions(
      CatalogTableIdentifier catalogTableIdentifier, List<CatalogPartition> partitionsToDrop) {
    HierarchicalTableIdentifier tableIdentifier =
        toHierarchicalTableIdentifier(catalogTableIdentifier);
    if (isNullOrEmpty(partitionsToDrop)) {
      log.info("No partitions to drop for {}", tableIdentifier);
      return;
    }
    log.info("Drop {} CatalogPartition(s) in table {}", partitionsToDrop.size(), tableIdentifier);
    try {
      List<BatchDeletePartitionResponse> responses = new ArrayList<>();

      CollectionUtils.batches(partitionsToDrop, glueCatalogConfig.getMaxPartitionsPerRequest())
          .forEach(
              batch -> {
                List<PartitionValueList> partitionValueLists =
                    batch.stream()
                        .map(
                            CatalogPartition ->
                                PartitionValueList.builder()
                                    .values(CatalogPartition.getValues())
                                    .build())
                        .collect(Collectors.toList());

                BatchDeletePartitionRequest batchDeletePartitionRequest =
                    BatchDeletePartitionRequest.builder()
                        .databaseName(tableIdentifier.getDatabaseName())
                        .tableName(tableIdentifier.getTableName())
                        .partitionsToDelete(partitionValueLists)
                        .build();
                responses.add(glueClient.batchDeletePartition(batchDeletePartitionRequest));
              });

      responses.forEach(
          response -> {
            if (CollectionUtils.nonEmpty(response.errors())) {
              throw new CatalogSyncException(
                  "Fail to drop partitions to "
                      + tableIdentifier
                      + " with error(s): "
                      + response.errors());
            }
          });
    } catch (Exception e) {
      throw new CatalogSyncException("Fail to drop partitions to " + tableIdentifier, e);
    }
  }

  @Override
  public Map<String, String> getTableProperties(
      CatalogTableIdentifier tableIdentifier, List<String> keysToRetrieve) {
    try {
      Table table =
          GlueCatalogTableUtils.getTable(
              glueClient, glueCatalogConfig.getCatalogId(), tableIdentifier);
      Map<String, String> tableParameters = table.parameters();

      return keysToRetrieve.stream()
          .filter(tableParameters::containsKey)
          .collect(Collectors.toMap(key -> key, tableParameters::get));
    } catch (Exception e) {
      throw new CatalogSyncException(
          "failed to get last time synced properties for table " + tableIdentifier, e);
    }
  }

  @Override
  public void updateTableProperties(
      CatalogTableIdentifier catalogTableIdentifier, Map<String, String> propertiesToUpdate) {
    HierarchicalTableIdentifier tableIdentifier =
        toHierarchicalTableIdentifier(catalogTableIdentifier);
    if (isNullOrEmpty(propertiesToUpdate)) {
      return;
    }
    try {
      Table table =
          GlueCatalogTableUtils.getTable(
              glueClient, glueCatalogConfig.getCatalogId(), catalogTableIdentifier);

      final Map<String, String> newParams = new HashMap<>();
      newParams.putAll(table.parameters());
      newParams.putAll(propertiesToUpdate);

      final Instant now = Instant.now();
      TableInput updatedTableInput =
          TableInput.builder()
              .name(tableIdentifier.getTableName())
              .tableType(table.tableType())
              .parameters(newParams)
              .partitionKeys(table.partitionKeys())
              .storageDescriptor(table.storageDescriptor())
              .lastAccessTime(now)
              .lastAnalyzedTime(now)
              .build();

      UpdateTableRequest request =
          UpdateTableRequest.builder()
              .databaseName(tableIdentifier.getDatabaseName())
              .tableInput(updatedTableInput)
              .skipArchive(true)
              .build();
      glueClient.updateTable(request);
    } catch (Exception e) {
      throw new CatalogSyncException(
          "Fail to update last synced params for table "
              + tableIdentifier
              + ": "
              + propertiesToUpdate,
          e);
    }
  }

  private PartitionInput createPartitionInput(Table table, CatalogPartition partition) {
    StorageDescriptor sd = table.storageDescriptor();
    StorageDescriptor partitionSD =
        sd.copy(copySd -> copySd.location(partition.getStorageLocation()));
    return PartitionInput.builder()
        .values(partition.getValues())
        .storageDescriptor(partitionSD)
        .build();
  }
}
