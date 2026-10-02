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
 
package org.apache.xtable.hms;

import static org.apache.xtable.catalog.CatalogUtils.toHierarchicalTableIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.exception.TableNotFoundException;

import org.apache.xtable.catalog.CatalogPartition;
import org.apache.xtable.catalog.CatalogPartitionSyncOperations;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;

@Log4j2
public class HMSCatalogPartitionSyncOperations implements CatalogPartitionSyncOperations {

  private final IMetaStoreClient metaStoreClient;
  private final HMSCatalogConfig catalogConfig;

  public HMSCatalogPartitionSyncOperations(
      IMetaStoreClient metaStoreClient, HMSCatalogConfig hmsCatalogConfig) {
    this.metaStoreClient = metaStoreClient;
    this.catalogConfig = hmsCatalogConfig;
  }

  @Override
  public List<CatalogPartition> getAllPartitions(CatalogTableIdentifier catalogTableIdentifier) {
    HierarchicalTableIdentifier tableIdentifier =
        toHierarchicalTableIdentifier(catalogTableIdentifier);
    try {
      return metaStoreClient
          .listPartitions(
              tableIdentifier.getDatabaseName(), tableIdentifier.getTableName(), (short) -1)
          .stream()
          .map(p -> new CatalogPartition(p.getValues(), p.getSd().getLocation()))
          .collect(Collectors.toList());
    } catch (TException e) {
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
    log.info("Adding partitions {} to table {}", partitionsToAdd.size(), tableIdentifier);
    try {
      StorageDescriptor sd =
          metaStoreClient
              .getTable(tableIdentifier.getDatabaseName(), tableIdentifier.getTableName())
              .getSd();

      CollectionUtils.batches(partitionsToAdd, catalogConfig.getMaxPartitionsPerRequest())
          .forEach(
              batch -> {
                List<Partition> partitionList = new ArrayList<>();
                batch.forEach(
                    partition -> {
                      partitionList.add(createPartition(tableIdentifier, partition, sd));
                    });
                try {
                  metaStoreClient.add_partitions(partitionList, true, false);
                } catch (TException e) {
                  log.error("{} add partition failed", tableIdentifier, e);
                  throw new CatalogSyncException(tableIdentifier + " add partition failed", e);
                }
                log.info("Add batch partitions done: {}", partitionList.size());
              });
    } catch (TException e) {
      log.error("Failed to add partitions to table {}", tableIdentifier, e);
      throw new CatalogSyncException(tableIdentifier + " add partition failed", e);
    }
  }

  @Override
  public void updatePartitionsToTable(
      CatalogTableIdentifier catalogTableIdentifier, List<CatalogPartition> changedPartitions) {
    HierarchicalTableIdentifier tableIdentifier =
        toHierarchicalTableIdentifier(catalogTableIdentifier);
    try {
      Table table =
          metaStoreClient.getTable(
              tableIdentifier.getDatabaseName(), tableIdentifier.getTableName());
      StorageDescriptor tableSd = table.getSd();

      List<Partition> updatedPartitions = new ArrayList<>();

      changedPartitions.forEach(
          partition -> {
            updatedPartitions.add(createPartition(tableIdentifier, partition, tableSd));
          });

      // Update partitions
      metaStoreClient.alter_partitions(
          tableIdentifier.getDatabaseName(), tableIdentifier.getTableName(), updatedPartitions);
    } catch (TException e) {
      throw new CatalogSyncException(
          "Failed to update partitions for the table " + tableIdentifier, e);
    }
  }

  @Override
  public void dropPartitions(
      CatalogTableIdentifier catalogTableIdentifier, List<CatalogPartition> partitionsToDrop) {
    HierarchicalTableIdentifier tableIdentifier =
        toHierarchicalTableIdentifier(catalogTableIdentifier);
    try {
      for (CatalogPartition partition : partitionsToDrop) {
        metaStoreClient.dropPartition(
            tableIdentifier.getDatabaseName(),
            tableIdentifier.getTableName(),
            partition.getValues(),
            false);
      }
    } catch (TException e) {
      throw new CatalogSyncException("Failed to drop partitions for table " + tableIdentifier, e);
    }
  }

  @Override
  public Map<String, String> getTableProperties(
      CatalogTableIdentifier catalogTableIdentifier, List<String> keysToRetrieve) {
    HierarchicalTableIdentifier tableIdentifier =
        toHierarchicalTableIdentifier(catalogTableIdentifier);
    try {
      Table table =
          metaStoreClient.getTable(
              tableIdentifier.getDatabaseName(), tableIdentifier.getTableName());
      Map<String, String> tableParameters = table.getParameters();

      return keysToRetrieve.stream()
          .filter(tableParameters::containsKey)
          .collect(Collectors.toMap(key -> key, tableParameters::get));
    } catch (TableNotFoundException | TException e) {
      throw new CatalogSyncException(
          "failed to fetch last time synced properties for table" + tableIdentifier, e);
    }
  }

  @Override
  public void updateTableProperties(
      CatalogTableIdentifier catalogTableIdentifier, Map<String, String> propertiesToUpdate) {
    HierarchicalTableIdentifier tableIdentifier =
        toHierarchicalTableIdentifier(catalogTableIdentifier);
    try {
      if (propertiesToUpdate == null || propertiesToUpdate.isEmpty()) {
        return;
      }

      Table table =
          metaStoreClient.getTable(
              tableIdentifier.getDatabaseName(), tableIdentifier.getTableName());
      Map<String, String> tableParameters = table.getParameters();
      tableParameters.putAll(propertiesToUpdate);
      table.setParameters(tableParameters);
      metaStoreClient.alter_table(
          tableIdentifier.getDatabaseName(), tableIdentifier.getTableName(), table);
    } catch (TableNotFoundException | TException e) {
      throw new CatalogSyncException(
          "failed to update last time synced properties for table" + tableIdentifier, e);
    }
  }

  private Partition createPartition(
      HierarchicalTableIdentifier tableIdentifier,
      CatalogPartition partition,
      StorageDescriptor sd) {
    StorageDescriptor partitionSd = new StorageDescriptor();
    partitionSd.setCols(sd.getCols());
    partitionSd.setInputFormat(sd.getInputFormat());
    partitionSd.setOutputFormat(sd.getOutputFormat());
    partitionSd.setSerdeInfo(sd.getSerdeInfo());
    partitionSd.setLocation(partition.getStorageLocation());

    return new Partition(
        partition.getValues(),
        tableIdentifier.getDatabaseName(),
        tableIdentifier.getTableName(),
        0,
        0,
        partitionSd,
        null);
  }
}
