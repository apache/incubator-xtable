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
 
package org.apache.xtable.catalog;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.xtable.model.catalog.CatalogTableIdentifier;

/**
 * Defines operations for managing partitions in an external catalog.
 *
 * <p>This interface provides methods to perform CRUD (Create, Read, Update, Delete) operations on
 * partitions associated with a table in an external catalog system.
 */
public interface CatalogPartitionSyncOperations {

  /**
   * Retrieves all partitions associated with the specified table.
   *
   * @param tableIdentifier an object identifying the table whose partitions are to be fetched.
   * @return a list of {@link CatalogPartition} objects representing all partitions of the specified
   *     table.
   */
  List<CatalogPartition> getAllPartitions(CatalogTableIdentifier tableIdentifier);

  /**
   * Adds new partitions to the specified table in the catalog.
   *
   * @param tableIdentifier an object identifying the table where partitions are to be added.
   * @param partitionsToAdd a list of partitions to be added to the table.
   */
  void addPartitionsToTable(
      CatalogTableIdentifier tableIdentifier, List<CatalogPartition> partitionsToAdd);

  /**
   * Updates the specified partitions for a table in the catalog.
   *
   * @param tableIdentifier an object identifying the table whose partitions are to be updated.
   * @param changedPartitions a list of partitions to be updated in the table.
   */
  void updatePartitionsToTable(
      CatalogTableIdentifier tableIdentifier, List<CatalogPartition> changedPartitions);

  /**
   * Removes the specified partitions from a table in the catalog.
   *
   * @param tableIdentifier an object identifying the table from which partitions are to be dropped.
   * @param partitionsToDrop a list of partitions to be removed from the table.
   */
  void dropPartitions(
      CatalogTableIdentifier tableIdentifier, List<CatalogPartition> partitionsToDrop);

  /**
   * Retrieves the properties indicating the last synchronization state for the given table.
   *
   * <p>This method provides a default implementation that returns an empty map. Implementations of
   * this interface can override it to fetch the actual last synced properties from a catalog.
   *
   * @param tableIdentifier the identifier of the table whose last synced properties are to be
   *     fetched.
   * @param keysToRetrieve a list of keys representing the specific properties to retrieve.
   * @return a map of key-value pairs representing the last synchronization properties.
   */
  default Map<String, String> getTableProperties(
      CatalogTableIdentifier tableIdentifier, List<String> keysToRetrieve) {
    return new HashMap<>();
  }

  /**
   * Updates the properties indicating the last synchronization state for the given table.
   *
   * <p>This method provides a default implementation that performs no operation. Implementations of
   * this interface can override it to update the last synced properties in a catalog.
   *
   * @param tableIdentifier the identifier of the table whose last synced properties are to be
   *     updated.
   * @param propertiesToUpdate a map of key-value pairs representing the updated properties.
   */
  default void updateTableProperties(
      CatalogTableIdentifier tableIdentifier, Map<String, String> propertiesToUpdate) {}
}
