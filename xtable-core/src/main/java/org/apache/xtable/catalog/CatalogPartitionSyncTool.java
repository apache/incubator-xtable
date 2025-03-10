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

import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;

/**
 * Defines methods to synchronize all partitions from the storage to the catalog. Implementations of
 * this interface will handle the logic for syncing partitions, including detecting partition
 * changes and updating the catalog accordingly.
 */
public interface CatalogPartitionSyncTool {

  /**
   * Syncs all partitions on storage to the catalog.
   *
   * @param oneTable The object representing the table whose partitions are being synced. This
   *     object contains necessary details to perform the sync operation.
   * @param tableIdentifier The table in the catalog.
   * @return {@code true} if one or more partition(s) are changed in the catalog; {@code false}
   *     otherwise.
   */
  public boolean syncPartitions(InternalTable oneTable, CatalogTableIdentifier tableIdentifier);
}
