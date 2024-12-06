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
 
package org.apache.xtable.spi.sync;

import static org.apache.xtable.spi.sync.CatalogUtils.hasStorageDescriptorLocationChanged;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.commons.lang3.StringUtils;

import org.apache.xtable.conversion.ExternalCatalog;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.exception.CatalogRefreshException;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.model.sync.SyncResult.CatalogSyncStatus;

@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CatalogSync {
  private static final CatalogSync INSTANCE = new CatalogSync();

  public static CatalogSync getInstance() {
    return INSTANCE;
  }

  public Map<String, List<CatalogSyncStatus>> syncTable(
      Collection<CatalogSyncOperations> catalogSyncOperations, InternalTable table) {
    Map<String, List<CatalogSyncStatus>> results = new HashMap<>();
    for (CatalogSyncOperations catalogSyncOperation : catalogSyncOperations) {
      results.computeIfAbsent(catalogSyncOperation.getTableFormat(), k -> new ArrayList<>());
      results
          .get(catalogSyncOperation.getTableFormat())
          .add(getCatalogSyncStatus(catalogSyncOperation, table));
    }
    return results;
  }

  private CatalogSyncStatus getCatalogSyncStatus(
      CatalogSyncOperations catalogSyncOperation, InternalTable table) {
    ExternalCatalog.TableIdentifier tableIdentifier = catalogSyncOperation.getTableIdentifier();
    boolean doesDatabaseExists =
        catalogSyncOperation.getDatabase(tableIdentifier.getDatabaseName()) != null;
    if (!doesDatabaseExists) {
      catalogSyncOperation.createDatabase(tableIdentifier.getDatabaseName());
    }
    Object catalogTable = catalogSyncOperation.getTable(tableIdentifier);
    String storageDescriptorLocation =
        catalogSyncOperation.getStorageDescriptorLocation(catalogTable);
    if (catalogTable == null) {
      catalogSyncOperation.createTable(table, tableIdentifier);
    } else if (hasStorageDescriptorLocationChanged(
        storageDescriptorLocation, table.getBasePath())) {
      // Replace table if there is a mismatch between hmsTable location and Xtable basePath.
      // Possible reasons could be:
      //  1) hms table (manually) created with a different location before and need to be
      // re-created with a new basePath
      //  2) XTable basePath changes due to migration or other reasons
      String oldLocation =
          StringUtils.isEmpty(storageDescriptorLocation) ? "null" : storageDescriptorLocation;
      log.warn(
          "StorageDescriptor location changed from {} to {}, re-creating table",
          oldLocation,
          table.getBasePath());
      catalogSyncOperation.createOrReplaceTable(table, tableIdentifier);
    } else {
      try {
        log.debug("Table metadata changed, refreshing table");
        catalogSyncOperation.refreshTable(table, catalogTable, tableIdentifier);
      } catch (CatalogRefreshException e) {
        log.warn("Table refresh failed, re-creating table", e);
        catalogSyncOperation.createOrReplaceTable(table, tableIdentifier);
      }
    }
    return CatalogSyncStatus.builder()
        .catalogIdentifier(catalogSyncOperation.getCatalogIdentifier())
        .statusCode(SyncResult.SyncStatusCode.SUCCESS)
        .build();
  }
}
