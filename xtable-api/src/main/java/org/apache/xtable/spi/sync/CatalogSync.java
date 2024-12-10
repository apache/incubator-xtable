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

import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.model.sync.SyncResult.CatalogSyncStatus;

@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CatalogSync {
  private static final CatalogSync INSTANCE = new CatalogSync();

  public static CatalogSync getInstance() {
    return INSTANCE;
  }

  public <TABLE> Map<String, List<CatalogSyncStatus>> syncTable(
      Collection<CatalogSyncClient<TABLE>> catalogSyncClients, InternalTable table) {
    Map<String, List<CatalogSyncStatus>> results = new HashMap<>();
    for (CatalogSyncClient<TABLE> catalogSyncClient : catalogSyncClients) {
      results.computeIfAbsent(catalogSyncClient.getTableFormat(), k -> new ArrayList<>());
      try {
        results
            .get(catalogSyncClient.getTableFormat())
            .add(getCatalogSyncStatus(catalogSyncClient, table));
      } catch (Exception e) {
        log.error(
            "Catalog sync failed for table {} using catalogSync {}",
            table.getBasePath(),
            catalogSyncClient.getCatalogImpl());
        results
            .get(catalogSyncClient.getTableFormat())
            .add(
                getCatalogSyncFailureStatus(
                    catalogSyncClient.getCatalogIdentifier(),
                    catalogSyncClient.getCatalogImpl(),
                    e));
      }
    }
    return results;
  }

  private <TABLE> CatalogSyncStatus getCatalogSyncStatus(
      CatalogSyncClient<TABLE> catalogSyncClient, InternalTable table) {
    CatalogTableIdentifier tableIdentifier = catalogSyncClient.getTableIdentifier();
    if (!catalogSyncClient.hasDatabase(tableIdentifier.getDatabaseName())) {
      catalogSyncClient.createDatabase(tableIdentifier.getDatabaseName());
    }
    TABLE catalogTable = catalogSyncClient.getTable(tableIdentifier);
    String storageDescriptorLocation = catalogSyncClient.getStorageDescriptorLocation(catalogTable);
    if (catalogTable == null) {
      catalogSyncClient.createTable(table, tableIdentifier);
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
      catalogSyncClient.createOrReplaceTable(table, tableIdentifier);
    } else {
      log.debug("Table metadata changed, refreshing table");
      catalogSyncClient.refreshTable(table, catalogTable, tableIdentifier);
    }
    return CatalogSyncStatus.builder()
        .catalogIdentifier(catalogSyncClient.getCatalogIdentifier())
        .statusCode(SyncResult.SyncStatusCode.SUCCESS)
        .build();
  }

  private CatalogSyncStatus getCatalogSyncFailureStatus(
      String catalogIdentifier, String catalogImpl, Exception e) {
    return CatalogSyncStatus.builder()
        .catalogIdentifier(catalogIdentifier)
        .statusCode(SyncResult.SyncStatusCode.ERROR)
        .errorDetails(
            SyncResult.ErrorDetails.builder()
                .errorMessage(e.getMessage())
                .errorDescription("catalogSync failed for " + catalogImpl)
                .build())
        .build();
  }
}
