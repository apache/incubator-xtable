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
 
package org.apache.xtable.iceberg;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;

@AllArgsConstructor(staticName = "of")
@Log4j2
class IcebergTableManager {
  private static final Map<IcebergCatalogConfig, Catalog> CATALOG_CACHE = new ConcurrentHashMap<>();
  private final Configuration hadoopConfiguration;

  @Getter(lazy = true, value = lombok.AccessLevel.PRIVATE)
  private final HadoopTables hadoopTables = new HadoopTables(hadoopConfiguration);

  Table getTable(
      IcebergCatalogConfig catalogConfig, TableIdentifier tableIdentifier, String basePath) {
    return getCatalog(catalogConfig)
        .map(catalog -> catalog.loadTable(tableIdentifier))
        .orElseGet(() -> getHadoopTables().load(basePath));
  }

  boolean tableExists(
      IcebergCatalogConfig catalogConfig, TableIdentifier tableIdentifier, String basePath) {
    return getCatalog(catalogConfig)
        .map(catalog -> catalog.tableExists(tableIdentifier))
        .orElseGet(() -> getHadoopTables().exists(basePath));
  }

  Table getOrCreateTable(
      IcebergCatalogConfig catalogConfig,
      TableIdentifier tableIdentifier,
      String basePath,
      Schema schema,
      PartitionSpec partitionSpec) {
    if (tableExists(catalogConfig, tableIdentifier, basePath)) {
      return getTable(catalogConfig, tableIdentifier, basePath);
    } else {
      try {
        return getCatalog(catalogConfig)
            .map(
                catalog ->
                    catalog.createTable(
                        tableIdentifier,
                        schema,
                        partitionSpec,
                        basePath,
                        getDefaultMappingProperties(schema)))
            .orElseGet(
                () ->
                    getHadoopTables()
                        .create(
                            schema,
                            partitionSpec,
                            SortOrder.unsorted(),
                            getDefaultMappingProperties(schema),
                            basePath));
      } catch (AlreadyExistsException ex) {
        log.info("Table {} not created since it already exists", tableIdentifier);
        return getTable(catalogConfig, tableIdentifier, basePath);
      }
    }
  }

  private Map<String, String> getDefaultMappingProperties(Schema schema) {
    return Collections.singletonMap(
        TableProperties.DEFAULT_NAME_MAPPING, NameMappingParser.toJson(MappingUtil.create(schema)));
  }

  private Optional<Catalog> getCatalog(IcebergCatalogConfig catalogConfig) {
    if (catalogConfig == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(
        CATALOG_CACHE.computeIfAbsent(
            catalogConfig,
            config ->
                CatalogUtil.loadCatalog(
                    config.getCatalogImpl(),
                    config.getCatalogName(),
                    config.getCatalogOptions(),
                    hadoopConfiguration)));
  }
}
