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
import java.util.function.Supplier;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.hadoop.HadoopTables;

@AllArgsConstructor(staticName = "of")
@Log4j2
public class IcebergTableManager {
  /**
   * Hadoop configuration key selecting the Iceberg table format version to write when creating a
   * table. Defaults to {@link #DEFAULT_ICEBERG_FORMAT_VERSION}.
   */
  public static final String ICEBERG_FORMAT_VERSION = "xtable.iceberg.format-version";

  /**
   * Hadoop configuration key: when true, a table this manager creates does not keep the empty
   * placeholder schema it is first created with (see {@link #createTableIfMissing}). Some readers
   * refuse a table whose metadata holds a schema with no fields, and the AWS Glue Iceberg REST
   * endpoint drops the table's name mapping property for such a table. Defaults to false.
   */
  public static final String ICEBERG_REMOVE_PLACEHOLDER_SCHEMA =
      "xtable.iceberg.remove-placeholder-schema";

  private static final int DEFAULT_ICEBERG_FORMAT_VERSION = 2;
  private static final Map<IcebergCatalogConfig, Catalog> CATALOG_CACHE = new ConcurrentHashMap<>();
  private final Configuration hadoopConfiguration;

  @Getter(lazy = true, value = lombok.AccessLevel.PRIVATE)
  private final HadoopTables hadoopTables = new HadoopTables(hadoopConfiguration);

  public Table getTable(
      IcebergCatalogConfig catalogConfig, TableIdentifier tableIdentifier, String basePath) {
    return withCatalogClassLoader(
        catalogConfig,
        () ->
            getCatalog(catalogConfig)
                .map(catalog -> catalog.loadTable(tableIdentifier))
                .orElseGet(() -> getHadoopTables().load(basePath)));
  }

  public boolean tableExists(
      IcebergCatalogConfig catalogConfig, TableIdentifier tableIdentifier, String basePath) {
    return withCatalogClassLoader(
        catalogConfig,
        () ->
            getCatalog(catalogConfig)
                .map(catalog -> catalog.tableExists(tableIdentifier))
                .orElseGet(() -> getHadoopTables().exists(basePath)));
  }

  Table getOrCreateTable(
      IcebergCatalogConfig catalogConfig,
      TableIdentifier tableIdentifier,
      String basePath,
      Schema schema,
      PartitionSpec partitionSpec) {
    return withCatalogClassLoader(
        catalogConfig,
        () ->
            createTableIfMissing(catalogConfig, tableIdentifier, basePath, schema, partitionSpec));
  }

  private Table createTableIfMissing(
      IcebergCatalogConfig catalogConfig,
      TableIdentifier tableIdentifier,
      String basePath,
      Schema schema,
      PartitionSpec partitionSpec) {
    if (tableExists(catalogConfig, tableIdentifier, basePath)) {
      return getTable(catalogConfig, tableIdentifier, basePath);
    } else {
      try {
        // initialize the table with an empty schema, then manually set the schema to prevent the
        // Iceberg API from remapping the field IDs.
        Table tableWithEmptySchema =
            getCatalog(catalogConfig)
                .map(
                    catalog ->
                        catalog.createTable(
                            tableIdentifier,
                            new Schema(),
                            PartitionSpec.unpartitioned(),
                            basePath,
                            Collections.emptyMap()))
                .orElseGet(
                    () ->
                        getHadoopTables()
                            .create(
                                new Schema(),
                                PartitionSpec.unpartitioned(),
                                Collections.emptyMap(),
                                basePath));
        // set the schema with the provided field IDs
        TableOperations operations = ((BaseTable) tableWithEmptySchema).operations();
        TableMetadata tableMetadata = operations.current();
        TableMetadata.Builder builder = TableMetadata.buildFrom(tableMetadata);
        builder.setCurrentSchema(schema, schema.highestFieldId());
        builder.setDefaultPartitionSpec(partitionSpec);
        int formatVersion =
            hadoopConfiguration.getInt(ICEBERG_FORMAT_VERSION, DEFAULT_ICEBERG_FORMAT_VERSION);
        // Requesting the version the table was created with is a no-op; Iceberg rejects a lower
        // one.
        builder.upgradeFormatVersion(formatVersion);
        if (hadoopConfiguration.getBoolean(ICEBERG_REMOVE_PLACEHOLDER_SCHEMA, false)
            && tableMetadata.schema().columns().isEmpty()
            && !schema.columns().isEmpty()) {
          removeSchema(builder, tableMetadata.currentSchemaId());
        }
        operations.commit(tableMetadata, builder.build());
        return getTable(catalogConfig, tableIdentifier, basePath);
      } catch (AlreadyExistsException ex) {
        log.info("Table {} not created since it already exists", tableIdentifier);
        return getTable(catalogConfig, tableIdentifier, basePath);
      }
    }
  }

  /**
   * Removes the empty placeholder schema in the same commit that sets the real one. Iceberg exposes
   * schema removal only through its metadata builder, package-private, and through {@code
   * ExpireSnapshots.cleanExpiredMetadata}, which also removes partition specs; the AWS Glue Iceberg
   * REST endpoint rejects a {@code remove-partition-specs} update, so the unused placeholder spec
   * is kept. A REST catalog receives this as a {@code remove-schemas} update. If the builder method
   * is not available, the placeholder stays.
   */
  private static void removeSchema(TableMetadata.Builder builder, int schemaId) {
    try {
      java.lang.reflect.Method removeSchemas =
          TableMetadata.Builder.class.getDeclaredMethod("removeSchemas", Iterable.class);
      removeSchemas.setAccessible(true);
      removeSchemas.invoke(builder, Collections.singleton(schemaId));
    } catch (ReflectiveOperationException | RuntimeException e) {
      log.warn(
          "Could not remove the placeholder schema {}; it stays in the table metadata",
          schemaId,
          e);
    }
  }

  private Optional<Catalog> getCatalog(IcebergCatalogConfig catalogConfig) {
    if (catalogConfig == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(CATALOG_CACHE.computeIfAbsent(catalogConfig, this::loadCatalog));
  }

  private Catalog loadCatalog(IcebergCatalogConfig config) {
    return CatalogUtil.loadCatalog(
        config.getCatalogImpl(),
        config.getCatalogName(),
        config.getCatalogOptions(),
        hadoopConfiguration);
  }

  /**
   * Runs a catalog operation with a context class loader that can see the catalog. Iceberg loads
   * the catalog class, and classes the catalog loads later such as its HTTP client configuration,
   * through the thread's context class loader. On a shared ForkJoin pool thread (a parallel stream)
   * that is the system class loader, which cannot see jars added at runtime such as Spark's {@code
   * --jars}.
   */
  private static <T> T withCatalogClassLoader(IcebergCatalogConfig config, Supplier<T> action) {
    if (config == null) {
      return action.get();
    }
    Thread thread = Thread.currentThread();
    ClassLoader previous = thread.getContextClassLoader();
    ClassLoader loader = catalogClassLoader(config.getCatalogImpl(), previous);
    if (loader == previous) {
      return action.get();
    }
    thread.setContextClassLoader(loader);
    try {
      return action.get();
    } finally {
      thread.setContextClassLoader(previous);
    }
  }

  private static ClassLoader catalogClassLoader(String catalogImpl, ClassLoader contextLoader) {
    if (contextLoader != null) {
      try {
        Class.forName(catalogImpl, false, contextLoader);
        return contextLoader;
      } catch (ClassNotFoundException e) {
        log.debug("Catalog {} is not visible to the context class loader", catalogImpl);
      }
    }
    return IcebergTableManager.class.getClassLoader();
  }
}
