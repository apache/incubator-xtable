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
 
package org.apache.xtable.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.hudi.catalog.HoodieCatalog;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.iceberg.aws.glue.GlueCatalog;
import org.apache.iceberg.spark.SparkCatalog;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.catalog.TableFormatUtils;
import org.apache.xtable.model.storage.TableFormat;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;

/**
 * XTable Spark Catalog Plugin - Unified catalog implementation for both Hudi and Iceberg tables.
 * This plugin leverages XTable's existing format detection capabilities and provides a seamless
 * experience for querying mixed table formats through a single Spark catalog.
 */
public class XTableSparkCatalog implements CatalogPlugin, TableCatalog, SupportsNamespaces {

  private static final Logger LOG = LoggerFactory.getLogger(XTableSparkCatalog.class);

  private String catalogName;
  private SparkCatalog icebergCatalog;
  private HoodieCatalog hudiCatalog;
  private GlueClient glueClient;
  private CaseInsensitiveStringMap options;

  @Override
  public void initialize(String name, CaseInsensitiveStringMap options) {
    this.catalogName = name;
    this.options = options;

    LOG.info("Initializing XTable Spark Catalog Plugin with name: {}", name);

    this.glueClient = buildGlueClient(options);

    SparkSession spark = SparkSession.active();
    spark.conf().set("hoodie.schema.on.read.enable", "true");

    initializeIcebergCatalog(name, options);
    initializeHudiCatalog(options);
  }

  private GlueClient buildGlueClient(CaseInsensitiveStringMap options) {
    String region =
        options.getOrDefault("glue.region", options.getOrDefault("aws.region", "us-west-2"));

    LOG.info("Configuring Glue client with region: {}", region);

    GlueClient client =
        GlueClient.builder().region(software.amazon.awssdk.regions.Region.of(region)).build();

    // Log catalog ID if specified (for cross-account access)
    if (options.containsKey("catalog-id")) {
      String catalogId = options.get("catalog-id");
      LOG.info("Using Glue catalog ID: {} (for cross-account access)", catalogId);
    }

    return client;
  }

  private void initializeIcebergCatalog(String name, CaseInsensitiveStringMap options) {
    LOG.info("Initializing Iceberg sub-catalog");

    icebergCatalog = new SparkCatalog();

    Map<String, String> icebergOptions = new HashMap<>(options);
    icebergOptions.put("catalog-impl", GlueCatalog.class.getName());

    if (options.containsKey("warehouse")) {
      icebergOptions.put("warehouse", options.get("warehouse"));
    }

    // Pass catalog-id for cross-account Glue access if specified
    if (options.containsKey("catalog-id")) {
      icebergOptions.put("catalog-id", options.get("catalog-id"));
      LOG.info("Configured Iceberg catalog with catalog-id: {}", options.get("catalog-id"));
    }

    icebergCatalog.initialize(name, new CaseInsensitiveStringMap(icebergOptions));
  }

  private void initializeHudiCatalog(CaseInsensitiveStringMap options) {
    LOG.info("Initializing Hudi sub-catalog");

    hudiCatalog = new HoodieCatalog();

    // Set up delegation to spark_catalog for metastore operations
    // This allows Hudi to access the underlying Glue/Hive metastore through spark_catalog
    try {
      SparkSession spark = SparkSession.active();
      CatalogPlugin sparkCatalog = spark.sessionState().catalogManager().catalog("spark_catalog");
      hudiCatalog.setDelegateCatalog(sparkCatalog);
      LOG.info("Successfully set spark_catalog as delegate for HoodiCatalog");
    } catch (Exception e) {
      LOG.error(
          "Failed to set delegate catalog for HoodieCatalog. This may cause table operations to fail.",
          e);
      throw new RuntimeException(
          "Failed to initialize HoodieCatalog with spark_catalog delegate", e);
    }

    Map<String, String> hudiOptions = new HashMap<>(options);

    hudiOptions.put("provider", "hudi");
    hudiOptions.put("spark.sql.sources.provider", "hudi");

    LOG.debug("HudiCatalog options: {}", hudiOptions);

    hudiCatalog.initialize("spark_catalog", new CaseInsensitiveStringMap(hudiOptions));
  }

  @Override
  public String name() {
    return catalogName;
  }

  @Override
  public Identifier[] listTables(String[] namespace) {
    Set<Identifier> allTables = new HashSet<>();

    if (icebergCatalog != null) {
      try {
        allTables.addAll(Arrays.asList(icebergCatalog.listTables(namespace)));
      } catch (Exception e) {
        LOG.warn("Failed to list Iceberg tables in namespace: {}", Arrays.toString(namespace), e);
      }
    }

    if (hudiCatalog != null) {
      try {
        allTables.addAll(Arrays.asList(hudiCatalog.listTables(namespace)));
      } catch (Exception e) {
        LOG.warn("Failed to list Hudi tables in namespace: {}", Arrays.toString(namespace), e);
      }
    }

    LOG.info("Found {} tables in namespace: {}", allTables.size(), Arrays.toString(namespace));
    return allTables.toArray(new Identifier[0]);
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    LOG.info("Loading table: {}", ident);

    String tableFormat = getTableFormat(ident);

    try {
      switch (tableFormat.toUpperCase()) {
        case TableFormat.ICEBERG:
          LOG.info("Loading as Iceberg table: {}", ident);
          return icebergCatalog.loadTable(ident);

        case TableFormat.HUDI:
          LOG.info("Loading as Hudi table: {}", ident);
          Table hudiTable = hudiCatalog.loadTable(ident);
          LOG.debug(
              "HudiCatalog returned table class: {}, name: {}",
              hudiTable.getClass().getName(),
              hudiTable.name());
          return hudiTable;

        default:
          throw new NoSuchTableException(ident);
      }
    } catch (Exception e) {
      LOG.error("Failed to load table: {}", ident, e);
      throw new NoSuchTableException(ident);
    }
  }

  @Override
  @SuppressWarnings("RedundantThrows")
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException, NoSuchNamespaceException {

    LOG.info("Creating table: {} with properties: {}", ident, properties);

    String tableFormat = determineTableFormatFromProperties(properties);

    Table table;

    switch (tableFormat.toUpperCase()) {
      case TableFormat.HUDI:
        LOG.info("Creating Hudi table: {}", ident);
        table = hudiCatalog.createTable(ident, schema, partitions, properties);
        break;

      case TableFormat.ICEBERG:
        LOG.info("Creating Iceberg table: {}", ident);
        table = icebergCatalog.createTable(ident, schema, partitions, properties);
        break;

      default:
        LOG.info("No specific format specified, defaulting to Iceberg for table: {}", ident);
        table = icebergCatalog.createTable(ident, schema, partitions, properties);
        break;
    }

    return table;
  }

  @Override
  public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
    String tableFormat = getTableFormat(ident);

    switch (tableFormat.toUpperCase()) {
      case TableFormat.ICEBERG:
        return icebergCatalog.alterTable(ident, changes);

      case TableFormat.HUDI:
        return hudiCatalog.alterTable(ident, changes);

      default:
        throw new NoSuchTableException(ident);
    }
  }

  @Override
  public boolean dropTable(Identifier ident) {
    LOG.info("Dropping table: {}", ident);

    try {
      String tableFormat = getTableFormat(ident);
      boolean dropped = false;

      switch (tableFormat.toUpperCase()) {
        case TableFormat.ICEBERG:
          dropped = icebergCatalog.dropTable(ident);
          break;

        case TableFormat.HUDI:
          dropped = hudiCatalog.dropTable(ident);
          break;
      }

      return dropped;
    } catch (Exception e) {
      LOG.error("Failed to drop table: {}", ident, e);
      return false;
    }
  }

  @Override
  public void renameTable(Identifier oldIdent, Identifier newIdent)
      throws NoSuchTableException, TableAlreadyExistsException {

    String tableFormat = getTableFormat(oldIdent);

    switch (tableFormat.toUpperCase()) {
      case TableFormat.ICEBERG:
        icebergCatalog.renameTable(oldIdent, newIdent);
        break;

      case TableFormat.HUDI:
        hudiCatalog.renameTable(oldIdent, newIdent);
        break;

      default:
        throw new NoSuchTableException(oldIdent);
    }
  }

  @Override
  public void createNamespace(String[] namespace, Map<String, String> metadata)
      throws NamespaceAlreadyExistsException {
    LOG.info("Creating namespace: {} with metadata: {}", Arrays.toString(namespace), metadata);

    // Since both Hudi (via Hive-Glue connector) and Iceberg (via direct Glue) use the same
    // underlying Glue database, only create the namespace once using Iceberg catalog.
    icebergCatalog.createNamespace(namespace, metadata);

    LOG.info("Namespace created successfully: {}", Arrays.toString(namespace));
  }

  @Override
  public void alterNamespace(String[] namespace, NamespaceChange... changes)
      throws NoSuchNamespaceException {
    icebergCatalog.alterNamespace(namespace, changes);
  }

  @Override
  @SuppressWarnings("RedundantThrows")
  public boolean dropNamespace(String[] namespace, boolean cascade)
      throws NoSuchNamespaceException, NonEmptyNamespaceException {
    return icebergCatalog.dropNamespace(namespace, cascade);
  }

  @Override
  @SuppressWarnings("RedundantThrows")
  public String[][] listNamespaces() throws NoSuchNamespaceException {
    return icebergCatalog.listNamespaces();
  }

  @Override
  public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
    return icebergCatalog.listNamespaces(namespace);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(String[] namespace)
      throws NoSuchNamespaceException {

    LOG.info("Loading namespace metadata for: {}", Arrays.toString(namespace));

    // Use Iceberg catalog as the primary source for namespace metadata
    // since it has direct Glue access and more complete metadata support.
    try {
      Map<String, String> metadata = icebergCatalog.loadNamespaceMetadata(namespace);
      LOG.info("Found namespace metadata via Iceberg catalog for: {}", Arrays.toString(namespace));
      return metadata != null ? metadata : new HashMap<>();
    } catch (NoSuchNamespaceException e) {
      LOG.warn(
          "Namespace not found via Iceberg catalog, trying Hudi catalog: {}",
          Arrays.toString(namespace));

      // Fall back to Hudi catalog if Iceberg fails
      try {
        Map<String, String> metadata = hudiCatalog.loadNamespaceMetadata(namespace);
        LOG.info("Found namespace metadata via Hudi catalog for: {}", Arrays.toString(namespace));
        return metadata != null ? metadata : new HashMap<>();
      } catch (NoSuchNamespaceException hudiException) {
        LOG.error("Namespace not found in any catalog: {}", Arrays.toString(namespace));
        throw new NoSuchNamespaceException(namespace);
      } catch (Exception hudiException) {
        LOG.warn(
            "Failed to load Hudi namespace metadata for: {}",
            Arrays.toString(namespace),
            hudiException);
        throw new NoSuchNamespaceException(namespace);
      }
    } catch (Exception e) {
      LOG.warn("Failed to load Iceberg namespace metadata for: {}", Arrays.toString(namespace), e);
      throw new NoSuchNamespaceException(namespace);
    }
  }

  private String getTableFormat(Identifier ident) throws NoSuchTableException {
    String database = ident.namespace()[0];
    String tableName = ident.name();

    LOG.debug("Determining table format for: database='{}', table='{}'", database, tableName);

    try {
      GetTableRequest.Builder requestBuilder =
          GetTableRequest.builder().databaseName(database).name(tableName);

      if (options != null && options.containsKey("catalog-id")) {
        String catalogId = options.get("catalog-id");
        requestBuilder.catalogId(catalogId);
        LOG.debug("Using catalog ID: {}", catalogId);
      }

      GetTableRequest request = requestBuilder.build();
      GetTableResponse response = glueClient.getTable(request);
      software.amazon.awssdk.services.glue.model.Table glueTable = response.table();

      // Use XTable's TableFormatUtils for consistent format detection
      Map<String, String> parameters = glueTable.parameters();
      try {
        String tableFormat = TableFormatUtils.getTableFormat(parameters);
        LOG.debug("Detected table format '{}' for table: {}", tableFormat, ident);
        return tableFormat;
      } catch (IllegalArgumentException e) {
        LOG.warn(
            "Failed to determine table format from Glue parameters for: {}, trying StorageDescriptor",
            ident);

        // Fallback 1: Check StorageDescriptor for format hints
        StorageDescriptor sd = glueTable.storageDescriptor();
        if (sd != null) {
          String inputFormat = sd.inputFormat();
          String outputFormat = sd.outputFormat();
          String serdeLib = sd.serdeInfo() != null ? sd.serdeInfo().serializationLibrary() : null;

          if (isHudiFormat(inputFormat, outputFormat, serdeLib)) {
            LOG.debug("Detected Hudi format from StorageDescriptor for: {}", ident);
            return TableFormat.HUDI;
          }

          if (isIcebergFormat(inputFormat, outputFormat, serdeLib)) {
            LOG.debug("Detected Iceberg format from StorageDescriptor for: {}", ident);
            return TableFormat.ICEBERG;
          }
        }

        // Fallback 2: try loading from each catalog to determine format
        LOG.warn("StorageDescriptor check inconclusive, attempting catalog loading for: {}", ident);
      }

      try {
        icebergCatalog.loadTable(ident);
        return TableFormat.ICEBERG;
      } catch (Exception icebergException) {
        try {
          hudiCatalog.loadTable(ident);
          return TableFormat.HUDI;
        } catch (Exception hudiException) {
          throw new NoSuchTableException(ident);
        }
      }
    } catch (GlueException e) {
      LOG.error("Failed to get table from Glue: {}", ident, e);

      // Fallback: try loading from each catalog to determine format
      LOG.warn("Glue lookup failed, attempting catalog fallback for: {}", ident);

      try {
        icebergCatalog.loadTable(ident);
        return TableFormat.ICEBERG;
      } catch (Exception icebergException) {
        try {
          hudiCatalog.loadTable(ident);
          return TableFormat.HUDI;
        } catch (Exception hudiException) {
          throw new NoSuchTableException(ident);
        }
      }
    }
  }

  @VisibleForTesting
  String determineTableFormatFromProperties(Map<String, String> properties) {
    if (properties == null) {
      return TableFormat.ICEBERG;
    }

    try {
      return TableFormatUtils.getTableFormat(properties);
    } catch (IllegalArgumentException e) {
      String provider = properties.getOrDefault("provider", "").toLowerCase();
      String tableType = properties.getOrDefault("table_type", "").toUpperCase();

      if (provider.contains("hudi") || TableFormat.HUDI.equals(tableType)) {
        return TableFormat.HUDI;
      } else if (provider.contains("iceberg") || TableFormat.ICEBERG.equals(tableType)) {
        return TableFormat.ICEBERG;
      } else {
        return TableFormat.ICEBERG;
      }
    }
  }

  @VisibleForTesting
  boolean isHudiFormat(String inputFormat, String outputFormat, String serdeLib) {
    return (inputFormat != null && inputFormat.toLowerCase().contains("hudi"))
        || (outputFormat != null && outputFormat.toLowerCase().contains("hudi"))
        || (serdeLib != null && serdeLib.toLowerCase().contains("hudi"));
  }

  @VisibleForTesting
  boolean isIcebergFormat(String inputFormat, String outputFormat, String serdeLib) {
    return (inputFormat != null && inputFormat.toLowerCase().contains("iceberg"))
        || (outputFormat != null && outputFormat.toLowerCase().contains("iceberg"))
        || (serdeLib != null && serdeLib.toLowerCase().contains("iceberg"));
  }
}
