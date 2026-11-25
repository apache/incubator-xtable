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
 
package org.apache.xtable;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import lombok.Getter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hudi.common.table.HoodieTableMetaClient;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import org.apache.xtable.iceberg.IcebergCatalogConfig;
import org.apache.xtable.iceberg.IcebergTableManager;

/**
 * Settings of the Iceberg pluggable table format for one Hudi table, read from {@code
 * hoodie.properties} under the {@code hoodie.table.format.iceberg.} prefix so that every writer and
 * table service publishes to the same Iceberg table. When the table config names no catalog, the
 * catalog is taken from the Hadoop configuration under {@code xtable.iceberg.catalog.}, and without
 * either the Iceberg metadata is kept at the Hudi base path.
 *
 * <p>A table the format creates gets the format version from the table config, else from {@code
 * xtable.iceberg.format-version} in the Hadoop configuration, else {@value
 * #DEFAULT_FORMAT_VERSION}; an existing table keeps its version.
 */
@Getter
public class IcebergFormatConfig {
  public static final String PREFIX = "hoodie.table.format.iceberg.";
  public static final String CATALOG_NAME = PREFIX + "catalog.name";
  public static final String CATALOG_IMPL = PREFIX + "catalog.impl";
  public static final String CATALOG_OPTION_PREFIX = PREFIX + "catalog.option.";
  /** Dot separated namespace of the Iceberg table, for example {@code db} or {@code a.b}. */
  public static final String NAMESPACE = PREFIX + "namespace";

  public static final String FORMAT_VERSION = PREFIX + "format-version";

  public static final String HADOOP_PREFIX = "xtable.iceberg.catalog.";
  public static final String HADOOP_CATALOG_NAME = HADOOP_PREFIX + "name";
  public static final String HADOOP_CATALOG_IMPL = HADOOP_PREFIX + "impl";
  public static final String HADOOP_CATALOG_OPTION_PREFIX = HADOOP_PREFIX + "option.";

  static final String DEFAULT_CATALOG_NAME = "iceberg";
  static final int DEFAULT_FORMAT_VERSION = 3;

  /** Null when the Iceberg metadata is kept at the Hudi base path. */
  private final IcebergCatalogConfig catalogConfig;

  /** Null when the table is not in a namespace. */
  private final String[] namespace;

  private final int formatVersion;

  private IcebergFormatConfig(
      IcebergCatalogConfig catalogConfig, String[] namespace, int formatVersion) {
    this.catalogConfig = catalogConfig;
    this.namespace = namespace;
    this.formatVersion = formatVersion;
  }

  public static IcebergFormatConfig of(HoodieTableMetaClient metaClient) {
    return of(
        metaClient.getTableConfig().getProps(),
        (Configuration) metaClient.getStorageConf().unwrap());
  }

  /**
   * @param tableConfig the table's {@code hoodie.properties}
   * @param hadoopConf consulted for the catalog and the format version only when the table config
   *     sets none; may be null
   * @throws IllegalArgumentException when a catalog is named without an implementation, or the
   *     format version is not 2 or 3
   */
  public static IcebergFormatConfig of(Properties tableConfig, Configuration hadoopConf) {
    String namespace = tableConfig.getProperty(NAMESPACE);
    return new IcebergFormatConfig(
        catalogConfig(tableConfig, hadoopConf),
        namespace == null || namespace.isEmpty() ? null : namespace.split("\\."),
        formatVersion(tableConfig, hadoopConf));
  }

  private static int formatVersion(Properties tableConfig, Configuration hadoopConf) {
    String version = tableConfig.getProperty(FORMAT_VERSION);
    if (version == null && hadoopConf != null) {
      version = hadoopConf.get(IcebergTableManager.ICEBERG_FORMAT_VERSION);
    }
    if (version == null) {
      return DEFAULT_FORMAT_VERSION;
    }
    String trimmed = version.trim();
    if (!trimmed.equals("2") && !trimmed.equals("3")) {
      throw new IllegalArgumentException(
          String.format("The Iceberg format version must be 2 or 3 but is %s", version));
    }
    return Integer.parseInt(trimmed);
  }

  private static IcebergCatalogConfig catalogConfig(
      Properties tableConfig, Configuration hadoopConf) {
    String name = tableConfig.getProperty(CATALOG_NAME);
    String impl = tableConfig.getProperty(CATALOG_IMPL);
    if (name != null || impl != null) {
      if (impl == null) {
        throw new IllegalArgumentException(
            String.format("%s is set to %s but %s is not set", CATALOG_NAME, name, CATALOG_IMPL));
      }
      Map<String, String> options = new HashMap<>();
      for (String key : tableConfig.stringPropertyNames()) {
        if (key.startsWith(CATALOG_OPTION_PREFIX)) {
          options.put(key.substring(CATALOG_OPTION_PREFIX.length()), tableConfig.getProperty(key));
        }
      }
      return catalogConfig(name, impl, options);
    }
    if (hadoopConf == null || hadoopConf.get(HADOOP_CATALOG_IMPL) == null) {
      return null;
    }
    return catalogConfig(
        hadoopConf.get(HADOOP_CATALOG_NAME),
        hadoopConf.get(HADOOP_CATALOG_IMPL),
        new HashMap<>(hadoopConf.getPropsWithPrefix(HADOOP_CATALOG_OPTION_PREFIX)));
  }

  private static IcebergCatalogConfig catalogConfig(
      String name, String impl, Map<String, String> options) {
    return IcebergCatalogConfig.builder()
        .catalogName(name == null ? DEFAULT_CATALOG_NAME : name)
        .catalogImpl(impl)
        .catalogOptions(options)
        .build();
  }

  public TableIdentifier tableIdentifier(String tableName) {
    return namespace == null
        ? TableIdentifier.of(tableName)
        : TableIdentifier.of(Namespace.of(namespace), tableName);
  }

  public boolean tableExists(IcebergTableManager tableManager, HoodieTableMetaClient metaClient) {
    return tableManager.tableExists(
        catalogConfig,
        tableIdentifier(metaClient.getTableConfig().getTableName()),
        metaClient.getBasePath().toString());
  }

  /**
   * @throws IllegalStateException when the catalog names a table at another location than the Hudi
   *     base path, as when two Hudi tables of the same name share a catalog and namespace
   */
  public Table getTable(IcebergTableManager tableManager, HoodieTableMetaClient metaClient) {
    String basePath = metaClient.getBasePath().toString();
    TableIdentifier identifier = tableIdentifier(metaClient.getTableConfig().getTableName());
    Table table = tableManager.getTable(catalogConfig, identifier, basePath);
    if (catalogConfig != null && !sameLocation(table.location(), basePath)) {
      throw new IllegalStateException(
          String.format(
              "Iceberg table %s in catalog %s is located at %s, not at the Hudi base path %s, so it"
                  + " belongs to another Hudi table; give each Hudi table its own name or namespace"
                  + " in the catalog",
              identifier, catalogConfig.getCatalogName(), table.location(), basePath));
    }
    return table;
  }

  /**
   * Compares the paths and authorities of two locations. A missing scheme matches any scheme, and
   * the S3 schemes match each other, since a catalog may record the location with another S3
   * filesystem's scheme than the writer uses.
   */
  static boolean sameLocation(String location, String basePath) {
    URI first = new Path(location).toUri();
    URI second = new Path(basePath).toUri();
    return Objects.equals(trimTrailingSlash(first.getPath()), trimTrailingSlash(second.getPath()))
        && Objects.equals(first.getAuthority(), second.getAuthority())
        && compatibleSchemes(first.getScheme(), second.getScheme());
  }

  private static String trimTrailingSlash(String path) {
    return path.length() > 1 && path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
  }

  private static boolean compatibleSchemes(String first, String second) {
    if (first == null || second == null || first.equalsIgnoreCase(second)) {
      return true;
    }
    return isS3(first) && isS3(second);
  }

  private static boolean isS3(String scheme) {
    return scheme.equalsIgnoreCase("s3")
        || scheme.equalsIgnoreCase("s3a")
        || scheme.equalsIgnoreCase("s3n");
  }
}
