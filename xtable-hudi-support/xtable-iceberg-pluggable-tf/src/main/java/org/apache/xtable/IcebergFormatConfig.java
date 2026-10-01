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

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.table.HoodieTableMetaClient;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import org.apache.xtable.iceberg.IcebergCatalogConfig;
import org.apache.xtable.iceberg.IcebergTableManager;

/**
 * Settings of the Iceberg pluggable table format for one Hudi table. They live in {@code
 * hoodie.properties} under the {@code hoodie.table.format.iceberg.} prefix and reach the format
 * through {@link org.apache.hudi.common.HoodieTableFormat#init(Properties)}, so every writer or
 * table-service job that opens the table publishes to the same Iceberg catalog table. When the
 * table config carries no catalog, the Hadoop configuration is consulted under {@code
 * xtable.iceberg.catalog.} as a job-wide fallback, and failing that the Iceberg metadata lives at
 * the Hudi base path (HadoopTables).
 */
@Getter
@EqualsAndHashCode
@ToString
public class IcebergFormatConfig implements Serializable {
  private static final long serialVersionUID = 1L;

  public static final String PREFIX = "hoodie.table.format.iceberg.";
  public static final String CATALOG_NAME = PREFIX + "catalog.name";
  public static final String CATALOG_IMPL = PREFIX + "catalog.impl";
  public static final String CATALOG_OPTION_PREFIX = PREFIX + "catalog.option.";
  /** Dot separated Iceberg namespace of the table, e.g. {@code db} or {@code a.b}. */
  public static final String NAMESPACE = PREFIX + "namespace";
  /**
   * Whether the Hudi meta columns are part of the published Iceberg schema. Default true, matching
   * what the Hudi to Iceberg translation has always published; the Spark plugin turns it off so an
   * Iceberg table keeps the schema the user declared.
   */
  public static final String EXPOSE_META_FIELDS = PREFIX + "expose-meta-fields";

  public static final String HADOOP_PREFIX = "xtable.iceberg.catalog.";
  public static final String HADOOP_CATALOG_NAME = HADOOP_PREFIX + "name";
  public static final String HADOOP_CATALOG_IMPL = HADOOP_PREFIX + "impl";
  public static final String HADOOP_CATALOG_OPTION_PREFIX = HADOOP_PREFIX + "option.";

  private final String catalogName;
  private final String catalogImpl;
  private final Map<String, String> catalogOptions;
  private final String[] namespace;
  private final boolean exposeMetaFields;

  private IcebergFormatConfig(
      String catalogName,
      String catalogImpl,
      Map<String, String> catalogOptions,
      String[] namespace,
      boolean exposeMetaFields) {
    this.catalogName = catalogName;
    this.catalogImpl = catalogImpl;
    this.catalogOptions = Collections.unmodifiableMap(new HashMap<>(catalogOptions));
    this.namespace = namespace;
    this.exposeMetaFields = exposeMetaFields;
  }

  public static IcebergFormatConfig empty() {
    return new IcebergFormatConfig(null, null, Collections.emptyMap(), null, true);
  }

  public static IcebergFormatConfig fromProperties(Properties properties) {
    if (properties == null) {
      return empty();
    }
    Map<String, String> options = new HashMap<>();
    for (String key : properties.stringPropertyNames()) {
      if (key.startsWith(CATALOG_OPTION_PREFIX)) {
        options.put(key.substring(CATALOG_OPTION_PREFIX.length()), properties.getProperty(key));
      }
    }
    String namespace = properties.getProperty(NAMESPACE);
    return new IcebergFormatConfig(
        properties.getProperty(CATALOG_NAME),
        properties.getProperty(CATALOG_IMPL),
        options,
        namespace == null || namespace.isEmpty() ? null : namespace.split("\\."),
        Boolean.parseBoolean(properties.getProperty(EXPOSE_META_FIELDS, "true")));
  }

  /** Fills in the catalog from the Hadoop configuration when the table config has none. */
  public IcebergFormatConfig resolve(Configuration hadoopConf) {
    if (catalogImpl != null || hadoopConf == null) {
      return this;
    }
    String impl = hadoopConf.get(HADOOP_CATALOG_IMPL);
    if (impl == null) {
      return this;
    }
    Map<String, String> options = new HashMap<>();
    for (Map.Entry<String, String> entry :
        hadoopConf.getPropsWithPrefix(HADOOP_CATALOG_OPTION_PREFIX).entrySet()) {
      options.put(entry.getKey(), entry.getValue());
    }
    return new IcebergFormatConfig(
        hadoopConf.get(HADOOP_CATALOG_NAME, "iceberg"), impl, options, namespace, exposeMetaFields);
  }

  /** Null when the Iceberg metadata is kept at the Hudi base path instead of a catalog. */
  public IcebergCatalogConfig catalogConfig() {
    if (catalogImpl == null) {
      return null;
    }
    return IcebergCatalogConfig.builder()
        .catalogName(catalogName == null ? "iceberg" : catalogName)
        .catalogImpl(catalogImpl)
        .catalogOptions(catalogOptions)
        .build();
  }

  public TableIdentifier tableIdentifier(HoodieTableMetaClient metaClient) {
    String tableName = metaClient.getTableConfig().getTableName();
    return namespace == null
        ? TableIdentifier.of(tableName)
        : TableIdentifier.of(Namespace.of(namespace), tableName);
  }

  public boolean tableExists(IcebergTableManager tableManager, HoodieTableMetaClient metaClient) {
    return tableManager.tableExists(
        catalogConfig(), tableIdentifier(metaClient), metaClient.getBasePath().toString());
  }

  public Table getTable(IcebergTableManager tableManager, HoodieTableMetaClient metaClient) {
    return tableManager.getTable(
        catalogConfig(), tableIdentifier(metaClient), metaClient.getBasePath().toString());
  }

  /** The table-config form of this config, for persisting into {@code hoodie.properties}. */
  public Properties toProperties() {
    Properties props = new Properties();
    if (catalogName != null) {
      props.setProperty(CATALOG_NAME, catalogName);
    }
    if (catalogImpl != null) {
      props.setProperty(CATALOG_IMPL, catalogImpl);
    }
    for (Map.Entry<String, String> entry : catalogOptions.entrySet()) {
      props.setProperty(CATALOG_OPTION_PREFIX + entry.getKey(), entry.getValue());
    }
    if (namespace != null && namespace.length > 0) {
      props.setProperty(NAMESPACE, String.join(".", namespace));
    }
    props.setProperty(EXPOSE_META_FIELDS, Boolean.toString(exposeMetaFields));
    return props;
  }

  public static IcebergFormatConfig of(
      String catalogName,
      String catalogImpl,
      Map<String, String> catalogOptions,
      String[] namespace,
      boolean exposeMetaFields) {
    return new IcebergFormatConfig(
        catalogName, catalogImpl, catalogOptions, namespace, exposeMetaFields);
  }
}
