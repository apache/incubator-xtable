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

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.iceberg.spark.source.SparkTable;

/**
 * Drop-in replacement for {@link SparkCatalog}: same {@code type}/{@code uri}/{@code warehouse}
 * options, same Iceberg catalog underneath. Tables carrying {@code hudi.managed=true} come back as
 * {@link HudiSparkTable} so their writes are routed through Hudi; every other table is untouched.
 */
public class HudiSparkCatalog extends SparkCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(HudiSparkCatalog.class);

  private String catalogName;
  private String catalogImpl;
  private Map<String, String> catalogOptions = new HashMap<>();
  private boolean refreshEagerly;
  private boolean manageNewTables = true;

  @Override
  protected Catalog buildIcebergCatalog(String name, CaseInsensitiveStringMap options) {
    Catalog catalog = super.buildIcebergCatalog(name, options);
    this.catalogName = name;
    // buildIcebergCatalog returns the raw catalog; CachingCatalog wrapping happens afterwards
    this.catalogImpl = catalog.getClass().getName();
    Map<String, String> persisted = new HashMap<>();
    for (Map.Entry<String, String> entry : options.entrySet()) {
      String key = entry.getKey();
      if (key.equals("type")
          || key.equals(CatalogProperties.CATALOG_IMPL)
          || key.equals(CatalogProperties.CACHE_ENABLED)
          || key.startsWith("cache.")
          || key.startsWith("hudi.")) {
        continue;
      }
      persisted.put(key, entry.getValue());
    }
    this.catalogOptions = persisted;
    boolean cacheEnabled =
        options.getBoolean(
            CatalogProperties.CACHE_ENABLED, CatalogProperties.CACHE_ENABLED_DEFAULT);
    this.refreshEagerly = !cacheEnabled;
    this.manageNewTables =
        options.getBoolean(
            HudiIcebergConf.CATALOG_OPTION_MANAGE_NEW_TABLES,
            SparkSession.active()
                .conf()
                .get(HudiIcebergConf.MANAGE_NEW_TABLES, "true")
                .equalsIgnoreCase("true"));
    LOG.info(
        "Hudi-managed Iceberg catalog '{}' over {} (manage new tables: {})",
        name,
        catalogImpl,
        manageNewTables);
    return catalog;
  }

  @Override
  public Table loadTable(Identifier ident) throws NoSuchTableException {
    return wrap(ident, super.loadTable(ident));
  }

  @Override
  public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
    return wrap(ident, super.loadTable(ident, version));
  }

  @Override
  public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
    return wrap(ident, super.loadTable(ident, timestamp));
  }

  @Override
  public Table createTable(
      Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
      throws TableAlreadyExistsException {
    Map<String, String> props = properties;
    if (manageNewTables && !properties.containsKey(HudiIcebergConf.TABLE_PROP_MANAGED)) {
      props = new HashMap<>(properties);
      props.put(HudiIcebergConf.TABLE_PROP_MANAGED, "true");
    }
    return wrap(ident, super.createTable(ident, schema, partitions, props));
  }

  private Table wrap(Identifier ident, Table table) {
    if (!(table instanceof SparkTable) || table instanceof HudiSparkTable) {
      return table;
    }
    org.apache.iceberg.Table icebergTable = ((SparkTable) table).table();
    if (!Boolean.parseBoolean(
        icebergTable.properties().getOrDefault(HudiIcebergConf.TABLE_PROP_MANAGED, "false"))) {
      return table;
    }
    HudiTableContext context =
        HudiTableContext.from(
            ident,
            icebergTable,
            catalogName,
            catalogImpl,
            catalogOptions,
            SparkSession.active().sessionState().newHadoopConf());
    if (context.getUnsupportedReason() != null) {
      LOG.warn(
          "Table {} is marked hudi.managed but stays on native Iceberg writes: {}",
          ident,
          context.getUnsupportedReason());
      return table;
    }
    return new HudiSparkTable(icebergTable, refreshEagerly, context);
  }
}
