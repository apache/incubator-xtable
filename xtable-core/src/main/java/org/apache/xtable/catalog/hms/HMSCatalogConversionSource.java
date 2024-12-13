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
 
package org.apache.xtable.catalog.hms;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.xtable.conversion.SourceCatalog;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.spi.extractor.CatalogConversionSource;

public class HMSCatalogConversionSource implements CatalogConversionSource {

  private static final String TABLE_TYPE_PROP = "table_type";
  private final SourceCatalog sourceCatalog;
  private final HMSCatalogConfig hmsCatalogConfig;
  private final IMetaStoreClient metaStoreClient;

  public HMSCatalogConversionSource(SourceCatalog sourceCatalog, Configuration configuration) {
    this.sourceCatalog = sourceCatalog;
    this.hmsCatalogConfig =
        HMSCatalogConfig.of(sourceCatalog.getCatalogConfig().getCatalogOptions());
    try {
      this.metaStoreClient = new HMSClient(hmsCatalogConfig, configuration).getMSC();
    } catch (MetaException | HiveException e) {
      throw new RuntimeException("HiveMetastoreClient could not be created", e);
    }
  }

  @VisibleForTesting
  HMSCatalogConversionSource(
      SourceCatalog sourceCatalog,
      IMetaStoreClient metaStoreClient,
      HMSCatalogConfig hmsCatalogConfig) {
    this.sourceCatalog = sourceCatalog;
    this.hmsCatalogConfig = hmsCatalogConfig;
    this.metaStoreClient = metaStoreClient;
  }

  @Override
  public SourceTable getSourceTable(CatalogTableIdentifier tableIdentifier) {
    Table table;
    try {
      table =
          metaStoreClient.getTable(
              tableIdentifier.getDatabaseName(), tableIdentifier.getTableName());
    } catch (Exception e) {
      throw new CatalogSyncException("Failed to get table: " + tableIdentifier.getId(), e);
    }

    String tableFormat = table.getParameters().get(TABLE_TYPE_PROP);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(tableFormat), "TableFormat must not be null or empty");
    Properties tableProperties = new Properties();
    tableProperties.putAll(table.getParameters());
    return SourceTable.builder()
        .name(table.getTableName())
        .basePath(table.getSd().getLocation())
        // TODO: check if this holds true for all the formats
        .dataPath(table.getSd().getLocation())
        .formatName(tableFormat)
        .catalogConfig(sourceCatalog.getCatalogConfig())
        .additionalProperties(tableProperties)
        .build();
  }
}
