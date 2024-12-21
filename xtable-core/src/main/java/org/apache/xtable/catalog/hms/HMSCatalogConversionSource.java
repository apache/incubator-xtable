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

import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;

import java.util.Locale;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;

import com.google.common.base.Strings;

import org.apache.xtable.catalog.TableFormatUtils;
import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.spi.extractor.CatalogConversionSource;

public class HMSCatalogConversionSource implements CatalogConversionSource {

  private final HMSCatalogConfig hmsCatalogConfig;
  private final IMetaStoreClient metaStoreClient;

  public HMSCatalogConversionSource(
      ExternalCatalogConfig catalogConfig, Configuration configuration) {
    this.hmsCatalogConfig = HMSCatalogConfig.of(catalogConfig.getCatalogProperties());
    try {
      this.metaStoreClient = new HMSClientProvider(hmsCatalogConfig, configuration).getMSC();
    } catch (MetaException | HiveException e) {
      throw new CatalogSyncException("HiveMetastoreClient could not be created", e);
    }
  }

  @Override
  public SourceTable getSourceTable(CatalogTableIdentifier tableIdentifier) {
    try {
      Table table =
          metaStoreClient.getTable(
              tableIdentifier.getDatabaseName(), tableIdentifier.getTableName());
      if (table == null) {
        throw new IllegalStateException(String.format("table: %s not found", tableIdentifier));
      }

      String tableFormat = table.getParameters().get(TABLE_TYPE_PROP).toUpperCase(Locale.ENGLISH);
      if (!Strings.isNullOrEmpty(tableFormat)) {
        throw new IllegalStateException("TableFormat must not be null or empty");
      }

      String tableLocation = table.getSd().getLocation();
      String dataPath =
          TableFormatUtils.getTableDataLocation(tableFormat, tableLocation, table.getParameters());

      Properties tableProperties = new Properties();
      tableProperties.putAll(table.getParameters());
      return SourceTable.builder()
          .name(table.getTableName())
          .basePath(tableLocation)
          .dataPath(dataPath)
          .formatName(tableFormat)
          .additionalProperties(tableProperties)
          .build();
    } catch (TException e) {
      throw new CatalogSyncException("Failed to get table: " + tableIdentifier, e);
    }
  }
}
