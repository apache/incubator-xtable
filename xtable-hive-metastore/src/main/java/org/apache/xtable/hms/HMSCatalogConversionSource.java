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
 
package org.apache.xtable.hms;

import static org.apache.xtable.catalog.CatalogUtils.toHierarchicalTableIdentifier;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.thrift.TException;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.catalog.TableFormatUtils;
import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.spi.extractor.CatalogConversionSource;

public class HMSCatalogConversionSource implements CatalogConversionSource {

  private HMSCatalogConfig hmsCatalogConfig;
  private IMetaStoreClient metaStoreClient;

  // For loading the instance using ServiceLoader
  public HMSCatalogConversionSource() {}

  public HMSCatalogConversionSource(
      ExternalCatalogConfig catalogConfig, Configuration configuration) {
    _init(catalogConfig, configuration);
  }

  @VisibleForTesting
  HMSCatalogConversionSource(HMSCatalogConfig hmsCatalogConfig, IMetaStoreClient metaStoreClient) {
    this.hmsCatalogConfig = hmsCatalogConfig;
    this.metaStoreClient = metaStoreClient;
  }

  @Override
  public SourceTable getSourceTable(CatalogTableIdentifier tblIdentifier) {
    HierarchicalTableIdentifier tableIdentifier = toHierarchicalTableIdentifier(tblIdentifier);
    try {
      Table table =
          metaStoreClient.getTable(
              tableIdentifier.getDatabaseName(), tableIdentifier.getTableName());
      if (table == null) {
        throw new IllegalStateException(
            String.format("table: %s is null", tableIdentifier.getId()));
      }

      String tableFormat = TableFormatUtils.getTableFormat(table.getParameters());
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
      throw new CatalogSyncException("Failed to get table: " + tableIdentifier.getId(), e);
    }
  }

  @Override
  public String getCatalogType() {
    return CatalogType.HMS;
  }

  @Override
  public void init(ExternalCatalogConfig catalogConfig, Configuration configuration) {
    _init(catalogConfig, configuration);
  }

  private void _init(ExternalCatalogConfig catalogConfig, Configuration configuration) {
    this.hmsCatalogConfig = HMSCatalogConfig.of(catalogConfig.getCatalogProperties());
    try {
      this.metaStoreClient = new HMSClientProvider(hmsCatalogConfig, configuration).getMSC();
    } catch (MetaException | HiveException e) {
      throw new CatalogSyncException("HiveMetastoreClient could not be created", e);
    }
  }
}
