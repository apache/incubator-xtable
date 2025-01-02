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
 
package org.apache.xtable.glue;

import static org.apache.xtable.catalog.CatalogUtils.castToHierarchicalTableIdentifier;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

import org.apache.xtable.catalog.TableFormatUtils;
import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.catalog.HierarchicalTableIdentifier;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.spi.extractor.CatalogConversionSource;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.Table;

public class GlueCatalogConversionSource implements CatalogConversionSource {
  private final GlueClient glueClient;
  private final GlueCatalogConfig glueCatalogConfig;

  public GlueCatalogConversionSource(
      ExternalCatalogConfig catalogConfig, Configuration configuration) {
    this.glueCatalogConfig = GlueCatalogConfig.of(catalogConfig.getCatalogProperties());
    this.glueClient = new DefaultGlueClientFactory(glueCatalogConfig).getGlueClient();
  }

  @VisibleForTesting
  public GlueCatalogConversionSource(GlueCatalogConfig glueCatalogConfig, GlueClient glueClient) {
    this.glueCatalogConfig = glueCatalogConfig;
    this.glueClient = glueClient;
  }

  @Override
  public SourceTable getSourceTable(CatalogTableIdentifier tblIdentifier) {
    HierarchicalTableIdentifier tableIdentifier = castToHierarchicalTableIdentifier(tblIdentifier);
    try {
      GetTableResponse response =
          glueClient.getTable(
              GetTableRequest.builder()
                  .catalogId(glueCatalogConfig.getCatalogId())
                  .databaseName(tableIdentifier.getDatabaseName())
                  .name(tableIdentifier.getTableName())
                  .build());
      Table table = response.table();
      if (table == null) {
        throw new IllegalStateException(
            String.format("table: %s is null", tableIdentifier.getId()));
      }

      String tableFormat = TableFormatUtils.getTableFormat(table.parameters());
      String tableLocation = table.storageDescriptor().location();
      String dataPath =
          TableFormatUtils.getTableDataLocation(tableFormat, tableLocation, table.parameters());

      Properties tableProperties = new Properties();
      tableProperties.putAll(table.parameters());
      return SourceTable.builder()
          .name(table.name())
          .basePath(tableLocation)
          .dataPath(dataPath)
          .formatName(tableFormat)
          .additionalProperties(tableProperties)
          .build();
    } catch (GlueException e) {
      throw new CatalogSyncException("Failed to get table: " + tableIdentifier.getId(), e);
    }
  }

  @Override
  public String getCatalogType() {
    return CatalogType.GLUE;
  }
}
