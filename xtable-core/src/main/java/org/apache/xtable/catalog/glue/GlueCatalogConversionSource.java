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
 
package org.apache.xtable.catalog.glue;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import org.apache.xtable.conversion.SourceCatalog;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.spi.extractor.CatalogConversionSource;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.Table;

/** AWS Glue implementation for CatalogConversionSource */
public class GlueCatalogConversionSource implements CatalogConversionSource {

  private static final String TABLE_TYPE_PROP = "table_type";
  private final SourceCatalog sourceCatalog;
  private final GlueCatalogConfig glueCatalogConfig;
  private final GlueClient glueClient;

  public GlueCatalogConversionSource(SourceCatalog sourceCatalog, Configuration configuration) {
    this.sourceCatalog = sourceCatalog;
    this.glueCatalogConfig =
        GlueCatalogConfig.of(sourceCatalog.getCatalogConfig().getCatalogOptions());
    this.glueClient = new DefaultGlueClientFactory(glueCatalogConfig).getGlueClient();
  }

  @VisibleForTesting
  GlueCatalogConversionSource(
      SourceCatalog sourceCatalog, GlueClient glueClient, GlueCatalogConfig glueCatalogConfig) {
    this.sourceCatalog = sourceCatalog;
    this.glueCatalogConfig = glueCatalogConfig;
    this.glueClient = glueClient;
  }

  @Override
  public SourceTable getSourceTable(CatalogTableIdentifier tableIdentifier) {
    Table table;
    try {
      GetTableResponse response =
          glueClient.getTable(
              GetTableRequest.builder()
                  .catalogId(glueCatalogConfig.getCatalogId())
                  .databaseName(tableIdentifier.getDatabaseName())
                  .name(tableIdentifier.getTableName())
                  .build());
      table = response.table();
    } catch (Exception e) {
      throw new RuntimeException("Failed to get table: " + tableIdentifier.getId(), e);
    }

    String tableFormat = table.parameters().get(TABLE_TYPE_PROP);
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(tableFormat), "TableFormat must not be null or empty");
    Properties tableProperties = new Properties();
    tableProperties.putAll(table.parameters());
    return SourceTable.builder()
        .name(table.name())
        .basePath(table.storageDescriptor().location())
        // TODO: check if this holds true for all the formats
        .dataPath(table.storageDescriptor().location())
        .formatName(tableFormat)
        .catalogConfig(sourceCatalog.getCatalogConfig())
        .additionalProperties(tableProperties)
        .build();
  }
}
