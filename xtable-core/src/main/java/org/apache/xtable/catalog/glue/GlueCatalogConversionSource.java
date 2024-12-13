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

import org.apache.hadoop.conf.Configuration;

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

public class GlueCatalogConversionSource implements CatalogConversionSource {

  protected static final String TABLE_TYPE_PROP = "table_type";
  protected final SourceCatalog sourceCatalog;
  protected final GlueCatalogConfig glueCatalogConfig;
  protected final GlueClient glueClient;
  protected final Configuration configuration;

  public GlueCatalogConversionSource(SourceCatalog sourceCatalog, Configuration configuration) {
    this.sourceCatalog = sourceCatalog;
    this.glueCatalogConfig =
        GlueCatalogConfig.createConfig(sourceCatalog.getCatalogConfig().getCatalogOptions());
    this.glueClient = new DefaultGlueClientFactory(glueCatalogConfig).getGlueClient();
    this.configuration = configuration;
  }

  public GlueCatalogConversionSource(
      SourceCatalog sourceCatalog,
      GlueClient glueClient,
      GlueCatalogConfig glueCatalogConfig,
      Configuration configuration) {
    this.sourceCatalog = sourceCatalog;
    this.glueCatalogConfig = glueCatalogConfig;
    this.glueClient = glueClient;
    this.configuration = configuration;
  }

  @Override
  public SourceTable getSourceTable(CatalogTableIdentifier tableIdentifier) {
    try {
      GetTableResponse response =
          glueClient.getTable(
              GetTableRequest.builder()
                  .catalogId(glueCatalogConfig.getCatalogId())
                  .databaseName(tableIdentifier.getDatabaseName())
                  .name(tableIdentifier.getTableName())
                  .build());
      Table table = response.table();
      String tableFormat = table.parameters().get(TABLE_TYPE_PROP);
      Preconditions.checkArgument(
          !Strings.isNullOrEmpty(tableFormat), "TableFormat must not be null or empty");
      return SourceTable.builder()
          .name(table.name())
          .basePath(table.storageDescriptor().location())
          .formatName(tableFormat)
          .build();
    } catch (Exception e) {
      throw new RuntimeException("Failed to get table: " + tableIdentifier.getId(), e);
    }
  }
}
