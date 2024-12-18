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
 
package org.apache.xtable.catalog;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetCatalogConfig;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.spi.extractor.CatalogConversionSource;
import org.apache.xtable.spi.sync.CatalogSyncClient;

class TestCatalogConversionFactory {

  @Test
  void createSourceForConfig() {
    ExternalCatalogConfig sourceCatalog =
        ExternalCatalogConfig.builder()
            .catalogName("catalogName")
            .catalogImpl(TestCatalogImpl.class.getName())
            .catalogOptions(Collections.emptyMap())
            .build();
    CatalogConversionSource catalogConversionSource =
        CatalogConversionFactory.createCatalogConversionSource(sourceCatalog, new Configuration());
    assertEquals(catalogConversionSource.getClass().getName(), TestCatalogImpl.class.getName());
  }

  @Test
  void createForCatalog() {
    TargetCatalogConfig targetCatalogConfig =
        TargetCatalogConfig.builder()
            .catalogConfig(
                ExternalCatalogConfig.builder()
                    .catalogName("catalogName")
                    .catalogImpl(TestCatalogImpl.class.getName())
                    .catalogOptions(Collections.emptyMap())
                    .build())
            .catalogTableIdentifier(
                CatalogTableIdentifier.builder()
                    .databaseName("target-database")
                    .tableName("target-tableName")
                    .build())
            .build();
    CatalogSyncClient catalogSyncClient =
        CatalogConversionFactory.getInstance()
            .createCatalogSyncClient(targetCatalogConfig.getCatalogConfig(), new Configuration());
    assertEquals(catalogSyncClient.getClass().getName(), TestCatalogImpl.class.getName());
  }

  public static class TestCatalogImpl
      implements CatalogSyncClient<Object>, CatalogConversionSource {

    public TestCatalogImpl(ExternalCatalogConfig catalogConfig, Configuration hadoopConf) {}

    @Override
    public SourceTable getSourceTable(CatalogTableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    public String getCatalogName() {
      return null;
    }

    @Override
    public String getStorageDescriptorLocation(Object o) {
      return null;
    }

    @Override
    public boolean hasDatabase(String databaseName) {
      return false;
    }

    @Override
    public void createDatabase(String databaseName) {}

    @Override
    public Object getTable(CatalogTableIdentifier tableIdentifier) {
      return null;
    }

    @Override
    public void createTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {}

    @Override
    public void refreshTable(
        InternalTable table, Object catalogTable, CatalogTableIdentifier tableIdentifier) {}

    @Override
    public void createOrReplaceTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {}

    @Override
    public void dropTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {}

    @Override
    public void close() throws Exception {}
  }
}
