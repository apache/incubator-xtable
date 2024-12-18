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
 
package org.apache.xtable.utilities;

import static org.junit.jupiter.api.Assertions.*;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.spi.extractor.CatalogConversionSource;
import org.apache.xtable.spi.sync.CatalogSyncClient;

class TestRunCatalogSync {

  @SneakyThrows
  @Test
  void testMain() {
    String catalogConfigYamlPath =
        TestRunCatalogSync.class.getClassLoader().getResource("catalogConfig.yaml").getPath();
    String[] args = {"-catalogConfig", catalogConfigYamlPath};
    // Ensure yaml gets parsed and no op-sync implemented in TestCatalogImpl is called.
    assertDoesNotThrow(() -> RunCatalogSync.main(args));
  }

  public static class TestCatalogImpl implements CatalogConversionSource, CatalogSyncClient {

    public TestCatalogImpl(ExternalCatalogConfig catalogConfig, Configuration hadoopConf) {}

    @Override
    public SourceTable getSourceTable(CatalogTableIdentifier tableIdentifier) {
      return SourceTable.builder()
          .name("source_table_name")
          .basePath("file://base_path/v1/")
          .formatName("ICEBERG")
          .build();
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
