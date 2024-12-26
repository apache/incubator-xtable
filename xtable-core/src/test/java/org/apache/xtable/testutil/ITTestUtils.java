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
 
package org.apache.xtable.testutil;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Assertions;

import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.DataLayoutStrategy;
import org.apache.xtable.spi.extractor.CatalogConversionSource;
import org.apache.xtable.spi.sync.CatalogSyncClient;

public class ITTestUtils {

  public static void validateTable(
      InternalTable internalTable,
      String tableName,
      String tableFormat,
      InternalSchema readSchema,
      DataLayoutStrategy dataLayoutStrategy,
      String basePath,
      List<InternalPartitionField> partitioningFields) {
    Assertions.assertEquals(tableName, internalTable.getName());
    Assertions.assertEquals(tableFormat, internalTable.getTableFormat());
    Assertions.assertEquals(readSchema, internalTable.getReadSchema());
    Assertions.assertEquals(dataLayoutStrategy, internalTable.getLayoutStrategy());
    Assertions.assertEquals(basePath, internalTable.getBasePath());
    Assertions.assertEquals(partitioningFields, internalTable.getPartitioningFields());
  }

  public static class TestCatalogSyncImpl implements CatalogSyncClient {

    public TestCatalogSyncImpl(
        ExternalCatalogConfig catalogConfig, String tableFormat, Configuration hadoopConf) {}

    @Override
    public String getCatalogId() {
      return null;
    }

    @Override
    public String getStorageLocation(Object o) {
      return null;
    }

    @Override
    public boolean hasDatabase(CatalogTableIdentifier tableIdentifier) {
      return false;
    }

    @Override
    public void createDatabase(CatalogTableIdentifier tableIdentifier) {}

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

  public static class TestCatalogConversionSourceImpl implements CatalogConversionSource {
    public TestCatalogConversionSourceImpl(
        ExternalCatalogConfig sourceCatalogConfig, Configuration configuration) {}

    @Override
    public SourceTable getSourceTable(CatalogTableIdentifier tableIdentifier) {
      return SourceTable.builder()
          .name("source_table_name")
          .basePath("file://base_path/v1/")
          .formatName("ICEBERG")
          .build();
    }
  }
}
