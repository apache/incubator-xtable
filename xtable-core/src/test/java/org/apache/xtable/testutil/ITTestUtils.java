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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  public static final String TEST_CATALOG_TYPE = "test";

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
    private static final Map<String, Integer> FUNCTION_CALLS = new HashMap<>();

    public TestCatalogSyncImpl(
        ExternalCatalogConfig catalogConfig, String tableFormat, Configuration hadoopConf) {}

    public TestCatalogSyncImpl() {}

    @Override
    public String getCatalogId() {
      trackFunctionCall();
      return null;
    }

    @Override
    public String getCatalogType() {
      return TEST_CATALOG_TYPE;
    }

    @Override
    public String getStorageLocation(Object o) {
      trackFunctionCall();
      return null;
    }

    @Override
    public boolean hasDatabase(CatalogTableIdentifier tableIdentifier) {
      trackFunctionCall();
      return false;
    }

    @Override
    public void createDatabase(CatalogTableIdentifier tableIdentifier) {
      trackFunctionCall();
    }

    @Override
    public Object getTable(CatalogTableIdentifier tableIdentifier) {
      trackFunctionCall();
      return null;
    }

    @Override
    public void createTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
      trackFunctionCall();
    }

    @Override
    public void refreshTable(
        InternalTable table, Object catalogTable, CatalogTableIdentifier tableIdentifier) {
      trackFunctionCall();
    }

    @Override
    public void createOrReplaceTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
      trackFunctionCall();
    }

    @Override
    public void dropTable(InternalTable table, CatalogTableIdentifier tableIdentifier) {
      trackFunctionCall();
    }

    @Override
    public void close() throws Exception {
      trackFunctionCall();
    }

    private void trackFunctionCall() {
      String methodName = Thread.currentThread().getStackTrace()[2].getMethodName();
      FUNCTION_CALLS.put(methodName, FUNCTION_CALLS.getOrDefault(methodName, 0) + 1);
    }

    public static Map<String, Integer> getFunctionCalls() {
      return FUNCTION_CALLS;
    }
  }

  public static class TestCatalogConversionSourceImpl implements CatalogConversionSource {
    public TestCatalogConversionSourceImpl(
        ExternalCatalogConfig sourceCatalogConfig, Configuration configuration) {}

    public TestCatalogConversionSourceImpl() {}

    @Override
    public SourceTable getSourceTable(CatalogTableIdentifier tableIdentifier) {
      return SourceTable.builder()
          .name("source_table_name")
          .basePath("file://base_path/v1/")
          .formatName("ICEBERG")
          .build();
    }

    @Override
    public String getCatalogType() {
      return TEST_CATALOG_TYPE;
    }
  }
}
