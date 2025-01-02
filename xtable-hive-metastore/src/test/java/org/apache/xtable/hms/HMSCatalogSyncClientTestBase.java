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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.mockito.Mock;

import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.ThreePartHierarchicalTableIdentifier;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.model.storage.TableFormat;

public class HMSCatalogSyncClientTestBase {

  @Mock protected IMetaStoreClient mockMetaStoreClient;
  @Mock protected HMSCatalogConfig mockHMSCatalogConfig;
  @Mock protected HMSSchemaExtractor mockHmsSchemaExtractor;
  protected Configuration testConfiguration = new Configuration();

  protected static final String TEST_HMS_DATABASE = "hms_db";
  protected static final String TEST_HMS_TABLE = "hms_table";
  protected static final String TEST_BASE_PATH = "base-path";
  protected static final String TEST_CATALOG_NAME = "hms-1";
  protected static final ExternalCatalogConfig TEST_CATALOG_CONFIG =
      ExternalCatalogConfig.builder()
          .catalogId(TEST_CATALOG_NAME)
          .catalogType(CatalogType.HMS)
          .catalogSyncClientImpl(HMSCatalogSyncClient.class.getCanonicalName())
          .catalogProperties(Collections.emptyMap())
          .build();

  protected static final String ICEBERG_METADATA_FILE_LOCATION = "base-path/metadata";
  protected static final String ICEBERG_METADATA_FILE_LOCATION_V2 = "base-path/v2-metadata";
  protected static final InternalTable TEST_ICEBERG_INTERNAL_TABLE =
      InternalTable.builder()
          .basePath(TEST_BASE_PATH)
          .tableFormat(TableFormat.ICEBERG)
          .readSchema(InternalSchema.builder().fields(Collections.emptyList()).build())
          .build();
  protected static final InternalTable TEST_HUDI_INTERNAL_TABLE =
      InternalTable.builder()
          .basePath(TEST_BASE_PATH)
          .tableFormat(TableFormat.HUDI)
          .readSchema(InternalSchema.builder().fields(Collections.emptyList()).build())
          .build();
  protected static final ThreePartHierarchicalTableIdentifier TEST_CATALOG_TABLE_IDENTIFIER =
      new ThreePartHierarchicalTableIdentifier(TEST_HMS_DATABASE, TEST_HMS_TABLE);

  protected Table newTable(String dbName, String tableName) {
    return newTable(dbName, tableName, new HashMap<>());
  }

  protected Table newTable(String dbName, String tableName, Map<String, String> params) {
    Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);
    table.setParameters(params);
    return table;
  }

  protected Table newTable(
      String dbName, String tableName, Map<String, String> params, StorageDescriptor sd) {
    Table table = newTable(dbName, tableName, params);
    table.setSd(sd);
    return table;
  }

  protected Database newDatabase(String dbName) {
    return new Database(
        dbName, "Created by " + HMSCatalogSyncClient.class.getName(), null, Collections.emptyMap());
  }
}
