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

import static org.apache.xtable.glue.GlueCatalogSyncClient.GLUE_EXTERNAL_TABLE_TYPE;

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.mockito.Mock;

import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.CatalogTableIdentifier;
import org.apache.xtable.model.schema.InternalSchema;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

public class GlueCatalogSyncClientTestBase {

  @Mock protected GlueClient mockGlueClient;
  @Mock protected GlueCatalogConfig mockGlueCatalogConfig;
  @Mock protected GlueSchemaExtractor mockGlueSchemaExtractor;
  protected Configuration mockConfiguration = new Configuration();

  protected static final String TEST_GLUE_DATABASE = "glue_db";
  protected static final String TEST_GLUE_TABLE = "glue_table";
  protected static final String TEST_GLUE_CATALOG_ID = "aws-account-id";
  protected static final String TEST_BASE_PATH = "base-path";
  protected static final InternalTable TEST_INTERNAL_TABLE =
      InternalTable.builder()
          .basePath(TEST_BASE_PATH)
          .readSchema(InternalSchema.builder().fields(Collections.emptyList()).build())
          .build();
  protected static final CatalogTableIdentifier TEST_CATALOG_TABLE_IDENTIFIER =
      CatalogTableIdentifier.builder().databaseName(TEST_GLUE_DATABASE).tableName(TEST_GLUE_TABLE).build();

  protected GetDatabaseRequest getDbRequest(String dbName) {
    return GetDatabaseRequest.builder().catalogId(TEST_GLUE_CATALOG_ID).name(dbName).build();
  }

  protected GetTableRequest getTableRequest(String dbName, String tableName) {
    return GetTableRequest.builder()
        .catalogId(TEST_GLUE_CATALOG_ID)
        .databaseName(dbName)
        .name(tableName)
        .build();
  }

  protected CreateDatabaseRequest createDbRequest(String dbName) {
    return CreateDatabaseRequest.builder()
        .catalogId(TEST_GLUE_CATALOG_ID)
        .databaseInput(
            DatabaseInput.builder()
                .name(dbName)
                .description("Created by " + IcebergGlueCatalogSyncClient.class.getName())
                .build())
        .build();
  }

  protected CreateTableRequest createTableRequest(
      String dbName, String tableName, Map<String, String> parameters) {
    return CreateTableRequest.builder()
        .catalogId(TEST_GLUE_CATALOG_ID)
        .databaseName(dbName)
        .tableInput(
            TableInput.builder()
                .name(tableName)
                .tableType(GLUE_EXTERNAL_TABLE_TYPE)
                .parameters(parameters)
                .storageDescriptor(
                    StorageDescriptor.builder()
                        .location(TEST_INTERNAL_TABLE.getBasePath())
                        .columns(Collections.emptyList())
                        .build())
                .build())
        .build();
  }

  protected UpdateTableRequest updateTableRequest(
      String dbName, String tableName, Map<String, String> parameters) {
    return UpdateTableRequest.builder()
        .catalogId(TEST_GLUE_CATALOG_ID)
        .databaseName(dbName)
        .skipArchive(true)
        .tableInput(
            TableInput.builder()
                .name(tableName)
                .tableType(GLUE_EXTERNAL_TABLE_TYPE)
                .parameters(parameters)
                .storageDescriptor(
                    StorageDescriptor.builder()
                        .location(TEST_INTERNAL_TABLE.getBasePath())
                        .columns(Collections.emptyList())
                        .build())
                .build())
        .build();
  }

  protected DeleteTableRequest deleteTableRequest(String dbName, String tableName) {
    return DeleteTableRequest.builder()
        .catalogId(TEST_GLUE_CATALOG_ID)
        .databaseName(dbName)
        .name(tableName)
        .build();
  }

  protected Table getGlueTable(String dbName, String tableName, String location) {
    return Table.builder()
        .databaseName(dbName)
        .name(tableName)
        .storageDescriptor(StorageDescriptor.builder().location(location).build())
        .build();
  }
}
