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

import static org.apache.xtable.glue.TestGlueSchemaExtractor.getColumn;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.mockito.Mock;

import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.ThreePartHierarchicalTableIdentifier;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.schema.PartitionTransformType;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.model.storage.TableFormat;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;

public class GlueCatalogSyncTestBase {

  @Mock protected GlueClient mockGlueClient;
  @Mock protected GlueCatalogConfig mockGlueCatalogConfig;
  @Mock protected GlueSchemaExtractor mockGlueSchemaExtractor;
  protected final Configuration testConfiguration = new Configuration();

  protected static final String TEST_GLUE_DATABASE = "glue_db";
  protected static final String TEST_GLUE_TABLE = "glue_table";
  protected static final String TEST_GLUE_CATALOG_ID = "aws-account-id";
  protected static final String TEST_BASE_PATH = "base-path";
  protected static final String TEST_CATALOG_NAME = "aws-glue-1";
  protected static final String ICEBERG_METADATA_FILE_LOCATION = "base-path/metadata";
  protected static final String ICEBERG_METADATA_FILE_LOCATION_v2 = "base-path/v2-metadata";
  protected static final InternalPartitionField PARTITION_FIELD =
      InternalPartitionField.builder()
          .sourceField(
              InternalField.builder()
                  .name("partitionField")
                  .schema(
                      InternalSchema.builder().name("string").dataType(InternalType.STRING).build())
                  .build())
          .transformType(PartitionTransformType.VALUE)
          .build();
  protected static final InternalSchema INTERNAL_SCHEMA =
      InternalSchema.builder()
          .dataType(InternalType.RECORD)
          .fields(
              Arrays.asList(
                  getInternalField("intField", "int", InternalType.INT),
                  getInternalField("stringField", "string", InternalType.STRING),
                  getInternalField("partitionField", "string", InternalType.STRING)))
          .build();
  protected static final InternalSchema UPDATED_INTERNAL_SCHEMA =
      InternalSchema.builder()
          .dataType(InternalType.RECORD)
          .fields(
              Arrays.asList(
                  getInternalField("intField", "int", InternalType.INT),
                  getInternalField("stringField", "string", InternalType.STRING),
                  getInternalField("partitionField", "string", InternalType.STRING),
                  getInternalField("booleanField", "boolean", InternalType.BOOLEAN)))
          .build();
  protected static final List<Column> PARTITION_KEYS =
      Collections.singletonList(getColumn("partitionField", "string"));
  protected static final List<Column> DELTA_GLUE_SCHEMA =
      Arrays.asList(
          getColumn(TableFormat.DELTA, "intField", "int"),
          getColumn(TableFormat.DELTA, "stringField", "string"));
  protected static final List<Column> UPDATED_DELTA_GLUE_SCHEMA =
      Arrays.asList(
          getColumn(TableFormat.DELTA, "booleanField", "boolean"),
          getColumn(TableFormat.DELTA, "intField", "int"),
          getColumn(TableFormat.DELTA, "stringField", "string"));
  protected static final List<Column> ICEBERG_GLUE_SCHEMA =
      Arrays.asList(
          getColumn(TableFormat.ICEBERG, "intField", "int"),
          getColumn(TableFormat.ICEBERG, "stringField", "string"),
          getColumn(TableFormat.ICEBERG, "partitionField", "string"));
  protected static final List<Column> UPDATED_ICEBERG_GLUE_SCHEMA =
      Arrays.asList(
          getColumn(TableFormat.ICEBERG, "intField", "int"),
          getColumn(TableFormat.ICEBERG, "stringField", "string"),
          getColumn(TableFormat.ICEBERG, "partitionField", "string"),
          getColumn(TableFormat.ICEBERG, "booleanField", "boolean"));
  protected static final InternalTable TEST_ICEBERG_INTERNAL_TABLE =
      InternalTable.builder()
          .basePath(TEST_BASE_PATH)
          .tableFormat(TableFormat.ICEBERG)
          .readSchema(INTERNAL_SCHEMA)
          .partitioningFields(Collections.singletonList(PARTITION_FIELD))
          .build();
  protected static final InternalTable TEST_UPDATED_ICEBERG_INTERNAL_TABLE =
      InternalTable.builder()
          .basePath(TEST_BASE_PATH)
          .tableFormat(TableFormat.ICEBERG)
          .readSchema(UPDATED_INTERNAL_SCHEMA)
          .partitioningFields(Collections.singletonList(PARTITION_FIELD))
          .build();
  protected static final InternalTable TEST_HUDI_INTERNAL_TABLE =
      InternalTable.builder()
          .basePath(TEST_BASE_PATH)
          .tableFormat(TableFormat.HUDI)
          .readSchema(INTERNAL_SCHEMA)
          .partitioningFields(Collections.singletonList(PARTITION_FIELD))
          .build();
  protected static final InternalTable TEST_EVOLVED_HUDI_INTERNAL_TABLE =
      InternalTable.builder()
          .basePath(TEST_BASE_PATH)
          .tableFormat(TableFormat.HUDI)
          .readSchema(UPDATED_INTERNAL_SCHEMA)
          .partitioningFields(Collections.singletonList(PARTITION_FIELD))
          .build();
  protected static final InternalTable TEST_DELTA_INTERNAL_TABLE =
      InternalTable.builder()
          .basePath(TEST_BASE_PATH)
          .tableFormat(TableFormat.DELTA)
          .readSchema(INTERNAL_SCHEMA)
          .partitioningFields(Collections.singletonList(PARTITION_FIELD))
          .build();
  protected static final InternalTable TEST_UPDATED_DELTA_INTERNAL_TABLE =
      InternalTable.builder()
          .basePath(TEST_BASE_PATH)
          .tableFormat(TableFormat.DELTA)
          .readSchema(UPDATED_INTERNAL_SCHEMA)
          .partitioningFields(Collections.singletonList(PARTITION_FIELD))
          .build();
  protected static final ThreePartHierarchicalTableIdentifier TEST_CATALOG_TABLE_IDENTIFIER =
      new ThreePartHierarchicalTableIdentifier(TEST_GLUE_DATABASE, TEST_GLUE_TABLE);
  protected static final ExternalCatalogConfig catalogConfig =
      ExternalCatalogConfig.builder()
          .catalogId(TEST_CATALOG_NAME)
          .catalogType(CatalogType.GLUE)
          .catalogSyncClientImpl(GlueCatalogSyncClient.class.getCanonicalName())
          .catalogProperties(Collections.emptyMap())
          .build();
  protected static final TableInput TEST_TABLE_INPUT = TableInput.builder().build();
  protected static final GlueException TEST_GLUE_EXCEPTION =
      (GlueException) GlueException.builder().message("something went wrong").build();

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
                .description("Created by " + GlueCatalogSyncClient.class.getName())
                .build())
        .build();
  }

  protected CreateTableRequest createTableRequest(String dbName, TableInput tableInput) {
    return CreateTableRequest.builder()
        .catalogId(TEST_GLUE_CATALOG_ID)
        .databaseName(dbName)
        .tableInput(tableInput)
        .build();
  }

  protected UpdateTableRequest updateTableRequest(String dbName, TableInput tableInput) {
    return UpdateTableRequest.builder()
        .catalogId(TEST_GLUE_CATALOG_ID)
        .databaseName(dbName)
        .skipArchive(true)
        .tableInput(tableInput)
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

  private static InternalField getInternalField(
      String fieldName, String schemaName, InternalType dataType) {
    return InternalField.builder()
        .name(fieldName)
        .schema(InternalSchema.builder().name(schemaName).dataType(dataType).build())
        .build();
  }
}
