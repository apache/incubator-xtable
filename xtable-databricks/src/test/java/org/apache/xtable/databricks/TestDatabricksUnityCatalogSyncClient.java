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
 
package org.apache.xtable.databricks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.databricks.sdk.core.error.platform.NotFound;
import com.databricks.sdk.service.catalog.ColumnInfo;
import com.databricks.sdk.service.catalog.CreateSchema;
import com.databricks.sdk.service.catalog.SchemaInfo;
import com.databricks.sdk.service.catalog.SchemasAPI;
import com.databricks.sdk.service.catalog.TableInfo;
import com.databricks.sdk.service.catalog.TablesAPI;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;

import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.ThreePartHierarchicalTableIdentifier;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.model.storage.TableFormat;

@ExtendWith(MockitoExtension.class)
public class TestDatabricksUnityCatalogSyncClient {

  @Mock private StatementExecutionAPI mockStatementExecution;
  @Mock private TablesAPI mockTablesApi;
  @Mock private SchemasAPI mockSchemasApi;

  @Test
  void testCreateTableDelta_NoColumns() {
    Map<String, String> props = new HashMap<>();
    props.put(DatabricksUnityCatalogConfig.HOST, "https://example.cloud.databricks.com");
    props.put(DatabricksUnityCatalogConfig.WAREHOUSE_ID, "wh-1");
    props.put(DatabricksUnityCatalogConfig.AUTH_TYPE, "oauth-m2m");
    ExternalCatalogConfig config =
        ExternalCatalogConfig.builder()
            .catalogId("uc")
            .catalogType(CatalogType.DATABRICKS_UC)
            .catalogProperties(props)
            .build();

    DatabricksUnityCatalogSyncClient client =
        new DatabricksUnityCatalogSyncClient(
            config,
            TableFormat.DELTA,
            new Configuration(),
            mockStatementExecution,
            mockTablesApi,
            mockSchemasApi);

    when(mockStatementExecution.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(
            new StatementResponse()
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED)));

    InternalTable table = InternalTable.builder().basePath("s3://bucket/path").build();
    ThreePartHierarchicalTableIdentifier tableIdentifier =
        new ThreePartHierarchicalTableIdentifier("main", "default", "people");

    client.createTable(table, tableIdentifier);

    ArgumentCaptor<ExecuteStatementRequest> requestCaptor =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(mockStatementExecution).executeStatement(requestCaptor.capture());
    ExecuteStatementRequest request = requestCaptor.getValue();
    assertEquals("wh-1", request.getWarehouseId());
    assertEquals(
        "CREATE TABLE IF NOT EXISTS main.default.people USING DELTA LOCATION 's3://bucket/path'",
        request.getStatement());
  }

  @Test
  void testCreateTableRejectsNonDelta() {
    Map<String, String> props = new HashMap<>();
    props.put(DatabricksUnityCatalogConfig.HOST, "https://example.cloud.databricks.com");
    props.put(DatabricksUnityCatalogConfig.WAREHOUSE_ID, "wh-1");
    ExternalCatalogConfig config =
        ExternalCatalogConfig.builder()
            .catalogId("uc")
            .catalogType(CatalogType.DATABRICKS_UC)
            .catalogProperties(props)
            .build();

    DatabricksUnityCatalogSyncClient client =
        new DatabricksUnityCatalogSyncClient(
            config,
            TableFormat.ICEBERG,
            new Configuration(),
            mockStatementExecution,
            mockTablesApi,
            mockSchemasApi);

    InternalTable table = InternalTable.builder().basePath("s3://bucket/path").build();
    ThreePartHierarchicalTableIdentifier tableIdentifier =
        new ThreePartHierarchicalTableIdentifier("main", "default", "people");

    assertThrows(CatalogSyncException.class, () -> client.createTable(table, tableIdentifier));
  }

  @Test
  void testCreateOrReplaceTableDelta() {
    Map<String, String> props = new HashMap<>();
    props.put(DatabricksUnityCatalogConfig.HOST, "https://example.cloud.databricks.com");
    props.put(DatabricksUnityCatalogConfig.WAREHOUSE_ID, "wh-1");
    ExternalCatalogConfig config =
        ExternalCatalogConfig.builder()
            .catalogId("uc")
            .catalogType(CatalogType.DATABRICKS_UC)
            .catalogProperties(props)
            .build();

    DatabricksUnityCatalogSyncClient client =
        new DatabricksUnityCatalogSyncClient(
            config,
            TableFormat.DELTA,
            new Configuration(),
            mockStatementExecution,
            mockTablesApi,
            mockSchemasApi);

    when(mockStatementExecution.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(
            new StatementResponse()
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED)));

    InternalTable table = InternalTable.builder().basePath("s3://bucket/path").build();
    ThreePartHierarchicalTableIdentifier tableIdentifier =
        new ThreePartHierarchicalTableIdentifier("main", "default", "people");

    client.createOrReplaceTable(table, tableIdentifier);

    ArgumentCaptor<ExecuteStatementRequest> requestCaptor =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(mockStatementExecution).executeStatement(requestCaptor.capture());
    ExecuteStatementRequest request = requestCaptor.getValue();
    assertEquals(
        "CREATE OR REPLACE TABLE main.default.people USING DELTA LOCATION 's3://bucket/path'",
        request.getStatement());
  }

  @Test
  void testCreateOrReplaceTableRejectsNonDelta() {
    Map<String, String> props = new HashMap<>();
    props.put(DatabricksUnityCatalogConfig.HOST, "https://example.cloud.databricks.com");
    props.put(DatabricksUnityCatalogConfig.WAREHOUSE_ID, "wh-1");
    ExternalCatalogConfig config =
        ExternalCatalogConfig.builder()
            .catalogId("uc")
            .catalogType(CatalogType.DATABRICKS_UC)
            .catalogProperties(props)
            .build();

    DatabricksUnityCatalogSyncClient client =
        new DatabricksUnityCatalogSyncClient(
            config,
            TableFormat.ICEBERG,
            new Configuration(),
            mockStatementExecution,
            mockTablesApi,
            mockSchemasApi);

    InternalTable table = InternalTable.builder().basePath("s3://bucket/path").build();
    ThreePartHierarchicalTableIdentifier tableIdentifier =
        new ThreePartHierarchicalTableIdentifier("main", "default", "people");

    assertThrows(
        CatalogSyncException.class, () -> client.createOrReplaceTable(table, tableIdentifier));
  }

  @Test
  void testRefreshTableSchemaEvolution() {
    Map<String, String> props = new HashMap<>();
    props.put(DatabricksUnityCatalogConfig.HOST, "https://example.cloud.databricks.com");
    props.put(DatabricksUnityCatalogConfig.WAREHOUSE_ID, "wh-1");
    ExternalCatalogConfig config =
        ExternalCatalogConfig.builder()
            .catalogId("uc")
            .catalogType(CatalogType.DATABRICKS_UC)
            .catalogProperties(props)
            .build();

    DatabricksUnityCatalogSyncClient client =
        new DatabricksUnityCatalogSyncClient(
            config,
            TableFormat.DELTA,
            new Configuration(),
            mockStatementExecution,
            mockTablesApi,
            mockSchemasApi);

    when(mockStatementExecution.executeStatement(any(ExecuteStatementRequest.class)))
        .thenReturn(
            new StatementResponse()
                .setStatus(new StatementStatus().setState(StatementState.SUCCEEDED)));

    InternalSchema idSchema =
        InternalSchema.builder()
            .name("id")
            .dataType(InternalType.INT)
            .isNullable(false)
            .comment("new")
            .build();
    InternalSchema ageSchema =
        InternalSchema.builder().name("age").dataType(InternalType.INT).isNullable(true).build();
    InternalSchema readSchema =
        InternalSchema.builder()
            .name("root")
            .dataType(InternalType.RECORD)
            .isNullable(true)
            .fields(
                java.util.Arrays.asList(
                    InternalField.builder().name("id").schema(idSchema).build(),
                    InternalField.builder().name("age").schema(ageSchema).build()))
            .build();

    InternalTable table =
        InternalTable.builder().readSchema(readSchema).basePath("s3://bucket/path").build();
    TableInfo catalogTable =
        new TableInfo()
            .setColumns(
                java.util.Arrays.asList(
                    new ColumnInfo()
                        .setName("id")
                        .setTypeText("int")
                        .setNullable(true)
                        .setComment("old"),
                    new ColumnInfo().setName("name").setTypeText("string").setNullable(true)));

    ThreePartHierarchicalTableIdentifier tableIdentifier =
        new ThreePartHierarchicalTableIdentifier("main", "default", "people");

    client.refreshTable(table, catalogTable, tableIdentifier);

    ArgumentCaptor<ExecuteStatementRequest> requestCaptor =
        ArgumentCaptor.forClass(ExecuteStatementRequest.class);
    verify(mockStatementExecution, org.mockito.Mockito.times(4))
        .executeStatement(requestCaptor.capture());

    List<ExecuteStatementRequest> requests = requestCaptor.getAllValues();
    assertEquals(
        "ALTER TABLE main.default.people ADD COLUMNS (`age` int)", requests.get(0).getStatement());
    assertEquals(
        "ALTER TABLE main.default.people ALTER COLUMN `id` SET NOT NULL",
        requests.get(1).getStatement());
    assertEquals(
        "ALTER TABLE main.default.people ALTER COLUMN `id` COMMENT 'new'",
        requests.get(2).getStatement());
    assertEquals(
        "ALTER TABLE main.default.people DROP COLUMN `name`", requests.get(3).getStatement());
  }

  @Test
  void testDropTable() {
    Map<String, String> props = new HashMap<>();
    props.put(DatabricksUnityCatalogConfig.HOST, "https://example.cloud.databricks.com");
    props.put(DatabricksUnityCatalogConfig.WAREHOUSE_ID, "wh-1");
    ExternalCatalogConfig config =
        ExternalCatalogConfig.builder()
            .catalogId("uc")
            .catalogType(CatalogType.DATABRICKS_UC)
            .catalogProperties(props)
            .build();

    DatabricksUnityCatalogSyncClient client =
        new DatabricksUnityCatalogSyncClient(
            config,
            TableFormat.DELTA,
            new Configuration(),
            mockStatementExecution,
            mockTablesApi,
            mockSchemasApi);

    ThreePartHierarchicalTableIdentifier tableIdentifier =
        new ThreePartHierarchicalTableIdentifier("main", "default", "people");

    client.dropTable(InternalTable.builder().basePath("s3://bucket/path").build(), tableIdentifier);

    verify(mockTablesApi).delete("main.default.people");
  }

  @Test
  void testDropTableFailure() {
    Map<String, String> props = new HashMap<>();
    props.put(DatabricksUnityCatalogConfig.HOST, "https://example.cloud.databricks.com");
    props.put(DatabricksUnityCatalogConfig.WAREHOUSE_ID, "wh-1");
    ExternalCatalogConfig config =
        ExternalCatalogConfig.builder()
            .catalogId("uc")
            .catalogType(CatalogType.DATABRICKS_UC)
            .catalogProperties(props)
            .build();

    DatabricksUnityCatalogSyncClient client =
        new DatabricksUnityCatalogSyncClient(
            config,
            TableFormat.DELTA,
            new Configuration(),
            mockStatementExecution,
            mockTablesApi,
            mockSchemasApi);

    ThreePartHierarchicalTableIdentifier tableIdentifier =
        new ThreePartHierarchicalTableIdentifier("main", "default", "people");

    doThrow(new RuntimeException("boom")).when(mockTablesApi).delete("main.default.people");

    assertThrows(
        CatalogSyncException.class,
        () ->
            client.dropTable(
                InternalTable.builder().basePath("s3://bucket/path").build(), tableIdentifier));
  }

  @Test
  void testHasDatabaseTrue() {
    Map<String, String> props = new HashMap<>();
    props.put(DatabricksUnityCatalogConfig.HOST, "https://example.cloud.databricks.com");
    props.put(DatabricksUnityCatalogConfig.WAREHOUSE_ID, "wh-1");
    ExternalCatalogConfig config =
        ExternalCatalogConfig.builder()
            .catalogId("uc")
            .catalogType(CatalogType.DATABRICKS_UC)
            .catalogProperties(props)
            .build();

    DatabricksUnityCatalogSyncClient client =
        new DatabricksUnityCatalogSyncClient(
            config,
            TableFormat.DELTA,
            new Configuration(),
            mockStatementExecution,
            mockTablesApi,
            mockSchemasApi);

    when(mockSchemasApi.get("main.default")).thenReturn(new SchemaInfo());

    ThreePartHierarchicalTableIdentifier tableIdentifier =
        new ThreePartHierarchicalTableIdentifier("main", "default", "people");
    boolean exists = client.hasDatabase(tableIdentifier);
    assertEquals(true, exists);

    verify(mockSchemasApi).get("main.default");
  }

  @Test
  void testHasDatabaseFalse() {
    Map<String, String> props = new HashMap<>();
    props.put(DatabricksUnityCatalogConfig.HOST, "https://example.cloud.databricks.com");
    props.put(DatabricksUnityCatalogConfig.WAREHOUSE_ID, "wh-1");
    ExternalCatalogConfig config =
        ExternalCatalogConfig.builder()
            .catalogId("uc")
            .catalogType(CatalogType.DATABRICKS_UC)
            .catalogProperties(props)
            .build();

    DatabricksUnityCatalogSyncClient client =
        new DatabricksUnityCatalogSyncClient(
            config,
            TableFormat.DELTA,
            new Configuration(),
            mockStatementExecution,
            mockTablesApi,
            mockSchemasApi);

    when(mockSchemasApi.get("main.default")).thenThrow(new NotFound("not found", null));

    ThreePartHierarchicalTableIdentifier tableIdentifier =
        new ThreePartHierarchicalTableIdentifier("main", "default", "people");
    boolean exists = client.hasDatabase(tableIdentifier);
    assertEquals(false, exists);
  }

  @Test
  void testHasDatabaseFailedStatement() {
    Map<String, String> props = new HashMap<>();
    props.put(DatabricksUnityCatalogConfig.HOST, "https://example.cloud.databricks.com");
    props.put(DatabricksUnityCatalogConfig.WAREHOUSE_ID, "wh-1");
    ExternalCatalogConfig config =
        ExternalCatalogConfig.builder()
            .catalogId("uc")
            .catalogType(CatalogType.DATABRICKS_UC)
            .catalogProperties(props)
            .build();

    DatabricksUnityCatalogSyncClient client =
        new DatabricksUnityCatalogSyncClient(
            config,
            TableFormat.DELTA,
            new Configuration(),
            mockStatementExecution,
            mockTablesApi,
            mockSchemasApi);

    when(mockSchemasApi.get("main.default")).thenThrow(new RuntimeException("boom"));

    ThreePartHierarchicalTableIdentifier tableIdentifier =
        new ThreePartHierarchicalTableIdentifier("main", "default", "people");
    assertThrows(CatalogSyncException.class, () -> client.hasDatabase(tableIdentifier));
  }

  @Test
  void testCreateDatabase() {
    Map<String, String> props = new HashMap<>();
    props.put(DatabricksUnityCatalogConfig.HOST, "https://example.cloud.databricks.com");
    props.put(DatabricksUnityCatalogConfig.WAREHOUSE_ID, "wh-1");
    ExternalCatalogConfig config =
        ExternalCatalogConfig.builder()
            .catalogId("uc")
            .catalogType(CatalogType.DATABRICKS_UC)
            .catalogProperties(props)
            .build();

    DatabricksUnityCatalogSyncClient client =
        new DatabricksUnityCatalogSyncClient(
            config,
            TableFormat.DELTA,
            new Configuration(),
            mockStatementExecution,
            mockTablesApi,
            mockSchemasApi);

    ThreePartHierarchicalTableIdentifier tableIdentifier =
        new ThreePartHierarchicalTableIdentifier("main", "default", "people");

    client.createDatabase(tableIdentifier);

    ArgumentCaptor<CreateSchema> requestCaptor = ArgumentCaptor.forClass(CreateSchema.class);
    verify(mockSchemasApi).create(requestCaptor.capture());
    CreateSchema request = requestCaptor.getValue();
    assertEquals("main", request.getCatalogName());
    assertEquals("default", request.getName());
  }

  @Test
  void testCreateDatabaseFailure() {
    Map<String, String> props = new HashMap<>();
    props.put(DatabricksUnityCatalogConfig.HOST, "https://example.cloud.databricks.com");
    props.put(DatabricksUnityCatalogConfig.WAREHOUSE_ID, "wh-1");
    ExternalCatalogConfig config =
        ExternalCatalogConfig.builder()
            .catalogId("uc")
            .catalogType(CatalogType.DATABRICKS_UC)
            .catalogProperties(props)
            .build();

    DatabricksUnityCatalogSyncClient client =
        new DatabricksUnityCatalogSyncClient(
            config,
            TableFormat.DELTA,
            new Configuration(),
            mockStatementExecution,
            mockTablesApi,
            mockSchemasApi);

    ThreePartHierarchicalTableIdentifier tableIdentifier =
        new ThreePartHierarchicalTableIdentifier("main", "default", "people");

    doThrow(new RuntimeException("boom")).when(mockSchemasApi).create(any(CreateSchema.class));

    assertThrows(CatalogSyncException.class, () -> client.createDatabase(tableIdentifier));
  }

  @Test
  void testGetTableFound() {
    Map<String, String> props = new HashMap<>();
    props.put(DatabricksUnityCatalogConfig.HOST, "https://example.cloud.databricks.com");
    props.put(DatabricksUnityCatalogConfig.WAREHOUSE_ID, "wh-1");
    ExternalCatalogConfig config =
        ExternalCatalogConfig.builder()
            .catalogId("uc")
            .catalogType(CatalogType.DATABRICKS_UC)
            .catalogProperties(props)
            .build();

    DatabricksUnityCatalogSyncClient client =
        new DatabricksUnityCatalogSyncClient(
            config,
            TableFormat.DELTA,
            new Configuration(),
            mockStatementExecution,
            mockTablesApi,
            mockSchemasApi);

    TableInfo tableInfo = new TableInfo().setStorageLocation("s3://bucket/path");
    when(mockTablesApi.get("main.default.people")).thenReturn(tableInfo);

    ThreePartHierarchicalTableIdentifier tableIdentifier =
        new ThreePartHierarchicalTableIdentifier("main", "default", "people");
    Object table = client.getTable(tableIdentifier);
    assertNotNull(table);

    verify(mockTablesApi).get("main.default.people");
  }

  @Test
  void testGetTableNotFound() {
    Map<String, String> props = new HashMap<>();
    props.put(DatabricksUnityCatalogConfig.HOST, "https://example.cloud.databricks.com");
    props.put(DatabricksUnityCatalogConfig.WAREHOUSE_ID, "wh-1");
    ExternalCatalogConfig config =
        ExternalCatalogConfig.builder()
            .catalogId("uc")
            .catalogType(CatalogType.DATABRICKS_UC)
            .catalogProperties(props)
            .build();

    DatabricksUnityCatalogSyncClient client =
        new DatabricksUnityCatalogSyncClient(
            config,
            TableFormat.DELTA,
            new Configuration(),
            mockStatementExecution,
            mockTablesApi,
            mockSchemasApi);

    when(mockTablesApi.get("main.default.people")).thenThrow(new NotFound("not found", null));

    ThreePartHierarchicalTableIdentifier tableIdentifier =
        new ThreePartHierarchicalTableIdentifier("main", "default", "people");
    Object table = client.getTable(tableIdentifier);
    assertNull(table);
  }

  @Test
  void testGetTableFailedStatement() {
    Map<String, String> props = new HashMap<>();
    props.put(DatabricksUnityCatalogConfig.HOST, "https://example.cloud.databricks.com");
    props.put(DatabricksUnityCatalogConfig.WAREHOUSE_ID, "wh-1");
    ExternalCatalogConfig config =
        ExternalCatalogConfig.builder()
            .catalogId("uc")
            .catalogType(CatalogType.DATABRICKS_UC)
            .catalogProperties(props)
            .build();

    DatabricksUnityCatalogSyncClient client =
        new DatabricksUnityCatalogSyncClient(
            config,
            TableFormat.DELTA,
            new Configuration(),
            mockStatementExecution,
            mockTablesApi,
            mockSchemasApi);

    when(mockTablesApi.get("main.default.people")).thenThrow(new RuntimeException("boom"));

    ThreePartHierarchicalTableIdentifier tableIdentifier =
        new ThreePartHierarchicalTableIdentifier("main", "default", "people");
    assertThrows(CatalogSyncException.class, () -> client.getTable(tableIdentifier));
  }
}
