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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.StatementExecutionAPI;
import com.databricks.sdk.service.sql.StatementResponse;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;

import org.apache.xtable.conversion.ExternalCatalogConfig;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.catalog.ThreePartHierarchicalTableIdentifier;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.model.storage.TableFormat;

@ExtendWith(MockitoExtension.class)
public class TestDatabricksUnityCatalogSyncClient {

  @Mock private StatementExecutionAPI mockStatementExecution;

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
            config, TableFormat.DELTA, new Configuration(), mockStatementExecution);

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
            config, TableFormat.ICEBERG, new Configuration(), mockStatementExecution);

    InternalTable table = InternalTable.builder().basePath("s3://bucket/path").build();
    ThreePartHierarchicalTableIdentifier tableIdentifier =
        new ThreePartHierarchicalTableIdentifier("main", "default", "people");

    assertThrows(CatalogSyncException.class, () -> client.createTable(table, tableIdentifier));
  }
}
