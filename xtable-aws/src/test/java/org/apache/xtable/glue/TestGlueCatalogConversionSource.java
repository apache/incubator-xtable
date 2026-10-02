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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.catalog.ThreePartHierarchicalTableIdentifier;
import org.apache.xtable.model.storage.CatalogType;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.extractor.CatalogConversionSource;

import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GlueException;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;

@ExtendWith(MockitoExtension.class)
public class TestGlueCatalogConversionSource {

  @Mock private GlueCatalogConfig mockCatalogConfig;
  @Mock private GlueClient mockGlueClient;
  private GlueCatalogConversionSource catalogConversionSource;
  private static final String GLUE_DB = "glue_db";
  private static final String GLUE_TABLE = "glue_tbl";
  private static final String TABLE_BASE_PATH = "/var/data/table";
  private static final String GLUE_CATALOG_ID = "aws-account-id";
  private static final ThreePartHierarchicalTableIdentifier tableIdentifier =
      new ThreePartHierarchicalTableIdentifier(GLUE_DB, GLUE_TABLE);
  private static final GetTableRequest getTableRequest =
      GetTableRequest.builder()
          .catalogId(GLUE_CATALOG_ID)
          .databaseName(GLUE_DB)
          .name(GLUE_TABLE)
          .build();

  void setup() {
    when(mockCatalogConfig.getCatalogId()).thenReturn(GLUE_CATALOG_ID);
    catalogConversionSource = new GlueCatalogConversionSource(mockCatalogConfig, mockGlueClient);
  }

  @Test
  void testGetSourceTable_errorGettingTableFromGlue() {
    setup();

    // error getting table from glue
    when(mockGlueClient.getTable(getTableRequest))
        .thenThrow(GlueException.builder().message("something went wrong").build());
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () -> catalogConversionSource.getSourceTable(tableIdentifier));
    assertEquals(
        String.format(
            "Failed to get table: %s.%s",
            tableIdentifier.getDatabaseName(), tableIdentifier.getTableName()),
        exception.getMessage());

    verify(mockGlueClient, times(1)).getTable(getTableRequest);
  }

  @Test
  void testGetSourceTable_tableNotFoundInGlue() {
    setup();

    // table not found in glue
    when(mockGlueClient.getTable(getTableRequest))
        .thenThrow(EntityNotFoundException.builder().message("table not found").build());
    CatalogSyncException exception =
        assertThrows(
            CatalogSyncException.class,
            () -> catalogConversionSource.getSourceTable(tableIdentifier));
    assertEquals(
        String.format(
            "Failed to get table: %s.%s",
            tableIdentifier.getDatabaseName(), tableIdentifier.getTableName()),
        exception.getMessage());

    verify(mockGlueClient, times(1)).getTable(getTableRequest);
  }

  @ParameterizedTest
  @CsvSource(value = {"ICEBERG", "HUDI", "DELTA"})
  void testGetSourceTable(String tableFormat) {
    setup();

    StorageDescriptor sd = StorageDescriptor.builder().location(TABLE_BASE_PATH).build();
    Map<String, String> tableParams = new HashMap<>();
    if (tableFormat.equals(TableFormat.ICEBERG)) {
      tableParams.put("write.data.path", String.format("%s/iceberg", TABLE_BASE_PATH));
      tableParams.put("table_type", tableFormat);
    } else {
      tableParams.put("spark.sql.sources.provider", tableFormat);
    }

    String dataPath =
        tableFormat.equals(TableFormat.ICEBERG)
            ? String.format("%s/iceberg", TABLE_BASE_PATH)
            : TABLE_BASE_PATH;
    SourceTable expected =
        newSourceTable(GLUE_TABLE, TABLE_BASE_PATH, dataPath, tableFormat, tableParams);
    when(mockGlueClient.getTable(getTableRequest))
        .thenReturn(
            GetTableResponse.builder()
                .table(newGlueTable(GLUE_DB, GLUE_TABLE, tableParams, sd))
                .build());
    SourceTable output = catalogConversionSource.getSourceTable(tableIdentifier);
    assertEquals(expected, output);

    verify(mockGlueClient, times(1)).getTable(getTableRequest);
  }

  private Table newGlueTable(
      String dbName, String tableName, Map<String, String> params, StorageDescriptor sd) {
    return Table.builder()
        .databaseName(dbName)
        .name(tableName)
        .parameters(params)
        .storageDescriptor(sd)
        .build();
  }

  private SourceTable newSourceTable(
      String tblName,
      String basePath,
      String dataPath,
      String tblFormat,
      Map<String, String> params) {
    Properties tblProperties = new Properties();
    tblProperties.putAll(params);
    return SourceTable.builder()
        .name(tblName)
        .basePath(basePath)
        .dataPath(dataPath)
        .formatName(tblFormat)
        .additionalProperties(tblProperties)
        .build();
  }

  @Test
  void testLoadInstanceByServiceLoader() {
    ServiceLoader<CatalogConversionSource> loader =
        ServiceLoader.load(CatalogConversionSource.class);
    CatalogConversionSource catalogConversionSource = null;

    for (CatalogConversionSource instance : loader) {
      if (instance.getCatalogType().equals(CatalogType.GLUE)) {
        catalogConversionSource = instance;
        break;
      }
    }
    assertNotNull(catalogConversionSource);
    assertEquals(
        catalogConversionSource.getClass().getName(), GlueCatalogConversionSource.class.getName());
  }
}
