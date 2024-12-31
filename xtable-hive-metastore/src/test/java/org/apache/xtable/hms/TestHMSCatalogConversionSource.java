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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import lombok.SneakyThrows;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.exception.CatalogSyncException;
import org.apache.xtable.model.catalog.ThreePartHierarchicalTableIdentifier;
import org.apache.xtable.model.storage.TableFormat;

@ExtendWith(MockitoExtension.class)
class TestHMSCatalogConversionSource {

  @Mock private HMSCatalogConfig mockCatalogConfig;
  @Mock private IMetaStoreClient mockMetaStoreClient;
  private HMSCatalogConversionSource catalogConversionSource;
  private static final String HMS_DB = "hms_db";
  private static final String HMS_TABLE = "hms_tbl";
  private static final String TABLE_BASE_PATH = "/var/data/table";
  private final ThreePartHierarchicalTableIdentifier tableIdentifier =
      new ThreePartHierarchicalTableIdentifier(HMS_DB, HMS_TABLE);

  @BeforeEach
  void init() {
    catalogConversionSource =
        new HMSCatalogConversionSource(mockCatalogConfig, mockMetaStoreClient);
  }

  @SneakyThrows
  @Test
  void testGetSourceTable_errorGettingTableFromHMS() {
    // error getting table from hms
    when(mockMetaStoreClient.getTable(HMS_DB, HMS_TABLE))
        .thenThrow(new TException("something went wrong"));
    assertThrows(
        CatalogSyncException.class, () -> catalogConversionSource.getSourceTable(tableIdentifier));

    verify(mockMetaStoreClient, times(1)).getTable(HMS_DB, HMS_TABLE);
  }

  @SneakyThrows
  @Test
  void testGetSourceTable_tableNotFoundInHMS() {
    // table not found in hms
    when(mockMetaStoreClient.getTable(HMS_DB, HMS_TABLE))
        .thenThrow(new NoSuchObjectException("table not found"));
    assertThrows(
        CatalogSyncException.class, () -> catalogConversionSource.getSourceTable(tableIdentifier));

    verify(mockMetaStoreClient, times(1)).getTable(HMS_DB, HMS_TABLE);
  }

  @SneakyThrows
  @Test
  void testGetSourceTable_tableFormatNotPresent() {
    // table format not present in table properties
    when(mockMetaStoreClient.getTable(HMS_DB, HMS_TABLE))
        .thenReturn(newHmsTable(HMS_DB, HMS_TABLE, Collections.emptyMap(), null));
    IllegalStateException exception =
        assertThrows(
            IllegalStateException.class,
            () -> catalogConversionSource.getSourceTable(tableIdentifier));
    assertEquals("TableFormat is null or empty for table: hms_db.hms_tbl", exception.getMessage());

    verify(mockMetaStoreClient, times(1)).getTable(HMS_DB, HMS_TABLE);
  }

  @SneakyThrows
  @ParameterizedTest
  @CsvSource(value = {"ICEBERG", "HUDI", "DELTA"})
  void testGetSourceTable(String tableFormat) {
    StorageDescriptor sd = new StorageDescriptor();
    sd.setLocation(TABLE_BASE_PATH);
    Map<String, String> tableParams = new HashMap<>();
    if (Objects.equals(tableFormat, TableFormat.ICEBERG)) {
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
        newSourceTable(HMS_TABLE, TABLE_BASE_PATH, dataPath, tableFormat, tableParams);
    when(mockMetaStoreClient.getTable(HMS_DB, HMS_TABLE))
        .thenReturn(newHmsTable(HMS_DB, HMS_TABLE, tableParams, sd));
    SourceTable output = catalogConversionSource.getSourceTable(tableIdentifier);
    assertEquals(expected, output);
  }

  private Table newHmsTable(
      String dbName, String tableName, Map<String, String> params, StorageDescriptor sd) {
    Table table = new Table();
    table.setDbName(dbName);
    table.setTableName(tableName);
    table.setParameters(params);
    table.setSd(sd);
    return table;
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
}
