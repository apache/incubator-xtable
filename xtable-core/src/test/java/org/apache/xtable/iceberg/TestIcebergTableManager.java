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
 
package org.apache.xtable.iceberg;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMappingParser;

public class TestIcebergTableManager {
  private static final String BASE_PATH = "file:///basePath/";
  private static final Map<String, String> OPTIONS = Collections.singletonMap("key", "value");
  private static final TableIdentifier IDENTIFIER = TableIdentifier.of("database1", "table1");
  private static final Configuration CONFIGURATION = new Configuration();
  private final Catalog mockCatalog = mock(Catalog.class);

  @Test
  void catalogGetTable() {
    String catalogName = "catalog1";
    StubCatalog.registerMock(catalogName, mockCatalog);
    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .catalogImpl(StubCatalog.class.getName())
            .catalogName(catalogName)
            .catalogOptions(OPTIONS)
            .build();
    Table mockTable = mock(Table.class);
    when(mockCatalog.loadTable(IDENTIFIER)).thenReturn(mockTable);

    IcebergTableManager tableManager = IcebergTableManager.of(CONFIGURATION);
    Table actual = tableManager.getTable(catalogConfig, IDENTIFIER, BASE_PATH);
    assertEquals(mockTable, actual);
    verify(mockCatalog).initialize(catalogName, OPTIONS);
  }

  @Test
  void catalogGetOrCreateWithExistingTable() {
    String catalogName = "catalog2";
    StubCatalog.registerMock(catalogName, mockCatalog);
    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .catalogImpl(StubCatalog.class.getName())
            .catalogName(catalogName)
            .catalogOptions(OPTIONS)
            .build();
    Table mockTable = mock(Table.class);
    when(mockCatalog.tableExists(IDENTIFIER)).thenReturn(true);
    when(mockCatalog.loadTable(IDENTIFIER)).thenReturn(mockTable);

    IcebergTableManager tableManager = IcebergTableManager.of(CONFIGURATION);
    Schema schema = new Schema();
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    Table actual =
        tableManager.getOrCreateTable(catalogConfig, IDENTIFIER, BASE_PATH, schema, partitionSpec);
    assertEquals(mockTable, actual);
    verify(mockCatalog).initialize(catalogName, OPTIONS);
    verify(mockCatalog, never()).createTable(any(), any(), any(), any());
  }

  @Test
  void catalogGetOrCreateWithNewTable() {
    String catalogName = "catalog3";
    StubCatalog.registerMock(catalogName, mockCatalog);
    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .catalogImpl(StubCatalog.class.getName())
            .catalogName(catalogName)
            .catalogOptions(OPTIONS)
            .build();
    Table mockTable = mock(Table.class);
    when(mockCatalog.tableExists(IDENTIFIER)).thenReturn(false);
    Schema schema = new Schema();
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    when(mockCatalog.createTable(
            IDENTIFIER,
            schema,
            partitionSpec,
            BASE_PATH,
            Collections.singletonMap(
                TableProperties.DEFAULT_NAME_MAPPING,
                NameMappingParser.toJson(MappingUtil.create(schema)))))
        .thenReturn(mockTable);

    IcebergTableManager tableManager = IcebergTableManager.of(CONFIGURATION);

    Table actual =
        tableManager.getOrCreateTable(catalogConfig, IDENTIFIER, BASE_PATH, schema, partitionSpec);
    assertEquals(mockTable, actual);
    verify(mockCatalog).initialize(catalogName, OPTIONS);
    verify(mockCatalog, never()).loadTable(any());
  }

  @Test
  void catalogGetOrCreateWithRaceConditionOnCreation() {
    String catalogName = "catalog4";
    StubCatalog.registerMock(catalogName, mockCatalog);
    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .catalogImpl(StubCatalog.class.getName())
            .catalogName(catalogName)
            .catalogOptions(OPTIONS)
            .build();
    Table mockTable = mock(Table.class);
    when(mockCatalog.tableExists(IDENTIFIER)).thenReturn(false);
    Schema schema = new Schema();
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    when(mockCatalog.createTable(
            IDENTIFIER,
            schema,
            partitionSpec,
            BASE_PATH,
            Collections.singletonMap(
                TableProperties.DEFAULT_NAME_MAPPING,
                NameMappingParser.toJson(MappingUtil.create(schema)))))
        .thenThrow(new AlreadyExistsException("Table already exists"));
    when(mockCatalog.loadTable(IDENTIFIER)).thenReturn(mockTable);

    IcebergTableManager tableManager = IcebergTableManager.of(CONFIGURATION);

    Table actual =
        tableManager.getOrCreateTable(catalogConfig, IDENTIFIER, BASE_PATH, schema, partitionSpec);
    assertEquals(mockTable, actual);
    verify(mockCatalog).initialize(catalogName, OPTIONS);
  }
}
