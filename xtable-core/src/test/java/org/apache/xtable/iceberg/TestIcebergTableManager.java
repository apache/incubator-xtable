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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;

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
    BaseTable mockInitialTable = mock(BaseTable.class);
    Table loadedTable = mock(Table.class);
    when(mockCatalog.tableExists(IDENTIFIER)).thenReturn(false);
    Schema schema = new Schema();
    PartitionSpec partitionSpec = PartitionSpec.unpartitioned();
    when(mockCatalog.createTable(
            eq(IDENTIFIER),
            any(),
            eq(PartitionSpec.unpartitioned()),
            eq(BASE_PATH),
            eq(Collections.emptyMap())))
        .thenReturn(mockInitialTable);
    when(mockCatalog.loadTable(IDENTIFIER)).thenReturn(loadedTable);

    TableOperations tableOperations = mock(TableOperations.class);
    when(mockInitialTable.operations()).thenReturn(tableOperations);
    TableMetadata initialMetadata = mock(TableMetadata.class);
    when(tableOperations.current()).thenReturn(initialMetadata);
    try (MockedStatic<TableMetadata> tableMetadataMockedStatic = mockStatic(TableMetadata.class)) {
      TableMetadata.Builder mockBuilder = mock(TableMetadata.Builder.class);
      tableMetadataMockedStatic
          .when(() -> TableMetadata.buildFrom(initialMetadata))
          .thenReturn(mockBuilder);
      TableMetadata updatedMetadata = mock(TableMetadata.class);
      when(mockBuilder.build()).thenReturn(updatedMetadata);

      IcebergTableManager tableManager = IcebergTableManager.of(CONFIGURATION);

      Table actual =
          tableManager.getOrCreateTable(
              catalogConfig, IDENTIFIER, BASE_PATH, schema, partitionSpec);
      assertEquals(loadedTable, actual);
      verify(mockCatalog).initialize(catalogName, OPTIONS);
      verify(tableOperations).commit(initialMetadata, updatedMetadata);
      verify(mockBuilder).setCurrentSchema(schema, schema.highestFieldId());
      verify(mockBuilder).setDefaultPartitionSpec(partitionSpec);
    }
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
            eq(IDENTIFIER), any(), any(), eq(BASE_PATH), eq(Collections.emptyMap())))
        .thenThrow(new AlreadyExistsException("Table already exists"));
    when(mockCatalog.loadTable(IDENTIFIER)).thenReturn(mockTable);

    IcebergTableManager tableManager = IcebergTableManager.of(CONFIGURATION);

    Table actual =
        tableManager.getOrCreateTable(catalogConfig, IDENTIFIER, BASE_PATH, schema, partitionSpec);
    assertEquals(mockTable, actual);
    verify(mockCatalog).initialize(catalogName, OPTIONS);
  }

  @Test
  void catalogOperationsWorkWhenTheContextClassLoaderCannotSeeTheCatalog() throws Exception {
    // A shared ForkJoin pool thread has the system class loader as its context loader, which
    // cannot see jars added at runtime; a loader that sees only the JDK stands in for it here.
    String catalogName = "catalogFromAnotherLoader";
    StubCatalog.registerMock(catalogName, mockCatalog);
    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .catalogImpl(StubCatalog.class.getName())
            .catalogName(catalogName)
            .catalogOptions(OPTIONS)
            .build();
    // Catalogs load more classes lazily through the context class loader, for example the AWS
    // catalogs' HTTP client configuration on the first S3 read.
    when(mockCatalog.tableExists(IDENTIFIER))
        .thenAnswer(
            invocation -> {
              Class.forName(
                  StubCatalog.class.getName(),
                  true,
                  Thread.currentThread().getContextClassLoader());
              return true;
            });
    IcebergTableManager tableManager = IcebergTableManager.of(CONFIGURATION);

    AtomicReference<Object> result = new AtomicReference<>();
    ClassLoader jdkOnlyLoader = new URLClassLoader(new URL[0], null);
    Thread thread =
        new Thread(
            () -> {
              try {
                result.set(tableManager.tableExists(catalogConfig, IDENTIFIER, BASE_PATH));
              } catch (Throwable t) {
                result.set(t);
              }
            });
    thread.setContextClassLoader(jdkOnlyLoader);
    thread.start();
    thread.join();

    assertEquals(Boolean.TRUE, result.get());
    verify(mockCatalog).initialize(catalogName, OPTIONS);
    assertTrue(thread.getContextClassLoader() == jdkOnlyLoader);
  }
}
