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
 
package org.apache.xtable.glue.table;

import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.PREVIOUS_METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.xtable.glue.GlueCatalogSyncClient.GLUE_EXTERNAL_TABLE_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import org.apache.iceberg.BaseTable;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.hadoop.HadoopTables;

import org.apache.xtable.glue.GlueCatalogSyncTestBase;
import org.apache.xtable.glue.GlueSchemaExtractor;
import org.apache.xtable.model.storage.TableFormat;

import software.amazon.awssdk.services.glue.model.Column;
import software.amazon.awssdk.services.glue.model.StorageDescriptor;
import software.amazon.awssdk.services.glue.model.Table;
import software.amazon.awssdk.services.glue.model.TableInput;

@ExtendWith(MockitoExtension.class)
public class TestIcebergGlueCatalogTableBuilder extends GlueCatalogSyncTestBase {

  @Mock private HadoopTables mockIcebergHadoopTables;
  @Mock private BaseTable mockIcebergBaseTable;
  @Mock private TableOperations mockIcebergTableOperations;
  @Mock private TableMetadata mockIcebergTableMetadata;
  private IcebergGlueCatalogTableBuilder icebergGlueCatalogTableBuilder;

  private IcebergGlueCatalogTableBuilder createIcebergGlueCatalogSyncHelper() {
    return new IcebergGlueCatalogTableBuilder(
        GlueSchemaExtractor.getInstance(), mockIcebergHadoopTables);
  }

  void setupCommonMocks() {
    icebergGlueCatalogTableBuilder = createIcebergGlueCatalogSyncHelper();
  }

  void mockIcebergHadoopTables() {
    when(mockIcebergHadoopTables.load(TEST_BASE_PATH)).thenReturn(mockIcebergBaseTable);
    mockIcebergMetadataFileLocation();
  }

  void mockIcebergMetadataFileLocation() {
    when(mockIcebergBaseTable.operations()).thenReturn(mockIcebergTableOperations);
    when(mockIcebergTableOperations.current()).thenReturn(mockIcebergTableMetadata);
    when(mockIcebergTableMetadata.metadataFileLocation())
        .thenReturn(ICEBERG_METADATA_FILE_LOCATION);
  }

  @Test
  void testGetCreateTableRequest() {
    setupCommonMocks();
    mockIcebergHadoopTables();

    TableInput expected =
        getCreateOrUpdateTableInput(
            TEST_GLUE_TABLE,
            icebergGlueCatalogTableBuilder.getTableParameters(mockIcebergBaseTable),
            getTestStorageDescriptor(ICEBERG_GLUE_SCHEMA));
    TableInput output =
        icebergGlueCatalogTableBuilder.getCreateTableRequest(
            TEST_ICEBERG_INTERNAL_TABLE, TEST_CATALOG_TABLE_IDENTIFIER);
    assertEquals(expected, output);
  }

  @Test
  void testGetUpdateTableRequest() {
    setupCommonMocks();
    mockIcebergHadoopTables();

    Map<String, String> glueTableParams = new HashMap<>();
    glueTableParams.put(METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION);
    Table glueTable =
        Table.builder()
            .parameters(glueTableParams)
            .storageDescriptor(getTestStorageDescriptor(ICEBERG_GLUE_SCHEMA))
            .build();

    Map<String, String> parameters = new HashMap<>();
    parameters.put(PREVIOUS_METADATA_LOCATION_PROP, glueTableParams.get(METADATA_LOCATION_PROP));
    when(mockIcebergTableMetadata.metadataFileLocation())
        .thenReturn(ICEBERG_METADATA_FILE_LOCATION_v2);
    parameters.put(METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION_v2);

    TableInput expected =
        getCreateOrUpdateTableInput(
            TEST_GLUE_TABLE, parameters, getTestStorageDescriptor(UPDATED_ICEBERG_GLUE_SCHEMA));
    TableInput output =
        icebergGlueCatalogTableBuilder.getUpdateTableRequest(
            TEST_UPDATED_ICEBERG_INTERNAL_TABLE, glueTable, TEST_CATALOG_TABLE_IDENTIFIER);
    assertEquals(expected, output);
  }

  @Test
  void testGetTableParameters() {
    icebergGlueCatalogTableBuilder = createIcebergGlueCatalogSyncHelper();
    mockIcebergMetadataFileLocation();
    Map<String, String> expected = new HashMap<>();
    expected.put(TABLE_TYPE_PROP, TableFormat.ICEBERG);
    expected.put(METADATA_LOCATION_PROP, ICEBERG_METADATA_FILE_LOCATION);
    Map<String, String> tableParameters =
        icebergGlueCatalogTableBuilder.getTableParameters(mockIcebergBaseTable);
    assertEquals(expected, tableParameters);
  }

  private TableInput getCreateOrUpdateTableInput(
      String tableName, Map<String, String> params, StorageDescriptor sd) {
    return TableInput.builder()
        .name(tableName)
        .tableType(GLUE_EXTERNAL_TABLE_TYPE)
        .parameters(params)
        .storageDescriptor(sd)
        .build();
  }

  private StorageDescriptor getTestStorageDescriptor(List<Column> columns) {
    return StorageDescriptor.builder().columns(columns).location(TEST_BASE_PATH).build();
  }
}
