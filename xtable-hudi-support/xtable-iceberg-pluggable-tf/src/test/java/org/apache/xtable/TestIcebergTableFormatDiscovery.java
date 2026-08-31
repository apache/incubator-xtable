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
 
package org.apache.xtable;

import static org.apache.hudi.hadoop.fs.HadoopFSUtils.getStorageConf;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;

/**
 * Verifies that Hudi resolves {@link IcebergTableFormat} through the ServiceLoader when the table
 * config carries {@code hoodie.table.format=ICEBERG}. This isolates format discovery from the write
 * path.
 */
class TestIcebergTableFormatDiscovery {

  @TempDir public static Path tempDir;

  @Test
  void resolvesIcebergFormatFromTableConfig() throws Exception {
    assertResolvedFormat(HoodieTableVersion.EIGHT, "table_v8");
  }

  @Test
  void resolvesIcebergFormatOnTableVersionSix() throws Exception {
    assertResolvedFormat(HoodieTableVersion.SIX, "table_v6");
  }

  @Test
  void defaultsToNativeFormatWhenUnset() throws Exception {
    String basePath = tempDir.resolve("table_native").toString();
    Configuration conf = new Configuration();
    HoodieTableMetaClient.newTableBuilder()
        .setTableName("table_native")
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setRecordKeyFields("id")
        .initTable(getStorageConf(conf), basePath);

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(getStorageConf(conf)).setBasePath(basePath).build();
    assertEquals("native", metaClient.getTableFormat().getName());
  }

  private void assertResolvedFormat(HoodieTableVersion tableVersion, String tableName)
      throws Exception {
    String basePath = tempDir.resolve(tableName).toString();
    Configuration conf = new Configuration();

    Properties properties = new Properties();
    properties.put(
        HoodieTableConfig.TABLE_FORMAT.key(), org.apache.xtable.model.storage.TableFormat.ICEBERG);

    HoodieTableMetaClient.newTableBuilder()
        .fromProperties(properties)
        .setTableName(tableName)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableVersion(tableVersion)
        .setRecordKeyFields("id")
        .initTable(getStorageConf(conf), basePath);

    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setConf(getStorageConf(conf)).setBasePath(basePath).build();

    // the value must survive a round trip through hoodie.properties
    assertEquals(
        org.apache.xtable.model.storage.TableFormat.ICEBERG,
        metaClient.getTableConfig().getString(HoodieTableConfig.TABLE_FORMAT),
        "hoodie.table.format was not persisted into hoodie.properties");

    // and the ServiceLoader must then resolve our implementation
    assertEquals(
        org.apache.xtable.model.storage.TableFormat.ICEBERG,
        metaClient.getTableFormat().getName(),
        "ServiceLoader did not resolve IcebergTableFormat");
  }
}
