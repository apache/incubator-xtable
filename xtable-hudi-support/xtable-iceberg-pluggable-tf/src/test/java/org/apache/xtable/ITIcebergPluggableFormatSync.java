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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.nio.file.Path;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;

import org.apache.xtable.model.storage.TableFormat;

/**
 * Proves the end to end contract of the pluggable table format: a Hudi write on a table configured
 * with {@code hoodie.table.format=ICEBERG} must produce readable Iceberg metadata at the same base
 * path, with no XTable sync job involved.
 */
class ITIcebergPluggableFormatSync {

  @TempDir public static Path tempDir;

  private static Properties icebergFormatProperties() {
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.TABLE_FORMAT.key(), TableFormat.ICEBERG);
    // IcebergTimelineFactory builds on the v2 timeline, so the table must not use the v1 layout
    // that xtable-core pins its other Hudi test tables to.
    properties.put(
        HoodieTableConfig.VERSION.key(), String.valueOf(HoodieTableVersion.EIGHT.versionCode()));
    // IcebergBackedTableMetadata lists the file system, so it cannot back a Hudi metadata table.
    properties.put(HoodieMetadataConfig.ENABLE.key(), "false");
    return properties;
  }

  @Test
  void insertProducesIcebergSnapshot() {
    String tableName = "pluggable_insert";
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE, icebergFormatProperties())) {

      assertEquals(
          TableFormat.ICEBERG,
          table.getMetaClient().getTableFormat().getName(),
          "the table was not created with the Iceberg pluggable format");

      table.insertRecords(100, true);

      Table icebergTable = new HadoopTables(new Configuration()).load(table.getBasePath());
      Snapshot snapshot = icebergTable.currentSnapshot();
      assertNotNull(snapshot, "the Hudi commit did not produce an Iceberg snapshot");
      assertEquals(
          "100", snapshot.summary().get("total-records"), "Iceberg row count does not match Hudi");
    }
  }
}
