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

import static org.apache.xtable.IcebergTableAssertions.assertIcebergReferencesExactly;
import static org.apache.xtable.IcebergTableAssertions.createWithTableConfig;
import static org.apache.xtable.IcebergTableAssertions.icebergTable;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import org.apache.iceberg.BaseTable;

/** The Iceberg format version of a table the pluggable format creates. */
class ITIcebergFormatVersion {
  @TempDir public static Path tempDir;

  private static int formatVersion(String basePath) {
    return ((BaseTable) icebergTable(basePath)).operations().current().formatVersion();
  }

  @Test
  void createsVersionThreeTablesByDefault() {
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            "format_version_default_" + UUID.randomUUID().toString().replace('-', '_'),
            tempDir,
            null,
            HoodieTableType.COPY_ON_WRITE)) {
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(10, true);
      assertEquals(3, formatVersion(table.getBasePath()));

      table.upsertRecords(inserts.subList(0, 5), true);
      assertEquals(3, formatVersion(table.getBasePath()));
      assertIcebergReferencesExactly(table.getBasePath(), table.getAllLatestBaseFilePaths());
    }
  }

  @Test
  void createsTheVersionTheTableConfigAsksForAndKeepsItAfterwards() {
    String tableName = "format_version_two_" + UUID.randomUUID().toString().replace('-', '_');
    Properties settings = new Properties();
    settings.setProperty(IcebergFormatConfig.FORMAT_VERSION, "2");
    String basePath;
    try (TestJavaHudiTable table = createWithTableConfig(tableName, tempDir, settings)) {
      basePath = table.getBasePath();
      table.insertRecords(10, true);
      assertEquals(2, formatVersion(basePath));
      HoodieTableMetaClient metaClient = table.getMetaClient();
      settings.setProperty(IcebergFormatConfig.FORMAT_VERSION, "3");
      HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), settings);
    }

    // The version only applies when the table is created; an existing table is not upgraded.
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      table.insertRecords(10, true);
      assertEquals(2, formatVersion(basePath));
      assertIcebergReferencesExactly(basePath, table.getAllLatestBaseFilePaths());
    }
  }
}
