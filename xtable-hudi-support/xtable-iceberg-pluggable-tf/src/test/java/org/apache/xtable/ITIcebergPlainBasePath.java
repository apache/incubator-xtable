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
import static org.apache.xtable.IcebergTableAssertions.icebergRowCount;
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

import org.apache.iceberg.Snapshot;

import org.apache.xtable.model.storage.TableFormat;

/**
 * A writer that passes a plain local path, as a Spark job or Hudi Streamer does, creates a table
 * whose base path has no scheme, while the file system view lists files under the storage's
 * qualified form. The files a commit adds and the files it supersedes have to be recorded under one
 * form, or Iceberg keeps the superseded version alongside the new one.
 */
class ITIcebergPlainBasePath {
  @TempDir public static Path tempDir;

  @Test
  void anUpsertReplacesThePreviousFileVersionAtAPlainBasePath() {
    String basePath = tempDir.resolve("plain_" + UUID.randomUUID()).toString();
    Properties properties = new Properties();
    properties.put(HoodieTableConfig.TABLE_FORMAT.key(), TableFormat.ICEBERG);
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchemaAtBasePath(
            "plain_base_path", basePath, null, HoodieTableType.COPY_ON_WRITE, properties)) {
      List<HoodieRecord<HoodieAvroPayload>> records = table.insertRecords(10, true);
      String upsertCommit = table.startCommit();
      table.upsertRecordsWithCommitAlreadyStarted(records.subList(0, 5), upsertCommit, true);

      Snapshot upsert = icebergTable(basePath).currentSnapshot();
      assertEquals("1", upsert.summary().get("deleted-data-files"), upsert.summary().toString());
      assertIcebergReferencesExactly(basePath, table.getAllLatestBaseFilePaths());
      assertEquals(10, icebergRowCount(basePath), "the superseded file must not be read twice");

      // The rollback removes the upsert's file, recorded without a scheme in the rollback
      // metadata, and brings the superseded version back.
      table.rollback(upsertCommit);
      Snapshot rollback = icebergTable(basePath).currentSnapshot();
      assertEquals(
          "1", rollback.summary().get("deleted-data-files"), rollback.summary().toString());
      assertIcebergReferencesExactly(basePath, table.getAllLatestBaseFilePaths());
      assertEquals(10, icebergRowCount(basePath));
    }
  }
}
