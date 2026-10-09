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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;

/**
 * A Hudi clean deletes base files from storage. The Iceberg metadata has to stop referencing them,
 * otherwise a scan resolves paths that no longer exist.
 */
class ITIcebergCleanRemovesFiles {

  @TempDir public static Path tempDir;

  @Test
  void cleanedBaseFilesAreNoLongerReferencedByIceberg() throws IOException {
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            "clean_removes_files", tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      // Rewrite the same records so older file slices become cleanable, mirroring the sequence
      // ITIcebergVariousActions uses before its own clean.
      String firstCommit = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForFirstCommit = table.generateRecords(100);
      table.insertRecordsWithCommitAlreadyStarted(insertsForFirstCommit, firstCommit, true);
      table.upsertRecords(insertsForFirstCommit.subList(30, 40), true);
      String secondCommit = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(100), secondCommit, true);

      Set<String> referencedBeforeClean = referencedDataFiles(table.getBasePath());
      assertFalse(referencedBeforeClean.isEmpty(), "expected Iceberg to reference data files");

      table.clean();

      Set<String> referencedAfterClean = referencedDataFiles(table.getBasePath());
      assertFalse(referencedAfterClean.isEmpty(), "the clean must not empty the table");

      for (String referenced : referencedAfterClean) {
        assertTrue(
            Files.exists(Paths.get(URI.create(referenced).getPath())),
            "Iceberg still references a path that is no longer on storage: " + referenced);
      }
    }
  }

  private static Set<String> referencedDataFiles(String basePath) throws IOException {
    Table icebergTable = new HadoopTables(new Configuration()).load(basePath);
    assertNotNull(icebergTable.currentSnapshot(), "expected an Iceberg snapshot to exist");
    Set<String> paths = new HashSet<>();
    try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        DataFile file = task.file();
        paths.add(file.path().toString());
      }
    }
    return paths;
  }
}
