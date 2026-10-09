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

import static org.apache.xtable.IcebergTableAssertions.reconstructedTimeline;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

/**
 * The reconstructed timeline is only as good as the Iceberg table it reads. Before the first commit
 * publishes a snapshot there is no Iceberg table and the pending Hudi instants stand as they are;
 * once commits exist, a missing Iceberg table means the metadata was removed, and the table must
 * refuse to load rather than appear empty.
 */
class ITIcebergMissingMetadata {
  @TempDir public static Path tempDir;

  @Test
  void pendingInstantsAreVisibleBeforeTheIcebergTableExists() {
    try (TestJavaHudiTable table = newTable()) {
      String firstCommit = table.startCommit();
      HoodieTimeline timeline = reconstructedTimeline(table.getBasePath());
      assertEquals(
          1,
          timeline.filterInflightsAndRequested().countInstants(),
          "the first, still pending, commit must be visible before any snapshot exists");
      table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(10), firstCommit, true);
      assertEquals(
          1, reconstructedTimeline(table.getBasePath()).filterCompletedInstants().countInstants());
    }
  }

  @Test
  void missingIcebergMetadataIsReportedRatherThanReadAsAnEmptyTable() throws Exception {
    try (TestJavaHudiTable table = newTable()) {
      table.insertRecords(10, true);
      table.insertRecords(10, true);
      deleteRecursively(Paths.get(URI.create(table.getBasePath())).resolve("metadata"));

      Exception exception =
          assertThrows(Exception.class, () -> reconstructedTimeline(table.getBasePath()));
      Throwable cause = exception;
      while (cause.getCause() != null && !(cause instanceof IllegalStateException)) {
        cause = cause.getCause();
      }
      assertTrue(
          cause instanceof IllegalStateException && cause.getMessage().contains("Iceberg"),
          "loading the table must name the missing Iceberg metadata: " + exception);
    }
  }

  private static void deleteRecursively(Path directory) throws IOException {
    try (Stream<Path> paths = Files.walk(directory)) {
      for (Path path :
          paths.sorted(Comparator.reverseOrder()).collect(java.util.stream.Collectors.toList())) {
        Files.delete(path);
      }
    }
  }

  private static TestJavaHudiTable newTable() {
    return TestJavaHudiTable.forStandardSchema(
        "missing_metadata_" + UUID.randomUUID(), tempDir, null, HoodieTableType.COPY_ON_WRITE);
  }
}
