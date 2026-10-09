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
import static org.apache.xtable.IcebergTableAssertions.assertNothingPending;
import static org.apache.xtable.IcebergTableAssertions.icebergRowCount;
import static org.apache.xtable.IcebergTableAssertions.reconstructedTimeline;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.SneakyThrows;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.exception.HoodieRestoreException;

import org.apache.xtable.hudi.HudiTestUtil;

/**
 * Operations the Iceberg table format does not support yet have to be refused before they change
 * the table, rather than leaving Hudi and Iceberg silently disagreeing.
 */
class ITIcebergUnsupportedOperations {
  @TempDir public static Path tempDir;
  private static SparkSession sparkSession;

  @BeforeAll
  static void setupOnce() {
    SparkConf sparkConf = HudiTestUtil.getSparkConf(tempDir);
    sparkSession =
        SparkSession.builder().config(HoodieReadClient.addHoodieSupport(sparkConf)).getOrCreate();
  }

  @AfterAll
  static void teardown() {
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

  @Test
  void restoreIsRefusedBeforeAnyDataFileIsDeleted() {
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            "restore_" + UUID.randomUUID(), tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      table.insertRecords(100, true);
      String savepointed =
          table.getMetaClient().reloadActiveTimeline().lastInstant().get().requestedTime();
      table.insertRecords(50, true);
      List<String> filesBeforeRestore = table.getAllLatestBaseFilePaths();
      table.getWriteClient().savepoint(savepointed, "user", "before restore");

      // Hudi runs the rollbacks of a restore without publishing them, so the completed-rollback
      // hook never fires and Iceberg would keep pointing at the files the restore deletes.
      HoodieRestoreException exception =
          assertThrows(
              HoodieRestoreException.class,
              () -> table.getWriteClient().restoreToSavepoint(savepointed));
      assertTrue(
          rootCause(exception) instanceof UnsupportedOperationException,
          "restore must be refused as unsupported: " + exception);

      assertIcebergReferencesExactly(table.getBasePath(), filesBeforeRestore);
      assertEquals(filesBeforeRestore, table.getAllLatestBaseFilePaths());
      assertEquals(150, icebergRowCount(sparkSession, table.getBasePath()));
      HoodieTimeline timeline = reconstructedTimeline(table.getBasePath());
      assertTrue(timeline.getRestoreTimeline().empty(), "no restore instant may be left behind");
      assertNothingPending(timeline);

      // The table stays writable.
      table.insertRecords(25, true);
      assertIcebergReferencesExactly(table.getBasePath(), table.getAllLatestBaseFilePaths());
      assertEquals(175, icebergRowCount(sparkSession, table.getBasePath()));
    }
  }

  @Test
  void mergeOnReadTableIsRejectedWhenTheTableIsCreated() {
    String tableName = "mor_" + UUID.randomUUID();
    // Log files never become Iceberg data files, so the format refuses the table as soon as Hudi
    // loads it, rather than publishing snapshots that silently miss every update.
    Exception exception =
        assertThrows(
            Exception.class,
            () ->
                TestJavaHudiTable.forStandardSchema(
                        tableName, tempDir, null, HoodieTableType.MERGE_ON_READ)
                    .close());
    Throwable rootCause = rootCause(exception);
    assertTrue(
        rootCause instanceof UnsupportedOperationException
            && rootCause.getMessage().contains("MERGE_ON_READ"),
        "merge-on-read must be refused as unsupported: " + exception);
    Path basePath = tempDir.resolve(tableName + "_v1");
    assertFalse(
        IcebergTableAssertions.icebergTableExists(basePath.toUri().toString()),
        "no Iceberg table may be created for a merge-on-read table");
    assertEquals(
        Collections.emptyList(),
        instantFiles(basePath.resolve(".hoodie").resolve("timeline")),
        "no Hudi instant may be written for a merge-on-read table");
  }

  @SneakyThrows
  private static List<String> instantFiles(Path timelineDir) {
    if (!Files.isDirectory(timelineDir)) {
      return Collections.emptyList();
    }
    try (Stream<Path> files = Files.list(timelineDir)) {
      return files
          .filter(Files::isRegularFile)
          .map(path -> path.getFileName().toString())
          .collect(Collectors.toList());
    }
  }

  private static Throwable rootCause(Throwable throwable) {
    Throwable cause = throwable;
    while (cause.getCause() != null && cause.getCause() != cause) {
      cause = cause.getCause();
    }
    return cause;
  }
}
