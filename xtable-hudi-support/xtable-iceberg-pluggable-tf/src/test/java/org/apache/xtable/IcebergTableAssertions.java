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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.storage.StoragePath;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataOperations;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;

/** Helpers shared by the integration tests that compare the Iceberg table against Hudi. */
final class IcebergTableAssertions {
  private IcebergTableAssertions() {}

  /**
   * The current snapshot is a replace whose data files keep the data sequence number of the
   * snapshot before it, as Iceberg's own rewrite of data files publishes a compaction.
   */
  static void assertPublishedAsARewrite(String basePath) {
    Table table = icebergTable(basePath);
    Snapshot snapshot = table.currentSnapshot();
    assertEquals(DataOperations.REPLACE, snapshot.operation());
    long startingSequenceNumber = table.snapshot(snapshot.parentId()).sequenceNumber();
    List<DataFile> added = new ArrayList<>();
    snapshot.addedDataFiles(table.io()).forEach(added::add);
    assertFalse(added.isEmpty(), "the rewrite added no data file");
    for (DataFile file : added) {
      assertEquals(startingSequenceNumber, file.dataSequenceNumber(), file.location());
    }
  }

  static Table icebergTable(String basePath) {
    return new HadoopTables(new Configuration()).load(basePath);
  }

  static boolean icebergTableExists(String basePath) {
    return new HadoopTables(new Configuration()).exists(basePath);
  }

  static void assertIcebergReferencesExactly(String basePath, List<String> expectedBaseFiles) {
    assertIcebergReferencesExactly(icebergTable(basePath), expectedBaseFiles);
  }

  /** Iceberg plans exactly the given Hudi base files, and each of them exists on storage. */
  @SneakyThrows
  static void assertIcebergReferencesExactly(Table icebergTable, List<String> expectedBaseFiles) {
    Set<String> referenced = new HashSet<>();
    try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        referenced.add(new StoragePath(task.file().path().toString()).toUri().getPath());
      }
    }
    // StoragePath parses a path the way Hudi spells it, with a space or a percent sign as is,
    // which java.net.URI rejects.
    Set<String> expected =
        expectedBaseFiles.stream()
            .map(path -> new StoragePath(path).toUri().getPath())
            .collect(Collectors.toSet());
    assertEquals(expected, referenced, "Iceberg must reference exactly the live Hudi base files");
    for (String path : referenced) {
      assertTrue(Files.exists(Paths.get(path)), "Iceberg references a missing file: " + path);
    }
  }

  /**
   * The given Hudi base files that hold rows. A base file without rows, which a delete of every row
   * of a file group writes, is left out of Iceberg.
   */
  static List<String> baseFilesWithRows(HoodieTableMetaClient metaClient, List<String> baseFiles) {
    return baseFiles.stream()
        .filter(
            path ->
                HoodieIOFactory.getIOFactory(metaClient.getStorage())
                        .getFileFormatUtils(new StoragePath(path))
                        .getRowCount(metaClient.getStorage(), new StoragePath(path))
                    > 0)
        .collect(Collectors.toList());
  }

  /**
   * Rows read through Iceberg's own reader, which plans and splits the data files the way any
   * Iceberg engine does and reads them back, rather than trusting the record counts in metadata.
   *
   * <p>No column is projected. The generic reader ignores the table's name mapping and resolves the
   * columns of Hudi's base files, which carry no field ids, by position, so a column it reads holds
   * the values of another column, see {@code ITIcebergMetaColumns}. The row count does not depend
   * on the columns read.
   */
  @SneakyThrows
  static long icebergRowCount(String basePath) {
    long count = 0;
    try (CloseableIterable<Record> records =
        IcebergGenerics.read(icebergTable(basePath)).select().build()) {
      for (Record ignored : records) {
        count++;
      }
    }
    return count;
  }

  static HoodieTimeline reconstructedTimeline(String basePath) {
    return HoodieTableMetaClient.builder()
        .setConf(getStorageConf(new Configuration()))
        .setBasePath(basePath)
        .setLoadActiveTimelineOnLoad(true)
        .build()
        .getActiveTimeline();
  }

  static List<String> completedCommitTimes(HoodieTimeline timeline) {
    return timeline
        .getCommitsTimeline()
        .filterCompletedInstants()
        .getInstantsAsStream()
        .map(HoodieInstant::requestedTime)
        .collect(Collectors.toList());
  }

  static void assertNothingPending(HoodieTimeline timeline) {
    assertTrue(
        timeline.filterInflightsAndRequested().empty(),
        "no instant should be left pending: "
            + timeline.filterInflightsAndRequested().getInstants());
  }

  /**
   * Hudi does not copy writer properties under {@code hoodie.table.format.} into {@code
   * hoodie.properties} when it creates a table, so the settings are written before the first commit
   * and the table is opened again to pick them up.
   */
  static TestJavaHudiTable createWithTableConfig(
      String tableName, Path tempDir, Properties settings) {
    try (TestJavaHudiTable created =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      HoodieTableMetaClient metaClient = created.getMetaClient();
      HoodieTableConfig.update(metaClient.getStorage(), metaClient.getMetaPath(), settings);
    }
    return TestJavaHudiTable.forStandardSchema(
        tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE);
  }
}
