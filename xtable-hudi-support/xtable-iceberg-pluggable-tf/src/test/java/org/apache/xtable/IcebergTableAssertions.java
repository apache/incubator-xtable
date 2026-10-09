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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;

/** Assertions shared by the integration tests that compare the Iceberg table against Hudi. */
final class IcebergTableAssertions {
  private IcebergTableAssertions() {}

  static Table icebergTable(String basePath) {
    return new HadoopTables(new Configuration()).load(basePath);
  }

  static boolean icebergTableExists(String basePath) {
    return new HadoopTables(new Configuration()).exists(basePath);
  }

  /** Iceberg plans exactly the given Hudi base files, and each of them exists on storage. */
  @SneakyThrows
  static void assertIcebergReferencesExactly(String basePath, List<String> expectedBaseFiles) {
    Set<String> referenced = new HashSet<>();
    try (CloseableIterable<FileScanTask> tasks = icebergTable(basePath).newScan().planFiles()) {
      for (FileScanTask task : tasks) {
        referenced.add(URI.create(task.file().path().toString()).getPath());
      }
    }
    Set<String> expected =
        expectedBaseFiles.stream()
            .map(path -> URI.create(path).getPath())
            .collect(Collectors.toSet());
    assertEquals(expected, referenced, "Iceberg must reference exactly the live Hudi base files");
    for (String path : referenced) {
      assertTrue(Files.exists(Paths.get(path)), "Iceberg references a missing file: " + path);
    }
  }

  /** Rows read through the Iceberg Spark reader, scanning the data files rather than metadata. */
  static long icebergRowCount(SparkSession sparkSession, String basePath) {
    return sparkSession.read().format("iceberg").load(basePath).collectAsList().size();
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
}
