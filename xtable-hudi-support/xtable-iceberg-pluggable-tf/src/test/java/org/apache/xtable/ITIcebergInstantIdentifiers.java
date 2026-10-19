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

import static org.apache.xtable.IcebergTableAssertions.icebergRowCount;
import static org.apache.xtable.IcebergTableAssertions.icebergTable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;

import org.apache.xtable.conversion.ConversionTargetFactory;
import org.apache.xtable.hudi.HudiTestUtil;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.spi.sync.ConversionTarget;

/**
 * How the Hudi instant behind each Iceberg snapshot is identified: the generic source identifier is
 * the instant's requested time, as it is for a Hudi table synced by the conversion controller, and
 * the table state recorded by an instant that writes no data is the schema of the latest commit
 * rather than the schema as of the instant's own requested time.
 */
class ITIcebergInstantIdentifiers {
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
  void sourceIdentifierIsTheRequestedTimeOfTheHudiInstant() {
    String tableName = "identifiers_" + UUID.randomUUID();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      table.insertRecords(10, true);
      HoodieInstant commit = table.getMetaClient().reloadActiveTimeline().lastInstant().get();

      Table icebergTable = icebergTable(table.getBasePath());
      Snapshot snapshot = icebergTable.currentSnapshot();
      TableSyncMetadata recorded =
          TableSyncMetadata.fromJson(snapshot.summary().get(TableSyncMetadata.XTABLE_METADATA))
              .get();
      assertEquals(commit.requestedTime(), recorded.getSourceIdentifier());

      // The public lookup from a Hudi commit to its Iceberg snapshot takes the requested time, the
      // identifier HudiConversionSource#getCommitIdentifier defines for every Hudi source.
      ConversionTarget target =
          ConversionTargetFactory.getInstance()
              .createForFormat(
                  IcebergTableFormat.targetTable(tableName, table.getBasePath()),
                  new Configuration());
      Optional<String> targetIdentifier = target.getTargetCommitIdentifier(commit.requestedTime());
      assertEquals(Optional.of(String.valueOf(snapshot.snapshotId())), targetIdentifier);
    }
  }

  private static long nonNullNewFieldCount(String format, String basePath) {
    return sparkSession
        .read()
        .format(format)
        .load(basePath)
        .filter("new_top_level_field is not null")
        .count();
  }

  @Test
  void savepointKeepsTheSchemaOfTheLatestCommit() {
    String tableName = "savepoint_schema_" + UUID.randomUUID();
    String firstCommit;
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      table.insertRecords(10, true);
      firstCommit =
          table.getMetaClient().reloadActiveTimeline().lastInstant().get().requestedTime();
    }
    // A later commit adds a column; the savepoint of the first commit then carries the first
    // commit's requested time, which must not roll the Iceberg schema back to that commit.
    try (TestJavaHudiTable table =
        TestJavaHudiTable.withAdditionalTopLevelField(
            tableName,
            tempDir,
            null,
            HoodieTableType.COPY_ON_WRITE,
            TestAbstractHudiTable.BASIC_SCHEMA)) {
      table.insertRecords(10, true);
      assertNotNull(
          icebergTable(table.getBasePath()).schema().findField("new_top_level_field"),
          "the evolved schema should reach Iceberg with the commit that introduced it");

      table.getWriteClient().savepoint(firstCommit, "user", "keep the first commit");

      Table icebergTable = icebergTable(table.getBasePath());
      assertNotNull(
          icebergTable.schema().findField("new_top_level_field"),
          "the savepoint snapshot must keep the latest schema");
      assertTrue(
          icebergTable.currentSnapshot().summary().containsKey(TableSyncMetadata.XTABLE_METADATA));
      assertEquals(
          firstCommit,
          TableSyncMetadata.fromJson(
                  icebergTable.currentSnapshot().summary().get(TableSyncMetadata.XTABLE_METADATA))
              .get()
              .getSourceIdentifier(),
          "the savepoint snapshot is identified by the savepointed commit's requested time");
      assertEquals(20, icebergRowCount(table.getBasePath()));
      // The harness fills the nullable column at random, so compare against the Hudi read rather
      // than a fixed count.
      assertEquals(
          nonNullNewFieldCount("hudi", table.getBasePath()),
          nonNullNewFieldCount("iceberg", table.getBasePath()),
          "values of the added column stay readable after the savepoint");
    }
  }
}
