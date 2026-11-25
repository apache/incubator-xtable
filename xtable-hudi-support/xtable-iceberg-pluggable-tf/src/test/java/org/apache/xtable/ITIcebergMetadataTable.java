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
import static org.apache.xtable.IcebergTableAssertions.assertIcebergReferencesExactly;
import static org.apache.xtable.IcebergTableAssertions.assertNothingPending;
import static org.apache.xtable.IcebergTableAssertions.baseFilesWithRows;
import static org.apache.xtable.IcebergTableAssertions.icebergRowCount;
import static org.apache.xtable.IcebergTableAssertions.reconstructedTimeline;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.storage.StoragePath;

/**
 * A writer that leaves the Hudi metadata table at its default keeps it, and the format serves
 * Hudi's metadata from it as the native format would, so index lookups that the file system listing
 * cannot answer work and the table stays consistent through the lifecycle hooks.
 */
class ITIcebergMetadataTable {
  @TempDir public static Path tempDir;

  /** The harness turns the metadata table off for pluggable formats; these tests turn it on. */
  private static Properties metadataTableEnabled(String... keyValues) {
    Properties properties = new Properties();
    properties.setProperty(HoodieMetadataConfig.ENABLE.key(), "true");
    for (int i = 0; i < keyValues.length; i += 2) {
      properties.setProperty(keyValues[i], keyValues[i + 1]);
    }
    return properties;
  }

  private static boolean metadataTableAvailable(TestJavaHudiTable table) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    return HoodieTableConfig.loadFromHoodieProps(metaClient.getStorage(), metaClient.getMetaPath())
        .isMetadataTableAvailable();
  }

  private static HoodieTableMetadata tableMetadata(TestJavaHudiTable table) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    return metaClient
        .getTableFormat()
        .getMetadataFactory()
        .create(
            new HoodieLocalEngineContext(getStorageConf(new Configuration())),
            metaClient.getStorage(),
            HoodieMetadataConfig.newBuilder().enable(true).build(),
            table.getBasePath(),
            false);
  }

  @Test
  void metadataTableIsBuiltAndServedWhenTheWriterKeepsIt() throws IOException {
    // The bloom index with hoodie.bloom.index.use.metadata reads column stats from the metadata
    // table, which the file system listing refuses, so this is the lookup a default writer
    // would fail with if the format did not serve the metadata table.
    Properties bloomIndexFromMetadata =
        metadataTableEnabled(
            HoodieIndexConfig.INDEX_TYPE.key(),
            HoodieIndex.IndexType.BLOOM.name(),
            HoodieIndexConfig.BLOOM_INDEX_USE_METADATA.key(),
            "true");
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            "mdt_" + UUID.randomUUID(),
            tempDir,
            "level:SIMPLE",
            HoodieTableType.COPY_ON_WRITE,
            bloomIndexFromMetadata)) {
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(100, true);
      assertTrue(
          metadataTableAvailable(table), "a writer at Hudi's defaults builds the metadata table");
      assertTrue(
          tableMetadata(table) instanceof HoodieBackedTableMetadata,
          "the format serves the metadata table once it exists");

      table.upsertRecords(inserts.subList(0, 30), true);
      table.deleteRecords(inserts.subList(30, 40), true);
      assertIcebergReferencesExactly(
          table.getBasePath(),
          baseFilesWithRows(table.getMetaClient(), table.getAllLatestBaseFilePaths()));
      assertEquals(90, icebergRowCount(table.getBasePath()));
      assertEquals(3, tableMetadata(table).getAllPartitionPaths().size());

      table.cluster();
      assertIcebergReferencesExactly(
          table.getBasePath(),
          baseFilesWithRows(table.getMetaClient(), table.getAllLatestBaseFilePaths()));
      assertEquals(90, icebergRowCount(table.getBasePath()));
      assertNothingPending(reconstructedTimeline(table.getBasePath()));
    }
  }

  @Test
  void crashedPublishIsRolledBackWithTheMetadataTable() throws IOException {
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            "mdt_" + UUID.randomUUID(),
            tempDir,
            null,
            HoodieTableType.COPY_ON_WRITE,
            metadataTableEnabled())) {
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(100, true);
      List<String> filesAfterInsert = table.getAllLatestBaseFilePaths();
      // The metadata table records the upsert before Hudi completes it; a read-only Iceberg
      // metadata directory then fails the publish, so the upsert is in the metadata table and
      // reported pending by the reconstructed timeline.
      Path metadataDirectory = Paths.get(URI.create(table.getBasePath())).resolve("metadata");
      Set<PosixFilePermission> writable = Files.getPosixFilePermissions(metadataDirectory);
      Files.setPosixFilePermissions(
          metadataDirectory, PosixFilePermissions.fromString("r-xr-xr-x"));
      assumeTrue(!Files.isWritable(metadataDirectory), "needs a user the permissions bind");
      String crashedUpsert;
      try {
        assertThrows(Exception.class, () -> table.upsertRecords(inserts.subList(0, 20), true));
        crashedUpsert =
            table.getMetaClient().reloadActiveTimeline().lastInstant().get().requestedTime();
      } finally {
        Files.setPosixFilePermissions(metadataDirectory, writable);
      }
      assertFalse(
          reconstructedTimeline(table.getBasePath())
              .filterCompletedInstants()
              .containsInstant(crashedUpsert));

      table.getWriteClient().rollback(crashedUpsert);
      assertIcebergReferencesExactly(table.getBasePath(), filesAfterInsert);
      assertEquals(100, icebergRowCount(table.getBasePath()));
      table.insertRecords(50, true);
      assertIcebergReferencesExactly(table.getBasePath(), table.getAllLatestBaseFilePaths());
      assertEquals(150, icebergRowCount(table.getBasePath()));
      assertNothingPending(reconstructedTimeline(table.getBasePath()));
    }
  }

  /**
   * Hudi's defaults keep 20 commits active and compact the metadata table every 10 delta commits,
   * so an archived commit is always compacted. The harness archives once five commits are active,
   * so the compaction interval is lowered below that here.
   */
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void archivalAndCleaningKeepTheMetadataTableListingComplete(boolean archival) throws IOException {
    HoodieArchivalConfig archivalConfig =
        archival
            ? HoodieArchivalConfig.newBuilder().archiveCommitsWith(3, 4).build()
            : HoodieArchivalConfig.newBuilder().archiveCommitsWith(50, 60).build();
    Properties compactEveryTwoDeltaCommits =
        metadataTableEnabled(HoodieMetadataConfig.COMPACT_NUM_DELTA_COMMITS.key(), "2");
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            "mdt_" + UUID.randomUUID(),
            tempDir,
            "level:SIMPLE",
            HoodieTableType.COPY_ON_WRITE,
            archivalConfig,
            compactEveryTwoDeltaCommits)) {
      List<HoodieRecord<HoodieAvroPayload>> records =
          new ArrayList<>(table.insertRecords(40, true));
      for (int i = 0; i < 12; i++) {
        records.addAll(table.insertRecords(10, true));
        table.upsertRecords(records.subList(0, 20), true);
        if (i > 1) {
          table.getWriteClient().clean();
        }
        assertTrue(metadataTableAvailable(table));
        Set<String> listedByStorage =
            table.getAllLatestBaseFilePaths().stream()
                .map(path -> new StoragePath(path).toUri().getPath())
                .collect(Collectors.toSet());
        Set<String> listedByMetadataTable = new HashSet<>();
        HoodieTableMetadata metadata = tableMetadata(table);
        for (String partition : metadata.getAllPartitionPaths()) {
          metadata
              .getAllFilesInPartition(new StoragePath(table.getBasePath(), partition))
              .forEach(file -> listedByMetadataTable.add(file.getPath().toUri().getPath()));
        }
        assertTrue(
            listedByMetadataTable.containsAll(listedByStorage),
            "round "
                + i
                + ": the metadata table listing misses live files: "
                + listedByStorage.stream()
                    .filter(path -> !listedByMetadataTable.contains(path))
                    .collect(Collectors.toList()));
        assertIcebergReferencesExactly(table.getBasePath(), table.getAllLatestBaseFilePaths());
        assertEquals(records.size(), icebergRowCount(table.getBasePath()), "round " + i);
      }
    }
  }

  @Test
  void archivalBeforeTheFirstMetadataTableCompactionKeepsWriting() throws IOException {
    // A metadata table built with the table has no compaction until the default interval of delta
    // commits, while the harness archives once five commits are active. The archive hook runs after
    // the write committed in Hudi and Iceberg, so it must not fail it.
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            "mdt_" + UUID.randomUUID(),
            tempDir,
            null,
            HoodieTableType.COPY_ON_WRITE,
            HoodieArchivalConfig.newBuilder().archiveCommitsWith(3, 4).build(),
            metadataTableEnabled())) {
      for (int i = 0; i < 8; i++) {
        table.insertRecords(10, true);
      }
      assertTrue(
          table.getMetaClient().getArchivedTimeline().reload().countInstants() > 0,
          "the writes must have archived commits");
      Set<String> listedByStorage =
          table.getAllLatestBaseFilePaths().stream()
              .map(path -> new StoragePath(path).toUri().getPath())
              .collect(Collectors.toSet());
      Set<String> listedByMetadataTable = new HashSet<>();
      HoodieTableMetadata metadata = tableMetadata(table);
      for (String partition : metadata.getAllPartitionPaths()) {
        metadata
            .getAllFilesInPartition(new StoragePath(table.getBasePath(), partition))
            .forEach(file -> listedByMetadataTable.add(file.getPath().toUri().getPath()));
      }
      assertEquals(listedByStorage, listedByMetadataTable);
      assertIcebergReferencesExactly(table.getBasePath(), table.getAllLatestBaseFilePaths());
      assertEquals(80, icebergRowCount(table.getBasePath()));
    }
  }

  @Test
  void writerWithTheMetadataTableDisabledKeepsTheTableConsistent() {
    String tableName = "mdt_" + UUID.randomUUID();
    String basePath = tempDir.resolve(tableName).toString();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchemaAtBasePath(
            tableName, basePath, null, HoodieTableType.COPY_ON_WRITE, metadataTableEnabled())) {
      table.insertRecords(100, true);
      assertTrue(metadataTableAvailable(table));
    }
    Properties metadataTableDisabled = new Properties();
    metadataTableDisabled.setProperty(HoodieMetadataConfig.ENABLE.key(), "false");
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchemaAtBasePath(
            tableName, basePath, null, HoodieTableType.COPY_ON_WRITE, metadataTableDisabled)) {
      table.insertRecords(50, true);
      assertIcebergReferencesExactly(basePath, table.getAllLatestBaseFilePaths());
      assertEquals(150, icebergRowCount(basePath));
      assertNothingPending(reconstructedTimeline(basePath));
    }
  }
}
