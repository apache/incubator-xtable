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
import static org.apache.xtable.IcebergTableAssertions.completedCommitTimes;
import static org.apache.xtable.IcebergTableAssertions.createWithTableConfig;
import static org.apache.xtable.IcebergTableAssertions.icebergTableExists;
import static org.apache.xtable.IcebergTableAssertions.reconstructedTimeline;
import static org.apache.xtable.MergeOnReadAssertions.deletedKeysByBaseFileInHudi;
import static org.apache.xtable.MergeOnReadAssertions.withLiveRows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.storage.StoragePath;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.inmemory.InMemoryCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.metrics.MetricsReport;
import org.apache.iceberg.metrics.MetricsReporter;
import org.apache.iceberg.metrics.ScanReport;
import org.apache.iceberg.types.Types;

import org.apache.xtable.iceberg.IcebergCatalogConfig;
import org.apache.xtable.iceberg.IcebergTableManager;
import org.apache.xtable.timeline.IcebergSnapshotInstants;

/**
 * A table whose settings name an Iceberg catalog and namespace is created in and published through
 * that catalog, with its location at the Hudi base path, by every hook and by the reconstructed
 * timeline.
 */
class ITIcebergCatalogTable {
  @TempDir public static Path tempDir;

  /** Remembers each loaded instance, so the test can reach the catalog the format publishes to. */
  public static class RegisteringInMemoryCatalog extends InMemoryCatalog {
    static final Map<String, RegisteringInMemoryCatalog> LOADED = new ConcurrentHashMap<>();

    @Override
    public void initialize(String name, Map<String, String> properties) {
      super.initialize(name, properties);
      LOADED.put(name, this);
    }
  }

  /** Records the scans planned on the tables of a catalog that names it as metrics reporter. */
  public static class RecordingScans implements MetricsReporter {
    static final List<ScanReport> SCANS = new CopyOnWriteArrayList<>();

    @Override
    public void report(MetricsReport report) {
      if (report instanceof ScanReport) {
        SCANS.add((ScanReport) report);
      }
    }
  }

  private final String tableName =
      "catalog_table_" + UUID.randomUUID().toString().replace('-', '_');
  private final Namespace namespace = Namespace.of("db", "hudi");
  private final TableIdentifier identifier = TableIdentifier.of(namespace, tableName);
  private final Properties settings = new Properties();
  private RegisteringInMemoryCatalog catalog;

  @BeforeEach
  void createCatalogNamespace() {
    String catalogName = "catalog_" + UUID.randomUUID().toString().replace('-', '_');
    settings.setProperty(IcebergFormatConfig.CATALOG_NAME, catalogName);
    settings.setProperty(
        IcebergFormatConfig.CATALOG_IMPL, RegisteringInMemoryCatalog.class.getName());
    settings.setProperty(IcebergFormatConfig.CATALOG_OPTION_PREFIX + "test-option", "value");
    settings.setProperty(IcebergFormatConfig.NAMESPACE, "db.hudi");

    // Load the catalog the way the format will, then create the namespace in it, as the database
    // of a real catalog exists before tables are written into it.
    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .catalogName(catalogName)
            .catalogImpl(RegisteringInMemoryCatalog.class.getName())
            .catalogOptions(Collections.singletonMap("test-option", "value"))
            .build();
    assertFalse(
        IcebergTableManager.of(new Configuration()).tableExists(catalogConfig, identifier, ""));
    catalog = RegisteringInMemoryCatalog.LOADED.get(catalogName);
    catalog.createNamespace(namespace);
  }

  @Test
  void publishesInsertUpsertRollbackCleanAndArchivalThroughTheCatalog() {
    try (TestJavaHudiTable table = createWithTableConfig(tableName, tempDir, settings)) {
      String basePath = table.getBasePath();
      String hudiBasePath = table.getMetaClient().getBasePath().toUri().getPath();

      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(20, true);
      assertTrue(catalog.tableExists(identifier), "the first commit must create the table");
      Table icebergTable = catalog.loadTable(identifier);
      assertEquals(hudiBasePath, new StoragePath(icebergTable.location()).toUri().getPath());
      assertFalse(
          icebergTableExists(basePath), "no path-based Iceberg table may exist at the base path");
      assertMatchesHudi(table, catalog.loadTable(identifier), Collections.emptySet());

      table.upsertRecords(inserts.subList(0, 10), true);
      assertMatchesHudi(table, catalog.loadTable(identifier), Collections.emptySet());
      List<String> earlyCommits =
          completedCommitTimes(table.getMetaClient().reloadActiveTimeline());

      String rolledBack = table.startCommit();
      table.upsertRecordsWithCommitAlreadyStarted(inserts.subList(10, 20), rolledBack, true);
      table.rollback(rolledBack);
      Set<String> rolledBackCommits = Collections.singleton(rolledBack);
      assertMatchesHudi(table, catalog.loadTable(identifier), rolledBackCommits);

      // The writer archives after each commit once the timeline passes the archival threshold,
      // so these commits archive the early ones; the explicit call archives anything left.
      for (int i = 0; i < 4; i++) {
        table.upsertRecords(inserts, true);
      }
      table.clean();
      assertMatchesHudi(table, catalog.loadTable(identifier), rolledBackCommits);
      table.getWriteClient().archive();

      List<String> activeCommits =
          completedCommitTimes(table.getMetaClient().reloadActiveTimeline());
      for (String earlyCommit : earlyCommits) {
        assertFalse(activeCommits.contains(earlyCommit), "Hudi did not archive " + earlyCommit);
      }
      Table archived = catalog.loadTable(identifier);
      for (Snapshot snapshot : archived.snapshots()) {
        String recorded =
            IcebergSnapshotInstants.recordedInstant(
                    snapshot, table.getMetaClient().getInstantGenerator())
                .requestedTime();
        assertFalse(
            earlyCommits.contains(recorded) || rolledBack.equals(recorded),
            "archival left the snapshot of " + recorded + " in the catalog table");
      }
      assertMatchesHudi(table, archived, rolledBackCommits);
      assertEquals(hudiBasePath, new StoragePath(archived.location()).toUri().getPath());

      assertEquals(
          completedCommitTimes(table.getMetaClient().reloadActiveTimeline()),
          completedCommitTimes(reconstructedTimeline(basePath)),
          "a reader that loads the table from its path must find the catalog table");
      assertNothingPending(reconstructedTimeline(basePath));
    }
  }

  @Test
  void rollbackOfTheOnlyCommitDescribesTheTableFromTheCatalog() {
    try (TestJavaHudiTable table = createWithTableConfig(tableName, tempDir, settings)) {
      String commit1 = table.startCommit();
      table.insertRecordsWithCommitAlreadyStarted(table.generateRecords(20), commit1, true);
      Types.StructType schemaBeforeRollback = catalog.loadTable(identifier).schema().asStruct();

      table.rollback(commit1);

      Table rolledBack = catalog.loadTable(identifier);
      assertEquals(schemaBeforeRollback, rolledBack.schema().asStruct());
      assertIcebergReferencesExactly(rolledBack, Collections.emptyList());
      assertMatchesHudi(table, rolledBack, Collections.singleton(commit1));
      assertNothingPending(reconstructedTimeline(table.getBasePath()));
    }
  }

  @Test
  void deletesLookUpTheDeletionVectorsOfTheirOwnPartitionOnly() throws IOException {
    String catalogName = "catalog_" + UUID.randomUUID().toString().replace('-', '_');
    IcebergCatalogConfig catalogConfig =
        IcebergCatalogConfig.builder()
            .catalogName(catalogName)
            .catalogImpl(RegisteringInMemoryCatalog.class.getName())
            .catalogOptions(
                Collections.singletonMap(
                    CatalogProperties.METRICS_REPORTER_IMPL, RecordingScans.class.getName()))
            .build();
    IcebergTableManager.of(new Configuration()).tableExists(catalogConfig, identifier, "");
    RegisteringInMemoryCatalog reportingCatalog =
        RegisteringInMemoryCatalog.LOADED.get(catalogName);
    reportingCatalog.createNamespace(namespace);
    Properties properties = MergeOnReadAssertions.deletionVectorTable(9);
    properties.setProperty(IcebergFormatConfig.CATALOG_NAME, catalogName);
    properties.setProperty(
        IcebergFormatConfig.CATALOG_IMPL, RegisteringInMemoryCatalog.class.getName());
    properties.setProperty(
        IcebergFormatConfig.CATALOG_OPTION_PREFIX + CatalogProperties.METRICS_REPORTER_IMPL,
        RecordingScans.class.getName());
    properties.setProperty(IcebergFormatConfig.NAMESPACE, "db.hudi");

    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, "level:SIMPLE", HoodieTableType.MERGE_ON_READ, properties)) {
      List<HoodieRecord<HoodieAvroPayload>> inserts = table.insertRecords(60, true);
      String partition = inserts.get(0).getPartitionPath();
      List<HoodieRecord<HoodieAvroPayload>> deletes =
          inserts.stream()
              .filter(record -> partition.equals(record.getPartitionPath()))
              .limit(5)
              .collect(Collectors.toList());
      List<String> liveFiles = table.getAllLatestBaseFilePaths();
      long filesInPartition =
          liveFiles.stream().filter(path -> path.contains("/" + partition + "/")).count();
      assertTrue(filesInPartition < liveFiles.size(), "files in more than one partition");

      RecordingScans.SCANS.clear();
      table.deleteRecords(deletes, true);

      List<ScanReport> scans =
          RecordingScans.SCANS.stream()
              .filter(scan -> scan.tableName().endsWith(tableName))
              .collect(Collectors.toList());
      assertEquals(1, scans.size(), scans.toString());
      assertEquals(
          filesInPartition, scans.get(0).scanMetrics().resultDataFiles().value(), scans.toString());
      Table icebergTable = reportingCatalog.loadTable(identifier);
      // A delete of the only rows of a base file removes the data file instead.
      assertMatchesHudi(
          table,
          icebergTable,
          Collections.emptySet(),
          new ArrayList<>(withLiveRows(deletedKeysByBaseFileInHudi(table.getBasePath())).keySet()));
      long liveRows = 0;
      try (CloseableIterable<FileScanTask> tasks = icebergTable.newScan().planFiles()) {
        for (FileScanTask task : tasks) {
          liveRows +=
              task.file().recordCount()
                  - task.deletes().stream().mapToLong(DeleteFile::recordCount).sum();
        }
      }
      assertEquals(inserts.size() - deletes.size(), liveRows);
    }
  }

  @Test
  void secondHudiTableOfTheSameNameIsRefusedTheFirstTablesIcebergTable() throws IOException {
    try (TestJavaHudiTable first = createWithTableConfig(tableName, tempDir, settings)) {
      first.insertRecords(20, true);
      List<String> firstFiles = first.getAllLatestBaseFilePaths();
      // The catalog names the first table's Iceberg table, so the second table must neither
      // rebuild its timeline from it nor publish into it; its writer refuses it on loading.
      Path secondDirectory =
          Files.createDirectories(tempDir.resolve("second_" + UUID.randomUUID()));
      assertThrows(
          Exception.class,
          () -> {
            try (TestJavaHudiTable second =
                createWithTableConfig(tableName, secondDirectory, settings)) {
              second.insertRecords(10, true);
            }
          });
      assertIcebergReferencesExactly(catalog.loadTable(identifier), firstFiles);
      assertMatchesHudi(first, catalog.loadTable(identifier), Collections.emptySet());
    }
  }

  /**
   * Iceberg references exactly Hudi's live base files, and the current snapshot's ancestry records
   * every completed instant on Hudi's active timeline and nothing else besides the rolled-back
   * commits, whose snapshots a rollback leaves in history.
   */
  private static void assertMatchesHudi(
      TestJavaHudiTable table, Table icebergTable, Set<String> rolledBackCommits) {
    assertMatchesHudi(table, icebergTable, rolledBackCommits, table.getAllLatestBaseFilePaths());
  }

  private static void assertMatchesHudi(
      TestJavaHudiTable table,
      Table icebergTable,
      Set<String> rolledBackCommits,
      List<String> expectedBaseFiles) {
    assertIcebergReferencesExactly(icebergTable, expectedBaseFiles);
    HoodieTableMetaClient metaClient = table.getMetaClient();
    Set<String> completed =
        metaClient
            .reloadActiveTimeline()
            .filterCompletedInstants()
            .getInstantsAsStream()
            .map(ITIcebergCatalogTable::instantKey)
            .collect(Collectors.toSet());
    Set<String> recorded = new HashSet<>();
    for (Snapshot snapshot : IcebergSnapshotInstants.ancestorsOldestFirst(icebergTable)) {
      HoodieInstant instant =
          IcebergSnapshotInstants.recordedInstant(snapshot, metaClient.getInstantGenerator());
      if (!rolledBackCommits.contains(instant.requestedTime())) {
        recorded.add(instantKey(instant));
      }
    }
    assertEquals(completed, recorded);
  }

  private static String instantKey(HoodieInstant instant) {
    return instant.requestedTime() + "." + instant.getAction();
  }
}
