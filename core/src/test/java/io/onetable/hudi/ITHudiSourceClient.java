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
 
package io.onetable.hudi;

import static io.onetable.hudi.HudiTestUtil.PartitionConfig;
import static java.util.stream.Collectors.groupingBy;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import io.onetable.TestJavaHudiTable;
import io.onetable.TestSparkHudiTable;
import io.onetable.model.CurrentCommitState;
import io.onetable.model.InstantsForIncrementalSync;
import io.onetable.model.OneSnapshot;
import io.onetable.model.TableChange;
import io.onetable.model.storage.OneDataFile;
import io.onetable.spi.DefaultSnapshotVisitor;

/**
 * A suite of functional tests that the extraction from Hudi to Intermediate representation works.
 */
public class ITHudiSourceClient {
  @TempDir public static Path tempDir;
  private static JavaSparkContext jsc;
  private static SparkSession sparkSession;
  private static final Configuration CONFIGURATION = new Configuration();

  @BeforeAll
  public static void setupOnce() {
    SparkConf sparkConf = HudiTestUtil.getSparkConf(tempDir);
    sparkSession =
        SparkSession.builder().config(HoodieReadClient.addHoodieSupport(sparkConf)).getOrCreate();
    sparkSession
        .sparkContext()
        .hadoopConfiguration()
        .set("parquet.avro.write-old-list-structure", "false");
    jsc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
  }

  @ParameterizedTest
  @MethodSource("testsForAllTableTypesAndPartitions")
  public void insertAndUpsertData(HoodieTableType tableType, PartitionConfig partitionConfig) {
    String tableName = "test_table_" + UUID.randomUUID();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {
      List<List<HoodieBaseFile>> allBaseFiles = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();

      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(100, "INFO");
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      table.insertRecords(100, "WARN", true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      table.upsertRecords(insertsForCommit1.subList(0, 20), true);
      allBaseFiles.add(table.getAllLatestBaseFiles());
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        table.compact();
        allBaseFiles.add(table.getAllLatestBaseFiles());
      }

      HudiClient hudiClient =
          getHudiSourceClient(
              CONFIGURATION, table.getBasePath(), partitionConfig.getOneTableConfig());
      // Get the current snapshot
      OneSnapshot oneSnapshot = hudiClient.getCurrentSnapshot();
      validateOneSnapshot(oneSnapshot, allBaseFiles.get(allBaseFiles.size() - 1));
      // Get second change in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant1))
              .build();
      CurrentCommitState<HoodieInstant> instantCurrentCommitState =
          hudiClient.getCurrentCommitState(instantsForIncrementalSync);
      for (HoodieInstant instant : instantCurrentCommitState.getCommitsToProcess()) {
        TableChange tableChange = hudiClient.getTableChangeForCommit(instant);
        allTableChanges.add(tableChange);
      }
      validateTableChanges(allBaseFiles, allTableChanges);
    }
  }

  @ParameterizedTest
  @MethodSource("testsForAllTableTypes")
  public void testsForDropPartition(HoodieTableType tableType) {
    String tableName = "test_table_" + UUID.randomUUID();
    try (TestSparkHudiTable table =
        TestSparkHudiTable.forStandardSchema(tableName, tempDir, jsc, "level:SIMPLE", tableType)) {
      List<List<HoodieBaseFile>> allBaseFiles = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();

      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(100);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      table.insertRecords(100, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      Map<String, List<HoodieRecord>> recordsByPartition =
          insertsForCommit1.stream().collect(groupingBy(HoodieRecord::getPartitionPath));
      String partitionToDelete = recordsByPartition.keySet().stream().sorted().findFirst().get();

      table.deletePartition(partitionToDelete, tableType);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      // Insert few records for deleted partition again to make it interesting.
      table.insertRecords(20, partitionToDelete, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      HudiClient hudiClient =
          getHudiSourceClient(CONFIGURATION, table.getBasePath(), "level:VALUE");
      // Get the current snapshot
      OneSnapshot oneSnapshot = hudiClient.getCurrentSnapshot();
      validateOneSnapshot(oneSnapshot, allBaseFiles.get(allBaseFiles.size() - 1));
      // Get changes in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant1))
              .build();
      CurrentCommitState<HoodieInstant> instantCurrentCommitState =
          hudiClient.getCurrentCommitState(instantsForIncrementalSync);
      for (HoodieInstant instant : instantCurrentCommitState.getCommitsToProcess()) {
        TableChange tableChange = hudiClient.getTableChangeForCommit(instant);
        allTableChanges.add(tableChange);
      }
      validateTableChanges(allBaseFiles, allTableChanges);
    }
  }

  @ParameterizedTest
  @MethodSource("testsForAllTableTypes")
  public void testsForDeletePartitionByRecords(HoodieTableType tableType) {
    String tableName = "test_table_" + UUID.randomUUID();
    try (TestSparkHudiTable table =
        TestSparkHudiTable.forStandardSchema(tableName, tempDir, jsc, "level:SIMPLE", tableType)) {
      List<List<HoodieBaseFile>> allBaseFiles = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();

      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(100);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      table.insertRecords(100, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      Map<String, List<HoodieRecord<HoodieAvroPayload>>> recordsByPartition =
          insertsForCommit1.stream().collect(groupingBy(HoodieRecord::getPartitionPath));
      String selectedPartition = recordsByPartition.keySet().stream().sorted().findAny().get();
      table.deleteRecords(recordsByPartition.get(selectedPartition), true);
      allBaseFiles.add(table.getAllLatestBaseFiles());
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        table.compact();
        allBaseFiles.add(table.getAllLatestBaseFiles());
      }

      // Insert few records for deleted partition again to make it interesting.
      table.insertRecords(20, selectedPartition, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      HudiClient hudiClient =
          getHudiSourceClient(CONFIGURATION, table.getBasePath(), "level:VALUE");
      // Get the current snapshot
      OneSnapshot oneSnapshot = hudiClient.getCurrentSnapshot();
      validateOneSnapshot(oneSnapshot, allBaseFiles.get(allBaseFiles.size() - 1));
      // Get changes in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant1))
              .build();
      CurrentCommitState<HoodieInstant> instantCurrentCommitState =
          hudiClient.getCurrentCommitState(instantsForIncrementalSync);
      for (HoodieInstant instant : instantCurrentCommitState.getCommitsToProcess()) {
        TableChange tableChange = hudiClient.getTableChangeForCommit(instant);
        allTableChanges.add(tableChange);
      }
      validateTableChanges(allBaseFiles, allTableChanges);
    }
  }

  @ParameterizedTest
  @MethodSource("testsForAllTableTypesAndPartitions")
  public void testsForClustering(HoodieTableType tableType, PartitionConfig partitionConfig) {
    String tableName = "test_table_" + UUID.randomUUID();
    try (TestSparkHudiTable table =
        TestSparkHudiTable.forStandardSchema(
            tableName, tempDir, jsc, partitionConfig.getHudiConfig(), tableType)) {
      List<List<HoodieBaseFile>> allBaseFiles = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();

      /*
       * Insert 100 records.
       * Insert 100 records.
       * Upsert 20 records from first commit.
       * Compact for MOR table.
       * Insert 100 records.
       * Run Clustering.
       * Insert 100 records.
       */

      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(100);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      table.insertRecords(100, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      table.upsertRecords(insertsForCommit1.subList(0, 20), true);
      allBaseFiles.add(table.getAllLatestBaseFiles());
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        table.compact();
        allBaseFiles.add(table.getAllLatestBaseFiles());
      }

      table.insertRecords(100, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      table.cluster();
      allBaseFiles.add(table.getAllLatestBaseFiles());

      table.insertRecords(100, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      HudiClient hudiClient =
          getHudiSourceClient(
              CONFIGURATION, table.getBasePath(), partitionConfig.getOneTableConfig());
      // Get the current snapshot
      OneSnapshot oneSnapshot = hudiClient.getCurrentSnapshot();
      validateOneSnapshot(oneSnapshot, allBaseFiles.get(allBaseFiles.size() - 1));
      // Get changes in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant1))
              .build();
      CurrentCommitState<HoodieInstant> instantCurrentCommitState =
          hudiClient.getCurrentCommitState(instantsForIncrementalSync);
      for (HoodieInstant instant : instantCurrentCommitState.getCommitsToProcess()) {
        TableChange tableChange = hudiClient.getTableChangeForCommit(instant);
        allTableChanges.add(tableChange);
      }
      validateTableChanges(allBaseFiles, allTableChanges);
    }
  }

  @ParameterizedTest
  @MethodSource("testsForAllTableTypesAndPartitions")
  public void testsForSavepointRestore(HoodieTableType tableType, PartitionConfig partitionConfig) {
    String tableName = "test_table_" + UUID.randomUUID();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {
      List<List<HoodieBaseFile>> allBaseFiles = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();

      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(50);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);

      table.insertRecords(50, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      //  Restore to commitInstant2.
      table.savepointRestoreForPreviousInstant();
      allBaseFiles.add(table.getAllLatestBaseFiles());

      table.insertRecords(50, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      HudiClient hudiClient =
          getHudiSourceClient(
              CONFIGURATION, table.getBasePath(), partitionConfig.getOneTableConfig());
      // Get the current snapshot
      OneSnapshot oneSnapshot = hudiClient.getCurrentSnapshot();
      validateOneSnapshot(oneSnapshot, allBaseFiles.get(allBaseFiles.size() - 1));
      // Get changes in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant1))
              .build();
      CurrentCommitState<HoodieInstant> instantCurrentCommitState =
          hudiClient.getCurrentCommitState(instantsForIncrementalSync);
      for (HoodieInstant instant : instantCurrentCommitState.getCommitsToProcess()) {
        TableChange tableChange = hudiClient.getTableChangeForCommit(instant);
        allTableChanges.add(tableChange);
      }
      validateTableChanges(allBaseFiles, allTableChanges);
    }
  }

  @ParameterizedTest
  @MethodSource("testsForAllTableTypesAndPartitions")
  public void testsForRollbacks(HoodieTableType tableType, PartitionConfig partitionConfig) {
    String tableName = "test_table_" + UUID.randomUUID();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {

      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(50);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      List<HoodieBaseFile> baseFilesAfterCommit1 = table.getAllLatestBaseFiles();

      String commitInstant2 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit2 = table.generateRecords(50);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit2, commitInstant2, true);
      List<HoodieBaseFile> baseFilesAfterCommit2 = table.getAllLatestBaseFiles();

      String commitInstant3 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit3 = table.generateRecords(50);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit3, commitInstant3, true);
      List<HoodieBaseFile> baseFilesAfterCommit3 = table.getAllLatestBaseFiles();

      table.rollback(commitInstant3);
      List<HoodieBaseFile> baseFilesAfterRollback = table.getAllLatestBaseFiles();

      String commitInstant4 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit4 = table.generateRecords(50);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit4, commitInstant4, true);
      List<HoodieBaseFile> baseFilesAfterCommit4 = table.getAllLatestBaseFiles();

      HudiClient hudiClient =
          getHudiSourceClient(
              CONFIGURATION, table.getBasePath(), partitionConfig.getOneTableConfig());
      // Get the current snapshot
      OneSnapshot oneSnapshot = hudiClient.getCurrentSnapshot();
      validateOneSnapshot(oneSnapshot, baseFilesAfterCommit4);
      // Get changes in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant1))
              .build();
      CurrentCommitState<HoodieInstant> instantCurrentCommitState =
          hudiClient.getCurrentCommitState(instantsForIncrementalSync);
      for (HoodieInstant instant : instantCurrentCommitState.getCommitsToProcess()) {
        TableChange tableChange = hudiClient.getTableChangeForCommit(instant);
        if (commitInstant2.equals(instant.getTimestamp())) {
          validateTableChange(baseFilesAfterCommit1, baseFilesAfterCommit2, tableChange);
        } else if ("rollback".equals(instant.getAction())) {
          validateTableChange(baseFilesAfterCommit3, baseFilesAfterRollback, tableChange);
        } else if (commitInstant4.equals(instant.getTimestamp())) {
          validateTableChange(baseFilesAfterRollback, baseFilesAfterCommit4, tableChange);
        } else {
          fail("Please add proper asserts here");
        }
      }
    }
  }

  @Disabled("TODO(vamshigv): Investigate and enable back")
  @ParameterizedTest
  @MethodSource("testsForAllTableTypesAndPartitions")
  public void testForClean(HoodieTableType tableType, PartitionConfig partitionConfig) {
    String tableName = "test_table_" + UUID.randomUUID();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {
      List<List<HoodieBaseFile>> allBaseFiles = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();

      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(50);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      String commitInstant2 = table.startCommit();
      table.upsertRecordsWithCommitAlreadyStarted(
          insertsForCommit1.subList(0, 20), commitInstant2, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        table.compact();
        allBaseFiles.add(table.getAllLatestBaseFiles());
      }
      String commitInstant3 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit3 = table.generateRecords(50);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit3, commitInstant3, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      table.clean();
      allBaseFiles.add(table.getAllLatestBaseFiles());

      String commitInstant4 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit4 = table.generateRecords(50);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit4, commitInstant4, true);
      allBaseFiles.add(table.getAllLatestBaseFiles());

      HudiClient hudiClient =
          getHudiSourceClient(
              CONFIGURATION, table.getBasePath(), partitionConfig.getOneTableConfig());
      // Get the current snapshot
      OneSnapshot oneSnapshot = hudiClient.getCurrentSnapshot();
      validateOneSnapshot(oneSnapshot, allBaseFiles.get(allBaseFiles.size() - 1));
      // Get changes in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant1))
              .build();
      CurrentCommitState<HoodieInstant> instantCurrentCommitState =
          hudiClient.getCurrentCommitState(instantsForIncrementalSync);
      for (HoodieInstant instant : instantCurrentCommitState.getCommitsToProcess()) {
        TableChange tableChange = hudiClient.getTableChangeForCommit(instant);
        allTableChanges.add(tableChange);
      }
      validateTableChanges(allBaseFiles, allTableChanges);
    }
  }

  private void validateOneSnapshot(OneSnapshot oneSnapshot, List<HoodieBaseFile> allBaseFiles) {
    assertNotNull(oneSnapshot);
    assertNotNull(oneSnapshot.getTable());
    List<String> allActivePaths =
        allBaseFiles.stream()
            .map(hoodieBaseFile -> hoodieBaseFile.getPath())
            .collect(Collectors.toList());
    List<String> onetablePaths =
        DefaultSnapshotVisitor.extractDataFilePaths(oneSnapshot.getDataFiles()).keySet().stream()
            .collect(Collectors.toList());
    Collections.sort(allActivePaths);
    Collections.sort(onetablePaths);
    assertEquals(allActivePaths, onetablePaths);
  }

  private static Stream<Arguments> testsForAllPartitions() {
    PartitionConfig unPartitionedConfig = PartitionConfig.of(null, null);
    PartitionConfig partitionedConfig = PartitionConfig.of("level:SIMPLE", "level:VALUE");
    return Stream.of(Arguments.of(unPartitionedConfig), Arguments.of(partitionedConfig));
  }

  private static Stream<Arguments> testsForAllTableTypes() {
    return Stream.of(
        Arguments.of(HoodieTableType.COPY_ON_WRITE), Arguments.of(HoodieTableType.MERGE_ON_READ));
  }

  private static Stream<Arguments> testsForAllTableTypesAndPartitions() {
    PartitionConfig unPartitionedConfig = PartitionConfig.of(null, null);
    PartitionConfig partitionedConfig = PartitionConfig.of("level:SIMPLE", "level:VALUE");
    List<PartitionConfig> partitionConfigs = Arrays.asList(unPartitionedConfig, partitionedConfig);
    List<HoodieTableType> tableTypes =
        Arrays.asList(HoodieTableType.COPY_ON_WRITE, HoodieTableType.MERGE_ON_READ);

    return tableTypes.stream()
        .flatMap(
            tableType -> partitionConfigs.stream().map(config -> Arguments.of(tableType, config)));
  }

  private void validateTableChanges(
      List<List<HoodieBaseFile>> allBaseFiles, List<TableChange> allTableChanges) {
    if (allTableChanges.isEmpty() && allBaseFiles.size() <= 1) {
      return;
    }
    assertTrue(
        allTableChanges.size() == allBaseFiles.size() - 1,
        "Number of table changes should be equal to number of commits - 1");
    IntStream.range(0, allBaseFiles.size() - 1)
        .forEach(
            i ->
                validateTableChange(
                    allBaseFiles.get(i), allBaseFiles.get(i + 1), allTableChanges.get(i)));
  }

  private void validateTableChange(
      List<HoodieBaseFile> baseFilesBefore,
      List<HoodieBaseFile> baseFilesAfter,
      TableChange tableChange) {
    assertNotNull(tableChange);
    assertNotNull(tableChange.getCurrentTableState());
    Set<String> filesForCommitBefore =
        baseFilesBefore.stream()
            .map(hoodieBaseFile -> hoodieBaseFile.getPath())
            .collect(Collectors.toSet());
    Set<String> filesForCommitAfter =
        baseFilesAfter.stream()
            .map(hoodieBaseFile -> hoodieBaseFile.getPath())
            .collect(Collectors.toSet());
    // Get files added by diffing filesForCommitAfter and filesForCommitBefore.
    Set<String> filesAdded =
        filesForCommitAfter.stream()
            .filter(file -> !filesForCommitBefore.contains(file))
            .collect(Collectors.toSet());
    // Get files removed by diffing filesForCommitBefore and filesForCommitAfter.
    Set<String> filesRemoved =
        filesForCommitBefore.stream()
            .filter(file -> !filesForCommitAfter.contains(file))
            .collect(Collectors.toSet());
    assertEquals(filesAdded, extractPathsFromDataFile(tableChange.getFilesDiff().getFilesAdded()));
    assertEquals(
        filesRemoved, extractPathsFromDataFile(tableChange.getFilesDiff().getFilesRemoved()));
  }

  private Set<String> extractPathsFromDataFile(Set<OneDataFile> dataFiles) {
    return dataFiles.stream().map(OneDataFile::getPhysicalPath).collect(Collectors.toSet());
  }

  private HudiClient getHudiSourceClient(
      Configuration conf, String basePath, String onetablePartitionConfig) {
    HoodieTableMetaClient hoodieTableMetaClient =
        HoodieTableMetaClient.builder()
            .setConf(conf)
            .setBasePath(basePath)
            .setLoadActiveTimelineOnLoad(true)
            .build();
    HudiSourcePartitionSpecExtractor partitionSpecExtractor =
        new ConfigurationBasedPartitionSpecExtractor(
            HudiSourceConfig.builder().partitionFieldSpecConfig(onetablePartitionConfig).build());
    return new HudiClient(hoodieTableMetaClient, partitionSpecExtractor);
  }
}
