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
 
package org.apache.xtable.hudi;

import static java.util.stream.Collectors.groupingBy;
import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.Value;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;

import org.apache.xtable.GenericTable;
import org.apache.xtable.TestJavaHudiTable;
import org.apache.xtable.TestSparkHudiTable;
import org.apache.xtable.ValidationTestHelper;
import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.TableChange;

/**
 * A suite of functional tests that the extraction from Hudi to Intermediate representation works.
 */
public class ITHudiConversionSourceSource {
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

  @AfterAll
  public static void teardown() {
    if (jsc != null) {
      jsc.close();
    }
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

  @ParameterizedTest
  @MethodSource("testsForAllTableTypesAndPartitions")
  public void insertAndUpsertData(
      HoodieTableType tableType, HudiTestUtil.PartitionConfig partitionConfig) {
    String tableName = GenericTable.getTableName();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {
      List<List<String>> allBaseFilePaths = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();

      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1;
      if (partitionConfig.getHudiConfig() != null) {
        insertsForCommit1 = table.generateRecords(100, "INFO");
      } else {
        insertsForCommit1 = table.generateRecords(100);
      }
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      if (partitionConfig.getHudiConfig() != null) {
        table.insertRecords(100, "WARN", true);
      } else {
        table.insertRecords(100, true);
      }
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      table.upsertRecords(insertsForCommit1.subList(0, 20), true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        table.compact();
        allBaseFilePaths.add(table.getAllLatestBaseFilePaths());
      }

      HudiConversionSource hudiClient =
          getHudiSourceClient(
              CONFIGURATION, table.getBasePath(), partitionConfig.getXTableConfig());
      // Get the current snapshot
      InternalSnapshot internalSnapshot = hudiClient.getCurrentSnapshot();
      ValidationTestHelper.validateSnapshot(
          internalSnapshot, allBaseFilePaths.get(allBaseFilePaths.size() - 1));
      // Get second change in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant1))
              .build();
      CommitsBacklog<HoodieInstant> instantCommitsBacklog =
          hudiClient.getCommitsBacklog(instantsForIncrementalSync);
      for (HoodieInstant instant : instantCommitsBacklog.getCommitsToProcess()) {
        TableChange tableChange = hudiClient.getTableChangeForCommit(instant);
        allTableChanges.add(tableChange);
      }
      ValidationTestHelper.validateTableChanges(allBaseFilePaths, allTableChanges);
    }
  }

  @Test
  public void testOnlyUpsertsAfterInserts() {
    HoodieTableType tableType = HoodieTableType.MERGE_ON_READ;
    HudiTestUtil.PartitionConfig partitionConfig = HudiTestUtil.PartitionConfig.of(null, null);
    String tableName = "test_table_" + UUID.randomUUID();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {
      List<List<String>> allBaseFilePaths = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();

      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1;
      if (partitionConfig.getHudiConfig() != null) {
        insertsForCommit1 = table.generateRecords(100, "INFO");
      } else {
        insertsForCommit1 = table.generateRecords(100);
      }
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      table.upsertRecords(insertsForCommit1.subList(0, 20), true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());
      table.deleteRecords(insertsForCommit1.subList(15, 30), true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      HudiConversionSource hudiClient =
          getHudiSourceClient(
              CONFIGURATION, table.getBasePath(), partitionConfig.getXTableConfig());
      // Get the current snapshot
      InternalSnapshot internalSnapshot = hudiClient.getCurrentSnapshot();
      ValidationTestHelper.validateSnapshot(
          internalSnapshot, allBaseFilePaths.get(allBaseFilePaths.size() - 1));
      // Get second change in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant1))
              .build();
      CommitsBacklog<HoodieInstant> instantCommitsBacklog =
          hudiClient.getCommitsBacklog(instantsForIncrementalSync);
      for (HoodieInstant instant : instantCommitsBacklog.getCommitsToProcess()) {
        TableChange tableChange = hudiClient.getTableChangeForCommit(instant);
        allTableChanges.add(tableChange);
      }
      ValidationTestHelper.validateTableChanges(allBaseFilePaths, allTableChanges);
    }
  }

  @Test
  public void testForIncrementalSyncSafetyCheck() {
    HoodieTableType tableType = HoodieTableType.COPY_ON_WRITE;
    HudiTestUtil.PartitionConfig partitionConfig = HudiTestUtil.PartitionConfig.of(null, null);
    String tableName = GenericTable.getTableName();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {
      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(100);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);

      table.upsertRecords(insertsForCommit1.subList(30, 40), true);

      String commitInstant2 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit2 = table.generateRecords(100);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit2, commitInstant2, true);

      table.clean(); // cleans up file groups from commitInstant1

      HudiConversionSource hudiClient =
          getHudiSourceClient(
              CONFIGURATION, table.getBasePath(), partitionConfig.getXTableConfig());
      // commitInstant1 is not safe for incremental sync as cleaner has run after and touched
      // related files.
      assertFalse(
          hudiClient.isIncrementalSyncSafeFrom(
              HudiInstantUtils.parseFromInstantTime(commitInstant1)));
      // commitInstant2 is safe for incremental sync as cleaner has no affect on data written in
      // this commit.
      assertTrue(
          hudiClient.isIncrementalSyncSafeFrom(
              HudiInstantUtils.parseFromInstantTime(commitInstant2)));
      // commit older by an hour is not present in table, hence not safe for incremental sync.
      Instant instantAsOfHourAgo = Instant.now().minus(1, ChronoUnit.HOURS);
      assertFalse(hudiClient.isIncrementalSyncSafeFrom(instantAsOfHourAgo));
    }
  }

  @ParameterizedTest
  @MethodSource("testsForAllTableTypes")
  public void testsForDropPartition(HoodieTableType tableType) {
    String tableName = "test_table_" + UUID.randomUUID();
    try (TestSparkHudiTable table =
        TestSparkHudiTable.forStandardSchema(tableName, tempDir, jsc, "level:SIMPLE", tableType)) {
      List<List<String>> allBaseFilePaths = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();

      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(100);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      table.insertRecords(100, true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      Map<String, List<HoodieRecord>> recordsByPartition =
          insertsForCommit1.stream().collect(groupingBy(HoodieRecord::getPartitionPath));
      String partitionToDelete = recordsByPartition.keySet().stream().sorted().findFirst().get();

      table.deletePartition(partitionToDelete, tableType);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      // Insert few records for deleted partition again to make it interesting.
      table.insertRecords(20, partitionToDelete, true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      HudiConversionSource hudiClient =
          getHudiSourceClient(CONFIGURATION, table.getBasePath(), "level:VALUE");
      // Get the current snapshot
      InternalSnapshot internalSnapshot = hudiClient.getCurrentSnapshot();
      ValidationTestHelper.validateSnapshot(
          internalSnapshot, allBaseFilePaths.get(allBaseFilePaths.size() - 1));
      // Get changes in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant1))
              .build();
      CommitsBacklog<HoodieInstant> instantCommitsBacklog =
          hudiClient.getCommitsBacklog(instantsForIncrementalSync);
      for (HoodieInstant instant : instantCommitsBacklog.getCommitsToProcess()) {
        TableChange tableChange = hudiClient.getTableChangeForCommit(instant);
        allTableChanges.add(tableChange);
      }
      ValidationTestHelper.validateTableChanges(allBaseFilePaths, allTableChanges);
    }
  }

  @ParameterizedTest
  @MethodSource("testsForAllTableTypes")
  public void testsForDeleteAllRecordsInPartition(HoodieTableType tableType) {
    String tableName = "test_table_" + UUID.randomUUID();
    try (TestSparkHudiTable table =
        TestSparkHudiTable.forStandardSchema(tableName, tempDir, jsc, "level:SIMPLE", tableType)) {
      List<List<String>> allBaseFilePaths = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();

      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(100);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      table.insertRecords(100, true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      Map<String, List<HoodieRecord<HoodieAvroPayload>>> recordsByPartition =
          insertsForCommit1.stream().collect(groupingBy(HoodieRecord::getPartitionPath));
      String selectedPartition = recordsByPartition.keySet().stream().sorted().findAny().get();
      table.deleteRecords(recordsByPartition.get(selectedPartition), true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        table.compact();
        allBaseFilePaths.add(table.getAllLatestBaseFilePaths());
      }

      // Insert few records for deleted partition again to make it interesting.
      table.insertRecords(20, selectedPartition, true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      HudiConversionSource hudiClient =
          getHudiSourceClient(CONFIGURATION, table.getBasePath(), "level:VALUE");
      // Get the current snapshot
      InternalSnapshot internalSnapshot = hudiClient.getCurrentSnapshot();
      ValidationTestHelper.validateSnapshot(
          internalSnapshot, allBaseFilePaths.get(allBaseFilePaths.size() - 1));
      // Get changes in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant1))
              .build();
      CommitsBacklog<HoodieInstant> instantCommitsBacklog =
          hudiClient.getCommitsBacklog(instantsForIncrementalSync);
      for (HoodieInstant instant : instantCommitsBacklog.getCommitsToProcess()) {
        TableChange tableChange = hudiClient.getTableChangeForCommit(instant);
        allTableChanges.add(tableChange);
      }
      ValidationTestHelper.validateTableChanges(allBaseFilePaths, allTableChanges);
    }
  }

  @ParameterizedTest
  @MethodSource("testsForAllTableTypesAndPartitions")
  public void testsForClustering(
      HoodieTableType tableType, HudiTestUtil.PartitionConfig partitionConfig) {
    String tableName = "test_table_" + UUID.randomUUID();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {
      List<List<String>> allBaseFilePaths = new ArrayList<>();
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
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      table.insertRecords(100, true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      table.upsertRecords(insertsForCommit1.subList(0, 20), true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());
      if (tableType == HoodieTableType.MERGE_ON_READ) {
        table.compact();
        allBaseFilePaths.add(table.getAllLatestBaseFilePaths());
      }

      table.insertRecords(100, true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      table.cluster();
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      table.insertRecords(100, true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      HudiConversionSource hudiClient =
          getHudiSourceClient(
              CONFIGURATION, table.getBasePath(), partitionConfig.getXTableConfig());
      // Get the current snapshot
      InternalSnapshot internalSnapshot = hudiClient.getCurrentSnapshot();
      ValidationTestHelper.validateSnapshot(
          internalSnapshot, allBaseFilePaths.get(allBaseFilePaths.size() - 1));
      // Get changes in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant1))
              .build();
      CommitsBacklog<HoodieInstant> instantCommitsBacklog =
          hudiClient.getCommitsBacklog(instantsForIncrementalSync);
      for (HoodieInstant instant : instantCommitsBacklog.getCommitsToProcess()) {
        TableChange tableChange = hudiClient.getTableChangeForCommit(instant);
        allTableChanges.add(tableChange);
      }
      ValidationTestHelper.validateTableChanges(allBaseFilePaths, allTableChanges);
    }
  }

  @ParameterizedTest
  @MethodSource("testsForAllTableTypesAndPartitions")
  public void testsForSavepointRestore(
      HoodieTableType tableType, HudiTestUtil.PartitionConfig partitionConfig) {
    String tableName = "test_table_" + UUID.randomUUID();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {
      List<List<String>> allBaseFilePaths = new ArrayList<>();
      List<TableChange> allTableChanges = new ArrayList<>();

      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(50);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      // This is the commit we're going to savepoint and restore to
      table.insertRecords(50, true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      List<HoodieRecord<HoodieAvroPayload>> recordList = table.insertRecords(50, true);
      Set<String> baseFilePaths = new HashSet<>(table.getAllLatestBaseFilePaths());
      table.upsertRecords(recordList.subList(0, 20), true);
      baseFilePaths.addAll(table.getAllLatestBaseFilePaths());
      // Note that restore removes all the new base files added by these two commits
      allBaseFilePaths.add(new ArrayList<>(baseFilePaths));

      table.savepointRestoreFromNthMostRecentInstant(2);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      table.insertRecords(50, true);
      allBaseFilePaths.add(table.getAllLatestBaseFilePaths());

      HudiConversionSource hudiClient =
          getHudiSourceClient(
              CONFIGURATION, table.getBasePath(), partitionConfig.getXTableConfig());
      // Get the current snapshot
      InternalSnapshot internalSnapshot = hudiClient.getCurrentSnapshot();
      ValidationTestHelper.validateSnapshot(
          internalSnapshot, allBaseFilePaths.get(allBaseFilePaths.size() - 1));
      // Get changes in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant1))
              .build();
      CommitsBacklog<HoodieInstant> instantCommitsBacklog =
          hudiClient.getCommitsBacklog(instantsForIncrementalSync);
      for (HoodieInstant instant : instantCommitsBacklog.getCommitsToProcess()) {
        TableChange tableChange = hudiClient.getTableChangeForCommit(instant);
        allTableChanges.add(tableChange);
      }

      IntStream.range(0, allTableChanges.size() - 1)
          .forEach(
              i -> {
                if (i == 1) {
                  // Savepoint: no change
                  ValidationTestHelper.validateTableChange(
                      allBaseFilePaths.get(i), allBaseFilePaths.get(i), allTableChanges.get(i));
                } else {
                  ValidationTestHelper.validateTableChange(
                      allBaseFilePaths.get(i), allBaseFilePaths.get(i + 1), allTableChanges.get(i));
                }
              });
    }
  }

  @ParameterizedTest
  @MethodSource("testsForAllTableTypesAndPartitions")
  public void testsForRollbacks(
      HoodieTableType tableType, HudiTestUtil.PartitionConfig partitionConfig) {
    String tableName = "test_table_" + UUID.randomUUID();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, partitionConfig.getHudiConfig(), tableType)) {

      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(50);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      List<String> baseFilesAfterCommit1 = table.getAllLatestBaseFilePaths();

      String commitInstant2 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit2 = table.generateRecords(50);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit2, commitInstant2, true);
      List<String> baseFilesAfterCommit2 = table.getAllLatestBaseFilePaths();

      String commitInstant3 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit3 = table.generateRecords(50);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit3, commitInstant3, true);
      List<String> baseFilesAfterCommit3 = table.getAllLatestBaseFilePaths();

      table.rollback(commitInstant3);
      List<String> baseFilesAfterRollback = table.getAllLatestBaseFilePaths();

      String commitInstant4 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit4 = table.generateRecords(50);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit4, commitInstant4, true);
      List<String> baseFilesAfterCommit4 = table.getAllLatestBaseFilePaths();

      HudiConversionSource hudiClient =
          getHudiSourceClient(
              CONFIGURATION, table.getBasePath(), partitionConfig.getXTableConfig());
      // Get the current snapshot
      InternalSnapshot internalSnapshot = hudiClient.getCurrentSnapshot();
      ValidationTestHelper.validateSnapshot(internalSnapshot, baseFilesAfterCommit4);
      // Get changes in Incremental format.
      InstantsForIncrementalSync instantsForIncrementalSync =
          InstantsForIncrementalSync.builder()
              .lastSyncInstant(HudiInstantUtils.parseFromInstantTime(commitInstant1))
              .build();
      CommitsBacklog<HoodieInstant> instantCommitsBacklog =
          hudiClient.getCommitsBacklog(instantsForIncrementalSync);
      for (HoodieInstant instant : instantCommitsBacklog.getCommitsToProcess()) {
        TableChange tableChange = hudiClient.getTableChangeForCommit(instant);
        if (commitInstant2.equals(instant.getTimestamp())) {
          ValidationTestHelper.validateTableChange(
              baseFilesAfterCommit1, baseFilesAfterCommit2, tableChange);
        } else if ("rollback".equals(instant.getAction())) {
          ValidationTestHelper.validateTableChange(
              baseFilesAfterCommit3, baseFilesAfterRollback, tableChange);
        } else if (commitInstant4.equals(instant.getTimestamp())) {
          ValidationTestHelper.validateTableChange(
              baseFilesAfterRollback, baseFilesAfterCommit4, tableChange);
        } else {
          fail("Please add proper asserts here");
        }
      }
    }
  }

  private static Stream<Arguments> testsForAllTableTypes() {
    return Stream.of(
        Arguments.of(HoodieTableType.COPY_ON_WRITE), Arguments.of(HoodieTableType.MERGE_ON_READ));
  }

  private static Stream<Arguments> testsForAllTableTypesAndPartitions() {
    HudiTestUtil.PartitionConfig unPartitionedConfig = HudiTestUtil.PartitionConfig.of(null, null);
    HudiTestUtil.PartitionConfig partitionedConfig =
        HudiTestUtil.PartitionConfig.of("level:SIMPLE", "level:VALUE");
    List<HudiTestUtil.PartitionConfig> partitionConfigs =
        Arrays.asList(unPartitionedConfig, partitionedConfig);
    List<HoodieTableType> tableTypes =
        Arrays.asList(HoodieTableType.COPY_ON_WRITE, HoodieTableType.MERGE_ON_READ);

    return tableTypes.stream()
        .flatMap(
            tableType -> partitionConfigs.stream().map(config -> Arguments.of(tableType, config)));
  }

  private HudiConversionSource getHudiSourceClient(
      Configuration conf, String basePath, String xTablePartitionConfig) {
    HoodieTableMetaClient hoodieTableMetaClient =
        HoodieTableMetaClient.builder()
            .setConf(conf)
            .setBasePath(basePath)
            .setLoadActiveTimelineOnLoad(true)
            .build();
    HudiSourcePartitionSpecExtractor partitionSpecExtractor =
        new ConfigurationBasedPartitionSpecExtractor(
            HudiSourceConfigImpl.builder().partitionFieldSpecConfig(xTablePartitionConfig).build());
    return new HudiConversionSource(hoodieTableMetaClient, partitionSpecExtractor);
  }

  private boolean checkIfNewFileGroupIsAdded(String activePath, TableChange tableChange) {
    String activePathFileGroupId = getFileGroupInfo(activePath).getFileId();
    String activePathCommitTime = getFileGroupInfo(activePath).getCommitTime();
    Map<String, String> fileIdToCommitTimeMap =
        tableChange.getFilesDiff().getFilesAdded().stream()
            .collect(
                Collectors.groupingBy(
                    oneDf -> getFileGroupInfo(oneDf.getPhysicalPath()).getFileId(),
                    Collectors.collectingAndThen(
                        Collectors.mapping(
                            oneDf -> getFileGroupInfo(oneDf.getPhysicalPath()).getCommitTime(),
                            Collectors.toList()),
                        list -> {
                          if (list.size() > 1) {
                            throw new IllegalStateException(
                                "Some fileIds have more than one commit time.");
                          }
                          return list.get(0);
                        })));
    if (!fileIdToCommitTimeMap.containsKey(activePathFileGroupId)) {
      return false;
    }
    Instant newCommitInstant =
        HudiInstantUtils.parseFromInstantTime(fileIdToCommitTimeMap.get(activePathFileGroupId));
    Instant oldCommitInstant = HudiInstantUtils.parseFromInstantTime(activePathCommitTime);
    return newCommitInstant.isAfter(oldCommitInstant);
  }

  private boolean checkIfFileIsRemoved(String activePath, TableChange tableChange) {
    String activePathFileGroupId = getFileGroupInfo(activePath).getFileId();
    String activePathCommitTime = getFileGroupInfo(activePath).getCommitTime();
    Map<String, String> fileIdToCommitTimeMap =
        tableChange.getFilesDiff().getFilesRemoved().stream()
            .collect(
                Collectors.groupingBy(
                    oneDf -> getFileGroupInfo(oneDf.getPhysicalPath()).getFileId(),
                    Collectors.collectingAndThen(
                        Collectors.mapping(
                            oneDf -> getFileGroupInfo(oneDf.getPhysicalPath()).getCommitTime(),
                            Collectors.toList()),
                        list -> {
                          if (list.size() > 1) {
                            throw new IllegalStateException(
                                "Some fileIds have more than one commit time.");
                          }
                          return list.get(0);
                        })));
    if (!fileIdToCommitTimeMap.containsKey(activePathFileGroupId)) {
      return false;
    }
    if (!fileIdToCommitTimeMap.get(activePathFileGroupId).equals(activePathCommitTime)) {
      return false;
    }
    return true;
  }

  private FileGroupInfo getFileGroupInfo(String path) {
    String[] pathParts = path.split("/");
    String fileName = pathParts[pathParts.length - 1];
    return FileGroupInfo.builder()
        .fileId(FSUtils.getFileId(fileName))
        .commitTime(FSUtils.getCommitTime(fileName))
        .build();
  }

  @Builder
  @Value
  private static class FileGroupInfo {
    String fileId;
    String commitTime;
  }
}
