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
 
package io.onetable.delta;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.onetable.TestSparkDeltaTable;
import io.onetable.model.CurrentCommitState;
import io.onetable.model.InstantsForIncrementalSync;
import io.onetable.model.OneSnapshot;
import io.onetable.model.TableChange;
import io.onetable.model.storage.OneDataFile;
import io.onetable.spi.DefaultSnapshotVisitor;

/*
 * All Tests planning to support.
 * 1. Inserts, Updates, Deletes combination.
 * 2. Add  partition.
 * 3. Change schema as per allowed evolution (adding columns etc...)
 * 4. Drop partition.
 * 5. Vaccum.
 * 6. Clustering.
 */
public class ITDeltaSourceClient {
  @TempDir private static Path tempDir;
  private static SparkSession sparkSession;

  @BeforeAll
  public static void setupOnce() {
    sparkSession =
        SparkSession.builder()
            .appName("TestDeltaTable")
            .master("local[4]")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate();
  }

  @AfterAll
  public static void teardown() {
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

  @Test
  public void insertsOnly() throws ParseException {
    TestSparkDeltaTable testSparkDeltaTable =
        new TestSparkDeltaTable("some_table", tempDir, sparkSession);
    List<List<String>> allActiveFiles = new ArrayList<>();
    List<TableChange> allTableChanges = new ArrayList<>();
    List<Row> rows = testSparkDeltaTable.insertRows(30);
    Long version1 = testSparkDeltaTable.getVersion();
    Long timestamp1 = testSparkDeltaTable.getLastCommitTimestamp();
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());
    testSparkDeltaTable.insertRows(20);
    Long version2 = testSparkDeltaTable.getVersion();
    Long timestamp2 = testSparkDeltaTable.getLastCommitTimestamp();
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());
    testSparkDeltaTable.insertRows(10);
    Long version3 = testSparkDeltaTable.getVersion();
    Long timestamp3 = testSparkDeltaTable.getLastCommitTimestamp();
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());
    testSparkDeltaTable.insertRows(40);
    Long version4 = testSparkDeltaTable.getVersion();
    Long timestamp4 = testSparkDeltaTable.getLastCommitTimestamp();
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());
    testSparkDeltaTable.insertRows(50);
    Long version5 = testSparkDeltaTable.getVersion();
    Long timestamp5 = testSparkDeltaTable.getLastCommitTimestamp();
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());
    DeltaSourceClient deltaSourceClient =
        new DeltaSourceClient(
            sparkSession, testSparkDeltaTable.getTableName(), testSparkDeltaTable.getBasePath());
    OneSnapshot oneSnapshot = deltaSourceClient.getCurrentSnapshot();
    validateOneSnapshot(oneSnapshot, allActiveFiles.get(allActiveFiles.size() - 1));
    // Get changes in incremental format.
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
            .build();
    CurrentCommitState<Long> currentCommitState =
        deltaSourceClient.getCurrentCommitState(instantsForIncrementalSync);
    for (Long version : currentCommitState.getCommitsToProcess()) {
      TableChange tableChange = deltaSourceClient.getTableChangeForCommit(version);
      allTableChanges.add(tableChange);
    }
    validateTableChanges(allActiveFiles, allTableChanges);
  }

  @Test
  public void canDelete() throws ParseException {
    TestSparkDeltaTable testSparkDeltaTable =
        new TestSparkDeltaTable("some_table", tempDir, sparkSession);
    List<List<String>> allActiveFiles = new ArrayList<>();
    List<TableChange> allTableChanges = new ArrayList<>();
    List<Row> rows = testSparkDeltaTable.insertRows(30);
    Long version1 = testSparkDeltaTable.getVersion();
    Long timestamp1 = testSparkDeltaTable.getLastCommitTimestamp();
    testSparkDeltaTable.upsertRowsAnother(rows);
    int x = 5;
  }

  private void validateOneSnapshot(OneSnapshot oneSnapshot, List<String> allActivePaths) {
    assertNotNull(oneSnapshot);
    assertNotNull(oneSnapshot.getTable());
    List<String> onetablePaths =
        new ArrayList<>(
            DefaultSnapshotVisitor.extractDataFilePaths(oneSnapshot.getDataFiles()).keySet());
    replaceFileScheme(allActivePaths);
    replaceFileScheme(onetablePaths);
    Collections.sort(allActivePaths);
    Collections.sort(onetablePaths);
    assertEquals(allActivePaths, onetablePaths);
  }

  private void validateTableChanges(
      List<List<String>> allActiveFiles, List<TableChange> allTableChanges) {
    if (allTableChanges.isEmpty() && allActiveFiles.size() <= 1) {
      return;
    }
    assertEquals(
        allTableChanges.size(),
        allActiveFiles.size() - 1,
        "Number of table changes should be equal to number of commits - 1");
    IntStream.range(0, allActiveFiles.size() - 1)
        .forEach(
            i ->
                validateTableChange(
                    allActiveFiles.get(i), allActiveFiles.get(i + 1), allTableChanges.get(i)));
  }

  private void validateTableChange(
      List<String> filePathsBefore, List<String> filePathsAfter, TableChange tableChange) {
    assertNotNull(tableChange);
    assertNotNull(tableChange.getCurrentTableState());
    replaceFileScheme(filePathsBefore);
    replaceFileScheme(filePathsAfter);
    Set<String> filesForCommitBefore = new HashSet<>(filePathsBefore);
    Set<String> filesForCommitAfter = new HashSet<>(filePathsAfter);
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

  private void replaceFileScheme(List<String> filePaths) {
    // if file paths start with file:///, replace it with file:/.
    filePaths.replaceAll(path -> path.replaceFirst("file:///", "file:/"));
  }
}
