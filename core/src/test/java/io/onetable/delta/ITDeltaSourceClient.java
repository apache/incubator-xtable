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

import java.nio.file.Path;
import java.text.ParseException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.onetable.TestSparkDeltaTable;
import io.onetable.model.CurrentCommitState;
import io.onetable.model.InstantsForIncrementalSync;
import io.onetable.model.TableChange;

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
    List<TableChange> allTableChanges = new ArrayList<>();
    testSparkDeltaTable.insertRows(30);
    Long version1 = testSparkDeltaTable.getVersion();
    Long timestamp1 = testSparkDeltaTable.getLastCommitTimestamp();
    testSparkDeltaTable.insertRows(20);
    Long version2 = testSparkDeltaTable.getVersion();
    Long timestamp2 = testSparkDeltaTable.getLastCommitTimestamp();
    testSparkDeltaTable.insertRows(10);
    Long version3 = testSparkDeltaTable.getVersion();
    Long timestamp3 = testSparkDeltaTable.getLastCommitTimestamp();
    testSparkDeltaTable.insertRows(40);
    Long version4 = testSparkDeltaTable.getVersion();
    Long timestamp4 = testSparkDeltaTable.getLastCommitTimestamp();
    testSparkDeltaTable.insertRows(50);
    Long version5 = testSparkDeltaTable.getVersion();
    Long timestamp5 = testSparkDeltaTable.getLastCommitTimestamp();
    DeltaSourceClient deltaSourceClient =
        new DeltaSourceClient(sparkSession, testSparkDeltaTable.getBasePath());
    // Get changes in incremental format.
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
            .build();
    deltaSourceClient.getTableChangeForCommit(1L);
    CurrentCommitState<Long> currentCommitState =
        deltaSourceClient.getCurrentCommitState(instantsForIncrementalSync);
    for (Long version : currentCommitState.getCommitsToProcess()) {
      TableChange tableChange = deltaSourceClient.getTableChangeForCommit(version);
      allTableChanges.add(tableChange);
    }
    // validate table changes.
    return;
  }
}
