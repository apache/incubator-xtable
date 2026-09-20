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
 
package org.apache.xtable.delta;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.actions.AddFile;

import scala.Option;

import org.apache.xtable.GenericTable;
import org.apache.xtable.TestSparkDeltaTable;
import org.apache.xtable.ValidationTestHelper;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.kernel.DeltaKernelConversionSourceProvider;
import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.extractor.ConversionSource;

public class ITDeltaDeleteVectorConvert {
  @TempDir private static Path tempDir;
  private static SparkSession sparkSession;

  private DeltaConversionSourceProvider conversionSourceProvider;
  private DeltaKernelConversionSourceProvider kernelConversionSourceProvider;

  @BeforeAll
  public static void setupOnce() {
    sparkSession =
        SparkSession.builder()
            .appName("TestDeltaTable")
            .master("local[4]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
            .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.default.parallelism", "1")
            .config("spark.serializer", KryoSerializer.class.getName())
            .getOrCreate();
  }

  @AfterAll
  public static void teardown() {
    if (sparkSession != null) {
      sparkSession.close();
    }
  }

  @BeforeEach
  void setUp() {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.defaultFS", "file:///");

    conversionSourceProvider = new DeltaConversionSourceProvider();
    conversionSourceProvider.init(hadoopConf);
    kernelConversionSourceProvider = new DeltaKernelConversionSourceProvider();
    kernelConversionSourceProvider.init(hadoopConf);
  }

  @Test
  public void unsupportedDeletionVectorsRequireExplicitOptIn() {
    String tableName = GenericTable.getTableName();
    TestSparkDeltaTable testSparkDeltaTable =
        new TestSparkDeltaTable(tableName, tempDir, sparkSession, null, false);
    testSparkDeltaTable
        .getSparkSession()
        .sql(
            "ALTER TABLE "
                + tableName
                + " SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)");

    List<Row> rows = testSparkDeltaTable.insertRows(50);
    Long timestampBeforeDelete = testSparkDeltaTable.getLastCommitTimestamp();
    testSparkDeltaTable.deleteRows(rows.subList(0, 10));
    Long timestampAfterDelete = testSparkDeltaTable.getLastCommitTimestamp();
    List<String> activeFilesAfterDelete = testSparkDeltaTable.getAllActiveFiles();

    SourceTable strictTableConfig =
        SourceTable.builder()
            .name(testSparkDeltaTable.getTableName())
            .basePath(testSparkDeltaTable.getBasePath())
            .formatName(TableFormat.DELTA)
            .build();
    assertRejectsDeletionVectors(
        conversionSourceProvider.getConversionSourceInstance(strictTableConfig),
        timestampAfterDelete);
    assertRejectsDeletionVectors(
        kernelConversionSourceProvider.getConversionSourceInstance(strictTableConfig),
        timestampAfterDelete);

    Properties allowUnsupportedDeletionVectors = new Properties();
    allowUnsupportedDeletionVectors.setProperty(
        DeltaConversionSourceConfig.ALLOW_UNSUPPORTED_DELETION_VECTORS, "true");
    SourceTable permissiveTableConfig =
        SourceTable.builder()
            .name(testSparkDeltaTable.getTableName())
            .basePath(testSparkDeltaTable.getBasePath())
            .formatName(TableFormat.DELTA)
            .additionalProperties(allowUnsupportedDeletionVectors)
            .build();
    assertContinuesWithDeletionVectors(
        conversionSourceProvider.getConversionSourceInstance(permissiveTableConfig),
        timestampBeforeDelete,
        activeFilesAfterDelete);
    assertContinuesWithDeletionVectors(
        kernelConversionSourceProvider.getConversionSourceInstance(permissiveTableConfig),
        timestampBeforeDelete,
        activeFilesAfterDelete);
  }

  @Test
  public void testInsertsUpsertsAndDeletes() {
    String tableName = GenericTable.getTableName();
    TestSparkDeltaTable testSparkDeltaTable =
        new TestSparkDeltaTable(tableName, tempDir, sparkSession, null, false);

    // enable deletion vectors for the test table
    testSparkDeltaTable
        .getSparkSession()
        .sql(
            "ALTER TABLE "
                + tableName
                + " SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)");

    List<List<String>> allActiveFiles = new ArrayList<>();
    List<TableChange> allTableChanges = new ArrayList<>();
    List<Row> rows = testSparkDeltaTable.insertRows(50);
    Long timestamp1 = testSparkDeltaTable.getLastCommitTimestamp();
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());

    List<Row> rows1 = testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());
    assertEquals(100L, testSparkDeltaTable.getNumRows());
    validateDeletedRecordCount(testSparkDeltaTable.getDeltaLog(), allActiveFiles.size() + 1, 0, 0);

    // upsert does not create delete vectors
    testSparkDeltaTable.upsertRows(rows.subList(0, 20));
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());
    assertEquals(100L, testSparkDeltaTable.getNumRows());
    validateDeletedRecordCount(testSparkDeltaTable.getDeltaLog(), allActiveFiles.size() + 1, 0, 0);

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());
    assertEquals(150L, testSparkDeltaTable.getNumRows());

    // delete a few rows with gaps in ids
    List<Row> rowsToDelete =
        rows1.subList(0, 10).stream()
            .filter(row -> (row.get(0).hashCode() % 2) == 0)
            .collect(Collectors.toList());
    rowsToDelete.addAll(rows.subList(35, 45));
    testSparkDeltaTable.deleteRows(rowsToDelete);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());
    assertEquals(135L, testSparkDeltaTable.getNumRows());
    validateDeletedRecordCount(testSparkDeltaTable.getDeltaLog(), allActiveFiles.size() + 1, 2, 15);

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());
    assertEquals(185L, testSparkDeltaTable.getNumRows());

    // delete a few rows from a file which already has a deletion vector, this should generate a
    // merged deletion vector file. Some rows were already deleted in the previous delete step.
    // This deletion step intentionally deletes the same rows again to test the merge.
    rowsToDelete = rows1.subList(5, 15);
    testSparkDeltaTable.deleteRows(rowsToDelete);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());
    assertEquals(178L, testSparkDeltaTable.getNumRows());
    validateDeletedRecordCount(testSparkDeltaTable.getDeltaLog(), allActiveFiles.size() + 1, 2, 22);

    testSparkDeltaTable.insertRows(50);
    allActiveFiles.add(testSparkDeltaTable.getAllActiveFiles());
    assertEquals(228L, testSparkDeltaTable.getNumRows());

    Properties sourceProperties = new Properties();
    sourceProperties.setProperty(
        DeltaConversionSourceConfig.ALLOW_UNSUPPORTED_DELETION_VECTORS, "true");
    SourceTable tableConfig =
        SourceTable.builder()
            .name(testSparkDeltaTable.getTableName())
            .basePath(testSparkDeltaTable.getBasePath())
            .formatName(TableFormat.DELTA)
            .additionalProperties(sourceProperties)
            .build();
    DeltaConversionSource conversionSource =
        conversionSourceProvider.getConversionSourceInstance(tableConfig);
    InternalSnapshot internalSnapshot = conversionSource.getCurrentSnapshot();

    //    validateDeltaPartitioning(internalSnapshot);
    ValidationTestHelper.validateSnapshot(
        internalSnapshot, allActiveFiles.get(allActiveFiles.size() - 1));

    // Get changes in incremental format.
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(Instant.ofEpochMilli(timestamp1))
            .build();
    CommitsBacklog<Long> commitsBacklog =
        conversionSource.getCommitsBacklog(instantsForIncrementalSync);
    for (Long version : commitsBacklog.getCommitsToProcess()) {
      TableChange tableChange = conversionSource.getTableChangeForCommit(version);
      allTableChanges.add(tableChange);
    }
    ValidationTestHelper.validateTableChanges(allActiveFiles, allTableChanges);
  }

  private void assertRejectsDeletionVectors(
      ConversionSource<Long> conversionSource, Long lastSyncTimestamp) {
    NotSupportedException fullSyncException =
        assertThrows(NotSupportedException.class, conversionSource::getCurrentSnapshot);
    assertTrue(fullSyncException.getMessage().contains("contains a deletion vector"));

    NotSupportedException incrementalSyncException =
        assertThrows(
            NotSupportedException.class,
            () -> {
              CommitsBacklog<Long> commitsBacklog =
                  conversionSource.getCommitsBacklog(
                      InstantsForIncrementalSync.builder()
                          .lastSyncInstant(Instant.ofEpochMilli(lastSyncTimestamp))
                          .build());
              for (Long version : commitsBacklog.getCommitsToProcess()) {
                conversionSource.getTableChangeForCommit(version);
              }
            });
    assertTrue(incrementalSyncException.getMessage().contains("contains a deletion vector"));
  }

  private void assertContinuesWithDeletionVectors(
      ConversionSource<Long> conversionSource,
      Long timestampBeforeDelete,
      List<String> expectedActiveFiles) {
    InternalSnapshot internalSnapshot = assertDoesNotThrow(conversionSource::getCurrentSnapshot);
    ValidationTestHelper.validateSnapshot(internalSnapshot, expectedActiveFiles);
    CommitsBacklog<Long> commitsBacklog =
        conversionSource.getCommitsBacklog(
            InstantsForIncrementalSync.builder()
                .lastSyncInstant(Instant.ofEpochMilli(timestampBeforeDelete))
                .build());
    assertFalse(commitsBacklog.getCommitsToProcess().isEmpty());
    for (Long version : commitsBacklog.getCommitsToProcess()) {
      TableChange tableChange =
          assertDoesNotThrow(() -> conversionSource.getTableChangeForCommit(version));
      assertTrue(tableChange.getFilesDiff().getFilesAdded().isEmpty());
      assertTrue(tableChange.getFilesDiff().getFilesRemoved().isEmpty());
    }
  }

  private void validateDeletedRecordCount(
      DeltaLog deltaLog, int version, int deleteVectorFileCount, int deletionRecordCount) {
    List<AddFile> allFiles =
        deltaLog.getSnapshotAt(version, Option.empty()).allFiles().collectAsList();
    List<AddFile> filesWithDeletionVectors =
        allFiles.stream().filter(f -> f.deletionVector() != null).collect(Collectors.toList());

    assertEquals(deleteVectorFileCount, filesWithDeletionVectors.size());
    assertEquals(
        deletionRecordCount,
        filesWithDeletionVectors.stream()
            .collect(Collectors.summarizingLong(AddFile::numDeletedRecords))
            .getSum());
  }
}
