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

import java.net.URI;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import lombok.extern.log4j.Log4j2;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.delta.DeltaConversionSourceProvider;
import org.apache.xtable.hudi.HudiTestUtil;
import org.apache.xtable.kernel.DeltaKernelConversionSourceProvider;
import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.extractor.ConversionSource;

/**
 * Empirical repro for https://github.com/apache/incubator-xtable/issues/779: does {@code
 * isIncrementalSyncSafeFrom} correctly report "not safe" once the underlying _delta_log commit
 * files backing an old instant have been physically removed, for both the Standalone and Kernel
 * Delta sources?
 *
 * <p>Per the Delta protocol spec (PROTOCOL.md, "Metadata cleanup"), real log retention cleanup is
 * REQUIRED to leave a checkpoint covering the oldest kept version before deleting older JSON commit
 * files -- readers should never be able to reach a state with commit files missing and no
 * checkpoint to fall back on. To reproduce that realistic, protocol-compliant state rather than an
 * artificial one, this test inserts enough rows to force Delta's default checkpoint interval (10
 * commits) to actually create a checkpoint, then deletes only the JSON commit files strictly older
 * than that checkpoint -- leaving the checkpoint itself and everything after it intact.
 */
@Log4j2
public class ITDeltaLogTruncationSafetyCheck {
  // Delta's default delta.checkpointInterval is 10 commits; insert enough rows to comfortably
  // pass that so a checkpoint actually gets written.
  private static final int NUM_COMMITS = 14;

  @TempDir public static Path tempDir;

  private static JavaSparkContext jsc;
  private static SparkSession sparkSession;

  @BeforeAll
  public static void setupOnce() {
    sparkSession = SparkSession.builder().config(HudiTestUtil.getSparkConf(tempDir)).getOrCreate();
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

  @Test
  public void testIsIncrementalSyncSafeFromAfterLogTruncation() throws Exception {
    String tableName = GenericTable.getTableName();
    Instant instantAfterV0;
    String basePath;
    try (TestSparkDeltaTable table =
        new TestSparkDeltaTable(tableName, tempDir, sparkSession, null, false)) {
      table.insertRows(10); // version 0
      instantAfterV0 = Instant.now();
      Thread.sleep(1100);

      for (int i = 1; i < NUM_COMMITS; i++) {
        table.insertRows(10);
        Thread.sleep(200);
      }
      basePath = table.getBasePath();
    }

    Path deltaLogDir = Paths.get(URI.create(basePath)).resolve("_delta_log");
    long checkpointVersion = findLatestCheckpointVersion(deltaLogDir);
    log.info("Latest checkpoint version found: {}", checkpointVersion);
    org.junit.jupiter.api.Assertions.assertTrue(
        checkpointVersion > 0,
        "Expected a checkpoint to have been created after "
            + NUM_COMMITS
            + " commits; increase NUM_COMMITS if delta.checkpointInterval has changed.");

    // Delete every JSON commit file strictly older than the checkpoint -- exactly what
    // protocol-compliant log retention cleanup does, per PROTOCOL.md's "Metadata cleanup"
    // section -- leaving the checkpoint (and everything from its version onward) intact.
    int deletedCount = 0;
    for (long version = 0; version < checkpointVersion; version++) {
      Path commitFile = deltaLogDir.resolve(String.format("%020d.json", version));
      if (Files.deleteIfExists(commitFile)) {
        deletedCount++;
      }
    }
    log.info(
        "Deleted {} commit file(s) older than checkpoint version {}",
        deletedCount,
        checkpointVersion);

    SourceTable sourceTable =
        SourceTable.builder()
            .name(tableName)
            .formatName(TableFormat.DELTA)
            .basePath(basePath)
            .dataPath(basePath)
            .additionalProperties(new Properties())
            .build();

    // instantAfterV0 is before the checkpoint, and its backing commit file has been deleted,
    // while a valid checkpoint covering that deletion exists -- the realistic, protocol-compliant
    // version of the "log truncated past the requested instant" scenario in #779.
    logSafetyCheckResult(
        "Standalone", new DeltaConversionSourceProvider(), sourceTable, instantAfterV0);
    logSafetyCheckResult(
        "Kernel", new DeltaKernelConversionSourceProvider(), sourceTable, instantAfterV0);
  }

  // Both isIncrementalSyncSafeFrom implementations correctly return false once a valid
  // checkpoint already covers the deleted commits (see
  // testIsIncrementalSyncSafeFromAfterLogTruncation
  // above). Delta's own docs on getEarliestDeltaFile/getEarliestRecreatableCommit explicitly warn
  // that "this version isn't guaranteed to exist when performing an action as a concurrent
  // operation can delete the file during cleanup" -- i.e. a TOCTOU race between the safety check
  // and the actual incremental read that follows it. This test targets that race directly: call
  // the safety check BEFORE any deletion (so it should say "safe"), then delete the pre-checkpoint
  // commit files (simulating retention cleanup running concurrently, right after the check
  // passed), then attempt the actual incremental read the check just approved.
  @Test
  public void testSafetyCheckRaceWithConcurrentCleanup_Standalone() throws Exception {
    raceCheckThenReadAfterConcurrentDeletion("Standalone", new DeltaConversionSourceProvider());
  }

  @Test
  public void testSafetyCheckRaceWithConcurrentCleanup_Kernel() throws Exception {
    raceCheckThenReadAfterConcurrentDeletion("Kernel", new DeltaKernelConversionSourceProvider());
  }

  private void raceCheckThenReadAfterConcurrentDeletion(
      String label, ConversionSourceProvider<Long> provider) throws Exception {
    // Each implementation gets its own freshly-built table, so one implementation's deletion of
    // pre-checkpoint files can't contaminate the other's "before deletion" baseline.
    String tableName = GenericTable.getTableName();
    Instant instantAfterV0;
    String basePath;
    try (TestSparkDeltaTable table =
        new TestSparkDeltaTable(tableName, tempDir, sparkSession, null, false)) {
      table.insertRows(10); // version 0
      instantAfterV0 = Instant.now();
      Thread.sleep(1100);

      for (int i = 1; i < NUM_COMMITS; i++) {
        table.insertRows(10);
        Thread.sleep(200);
      }
      basePath = table.getBasePath();
    }

    Path deltaLogDir = Paths.get(URI.create(basePath)).resolve("_delta_log");
    long checkpointVersion = findLatestCheckpointVersion(deltaLogDir);
    log.info("[{}] Latest checkpoint version found: {}", label, checkpointVersion);
    org.junit.jupiter.api.Assertions.assertTrue(checkpointVersion > 0, "Expected a checkpoint");

    SourceTable sourceTable =
        SourceTable.builder()
            .name(tableName)
            .formatName(TableFormat.DELTA)
            .basePath(basePath)
            .dataPath(basePath)
            .additionalProperties(new Properties())
            .build();

    provider.init(jsc.hadoopConfiguration());
    try (ConversionSource<Long> source = provider.getConversionSourceInstance(sourceTable)) {
      boolean safeBeforeDeletion = source.isIncrementalSyncSafeFrom(instantAfterV0);
      log.info(
          "[{}] isIncrementalSyncSafeFrom({}) BEFORE concurrent cleanup = {}",
          label,
          instantAfterV0,
          safeBeforeDeletion);

      // Simulate retention cleanup running concurrently, immediately after the check passed.
      int deletedCount = 0;
      for (long version = 0; version < checkpointVersion; version++) {
        Path commitFile = deltaLogDir.resolve(String.format("%020d.json", version));
        if (Files.deleteIfExists(commitFile)) {
          deletedCount++;
        }
      }
      log.info("[{}] Deleted {} commit file(s) after the safety check ran", label, deletedCount);

      if (safeBeforeDeletion) {
        try {
          CommitsBacklog<Long> backlog =
              source.getCommitsBacklog(
                  InstantsForIncrementalSync.builder().lastSyncInstant(instantAfterV0).build());
          log.info(
              "[{}] getCommitsBacklog after concurrent cleanup returned commitsToProcess={},"
                  + " expected commits 1..{} (checkpoint covers 0..{}) if nothing was silently"
                  + " skipped",
              label,
              backlog.getCommitsToProcess(),
              NUM_COMMITS - 1,
              checkpointVersion - 1);
          for (Long commit : backlog.getCommitsToProcess()) {
            source.getTableChangeForCommit(commit);
            log.info("[{}] getTableChangeForCommit({}) succeeded", label, commit);
          }
          log.info(
              "[{}] Read succeeded despite concurrent cleanup; commitsToProcess.size()={}"
                  + " (a value less than {} means early commits were silently dropped, not read)",
              label,
              backlog.getCommitsToProcess().size(),
              NUM_COMMITS - 1);
        } catch (Exception e) {
          log.error(
              "[{}] RACE CONFIRMED: safety check said safe, but the read that followed threw"
                  + " after concurrent cleanup: {}",
              label,
              e.toString());
        }
      }
    } catch (Exception e) {
      log.error(
          "[{}] isIncrementalSyncSafeFrom({}) itself threw: {}",
          label,
          instantAfterV0,
          e.toString());
    }
  }

  /** Returns the highest checkpoint version found in the log directory, or -1 if none exist. */
  private long findLatestCheckpointVersion(Path deltaLogDir) throws Exception {
    Pattern checkpointPattern = Pattern.compile("^(\\d{20})\\.checkpoint(\\..*)?\\.parquet$");
    long latest = -1;
    try (DirectoryStream<Path> stream = Files.newDirectoryStream(deltaLogDir)) {
      for (Path entry : stream) {
        Matcher matcher = checkpointPattern.matcher(entry.getFileName().toString());
        if (matcher.matches()) {
          latest = Math.max(latest, Long.parseLong(matcher.group(1)));
        }
      }
    }
    return latest;
  }

  private void logSafetyCheckResult(
      String label,
      ConversionSourceProvider<Long> provider,
      SourceTable sourceTable,
      Instant instant) {
    provider.init(jsc.hadoopConfiguration());
    try (ConversionSource<Long> source = provider.getConversionSourceInstance(sourceTable)) {
      boolean safe = source.isIncrementalSyncSafeFrom(instant);
      log.info("[{}] isIncrementalSyncSafeFrom({}) = {}", label, instant, safe);
      if (safe) {
        // The safety check said it's fine to sync incrementally from this instant. Try to
        // actually read the backlog from here, which is what would really happen next -- if
        // this throws, the safety check produced a false positive.
        try {
          CommitsBacklog<Long> backlog =
              source.getCommitsBacklog(
                  InstantsForIncrementalSync.builder().lastSyncInstant(instant).build());
          log.info(
              "[{}] getCommitsBacklog({}) succeeded, commits to process: {}",
              label,
              instant,
              backlog.getCommitsToProcess());
          for (Long commit : backlog.getCommitsToProcess()) {
            source.getTableChangeForCommit(commit);
          }
          log.info("[{}] getTableChangeForCommit succeeded for all commits in backlog", label);
        } catch (Exception e) {
          log.error(
              "[{}] isIncrementalSyncSafeFrom returned true but reading the backlog threw: {}",
              label,
              e.toString());
        }
      }
    } catch (Exception e) {
      log.error("[{}] isIncrementalSyncSafeFrom({}) threw: {}", label, instant, e.toString());
    }
  }
}
