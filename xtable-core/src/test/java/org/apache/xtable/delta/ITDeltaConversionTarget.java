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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.hudi.HudiTestUtil;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncMode;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.model.sync.SyncStatusCode;

/**
 * Writes Delta tables with the default Delta Standalone based {@link DeltaConversionTarget} through
 * {@link ConversionController}, then reads them with Spark Delta and compares the result with the
 * source. The tests change the session time zone of the shared Spark session, so they run one at a
 * time.
 */
@Execution(SAME_THREAD)
public class ITDeltaConversionTarget {
  // 2026-11-14T00:39:17.250Z
  private static final long MIN_TIMESTAMP_MICROS = 1794616757250000L;
  // 2026-11-14T00:39:18.926789Z
  private static final long MAX_TIMESTAMP_MICROS = 1794616758926789L;

  @TempDir public static Path tempDir;

  private static SparkSession sparkSession;
  private static JavaSparkContext jsc;

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

  /**
   * Delta readers skip files with the min and max stats in the Delta log. They add 1 ms to a
   * timestamp max, since Delta stats keep milliseconds. A filter just inside the min or max of a
   * timestamp column must return the same rows from the Delta target as from the source, whatever
   * the reader's session time zone.
   */
  @ParameterizedTest
  @ValueSource(strings = {"UTC", "Asia/Kolkata", "America/Los_Angeles"})
  public void testFilterNearTimestampMinAndMaxMatchesSource(String sessionTimeZone) {
    String tableName = "test_table_" + UUID.randomUUID().toString().replace("-", "_");
    String icebergTable = "default_iceberg.xtable_it." + tableName;
    String icebergPath = tempDir.resolve("xtable_it").resolve(tableName).toString();
    String deltaPath = tempDir.resolve(tableName + "_delta").toString();
    sparkSession.sql(
        String.format("CREATE TABLE %s (id INT, ts TIMESTAMP) USING iceberg", icebergTable));
    sparkSession.sql(
        String.format(
            "INSERT INTO %s VALUES (1, timestamp_micros(%d)), (2, timestamp_micros(%d))",
            icebergTable, MIN_TIMESTAMP_MICROS, MAX_TIMESTAMP_MICROS));
    syncToDelta(tableName, icebergPath, deltaPath);

    String justBelowMax = String.format("ts > timestamp_micros(%d)", MAX_TIMESTAMP_MICROS - 1);
    String justAboveMin = String.format("ts < timestamp_micros(%d)", MIN_TIMESTAMP_MICROS + 1);
    sparkSession.conf().set("spark.sql.session.timeZone", sessionTimeZone);
    try {
      for (String filter : new String[] {justBelowMax, justAboveMin}) {
        long sourceCount = sparkSession.table(icebergTable).filter(filter).count();
        long deltaCount =
            sparkSession.read().format("delta").load(deltaPath).filter(filter).count();
        assertEquals(1, sourceCount, "Source row count for: " + filter);
        assertEquals(
            sourceCount,
            deltaCount,
            String.format(
                "Delta row count for %s in session time zone %s", filter, sessionTimeZone));
      }
    } finally {
      sparkSession.conf().set("spark.sql.session.timeZone", "UTC");
    }
  }

  private void syncToDelta(String tableName, String icebergPath, String deltaPath) {
    ConversionConfig conversionConfig =
        ConversionConfig.builder()
            .sourceTable(
                SourceTable.builder()
                    .name(tableName)
                    .formatName(TableFormat.ICEBERG)
                    .basePath(icebergPath)
                    .dataPath(icebergPath + "/data")
                    .additionalProperties(new Properties())
                    .build())
            .targetTables(
                Collections.singletonList(
                    TargetTable.builder()
                        .name(tableName)
                        .formatName(TableFormat.DELTA)
                        .basePath(deltaPath)
                        .additionalProperties(new Properties())
                        .build()))
            .syncMode(SyncMode.INCREMENTAL)
            .build();
    IcebergConversionSourceProvider sourceProvider = new IcebergConversionSourceProvider();
    sourceProvider.init(jsc.hadoopConfiguration());
    Map<String, SyncResult> results =
        new ConversionController(jsc.hadoopConfiguration()).sync(conversionConfig, sourceProvider);
    assertEquals(
        SyncStatusCode.SUCCESS,
        results.get(TableFormat.DELTA).getTableFormatSyncStatus().getStatusCode());
  }
}
