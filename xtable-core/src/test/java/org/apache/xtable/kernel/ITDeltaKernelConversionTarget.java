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
 
package org.apache.xtable.kernel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.client.HoodieReadClient;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieTableType;

import org.apache.xtable.TestJavaHudiTable;
import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.delta.DeltaConversionTargetConfig;
import org.apache.xtable.hudi.HudiConversionSourceProvider;
import org.apache.xtable.hudi.HudiTestUtil;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncMode;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.model.sync.SyncStatusCode;

/**
 * Writes Delta tables with {@link DeltaKernelConversionTarget} through the normal {@link
 * ConversionController} path, then reads them with Spark Delta and compares the result with the
 * source and with the Delta Standalone target.
 */
public class ITDeltaKernelConversionTarget {
  @TempDir public static Path tempDir;

  private static SparkSession sparkSession;
  private static JavaSparkContext jsc;

  @BeforeAll
  public static void setupOnce() {
    sparkSession =
        SparkSession.builder()
            .config(HoodieReadClient.addHoodieSupport(HudiTestUtil.getSparkConf(tempDir)))
            .getOrCreate();
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
   * A Hudi timestamp-millis column has its stats in millis. Delta readers skip files using the
   * min/max stats in the Delta log, so stats written in the wrong unit drop rows from filtered
   * reads.
   */
  @Test
  public void testFilterOnMillisTimestampColumnMatchesSourceAndStandaloneTarget() {
    String tableName = "test_table_" + UUID.randomUUID();
    String filter =
        String.format(
            "timestamp_millis_nullable_field >= timestamp_millis(%d)",
            Instant.now().truncatedTo(ChronoUnit.DAYS).minus(2, ChronoUnit.DAYS).toEpochMilli());
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, null, HoodieTableType.COPY_ON_WRITE)) {
      String kernelPath = tempDir.resolve(tableName + "_kernel").toString();
      String standalonePath = tempDir.resolve(tableName + "_standalone").toString();

      table.insertRecords(100, true);
      syncToDelta(table, tableName, kernelPath, true);
      syncToDelta(table, tableName, standalonePath, false);
      table.insertRecords(100, true);
      syncToDelta(table, tableName, kernelPath, true);
      syncToDelta(table, tableName, standalonePath, false);

      long sourceCount =
          sparkSession.read().format("hudi").load(table.getBasePath()).filter(filter).count();
      assertTrue(
          sourceCount > 0 && sourceCount < 200,
          "Filter should select some but not all rows, selected " + sourceCount);
      long standaloneCount =
          sparkSession.read().format("delta").load(standalonePath).filter(filter).count();
      long kernelCount =
          sparkSession.read().format("delta").load(kernelPath).filter(filter).count();
      assertEquals(sourceCount, standaloneCount, "Standalone target row count for: " + filter);
      assertEquals(sourceCount, kernelCount, "Kernel target row count for: " + filter);
    }
  }

  private void syncToDelta(
      TestJavaHudiTable table, String tableName, String targetPath, boolean useKernel) {
    TypedProperties targetProperties = new TypedProperties();
    targetProperties.setProperty(DeltaConversionTargetConfig.USE_KERNEL, String.valueOf(useKernel));
    ConversionConfig conversionConfig =
        ConversionConfig.builder()
            .sourceTable(
                SourceTable.builder()
                    .name(tableName)
                    .formatName(TableFormat.HUDI)
                    .basePath(table.getBasePath())
                    .additionalProperties(new Properties())
                    .build())
            .targetTables(
                Collections.singletonList(
                    TargetTable.builder()
                        .name(tableName)
                        .formatName(TableFormat.DELTA)
                        .basePath(targetPath)
                        .additionalProperties(targetProperties)
                        .build()))
            .syncMode(SyncMode.INCREMENTAL)
            .build();
    HudiConversionSourceProvider sourceProvider = new HudiConversionSourceProvider();
    sourceProvider.init(jsc.hadoopConfiguration());
    Map<String, SyncResult> results =
        new ConversionController(jsc.hadoopConfiguration()).sync(conversionConfig, sourceProvider);
    assertEquals(
        SyncStatusCode.SUCCESS,
        results.get(TableFormat.DELTA).getTableFormatSyncStatus().getStatusCode());
  }
}
