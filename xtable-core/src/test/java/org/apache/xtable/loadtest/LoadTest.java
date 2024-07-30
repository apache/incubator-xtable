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
 
package org.apache.xtable.loadtest;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieArchivalConfig;

import org.apache.xtable.TestJavaHudiTable;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.PerTableConfig;
import org.apache.xtable.conversion.PerTableConfigImpl;
import org.apache.xtable.hudi.HudiConversionSourceProvider;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncMode;

/**
 * Tests that can be run manually to simulate lots of commits/partitions/files/etc. to understand
 * how the system behaves under these conditions.
 */
@Disabled
public class LoadTest {
  @TempDir public static Path tempDir;
  private static final Configuration CONFIGURATION = new Configuration();
  private ConversionSourceProvider<HoodieInstant> hudiConversionSourceProvider;

  @BeforeEach
  public void setup() {
    hudiConversionSourceProvider = new HudiConversionSourceProvider();
    hudiConversionSourceProvider.init(CONFIGURATION, Collections.emptyMap());
  }

  @Test
  void testFullSyncWithManyPartitions() {
    String tableName = "full_sync_many_partitions";
    int numPartitions = 1000;
    int numFilesPerPartition = 100;
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, "level:SIMPLE", HoodieTableType.COPY_ON_WRITE)) {
      for (int i = 0; i < numFilesPerPartition; i++) {
        table.insertRecords(
            1,
            IntStream.range(0, numPartitions)
                .mapToObj(partitionNumber -> "partition" + partitionNumber)
                .collect(Collectors.toList()),
            false);
      }
      PerTableConfig perTableConfig =
          PerTableConfigImpl.builder()
              .tableName(tableName)
              .targetTableFormats(Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA))
              .tableBasePath(table.getBasePath())
              .syncMode(SyncMode.FULL)
              .build();
      ConversionController conversionController = new ConversionController(CONFIGURATION);
      long start = System.currentTimeMillis();
      conversionController.sync(perTableConfig, hudiConversionSourceProvider);
      long end = System.currentTimeMillis();
      System.out.println("Full sync took " + (end - start) + "ms");
    }
  }

  @Test
  void testIncrementalSyncWithManyCommits() {
    String tableName = "incremental_sync_many_commmits";
    // creates a single file per partition per commit
    int numPartitionsUpdatedPerCommit = 1000;
    int numCommits = 100;
    HoodieArchivalConfig archivalConfig =
        HoodieArchivalConfig.newBuilder()
            .archiveCommitsWith(numCommits + 1, numCommits + 10)
            .build();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, "level:SIMPLE", HoodieTableType.COPY_ON_WRITE, archivalConfig)) {
      table.insertRecords(1, "partition0", false);
      PerTableConfig perTableConfig =
          PerTableConfigImpl.builder()
              .tableName(tableName)
              .targetTableFormats(Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA))
              .tableBasePath(table.getBasePath())
              .syncMode(SyncMode.INCREMENTAL)
              .build();
      // sync once to establish first commit
      ConversionController conversionController = new ConversionController(CONFIGURATION);
      conversionController.sync(perTableConfig, hudiConversionSourceProvider);
      for (int i = 0; i < numCommits; i++) {
        table.insertRecords(
            1,
            IntStream.range(0, numPartitionsUpdatedPerCommit)
                .mapToObj(partitionNumber -> "partition" + partitionNumber)
                .collect(Collectors.toList()),
            false);
      }

      long start = System.currentTimeMillis();
      conversionController.sync(perTableConfig, hudiConversionSourceProvider);
      long end = System.currentTimeMillis();
      System.out.println("Incremental sync took " + (end - start) + "ms");
    }
  }
}
