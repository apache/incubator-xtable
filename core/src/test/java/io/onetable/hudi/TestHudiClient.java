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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.time.Instant;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class TestHudiClient {

  @TempDir public static Path tempDir;

  /*
  @Test
  public void insertAndUpsertData() {
    String tableName = getTableName();
    try (TestHudiTable table =
        TestHudiTable.forStandardSchema(
            tableName, tempDir, null, partitionConfig.getHudiConfig(), tableType)) {
      List<HoodieRecord<HoodieAvroPayload>> insertedRecords = table.insertRecords(100, true);

      PerTableConfig perTableConfig =
          PerTableConfig.builder()
              .tableName(tableName)
              .targetTableFormats(targetTableFormats)
              .tableBasePath(table.getBasePath())
              .hudiSourceConfig(
                  HudiSourceConfig.builder()
                      .partitionFieldSpecConfig(partitionConfig.getOneTableConfig())
                      .build())
              .syncMode(syncMode)
              .build();
      OneTableClient oneTableClient = new OneTableClient(jsc.hadoopConfiguration());
      oneTableClient.sync(perTableConfig, hudiSourceClientProvider);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 100);

      table.insertRecords(100, true);
      table.upsertRecords(insertedRecords.subList(0, 20), true);

      syncWithCompactionIfRequired(tableType, table, perTableConfig, oneTableClient);
      checkDatasetEquivalence(TableFormat.HUDI, targetTableFormats, table.getBasePath(), 200);
  }
   */

  @Test
  public void testParseCommitTimeToInstant() {
    assertEquals(
        Instant.parse("2023-01-20T04:43:31.843Z"),
        HudiClient.parseFromInstantTime("20230120044331843"));
    assertEquals(
        Instant.parse("2023-01-20T04:43:31.999Z"),
        HudiClient.parseFromInstantTime("20230120044331"));
  }
}
