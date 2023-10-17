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
import java.util.List;

import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import io.onetable.TestHudiTable;
import io.onetable.model.OneSnapshot;

public class TestHudiInstantUtils {

  private static final boolean USE_SPARK_CLIENT_FOR_TABLE = false;

  @TempDir public static Path tempDir;
  private HudiClient hudiClient;

  @BeforeEach
  @SneakyThrows
  public void setup() {}

  /**
   * All tests to write. Combinations to test (MOR, COW), (Full Sync, Incremental Sync), (No
   * Partition & Partition). 1. Insert and upserts 2. Add Partition. 3. Delete Partition. 4. Add
   * Columns. 5. Table services (clean, cluster, savepoint & restore, rollback, compaction).
   */
  @Test
  @SneakyThrows
  public void insertAndUpsertData() {
    String tableName = "some_table_name";
    HudiTestUtil.PartitionConfig partitionConfig =
        HudiTestUtil.PartitionConfig.of("level:SIMPLE", "level:VALUE");
    HoodieTableType tableType = HoodieTableType.MERGE_ON_READ;
    try (TestHudiTable table =
        TestHudiTable.forStandardSchema(
            tableName,
            tempDir,
            null,
            partitionConfig.getHudiConfig(),
            tableType,
            USE_SPARK_CLIENT_FOR_TABLE)) {
      HoodieTableMetaClient hoodieTableMetaClient =
          HoodieTableMetaClient.withPropertyBuilder()
              .setTableType(HoodieTableType.MERGE_ON_READ)
              .setTableName(tableName + "_v1")
              .setPayloadClass(HoodieAvroPayload.class)
              .setPartitionFields("level")
              .initTable(new Configuration(), table.getBasePath());
      HudiSourcePartitionSpecExtractor partitionSpecExtractor =
          new ConfigurationBasedPartitionSpecExtractor(
              HudiSourceConfig.builder().partitionFieldSpecConfig("level:VALUE").build());
      hudiClient = new HudiClient(hoodieTableMetaClient, partitionSpecExtractor);

      String commitInstant1 = table.startCommit();
      List<HoodieRecord<HoodieAvroPayload>> insertsForCommit1 = table.generateRecords(100);
      table.insertRecordsWithCommitAlreadyStarted(insertsForCommit1, commitInstant1, true);
      // TODO(vamshigv): many small files created, fix it.
      // TODO(vamshigv): path not set correctly fix it.
      OneSnapshot oneSnapshot = hudiClient.getCurrentSnapshot();
      String commitInstant2 = table.startCommit();
      table.upsertRecordsWithCommitAlreadyStarted(
          insertsForCommit1.subList(0, 20), commitInstant2, true);
      OneSnapshot oneSnapshot1 = hudiClient.getCurrentSnapshot();
    }
  }

  @Test
  public void testParseCommitTimeToInstant() {
    assertEquals(
        Instant.parse("2023-01-20T04:43:31.843Z"),
        HudiInstantUtils.parseFromInstantTime("20230120044331843"));
    assertEquals(
        Instant.parse("2023-01-20T04:43:31.999Z"),
        HudiInstantUtils.parseFromInstantTime("20230120044331"));
  }

  @Test
  public void testInstantToCommit() {
    assertEquals(
        "20230120044331843",
        HudiInstantUtils.convertInstantToCommit(Instant.parse("2023-01-20T04:43:31.843Z")));
    assertEquals(
        "20230120044331000",
        HudiInstantUtils.convertInstantToCommit(Instant.parse("2023-01-20T04:43:31Z")));
  }
}
