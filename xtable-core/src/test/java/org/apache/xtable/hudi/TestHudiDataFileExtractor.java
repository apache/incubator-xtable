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

import static org.apache.xtable.ValidationTestHelper.getAllFilePaths;
import static org.apache.xtable.ValidationTestHelper.validateTableChange;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import org.apache.xtable.TestJavaHudiTable;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.storage.InternalDataFile;

public class TestHudiDataFileExtractor {

  @TempDir Path tempDir;

  @Test
  void getDiffForCommitWithSkipStatsOnUpdate() {
    String tableName = "test_" + UUID.randomUUID();
    try (TestJavaHudiTable table =
        TestJavaHudiTable.forStandardSchema(
            tableName, tempDir, "level:SIMPLE", HoodieTableType.COPY_ON_WRITE)) {
      HoodieTableMetaClient metaClient =
          HoodieTableMetaClient.builder()
              .setConf(new Configuration())
              .setBasePath(table.getBasePath())
              .setLoadActiveTimelineOnLoad(true)
              .build();
      HudiSourcePartitionSpecExtractor partitionSpecExtractor =
          new ConfigurationBasedPartitionSpecExtractor(
              HudiSourceConfig.fromPartitionFieldSpecConfig("level:VALUE"));
      HudiConversionSource source =
          new HudiConversionSource(metaClient, partitionSpecExtractor, false, true);

      List<HoodieRecord<HoodieAvroPayload>> insertedRecords = table.insertRecords(50, true);
      HoodieInstant firstCommit =
          metaClient.reloadActiveTimeline().filterCompletedInstants().lastInstant().get();
      InternalSnapshot snapshotAfterFirstCommit = source.getCurrentSnapshot();
      List<String> filesAfterFirstCommit = getAllFilePaths(snapshotAfterFirstCommit);

      table.upsertRecords(insertedRecords, true);
      HoodieTimeline completedTimeline =
          metaClient.reloadActiveTimeline().filterCompletedInstants();
      HoodieInstant secondCommit = completedTimeline.lastInstant().get();
      InternalSnapshot snapshotAfterSecondCommit = source.getCurrentSnapshot();
      List<String> filesAfterSecondCommit = getAllFilePaths(snapshotAfterSecondCommit);

      TableChange tableChange = source.getTableChangeForCommit(secondCommit);
      validateTableChange(filesAfterFirstCommit, filesAfterSecondCommit, tableChange);
      assertTrue(secondCommit.getTimestamp().compareTo(firstCommit.getTimestamp()) > 0);
      assertTrue(
          tableChange.getFilesDiff().getFilesAdded().stream()
              .map(file -> ((InternalDataFile) file).getColumnStats())
              .allMatch(List::isEmpty));
    }
  }
}
