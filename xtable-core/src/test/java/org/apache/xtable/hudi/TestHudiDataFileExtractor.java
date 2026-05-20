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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import org.apache.xtable.model.storage.InternalDataFile;

class TestHudiDataFileExtractor {

  @Test
  void recoverRemovedFile_returnsFileWhenPreviousCommitInTimeline() throws Exception {
    HudiDataFileExtractor extractor = buildExtractorWithBasePath("file:///tmp/test-table");
    String partition = "year=2023";
    String fileId = "fg-1";
    String prevCommitTime = "20230101000000000";
    String oldPath = partition + "/" + fileId + "_0-0-0_" + prevCommitTime + ".parquet";

    HoodieInstant prevInstant =
        new HoodieInstant(
            HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, prevCommitTime);
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    when(timeline.getInstants()).thenReturn(Collections.singletonList(prevInstant));
    when(timeline.getInstantDetails(prevInstant))
        .thenReturn(
            Option.of(singleStatCommit(partition, fileId, oldPath).toJsonString().getBytes()));

    Optional<InternalDataFile> result =
        extractor.recoverRemovedFile(
            timeline, partition, fileId, prevCommitTime, Collections.emptyList());

    assertTrue(result.isPresent());
    assertEquals(oldPath, result.get().getPhysicalPath());
  }

  @Test
  void recoverRemovedFile_returnsEmptyWhenPreviousCommitArchived() {
    HudiDataFileExtractor extractor = buildExtractorWithBasePath("file:///tmp/test-table");

    HoodieTimeline timeline = mock(HoodieTimeline.class);
    when(timeline.getInstants()).thenReturn(Collections.emptyList());

    Optional<InternalDataFile> result =
        extractor.recoverRemovedFile(
            timeline, "year=2023", "fg-1", "20230101000000000", Collections.emptyList());

    assertFalse(result.isPresent());
  }

  @Test
  void recoverRemovedFile_returnsEmptyWhenFileIdNotInPrevCommit() throws Exception {
    HudiDataFileExtractor extractor = buildExtractorWithBasePath("file:///tmp/test-table");
    String partition = "year=2023";
    String prevCommitTime = "20230101000000000";

    HoodieInstant prevInstant =
        new HoodieInstant(
            HoodieInstant.State.COMPLETED, HoodieTimeline.COMMIT_ACTION, prevCommitTime);
    HoodieTimeline timeline = mock(HoodieTimeline.class);
    when(timeline.getInstants()).thenReturn(Collections.singletonList(prevInstant));
    when(timeline.getInstantDetails(prevInstant))
        .thenReturn(
            Option.of(
                singleStatCommit(partition, "different-fg", partition + "/different-fg.parquet")
                    .toJsonString()
                    .getBytes()));

    Optional<InternalDataFile> result =
        extractor.recoverRemovedFile(
            timeline, partition, "fg-1", prevCommitTime, Collections.emptyList());

    assertFalse(result.isPresent());
  }

  private static HoodieCommitMetadata singleStatCommit(
      String partition, String fileId, String path) {
    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setFileId(fileId);
    stat.setPath(path);
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();
    metadata.addWriteStat(partition, stat);
    return metadata;
  }

  private static HudiDataFileExtractor buildExtractorWithBasePath(String basePathStr) {
    HoodieTableMetaClient metaClient = mock(HoodieTableMetaClient.class);
    HoodieTableConfig tableConfig = mock(HoodieTableConfig.class);
    when(metaClient.getHadoopConf()).thenReturn(new Configuration());
    when(metaClient.getTableConfig()).thenReturn(tableConfig);
    when(tableConfig.isMetadataTableAvailable()).thenReturn(false);
    when(metaClient.getBasePathV2()).thenReturn(new Path(basePathStr));
    return new HudiDataFileExtractor(metaClient, null, null);
  }
}
