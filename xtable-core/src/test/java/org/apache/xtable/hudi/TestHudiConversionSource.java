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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Test;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.Option;

class TestHudiConversionSource {

  private static byte[] serializeCleanMetadata(String earliestCommitToRetain) throws Exception {
    HoodieCleanMetadata metadata =
        HoodieCleanMetadata.newBuilder()
            .setStartCleanTime("000")
            .setTimeTakenInMillis(0L)
            .setTotalFilesDeleted(0)
            .setEarliestCommitToRetain(earliestCommitToRetain)
            .setBootstrapPartitionMetadata(new HashMap<>())
            .setPartitionMetadata(new HashMap<>())
            .build();
    return TimelineMetadataUtils.serializeCleanMetadata(metadata).get();
  }

  @Test
  void testIsIncrementalSyncSafeFromWithNullEarliestCommitToRetainNoCleanInstants()
      throws Exception {
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockActiveTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline mockCleanerTimeline = mock(HoodieTimeline.class);
    HoodieTimeline mockFilteredCleanerTimeline = mock(HoodieTimeline.class);
    HoodieTimeline mockCompletedCommitsTimeline = mock(HoodieTimeline.class);
    HoodieInstant mockCleanInstant = mock(HoodieInstant.class);
    HoodieInstant mockCommitInstant = mock(HoodieInstant.class);

    HoodieTableConfig mockTableConfig = mock(HoodieTableConfig.class);
    when(mockMetaClient.getActiveTimeline()).thenReturn(mockActiveTimeline);
    when(mockMetaClient.getHadoopConf()).thenReturn(new Configuration());
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    when(mockTableConfig.isMetadataTableAvailable()).thenReturn(false);
    when(mockMetaClient.getBasePathV2()).thenReturn(new Path("/tmp/test-table"));
    when(mockActiveTimeline.getCleanerTimeline()).thenReturn(mockCleanerTimeline);
    when(mockCleanerTimeline.filterCompletedInstants()).thenReturn(mockCleanerTimeline);
    when(mockCleanerTimeline.lastInstant()).thenReturn(Option.of(mockCleanInstant));
    // Use empty string — Strings.isNullOrEmpty("") is true, same behavior as null
    when(mockActiveTimeline.getInstantDetails(mockCleanInstant))
        .thenReturn(Option.of(serializeCleanMetadata("")));

    when(mockCleanerTimeline.filter(any())).thenReturn(mockFilteredCleanerTimeline);
    when(mockFilteredCleanerTimeline.getInstants()).thenReturn(Collections.emptyList());

    when(mockActiveTimeline.filterCompletedInstants()).thenReturn(mockCompletedCommitsTimeline);
    when(mockCompletedCommitsTimeline.findInstantsBeforeOrEquals(any(String.class)))
        .thenReturn(mockCompletedCommitsTimeline);
    when(mockCommitInstant.getTimestamp()).thenReturn("20200101120000000");
    when(mockCompletedCommitsTimeline.lastInstant()).thenReturn(Option.of(mockCommitInstant));

    HudiConversionSource hudiConversionSource =
        new HudiConversionSource(mockMetaClient, mock(PathBasedPartitionSpecExtractor.class));

    Instant testInstant = Instant.now().minusSeconds(3600);
    assertTrue(
        hudiConversionSource.isIncrementalSyncSafeFrom(testInstant),
        "isIncrementalSyncSafeFrom should return true when earliestCommitToRetain is null and no clean instants after last sync");
  }

  @Test
  void testIsIncrementalSyncSafeFromWithNullEarliestCommitToRetainWithCleanInstants()
      throws Exception {
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockActiveTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline mockCleanerTimeline = mock(HoodieTimeline.class);
    HoodieTimeline mockFilteredCleanerTimeline = mock(HoodieTimeline.class);
    HoodieTimeline mockCompletedCommitsTimeline = mock(HoodieTimeline.class);
    HoodieInstant mockCleanInstant = mock(HoodieInstant.class);
    HoodieInstant mockCleanInstantAfterSync = mock(HoodieInstant.class);
    HoodieInstant mockCommitInstant = mock(HoodieInstant.class);

    HoodieTableConfig mockTableConfig = mock(HoodieTableConfig.class);
    when(mockMetaClient.getActiveTimeline()).thenReturn(mockActiveTimeline);
    when(mockMetaClient.getHadoopConf()).thenReturn(new Configuration());
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    when(mockTableConfig.isMetadataTableAvailable()).thenReturn(false);
    when(mockMetaClient.getBasePathV2()).thenReturn(new Path("/tmp/test-table"));
    when(mockActiveTimeline.getCleanerTimeline()).thenReturn(mockCleanerTimeline);
    when(mockCleanerTimeline.filterCompletedInstants()).thenReturn(mockCleanerTimeline);
    when(mockCleanerTimeline.lastInstant()).thenReturn(Option.of(mockCleanInstant));
    // Use empty string — Strings.isNullOrEmpty("") is true, same behavior as null
    when(mockActiveTimeline.getInstantDetails(mockCleanInstant))
        .thenReturn(Option.of(serializeCleanMetadata("")));

    when(mockCleanerTimeline.filter(any())).thenReturn(mockFilteredCleanerTimeline);
    when(mockFilteredCleanerTimeline.getInstants())
        .thenReturn(Arrays.asList(mockCleanInstantAfterSync));

    when(mockActiveTimeline.filterCompletedInstants()).thenReturn(mockCompletedCommitsTimeline);
    when(mockCompletedCommitsTimeline.findInstantsBeforeOrEquals(any(String.class)))
        .thenReturn(mockCompletedCommitsTimeline);
    when(mockCommitInstant.getTimestamp()).thenReturn("20200101120000000");
    when(mockCompletedCommitsTimeline.lastInstant()).thenReturn(Option.of(mockCommitInstant));

    HudiConversionSource hudiConversionSource =
        new HudiConversionSource(mockMetaClient, mock(PathBasedPartitionSpecExtractor.class));

    Instant testInstant = Instant.now().minusSeconds(3600);
    assertFalse(
        hudiConversionSource.isIncrementalSyncSafeFrom(testInstant),
        "isIncrementalSyncSafeFrom should return false when earliestCommitToRetain is null but clean instants exist after last sync");
  }

  @Test
  void testIsIncrementalSyncSafeFromWithEmptyEarliestCommitToRetainWithCleanInstants()
      throws Exception {
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockActiveTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline mockCleanerTimeline = mock(HoodieTimeline.class);
    HoodieTimeline mockFilteredCleanerTimeline = mock(HoodieTimeline.class);
    HoodieTimeline mockCompletedCommitsTimeline = mock(HoodieTimeline.class);
    HoodieInstant mockCleanInstant = mock(HoodieInstant.class);
    HoodieInstant mockCleanInstantAfterSync = mock(HoodieInstant.class);
    HoodieInstant mockCommitInstant = mock(HoodieInstant.class);

    HoodieTableConfig mockTableConfig = mock(HoodieTableConfig.class);
    when(mockMetaClient.getActiveTimeline()).thenReturn(mockActiveTimeline);
    when(mockMetaClient.getHadoopConf()).thenReturn(new Configuration());
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    when(mockTableConfig.isMetadataTableAvailable()).thenReturn(false);
    when(mockMetaClient.getBasePathV2()).thenReturn(new Path("/tmp/test-table"));
    when(mockActiveTimeline.getCleanerTimeline()).thenReturn(mockCleanerTimeline);
    when(mockCleanerTimeline.filterCompletedInstants()).thenReturn(mockCleanerTimeline);
    when(mockCleanerTimeline.lastInstant()).thenReturn(Option.of(mockCleanInstant));
    when(mockActiveTimeline.getInstantDetails(mockCleanInstant))
        .thenReturn(Option.of(serializeCleanMetadata("")));

    when(mockCleanerTimeline.filter(any())).thenReturn(mockFilteredCleanerTimeline);
    when(mockFilteredCleanerTimeline.getInstants())
        .thenReturn(Arrays.asList(mockCleanInstantAfterSync));

    when(mockActiveTimeline.filterCompletedInstants()).thenReturn(mockCompletedCommitsTimeline);
    when(mockCompletedCommitsTimeline.findInstantsBeforeOrEquals(any(String.class)))
        .thenReturn(mockCompletedCommitsTimeline);
    when(mockCommitInstant.getTimestamp()).thenReturn("20200101120000000");
    when(mockCompletedCommitsTimeline.lastInstant()).thenReturn(Option.of(mockCommitInstant));

    HudiConversionSource hudiConversionSource =
        new HudiConversionSource(mockMetaClient, mock(PathBasedPartitionSpecExtractor.class));

    Instant testInstant = Instant.now().minusSeconds(3600);
    assertFalse(
        hudiConversionSource.isIncrementalSyncSafeFrom(testInstant),
        "isIncrementalSyncSafeFrom should return false when earliestCommitToRetain is empty and clean instants exist after last sync");
  }
}
