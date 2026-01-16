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
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

class TestHudiConversionSource {

  @Test
  void testIsIncrementalSyncSafeFromWithNullEarliestCommitToRetainNoCleanInstants()
      throws Exception {
    // Mock the dependencies
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockActiveTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline mockCleanerTimeline = mock(HoodieTimeline.class);
    HoodieTimeline mockFilteredCleanerTimeline = mock(HoodieTimeline.class);
    HoodieTimeline mockCompletedCommitsTimeline = mock(HoodieTimeline.class);
    HoodieInstant mockCleanInstant = mock(HoodieInstant.class);
    HoodieInstant mockCommitInstant = mock(HoodieInstant.class);
    HoodieCleanMetadata mockCleanMetadata = mock(HoodieCleanMetadata.class);

    // Set up the mock chain for cleaner timeline
    when(mockMetaClient.getActiveTimeline()).thenReturn(mockActiveTimeline);

    // Mock the Hadoop configuration to prevent NPE in HudiDataFileExtractor
    Configuration hadoopConf = new Configuration();
    hadoopConf.addResource("core-default.xml");
    when(mockMetaClient.getHadoopConf()).thenReturn(hadoopConf);

    // Mock table config to prevent NPE in HudiDataFileExtractor
    org.apache.hudi.common.table.HoodieTableConfig mockTableConfig =
        mock(org.apache.hudi.common.table.HoodieTableConfig.class);
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    when(mockTableConfig.isMetadataTableAvailable()).thenReturn(false);
    when(mockActiveTimeline.getCleanerTimeline()).thenReturn(mockCleanerTimeline);
    when(mockCleanerTimeline.filterCompletedInstants()).thenReturn(mockCleanerTimeline);
    when(mockCleanerTimeline.lastInstant()).thenReturn(Option.of(mockCleanInstant));
    when(mockActiveTimeline.deserializeInstantContent(mockCleanInstant, HoodieCleanMetadata.class))
        .thenReturn(mockCleanMetadata);

    // Set up the key behavior: earliestCommitToRetain is null
    when(mockCleanMetadata.getEarliestCommitToRetain()).thenReturn(null);

    // Set up mocks for handleEmptyEarliestCommitToRetain - no clean instants after last sync
    when(mockCleanerTimeline.filter(any())).thenReturn(mockFilteredCleanerTimeline);
    when(mockFilteredCleanerTimeline.getInstants()).thenReturn(Collections.emptyList());

    // Set up the mock chain for commit timeline (for doesCommitExistsAsOfInstant)
    when(mockActiveTimeline.filterCompletedInstants()).thenReturn(mockCompletedCommitsTimeline);
    when(mockCompletedCommitsTimeline.findInstantsBeforeOrEquals(any(String.class)))
        .thenReturn(mockCompletedCommitsTimeline);
    when(mockCompletedCommitsTimeline.lastInstant()).thenReturn(Option.of(mockCommitInstant));

    // Create the HudiConversionSource with proper Configuration
    Configuration conf = new Configuration();
    conf.addResource("core-default.xml");
    conf.addResource("core-site.xml");
    conf.addResource("hdfs-default.xml");
    conf.addResource("hdfs-site.xml");
    HudiConversionSource hudiConversionSource =
        new HudiConversionSource(
            mockMetaClient, mock(HudiSourcePartitionSpecExtractor.class), conf, new Properties());

    // Test that isIncrementalSyncSafeFrom returns true when earliestCommitToRetain is null
    // and no clean instants after last sync
    Instant testInstant = Instant.now().minusSeconds(3600); // 1 hour ago
    boolean result = hudiConversionSource.isIncrementalSyncSafeFrom(testInstant);

    // This should return true because when earliestCommitToRetain is null and no clean instants
    // after last sync,
    // handleEmptyEarliestCommitToRetain returns false, making isAffectedByCleanupProcess return
    // false
    assertTrue(
        result,
        "isIncrementalSyncSafeFrom should return true when earliestCommitToRetain is null and no clean instants after last sync");
  }

  @Test
  void testIsIncrementalSyncSafeFromWithNullEarliestCommitToRetainWithCleanInstants()
      throws Exception {
    // Mock the dependencies
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockActiveTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline mockCleanerTimeline = mock(HoodieTimeline.class);
    HoodieTimeline mockFilteredCleanerTimeline = mock(HoodieTimeline.class);
    HoodieTimeline mockCompletedCommitsTimeline = mock(HoodieTimeline.class);
    HoodieInstant mockCleanInstant = mock(HoodieInstant.class);
    HoodieInstant mockCleanInstantAfterSync = mock(HoodieInstant.class);
    HoodieInstant mockCommitInstant = mock(HoodieInstant.class);
    HoodieCleanMetadata mockCleanMetadata = mock(HoodieCleanMetadata.class);

    // Set up the mock chain for cleaner timeline
    when(mockMetaClient.getActiveTimeline()).thenReturn(mockActiveTimeline);

    // Mock the Hadoop configuration to prevent NPE in HudiDataFileExtractor
    Configuration hadoopConf = new Configuration();
    hadoopConf.addResource("core-default.xml");
    when(mockMetaClient.getHadoopConf()).thenReturn(hadoopConf);

    // Mock table config to prevent NPE in HudiDataFileExtractor
    org.apache.hudi.common.table.HoodieTableConfig mockTableConfig =
        mock(org.apache.hudi.common.table.HoodieTableConfig.class);
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    when(mockTableConfig.isMetadataTableAvailable()).thenReturn(false);
    when(mockActiveTimeline.getCleanerTimeline()).thenReturn(mockCleanerTimeline);
    when(mockCleanerTimeline.filterCompletedInstants()).thenReturn(mockCleanerTimeline);
    when(mockCleanerTimeline.lastInstant()).thenReturn(Option.of(mockCleanInstant));
    when(mockActiveTimeline.deserializeInstantContent(mockCleanInstant, HoodieCleanMetadata.class))
        .thenReturn(mockCleanMetadata);

    // Set up the key behavior: earliestCommitToRetain is null
    when(mockCleanMetadata.getEarliestCommitToRetain()).thenReturn(null);

    // Set up mocks for handleEmptyEarliestCommitToRetain - clean instants exist after last sync
    when(mockCleanerTimeline.filter(any())).thenReturn(mockFilteredCleanerTimeline);
    when(mockFilteredCleanerTimeline.getInstants())
        .thenReturn(Arrays.asList(mockCleanInstantAfterSync));

    // Set up the mock chain for commit timeline (for doesCommitExistsAsOfInstant)
    when(mockActiveTimeline.filterCompletedInstants()).thenReturn(mockCompletedCommitsTimeline);
    when(mockCompletedCommitsTimeline.findInstantsBeforeOrEquals(any(String.class)))
        .thenReturn(mockCompletedCommitsTimeline);
    when(mockCompletedCommitsTimeline.lastInstant()).thenReturn(Option.of(mockCommitInstant));

    // Create the HudiConversionSource with proper Configuration
    Configuration conf = new Configuration();
    conf.addResource("core-default.xml");
    conf.addResource("core-site.xml");
    conf.addResource("hdfs-default.xml");
    conf.addResource("hdfs-site.xml");
    HudiConversionSource hudiConversionSource =
        new HudiConversionSource(
            mockMetaClient, mock(HudiSourcePartitionSpecExtractor.class), conf, new Properties());

    // Test that isIncrementalSyncSafeFrom returns false when earliestCommitToRetain is null
    // but clean instants exist after last sync
    Instant testInstant = Instant.now().minusSeconds(3600); // 1 hour ago
    boolean result = hudiConversionSource.isIncrementalSyncSafeFrom(testInstant);

    // This should return false because when earliestCommitToRetain is null and clean instants exist
    // after last sync,
    // handleEmptyEarliestCommitToRetain returns true, making isAffectedByCleanupProcess return true
    assertFalse(
        result,
        "isIncrementalSyncSafeFrom should return false when earliestCommitToRetain is null but clean instants exist after last sync");
  }

  @Test
  void testIsIncrementalSyncSafeFromWithEmptyEarliestCommitToRetainWithCleanInstants()
      throws Exception {
    // Mock the dependencies
    HoodieTableMetaClient mockMetaClient = mock(HoodieTableMetaClient.class);
    HoodieActiveTimeline mockActiveTimeline = mock(HoodieActiveTimeline.class);
    HoodieTimeline mockCleanerTimeline = mock(HoodieTimeline.class);
    HoodieTimeline mockFilteredCleanerTimeline = mock(HoodieTimeline.class);
    HoodieTimeline mockCompletedCommitsTimeline = mock(HoodieTimeline.class);
    HoodieInstant mockCleanInstant = mock(HoodieInstant.class);
    HoodieInstant mockCleanInstantAfterSync = mock(HoodieInstant.class);
    HoodieInstant mockCommitInstant = mock(HoodieInstant.class);
    HoodieCleanMetadata mockCleanMetadata = mock(HoodieCleanMetadata.class);

    // Set up the mock chain for cleaner timeline
    when(mockMetaClient.getActiveTimeline()).thenReturn(mockActiveTimeline);

    // Mock the Hadoop configuration to prevent NPE in HudiDataFileExtractor
    Configuration hadoopConf = new Configuration();
    hadoopConf.addResource("core-default.xml");
    when(mockMetaClient.getHadoopConf()).thenReturn(hadoopConf);

    // Mock table config to prevent NPE in HudiDataFileExtractor
    org.apache.hudi.common.table.HoodieTableConfig mockTableConfig =
        mock(org.apache.hudi.common.table.HoodieTableConfig.class);
    when(mockMetaClient.getTableConfig()).thenReturn(mockTableConfig);
    when(mockTableConfig.isMetadataTableAvailable()).thenReturn(false);
    when(mockActiveTimeline.getCleanerTimeline()).thenReturn(mockCleanerTimeline);
    when(mockCleanerTimeline.filterCompletedInstants()).thenReturn(mockCleanerTimeline);
    when(mockCleanerTimeline.lastInstant()).thenReturn(Option.of(mockCleanInstant));
    when(mockActiveTimeline.deserializeInstantContent(mockCleanInstant, HoodieCleanMetadata.class))
        .thenReturn(mockCleanMetadata);

    // Set up the key behavior: earliestCommitToRetain is empty string
    when(mockCleanMetadata.getEarliestCommitToRetain()).thenReturn("");

    // Set up mocks for handleEmptyEarliestCommitToRetain - clean instants exist after last sync
    when(mockCleanerTimeline.filter(any())).thenReturn(mockFilteredCleanerTimeline);
    when(mockFilteredCleanerTimeline.getInstants())
        .thenReturn(Arrays.asList(mockCleanInstantAfterSync));

    // Set up the mock chain for commit timeline (for doesCommitExistsAsOfInstant)
    when(mockActiveTimeline.filterCompletedInstants()).thenReturn(mockCompletedCommitsTimeline);
    when(mockCompletedCommitsTimeline.findInstantsBeforeOrEquals(any(String.class)))
        .thenReturn(mockCompletedCommitsTimeline);
    when(mockCompletedCommitsTimeline.lastInstant()).thenReturn(Option.of(mockCommitInstant));

    // Create the HudiConversionSource with proper Configuration
    Configuration conf = new Configuration();
    conf.addResource("core-default.xml");
    conf.addResource("core-site.xml");
    conf.addResource("hdfs-default.xml");
    conf.addResource("hdfs-site.xml");
    HudiConversionSource hudiConversionSource =
        new HudiConversionSource(
            mockMetaClient, mock(HudiSourcePartitionSpecExtractor.class), conf, new Properties());

    // Test that isIncrementalSyncSafeFrom returns false when earliestCommitToRetain is
    // empty/whitespace
    // and clean instants exist after last sync
    Instant testInstant = Instant.now().minusSeconds(3600); // 1 hour ago
    boolean result = hudiConversionSource.isIncrementalSyncSafeFrom(testInstant);

    // This should return false because when earliestCommitToRetain is empty/whitespace and clean
    // instants exist after last sync,
    // handleEmptyEarliestCommitToRetain returns true, making isAffectedByCleanupProcess return true
    assertFalse(
        result,
        "isIncrementalSyncSafeFrom should return false when earliestCommitToRetain is empty/whitespace and clean instants exist after last sync");
  }
}
