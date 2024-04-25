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
 
package org.apache.xtable.conversion;

import static org.apache.xtable.GenericTable.getTableName;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.IncrementalTableChanges;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.model.metadata.TableSyncMetadata;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncMode;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.spi.extractor.ConversionSource;
import org.apache.xtable.spi.sync.ConversionTarget;
import org.apache.xtable.spi.sync.TableFormatSync;

public class TestConversionController {

  private final Configuration mockConf = mock(Configuration.class);
  private final ConversionSourceProvider<Instant> mockConversionSourceProvider =
      mock(ConversionSourceProvider.class);
  private final ConversionSource<Instant> mockConversionSource = mock(ConversionSource.class);
  private final ConversionTargetFactory mockConversionTargetFactory =
      mock(ConversionTargetFactory.class);
  private final TableFormatSync tableFormatSync = mock(TableFormatSync.class);
  private final ConversionTarget mockConversionTarget1 = mock(ConversionTarget.class);
  private final ConversionTarget mockConversionTarget2 = mock(ConversionTarget.class);

  @Test
  void testAllSnapshotSyncAsPerConfig() {
    SyncMode syncMode = SyncMode.FULL;
    InternalTable internalTable = getInternalTable();
    InternalSnapshot internalSnapshot = buildSnapshot(internalTable, "v1");
    Instant instantBeforeHour = Instant.now().minus(Duration.ofHours(1));
    SyncResult syncResult = buildSyncResult(syncMode, instantBeforeHour);
    Map<String, SyncResult> perTableResults = new HashMap<>();
    perTableResults.put(TableFormat.ICEBERG, syncResult);
    perTableResults.put(TableFormat.DELTA, syncResult);
    PerTableConfig perTableConfig =
        getPerTableConfig(Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA), syncMode);
    when(mockConversionSourceProvider.getConversionSourceInstance(perTableConfig))
        .thenReturn(mockConversionSource);
    when(mockConversionTargetFactory.createForFormat(TableFormat.ICEBERG, perTableConfig, mockConf))
        .thenReturn(mockConversionTarget1);
    when(mockConversionTargetFactory.createForFormat(TableFormat.DELTA, perTableConfig, mockConf))
        .thenReturn(mockConversionTarget2);
    when(mockConversionSource.getCurrentSnapshot()).thenReturn(internalSnapshot);
    when(tableFormatSync.syncSnapshot(
            argThat(containsAll(Arrays.asList(mockConversionTarget1, mockConversionTarget2))),
            eq(internalSnapshot)))
        .thenReturn(perTableResults);
    ConversionController conversionController =
        new ConversionController(mockConf, mockConversionTargetFactory, tableFormatSync);
    Map<String, SyncResult> result =
        conversionController.sync(perTableConfig, mockConversionSourceProvider);
    assertEquals(perTableResults, result);
  }

  @Test
  void testAllIncrementalSyncAsPerConfigAndNoFallbackNecessary() {
    SyncMode syncMode = SyncMode.INCREMENTAL;
    PerTableConfig perTableConfig =
        getPerTableConfig(Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA), syncMode);
    when(mockConversionSourceProvider.getConversionSourceInstance(perTableConfig))
        .thenReturn(mockConversionSource);
    when(mockConversionTargetFactory.createForFormat(TableFormat.ICEBERG, perTableConfig, mockConf))
        .thenReturn(mockConversionTarget1);
    when(mockConversionTargetFactory.createForFormat(TableFormat.DELTA, perTableConfig, mockConf))
        .thenReturn(mockConversionTarget2);

    Instant instantAsOfNow = Instant.now();
    Instant instantAt15 = getInstantAtLastNMinutes(instantAsOfNow, 15);
    Instant instantAt14 = getInstantAtLastNMinutes(instantAsOfNow, 14);
    Instant instantAt10 = getInstantAtLastNMinutes(instantAsOfNow, 10);
    Instant instantAt8 = getInstantAtLastNMinutes(instantAsOfNow, 8);
    Instant instantAt5 = getInstantAtLastNMinutes(instantAsOfNow, 5);
    Instant instantAt2 = getInstantAtLastNMinutes(instantAsOfNow, 2);
    when(mockConversionSource.isIncrementalSyncSafeFrom(eq(instantAt15))).thenReturn(true);
    when(mockConversionSource.isIncrementalSyncSafeFrom(eq(instantAt8))).thenReturn(true);

    // Iceberg last synced at instantAt10 and has pending instants at instantAt15.
    Instant icebergLastSyncInstant = instantAt10;
    List<Instant> pendingInstantsForIceberg = Collections.singletonList(instantAt15);
    // Delta last synced at instantAt5 and has pending instants at instantAt8.
    Instant deltaLastSyncInstant = instantAt5;
    List<Instant> pendingInstantsForDelta = Collections.singletonList(instantAt8);
    List<Instant> combinedPendingInstants =
        Stream.concat(pendingInstantsForIceberg.stream(), pendingInstantsForDelta.stream())
            .collect(Collectors.toList());
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(icebergLastSyncInstant)
            .pendingCommits(combinedPendingInstants)
            .build();
    List<Instant> instantsToProcess =
        Arrays.asList(instantAt15, instantAt14, instantAt10, instantAt8, instantAt5, instantAt2);
    CommitsBacklog<Instant> commitsBacklog =
        CommitsBacklog.<Instant>builder().commitsToProcess(instantsToProcess).build();
    Optional<TableSyncMetadata> conversionTarget1Metadata =
        Optional.of(TableSyncMetadata.of(icebergLastSyncInstant, pendingInstantsForIceberg));
    when(mockConversionTarget1.getTableMetadata()).thenReturn(conversionTarget1Metadata);
    Optional<TableSyncMetadata> conversionTarget2Metadata =
        Optional.of(TableSyncMetadata.of(deltaLastSyncInstant, pendingInstantsForDelta));
    when(mockConversionTarget2.getTableMetadata()).thenReturn(conversionTarget2Metadata);
    when(mockConversionSource.getCommitsBacklog(instantsForIncrementalSync))
        .thenReturn(commitsBacklog);
    List<TableChange> tableChanges = new ArrayList<>();
    for (Instant instant : instantsToProcess) {
      TableChange tableChange = getTableChange(instant);
      tableChanges.add(tableChange);
      when(mockConversionSource.getTableChangeForCommit(instant)).thenReturn(tableChange);
    }
    // Iceberg needs to sync last pending instant at instantAt15 and instants after last sync
    // instant
    // which is instantAt10 and so i.e. instantAt8, instantAt5, instantAt2.
    List<SyncResult> icebergSyncResults =
        buildSyncResults(Arrays.asList(instantAt15, instantAt8, instantAt5, instantAt2));
    // Delta needs to sync last pending instant at instantAt8 and instants after last sync instant
    // which is instantAt5 and so i.e. instantAt2.
    List<SyncResult> deltaSyncResults = buildSyncResults(Arrays.asList(instantAt8, instantAt2));
    IncrementalTableChanges incrementalTableChanges =
        IncrementalTableChanges.builder().tableChanges(tableChanges.iterator()).build();
    Map<String, List<SyncResult>> allResults = new HashMap<>();
    allResults.put(TableFormat.ICEBERG, icebergSyncResults);
    allResults.put(TableFormat.DELTA, deltaSyncResults);
    Map<ConversionTarget, TableSyncMetadata> conversionTargetToMetadata = new HashMap<>();
    conversionTargetToMetadata.put(mockConversionTarget1, conversionTarget1Metadata.get());
    conversionTargetToMetadata.put(mockConversionTarget2, conversionTarget2Metadata.get());
    when(tableFormatSync.syncChanges(
            eq(conversionTargetToMetadata), argThat(matches(incrementalTableChanges))))
        .thenReturn(allResults);
    Map<String, SyncResult> expectedSyncResult = new HashMap<>();
    expectedSyncResult.put(TableFormat.ICEBERG, getLastSyncResult(icebergSyncResults));
    expectedSyncResult.put(TableFormat.DELTA, getLastSyncResult(deltaSyncResults));
    ConversionController conversionController =
        new ConversionController(mockConf, mockConversionTargetFactory, tableFormatSync);
    Map<String, SyncResult> result =
        conversionController.sync(perTableConfig, mockConversionSourceProvider);
    assertEquals(expectedSyncResult, result);
  }

  @Test
  void testIncrementalSyncFallBackToSnapshotForAllFormats() {
    SyncMode syncMode = SyncMode.INCREMENTAL;
    InternalTable internalTable = getInternalTable();
    Instant instantBeforeHour = Instant.now().minus(Duration.ofHours(1));
    InternalSnapshot internalSnapshot = buildSnapshot(internalTable, "v1");
    SyncResult syncResult = buildSyncResult(syncMode, instantBeforeHour);
    Map<String, SyncResult> syncResults = new HashMap<>();
    syncResults.put(TableFormat.ICEBERG, syncResult);
    syncResults.put(TableFormat.DELTA, syncResult);
    PerTableConfig perTableConfig =
        getPerTableConfig(Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA), syncMode);
    when(mockConversionSourceProvider.getConversionSourceInstance(perTableConfig))
        .thenReturn(mockConversionSource);
    when(mockConversionTargetFactory.createForFormat(TableFormat.ICEBERG, perTableConfig, mockConf))
        .thenReturn(mockConversionTarget1);
    when(mockConversionTargetFactory.createForFormat(TableFormat.DELTA, perTableConfig, mockConf))
        .thenReturn(mockConversionTarget2);

    Instant instantAsOfNow = Instant.now();
    Instant instantAt5 = getInstantAtLastNMinutes(instantAsOfNow, 5);
    when(mockConversionSource.isIncrementalSyncSafeFrom(eq(instantAt5))).thenReturn(false);

    // Both Iceberg and Delta last synced at instantAt5 and have no pending instants.
    when(mockConversionTarget1.getTableMetadata())
        .thenReturn(Optional.of(TableSyncMetadata.of(instantAt5, Collections.emptyList())));
    when(mockConversionTarget2.getTableMetadata())
        .thenReturn(Optional.of(TableSyncMetadata.of(instantAt5, Collections.emptyList())));

    when(mockConversionSource.getCurrentSnapshot()).thenReturn(internalSnapshot);
    when(tableFormatSync.syncSnapshot(
            argThat(containsAll(Arrays.asList(mockConversionTarget1, mockConversionTarget2))),
            eq(internalSnapshot)))
        .thenReturn(syncResults);
    ConversionController conversionController =
        new ConversionController(mockConf, mockConversionTargetFactory, tableFormatSync);
    Map<String, SyncResult> result =
        conversionController.sync(perTableConfig, mockConversionSourceProvider);
    assertEquals(syncResults, result);
  }

  @Test
  void testIncrementalSyncFallbackToSnapshotForOnlySingleFormat() {
    SyncMode syncMode = SyncMode.INCREMENTAL;
    PerTableConfig perTableConfig =
        getPerTableConfig(Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA), syncMode);
    when(mockConversionSourceProvider.getConversionSourceInstance(perTableConfig))
        .thenReturn(mockConversionSource);
    when(mockConversionTargetFactory.createForFormat(TableFormat.ICEBERG, perTableConfig, mockConf))
        .thenReturn(mockConversionTarget1);
    when(mockConversionTargetFactory.createForFormat(TableFormat.DELTA, perTableConfig, mockConf))
        .thenReturn(mockConversionTarget2);

    Instant instantAsOfNow = Instant.now();
    Instant instantAt15 = getInstantAtLastNMinutes(instantAsOfNow, 15);
    Instant instantAt10 = getInstantAtLastNMinutes(instantAsOfNow, 10);
    Instant instantAt8 = getInstantAtLastNMinutes(instantAsOfNow, 8);
    Instant instantAt5 = getInstantAtLastNMinutes(instantAsOfNow, 5);
    Instant instantAt2 = getInstantAtLastNMinutes(instantAsOfNow, 2);

    when(mockConversionSource.isIncrementalSyncSafeFrom(eq(instantAt8))).thenReturn(true);
    when(mockConversionSource.isIncrementalSyncSafeFrom(eq(instantAt15))).thenReturn(false);

    // Iceberg last synced at instantAt10 and has pending instants at instantAt15.
    Instant icebergLastSyncInstant = instantAt10;
    List<Instant> pendingInstantsForIceberg = Collections.singletonList(instantAt15);
    // Delta last synced at instantAt5 and has pending instants at instantAt8.
    Instant deltaLastSyncInstant = instantAt5;
    List<Instant> pendingInstantsForDelta = Collections.singletonList(instantAt8);
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder()
            .lastSyncInstant(deltaLastSyncInstant)
            .pendingCommits(pendingInstantsForDelta)
            .build();
    List<Instant> instantsToProcess = Arrays.asList(instantAt8, instantAt2);
    CommitsBacklog<Instant> commitsBacklog =
        CommitsBacklog.<Instant>builder().commitsToProcess(instantsToProcess).build();
    when(mockConversionTarget1.getTableMetadata())
        .thenReturn(
            Optional.of(TableSyncMetadata.of(icebergLastSyncInstant, pendingInstantsForIceberg)));
    Optional<TableSyncMetadata> conversionTarget2Metadata =
        Optional.of(TableSyncMetadata.of(deltaLastSyncInstant, pendingInstantsForDelta));
    when(mockConversionTarget2.getTableMetadata()).thenReturn(conversionTarget2Metadata);
    when(mockConversionSource.getCommitsBacklog(instantsForIncrementalSync))
        .thenReturn(commitsBacklog);
    List<TableChange> tableChanges = new ArrayList<>();
    for (Instant instant : instantsToProcess) {
      TableChange tableChange = getTableChange(instant);
      tableChanges.add(tableChange);
      when(mockConversionSource.getTableChangeForCommit(instant)).thenReturn(tableChange);
    }
    // Iceberg needs to sync by snapshot since instant15 is affected by table clean-up.
    InternalTable internalTable = getInternalTable();
    Instant instantBeforeHour = Instant.now().minus(Duration.ofHours(1));
    InternalSnapshot internalSnapshot = buildSnapshot(internalTable, "v1");
    SyncResult syncResult = buildSyncResult(syncMode, instantBeforeHour);
    Map<String, SyncResult> snapshotResult =
        Collections.singletonMap(TableFormat.ICEBERG, syncResult);
    when(mockConversionSource.getCurrentSnapshot()).thenReturn(internalSnapshot);
    when(tableFormatSync.syncSnapshot(
            argThat(containsAll(Collections.singletonList(mockConversionTarget1))),
            eq(internalSnapshot)))
        .thenReturn(snapshotResult);
    // Delta needs to sync last pending instant at instantAt8 and instants after last sync instant
    // which is instantAt5 and so i.e. instantAt2.
    List<SyncResult> deltaSyncResults = buildSyncResults(Arrays.asList(instantAt8, instantAt2));
    IncrementalTableChanges incrementalTableChanges =
        IncrementalTableChanges.builder().tableChanges(tableChanges.iterator()).build();
    when(tableFormatSync.syncChanges(
            eq(Collections.singletonMap(mockConversionTarget2, conversionTarget2Metadata.get())),
            argThat(matches(incrementalTableChanges))))
        .thenReturn(Collections.singletonMap(TableFormat.DELTA, deltaSyncResults));
    Map<String, SyncResult> expectedSyncResult = new HashMap<>();
    expectedSyncResult.put(TableFormat.ICEBERG, syncResult);
    expectedSyncResult.put(TableFormat.DELTA, getLastSyncResult(deltaSyncResults));
    ConversionController conversionController =
        new ConversionController(mockConf, mockConversionTargetFactory, tableFormatSync);
    Map<String, SyncResult> result =
        conversionController.sync(perTableConfig, mockConversionSourceProvider);
    assertEquals(expectedSyncResult, result);
  }

  @Test
  void incrementalSyncWithNoPendingInstantsForAllFormats() {
    SyncMode syncMode = SyncMode.INCREMENTAL;
    PerTableConfig perTableConfig =
        getPerTableConfig(Arrays.asList(TableFormat.ICEBERG, TableFormat.DELTA), syncMode);
    when(mockConversionSourceProvider.getConversionSourceInstance(perTableConfig))
        .thenReturn(mockConversionSource);
    when(mockConversionTargetFactory.createForFormat(TableFormat.ICEBERG, perTableConfig, mockConf))
        .thenReturn(mockConversionTarget1);
    when(mockConversionTargetFactory.createForFormat(TableFormat.DELTA, perTableConfig, mockConf))
        .thenReturn(mockConversionTarget2);

    Instant instantAsOfNow = Instant.now();
    Instant instantAt5 = getInstantAtLastNMinutes(instantAsOfNow, 5);

    when(mockConversionSource.isIncrementalSyncSafeFrom(eq(instantAt5))).thenReturn(true);
    when(mockConversionSource.isIncrementalSyncSafeFrom(eq(instantAt5))).thenReturn(true);

    // Iceberg last synced at instantAt5, the last instant in the source
    Instant icebergLastSyncInstant = instantAt5;
    // Delta last synced at instantAt10
    Instant deltaLastSyncInstant = instantAt5;
    InstantsForIncrementalSync instantsForIncrementalSync =
        InstantsForIncrementalSync.builder().lastSyncInstant(deltaLastSyncInstant).build();
    List<Instant> instantsToProcess = Collections.emptyList();
    CommitsBacklog<Instant> commitsBacklog =
        CommitsBacklog.<Instant>builder().commitsToProcess(instantsToProcess).build();
    Optional<TableSyncMetadata> conversionTarget1Metadata =
        Optional.of(TableSyncMetadata.of(icebergLastSyncInstant, Collections.emptyList()));
    when(mockConversionTarget1.getTableMetadata()).thenReturn(conversionTarget1Metadata);
    Optional<TableSyncMetadata> conversionTarget2Metadata =
        Optional.of(TableSyncMetadata.of(deltaLastSyncInstant, Collections.emptyList()));
    when(mockConversionTarget2.getTableMetadata()).thenReturn(conversionTarget2Metadata);
    when(mockConversionSource.getCommitsBacklog(instantsForIncrementalSync))
        .thenReturn(commitsBacklog);
    Map<ConversionTarget, TableSyncMetadata> conversionTargetWithMetadata = new HashMap<>();
    conversionTargetWithMetadata.put(mockConversionTarget1, conversionTarget1Metadata.get());
    conversionTargetWithMetadata.put(mockConversionTarget2, conversionTarget2Metadata.get());
    when(tableFormatSync.syncChanges(
            eq(conversionTargetWithMetadata),
            argThat(
                matches(
                    IncrementalTableChanges.builder()
                        .tableChanges(Collections.<TableChange>emptyList().iterator())
                        .build()))))
        .thenReturn(Collections.emptyMap());
    // Iceberg and Delta have no commits to sync
    Map<String, SyncResult> expectedSyncResult = Collections.emptyMap();
    ConversionController conversionController =
        new ConversionController(mockConf, mockConversionTargetFactory, tableFormatSync);
    Map<String, SyncResult> result =
        conversionController.sync(perTableConfig, mockConversionSourceProvider);
    assertEquals(expectedSyncResult, result);
  }

  private SyncResult getLastSyncResult(List<SyncResult> syncResults) {
    return syncResults.get(syncResults.size() - 1);
  }

  private List<SyncResult> buildSyncResults(List<Instant> instantList) {
    return instantList.stream()
        .map(instant -> buildSyncResult(SyncMode.INCREMENTAL, instant))
        .collect(Collectors.toList());
  }

  private TableChange getTableChange(Instant instant) {
    return TableChange.builder().tableAsOfChange(getInternalTable(instant)).build();
  }

  private SyncResult buildSyncResult(SyncMode syncMode, Instant lastSyncedInstant) {
    return SyncResult.builder()
        .mode(syncMode)
        .lastInstantSynced(lastSyncedInstant)
        .status(SyncResult.SyncStatus.SUCCESS)
        .build();
  }

  private InternalSnapshot buildSnapshot(InternalTable internalTable, String version) {
    return InternalSnapshot.builder().table(internalTable).version(version).build();
  }

  private InternalTable getInternalTable() {
    return getInternalTable(Instant.now());
  }

  private InternalTable getInternalTable(Instant instant) {
    return InternalTable.builder().name("some_table").latestCommitTime(instant).build();
  }

  private Instant getInstantAtLastNMinutes(Instant currentInstant, int n) {
    return Instant.now().minus(Duration.ofMinutes(n));
  }

  private PerTableConfig getPerTableConfig(List<String> targetTableFormats, SyncMode syncMode) {
    return PerTableConfigImpl.builder()
        .tableName(getTableName())
        .tableBasePath("/tmp/doesnt/matter")
        .targetTableFormats(targetTableFormats)
        .syncMode(syncMode)
        .build();
  }

  private static <T> ArgumentMatcher<Collection<T>> containsAll(Collection<T> expected) {
    return actual -> actual.size() == expected.size() && actual.containsAll(expected);
  }

  private static ArgumentMatcher<IncrementalTableChanges> matches(
      IncrementalTableChanges expected) {
    return actual ->
        actual.getPendingCommits().equals(expected.getPendingCommits())
            && iteratorsMatch(actual.getTableChanges(), expected.getTableChanges());
  }

  private static <T> boolean iteratorsMatch(Iterator<T> first, Iterator<T> second) {
    while (first.hasNext() && second.hasNext()) {
      if (!first.next().equals(second.next())) {
        return false;
      }
    }
    return !first.hasNext() && !second.hasNext();
  }
}
