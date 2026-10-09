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
 
package org.apache.xtable.timeline;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.avro.model.HoodieRestorePlan;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.table.timeline.versioning.v2.ActiveTimelineV2;
import org.apache.hudi.common.table.timeline.versioning.v2.InstantComparatorV2;
import org.apache.hudi.common.util.Option;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;

import org.apache.xtable.iceberg.IcebergTableManager;

/**
 * The Hudi active timeline reconstructed from the Iceberg table: an instant counts as completed
 * only when a snapshot in the current snapshot's ancestry records it, so a rolled-back snapshot
 * that is still retained in table metadata cannot resurface its instant as completed.
 */
public class IcebergActiveTimeline extends ActiveTimelineV2 {
  public IcebergActiveTimeline(
      HoodieTableMetaClient metaClient,
      Set<String> includedExtensions,
      boolean applyLayoutFilters) {
    this.setInstants(getInstantsFromFileSystem(metaClient, includedExtensions, applyLayoutFilters));
    this.metaClient = metaClient;
  }

  public IcebergActiveTimeline(HoodieTableMetaClient metaClient) {
    this(metaClient, Collections.unmodifiableSet(VALID_EXTENSIONS_IN_ACTIVE_TIMELINE), true);
  }

  public IcebergActiveTimeline(HoodieTableMetaClient metaClient, boolean applyLayoutFilters) {
    this(
        metaClient,
        Collections.unmodifiableSet(VALID_EXTENSIONS_IN_ACTIVE_TIMELINE),
        applyLayoutFilters);
  }

  public IcebergActiveTimeline() {}

  @Override
  public HoodieActiveTimeline reload() {
    return new IcebergActiveTimeline(metaClient);
  }

  /**
   * Refuses a restore before its plan is written, so a refused restore leaves no pending instant
   * that would block the next write.
   */
  @Override
  public void saveToRestoreRequested(HoodieInstant instant, HoodieRestorePlan metadata) {
    throw restoreNotSupported(metaClient.getTableConfig());
  }

  /** A restore left pending by a native writer is refused before it schedules any rollback. */
  @Override
  public HoodieInstant transitionRestoreRequestedToInflight(HoodieInstant requestedInstant) {
    throw restoreNotSupported(metaClient.getTableConfig());
  }

  /**
   * Restore is not represented in Iceberg yet. Hudi runs the rollbacks of a restore with timeline
   * publishing skipped, so no snapshot would record the files a restore deletes, and the restore
   * instant itself would be reported as pending forever.
   *
   * <p>A restore that is already pending when this fires was scheduled by a writer that loaded the
   * table without this format on its classpath: Hudi then falls back to the native format without a
   * warning. The message says so, since that writer's commits never reach Iceberg either, and the
   * pending restore and rollback instants have to be removed by hand.
   */
  public static UnsupportedOperationException restoreNotSupported(HoodieTableConfig tableConfig) {
    return new UnsupportedOperationException(
        String.format(
            "The Iceberg table format does not support restore yet, so table %s cannot be"
                + " restored. A restore that is already pending was scheduled by a writer that"
                + " loaded the table without the Iceberg table format on its classpath, in which"
                + " case Hudi silently used the native format; remove the pending restore and"
                + " rollback instants from the timeline by hand before writing again",
            tableConfig.getTableName()));
  }

  /**
   * Requested time alone does not identify an instant: savepointing a commit produces a savepoint
   * instant at that commit's own requested time, so the action has to be part of the key or the two
   * collide and one is dropped from the reconstructed timeline.
   */
  static String instantKey(HoodieInstant instant) {
    return instant.requestedTime() + "." + instant.getAction();
  }

  protected List<HoodieInstant> getInstantsFromFileSystem(
      HoodieTableMetaClient metaClient,
      Set<String> includedExtensions,
      boolean applyLayoutFilters) {
    List<HoodieInstant> instantsFromHoodieTimeline =
        super.getInstantsFromFileSystem(metaClient, includedExtensions, applyLayoutFilters);
    IcebergTableManager icebergTableManager =
        IcebergTableManager.of((Configuration) metaClient.getStorageConf().unwrap());
    TableIdentifier tableIdentifier =
        TableIdentifier.of(metaClient.getTableConfig().getTableName());
    if (!icebergTableManager.tableExists(
        null, tableIdentifier, metaClient.getBasePath().toString())) {
      // Before the first commit publishes a snapshot there is no Iceberg table and every Hudi
      // instant is pending. Completed instants without an Iceberg table mean the Iceberg metadata
      // is gone, and reading the table as empty would let a writer rebuild it from nothing.
      List<HoodieInstant> completedInstants =
          instantsFromHoodieTimeline.stream()
              .filter(HoodieInstant::isCompleted)
              .collect(Collectors.toList());
      if (!completedInstants.isEmpty()) {
        throw new IllegalStateException(
            String.format(
                "Table %s has %d completed instants, the latest %s, but no Iceberg table at %s:"
                    + " the Iceberg metadata has been removed and has to be restored before the"
                    + " table can be used",
                metaClient.getTableConfig().getTableName(),
                completedInstants.size(),
                completedInstants.get(completedInstants.size() - 1),
                metaClient.getBasePath()));
      }
      return instantsFromHoodieTimeline;
    }
    Table icebergTable =
        icebergTableManager.getTable(null, tableIdentifier, metaClient.getBasePath().toString());
    List<Snapshot> ancestors = IcebergSnapshotInstants.ancestorsOldestFirst(icebergTable);
    Set<String> recordedInstantKeys = new HashSet<>();
    for (Snapshot snapshot : ancestors) {
      recordedInstantKeys.add(
          instantKey(
              IcebergSnapshotInstants.recordedInstant(snapshot, metaClient.getInstantGenerator())));
    }
    // Snapshots older than the oldest retained one have been expired, by the archiver or by an
    // Iceberg expire-snapshots run, so a completed instant older than the oldest recorded instant
    // is history rather than pending work.
    Option<HoodieInstant> oldestRecordedInstant =
        ancestors.isEmpty()
            ? Option.empty()
            : Option.of(
                IcebergSnapshotInstants.recordedInstant(
                    ancestors.get(0), metaClient.getInstantGenerator()));
    return instantsFromHoodieTimeline.stream()
        .map(
            instant -> {
              if (!instant.isCompleted()
                  || recordedInstantKeys.contains(instantKey(instant))
                  || completedBeforeRetainedHistory(instant, oldestRecordedInstant)) {
                return instant;
              }
              // Completed in Hudi but not recorded by Iceberg: the write did not finish, so the
              // next writer rolls it back.
              return new HoodieInstant(
                  HoodieInstant.State.INFLIGHT,
                  instant.getAction(),
                  instant.requestedTime(),
                  instant.getCompletionTime(),
                  InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR);
            })
        .sorted(InstantComparatorV2.REQUESTED_TIME_BASED_COMPARATOR)
        .collect(Collectors.toList());
  }

  /**
   * Whether a completed instant without a snapshot predates the retained Iceberg history on both
   * clocks. Requested time alone is not enough, and neither is completion time: under concurrent
   * writers a commit can complete before the oldest retained instant yet be requested after it,
   * which marks it as one whose hook never ran rather than one whose snapshot was expired, and it
   * has to stay pending so the next writer rolls it back. A savepoint shares the requested time of
   * the commit it keeps, so equal requested times count as older.
   */
  static boolean completedBeforeRetainedHistory(
      HoodieInstant instant, Option<HoodieInstant> oldestRecordedInstant) {
    if (!oldestRecordedInstant.isPresent()
        || instant.getCompletionTime() == null
        || oldestRecordedInstant.get().getCompletionTime() == null) {
      return false;
    }
    HoodieInstant oldest = oldestRecordedInstant.get();
    return InstantComparison.compareTimestamps(
            instant.requestedTime(),
            InstantComparison.LESSER_THAN_OR_EQUALS,
            oldest.requestedTime())
        && InstantComparison.compareTimestamps(
            instant.getCompletionTime(), InstantComparison.LESSER_THAN, oldest.getCompletionTime());
  }
}
