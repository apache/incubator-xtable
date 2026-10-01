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

import static org.apache.hudi.common.table.timeline.InstantComparison.LESSER_THAN_OR_EQUALS;

import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import lombok.Builder;
import lombok.NonNull;
import lombok.SneakyThrows;
import lombok.Value;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.InstantComparison;
import org.apache.hudi.common.util.Option;

import com.google.common.base.Strings;

import org.apache.xtable.collectors.CustomCollectors;
import org.apache.xtable.exception.ReadException;
import org.apache.xtable.model.CommitsBacklog;
import org.apache.xtable.model.InstantsForIncrementalSync;
import org.apache.xtable.model.InternalSnapshot;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.TableChange;
import org.apache.xtable.spi.extractor.ConversionSource;

public class HudiConversionSource implements ConversionSource<HoodieInstant> {

  private final HoodieTableMetaClient metaClient;
  private final HudiTableExtractor tableExtractor;
  private final HudiDataFileExtractor dataFileExtractor;

  public HudiConversionSource(
      HoodieTableMetaClient metaClient,
      PathBasedPartitionSpecExtractor sourcePartitionSpecExtractor) {
    this.metaClient = metaClient;
    this.tableExtractor =
        new HudiTableExtractor(new HudiSchemaExtractor(), sourcePartitionSpecExtractor);
    this.dataFileExtractor =
        new HudiDataFileExtractor(
            metaClient,
            new PathBasedPartitionValuesExtractor(
                sourcePartitionSpecExtractor.getPathToPartitionFieldFormat()),
            new HudiFileStatsExtractor(metaClient));
  }

  @Override
  public InternalTable getTable(HoodieInstant commit) {
    return tableExtractor.table(metaClient, commit);
  }

  @Override
  public InternalTable getCurrentTable() {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline completedTimeline = activeTimeline.filterCompletedInstants();
    return getTable(getLatestCompletedInstant(completedTimeline));
  }

  @Override
  public InternalSnapshot getCurrentSnapshot() {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline completedTimeline = activeTimeline.filterCompletedInstants();
    // get latest commit
    HoodieInstant latestCommit = getLatestCompletedInstant(completedTimeline);
    // On table version 9 (timeline layout V2) a commit becomes visible at its completion time, so
    // an instant with an earlier requested time may complete after the latest commit. Capture all
    // currently inflight/requested instants as pending so none are missed; on version 6 keep the
    // historical requested-time window.
    List<HoodieInstant> pendingInstants =
        usesCompletionTimeOrdering()
            ? activeTimeline.filterInflightsAndRequested().getInstants()
            : activeTimeline
                .filterInflightsAndRequested()
                .findInstantsBefore(latestCommit.requestedTime())
                .getInstants();
    InternalTable table = getTable(latestCommit);
    return InternalSnapshot.builder()
        .table(table)
        .partitionedDataFiles(dataFileExtractor.getFilesCurrentState(table))
        .pendingCommits(
            pendingInstants.stream()
                .map(
                    hoodieInstant ->
                        HudiInstantUtils.parseFromInstantTime(hoodieInstant.requestedTime()))
                .collect(CustomCollectors.toList(pendingInstants.size())))
        .sourceIdentifier(getCommitIdentifier(latestCommit))
        .build();
  }

  @Override
  public TableChange getTableChangeForCommit(HoodieInstant hoodieInstantForDiff) {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    // The set of commits visible as-of the diff commit is ordered by completion time on table
    // version 9 (timeline layout V2) and by requested time on version 6.
    HoodieTimeline visibleTimeline =
        usesCompletionTimeOrdering()
            ? activeTimeline
                .filterCompletedInstants()
                .findInstantsModifiedBeforeOrEqualsByCompletionTime(
                    hoodieInstantForDiff.getCompletionTime())
            : activeTimeline
                .filterCompletedInstants()
                .findInstantsBeforeOrEquals(hoodieInstantForDiff.requestedTime());
    InternalTable table = getTable(hoodieInstantForDiff);
    return TableChange.builder()
        .tableAsOfChange(table)
        .filesDiff(
            dataFileExtractor.getDiffForCommit(
                hoodieInstantForDiff, table, hoodieInstantForDiff, visibleTimeline))
        .sourceIdentifier(getCommitIdentifier(hoodieInstantForDiff))
        .build();
  }

  @Override
  public CommitsBacklog<HoodieInstant> getCommitsBacklog(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    Instant lastSyncInstant = instantsForIncrementalSync.getLastSyncInstant();
    List<Instant> lastPendingInstants = instantsForIncrementalSync.getPendingCommits();
    HoodieInstant lastInstantSynced = getCommitAtInstant(lastSyncInstant);
    CommitsPair commitsPair = getCompletedAndPendingCommitsAfterInstant(lastInstantSynced);
    CommitsPair lastPendingHoodieInstantsCommitsPair =
        getCompletedAndPendingCommitsForInstants(lastPendingInstants);
    List<HoodieInstant> commitsToProcessNext =
        mergeAndDedupLists(
            lastPendingHoodieInstantsCommitsPair.getCompletedCommits(),
            commitsPair.getCompletedCommits(),
            hoodieInstant -> hoodieInstant.requestedTime() + "_" + hoodieInstant.getAction(),
            instantOrdering());
    List<Instant> pendingInstantsToProcessNext =
        mergeAndDedupLists(
            lastPendingHoodieInstantsCommitsPair.getPendingCommits(),
            commitsPair.getPendingCommits(),
            Function.identity(),
            Comparator.naturalOrder());
    return CommitsBacklog.<HoodieInstant>builder()
        .commitsToProcess(commitsToProcessNext)
        .inFlightInstants(pendingInstantsToProcessNext)
        .build();
  }

  @Override
  public boolean isIncrementalSyncSafeFrom(Instant instant) {
    return doesCommitExistsAsOfInstant(instant) && !isAffectedByCleanupProcess(instant);
  }

  @Override
  public String getCommitIdentifier(HoodieInstant commit) {
    return commit.requestedTime();
  }

  private boolean doesCommitExistsAsOfInstant(Instant instant) {
    HoodieInstant hoodieInstant = getCommitAtInstant(instant);
    return hoodieInstant != null;
  }

  @SneakyThrows
  private boolean isAffectedByCleanupProcess(Instant instant) {
    // On table version 8+ the checkpoint is a completion time, but the cleaner retains commits by
    // requested time, so check the cleaner against the requested time of the synced commit.
    Instant cleanerCheckInstant =
        usesCompletionTimeOrdering()
            ? HudiInstantUtils.parseFromInstantTime(getCommitAtInstant(instant).requestedTime())
            : instant;
    Option<HoodieInstant> lastCleanInstant =
        metaClient.getActiveTimeline().getCleanerTimeline().filterCompletedInstants().lastInstant();
    if (!lastCleanInstant.isPresent()) {
      return false;
    }
    HoodieCleanMetadata cleanMetadata =
        metaClient.getActiveTimeline().readCleanMetadata(lastCleanInstant.get());
    String earliestCommitToRetain = cleanMetadata.getEarliestCommitToRetain();
    if (Strings.isNullOrEmpty(earliestCommitToRetain)) {
      return cleanInstantsOccurredSinceLastSyncedInstant(cleanerCheckInstant);
    }
    Instant earliestCommitToRetainInstant =
        HudiInstantUtils.parseFromInstantTime(earliestCommitToRetain);
    return earliestCommitToRetainInstant.isAfter(cleanerCheckInstant);
  }

  // When clean instants have empty earliestCommitToRetain, trigger full snapshot sync if any
  // clean instants occurred after the last synced instant to err on the side of caution
  private boolean cleanInstantsOccurredSinceLastSyncedInstant(Instant instant) {
    String lastSyncedCommitTime = HudiInstantUtils.convertInstantToCommit(instant);
    List<HoodieInstant> cleanInstantsAfterLastSync =
        metaClient
            .getActiveTimeline()
            .getCleanerTimeline()
            .filterCompletedInstants()
            .filter(
                cleanInstant ->
                    InstantComparison.compareTimestamps(
                        cleanInstant.requestedTime(),
                        InstantComparison.GREATER_THAN,
                        lastSyncedCommitTime))
            .getInstants();

    return !cleanInstantsAfterLastSync.isEmpty();
  }

  private CommitsPair getCompletedAndPendingCommitsForInstants(List<Instant> lastPendingInstants) {
    List<HoodieInstant> lastPendingHoodieInstants = getCommitsForInstants(lastPendingInstants);
    List<HoodieInstant> lastPendingHoodieInstantsCompleted =
        lastPendingHoodieInstants.stream()
            .filter(HoodieInstant::isCompleted)
            .collect(Collectors.toList());
    List<Instant> lastPendingHoodieInstantsStillPending =
        lastPendingHoodieInstants.stream()
            .filter(hoodieInstant -> hoodieInstant.isInflight() || hoodieInstant.isRequested())
            .map(
                hoodieInstant ->
                    HudiInstantUtils.parseFromInstantTime(hoodieInstant.requestedTime()))
            .collect(Collectors.toList());
    return CommitsPair.builder()
        .completedCommits(lastPendingHoodieInstantsCompleted)
        .pendingCommits(lastPendingHoodieInstantsStillPending)
        .build();
  }

  private HoodieTimeline getCompletedCommits() {
    return metaClient.getActiveTimeline().filterCompletedInstants();
  }

  private boolean usesCompletionTimeOrdering() {
    return HudiInstantUtils.usesCompletionTimeOrdering(metaClient);
  }

  private HoodieInstant getLatestCompletedInstant(HoodieTimeline completedTimeline) {
    return completedTimeline
        .getInstantsAsStream()
        .max(instantOrdering())
        .orElseThrow(
            () -> new ReadException("Unable to read latest commit from Hudi source table"));
  }

  /**
   * Selects the commits that completed after the last synced commit's completion time, ordered by
   * completion time. Unlike the requested-time path this also surfaces commits whose requested time
   * is older than the last synced commit but whose completion is newer (out-of-order completion).
   */
  private CommitsPair getCompletedAndPendingCommitsAfterCompletionTime(
      HoodieInstant commitInstant) {
    List<HoodieInstant> modifiedAfter =
        metaClient
            .getActiveTimeline()
            .findInstantsModifiedAfterByCompletionTime(commitInstant.getCompletionTime())
            .getInstants();
    List<HoodieInstant> completedInstants =
        modifiedAfter.stream()
            .filter(HoodieInstant::isCompleted)
            .sorted(Comparator.comparing(HoodieInstant::getCompletionTime))
            .collect(Collectors.toList());
    List<Instant> pendingInstants =
        modifiedAfter.stream()
            .filter(hoodieInstant -> hoodieInstant.isInflight() || hoodieInstant.isRequested())
            .map(
                hoodieInstant ->
                    HudiInstantUtils.parseFromInstantTime(hoodieInstant.requestedTime()))
            .collect(Collectors.toList());
    return CommitsPair.builder()
        .completedCommits(completedInstants)
        .pendingCommits(pendingInstants)
        .build();
  }

  private CommitsPair getCompletedAndPendingCommitsAfterInstant(HoodieInstant commitInstant) {
    if (usesCompletionTimeOrdering()) {
      return getCompletedAndPendingCommitsAfterCompletionTime(commitInstant);
    }
    // Table version 6 uses the old timeline view, so instants are selected and ordered by their
    // requested (instant) time.
    List<HoodieInstant> allInstants =
        metaClient
            .getActiveTimeline()
            .findInstantsAfter(commitInstant.requestedTime())
            .getInstants();
    // collect the completed instants & inflight instants from all the instants.
    List<HoodieInstant> completedInstants =
        allInstants.stream().filter(HoodieInstant::isCompleted).collect(Collectors.toList());
    // Nothing to sync as there are only pending commits.
    if (completedInstants.isEmpty()) {
      return CommitsPair.builder().completedCommits(completedInstants).build();
    }
    // remove from pending instants that are larger than the last completed instant.
    HoodieInstant lastCompletedInstant = completedInstants.get(completedInstants.size() - 1);
    List<Instant> pendingInstants =
        allInstants.stream()
            .filter(hoodieInstant -> hoodieInstant.isInflight() || hoodieInstant.isRequested())
            .filter(
                hoodieInstant ->
                    InstantComparison.compareTimestamps(
                        hoodieInstant.requestedTime(),
                        LESSER_THAN_OR_EQUALS,
                        lastCompletedInstant.requestedTime()))
            .map(
                hoodieInstant ->
                    HudiInstantUtils.parseFromInstantTime(hoodieInstant.requestedTime()))
            .collect(Collectors.toList());
    return CommitsPair.builder()
        .completedCommits(completedInstants)
        .pendingCommits(pendingInstants)
        .build();
  }

  private HoodieInstant getCommitAtInstant(Instant instant) {
    if (usesCompletionTimeOrdering()) {
      return getCommitAtCompletionTime(instant);
    }
    return getCompletedCommits()
        .findInstantsBeforeOrEquals(HudiInstantUtils.convertInstantToCommit(instant))
        .lastInstant()
        .orElse(null);
  }

  /**
   * Resolves a sync checkpoint on table version 8+, where the checkpoint is a completion time (see
   * {@link HudiInstantUtils#getSyncInstant}). A checkpoint written while the source table was on
   * version 6 holds a requested time, so an exact requested-time match is accepted next. Otherwise
   * the last commit that completed at or before the checkpoint is returned.
   */
  private HoodieInstant getCommitAtCompletionTime(Instant instant) {
    List<HoodieInstant> completedInstants =
        getCompletedCommits().getInstants().stream()
            .filter(hoodieInstant -> hoodieInstant.getCompletionTime() != null)
            .collect(Collectors.toList());
    Optional<HoodieInstant> completedAtInstant =
        completedInstants.stream()
            .filter(
                hoodieInstant ->
                    HudiInstantUtils.parseFromInstantTime(hoodieInstant.getCompletionTime())
                        .equals(instant))
            .findFirst();
    if (completedAtInstant.isPresent()) {
      return completedAtInstant.get();
    }
    // Savepoint instants reuse the requested time of the commit they pin, hence filtering.
    Optional<HoodieInstant> requestedAtInstant =
        completedInstants.stream()
            .filter(
                hoodieInstant -> !HoodieTimeline.SAVEPOINT_ACTION.equals(hoodieInstant.getAction()))
            .filter(
                hoodieInstant ->
                    HudiInstantUtils.parseFromInstantTime(hoodieInstant.requestedTime())
                        .equals(instant))
            .findFirst();
    if (requestedAtInstant.isPresent()) {
      return requestedAtInstant.get();
    }
    return completedInstants.stream()
        .filter(
            hoodieInstant ->
                !HudiInstantUtils.parseFromInstantTime(hoodieInstant.getCompletionTime())
                    .isAfter(instant))
        .max(Comparator.comparing(HoodieInstant::getCompletionTime))
        .orElse(null);
  }

  private List<HoodieInstant> getCommitsForInstants(List<Instant> instants) {
    if (instants == null || instants.isEmpty()) {
      return Collections.emptyList();
    }
    // Savepoint commits are not processed and commit time can overlap with other actions, hence
    // filtering.
    Map<Instant, HoodieInstant> instantHoodieInstantMap =
        metaClient.getActiveTimeline().getInstants().stream()
            .filter(instant -> !HoodieTimeline.SAVEPOINT_ACTION.equals(instant.getAction()))
            .collect(
                Collectors.toMap(
                    hoodieInstant ->
                        HudiInstantUtils.parseFromInstantTime(hoodieInstant.requestedTime()),
                    hoodieInstant -> hoodieInstant));
    return instants.stream()
        .map(instantHoodieInstantMap::get)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  /**
   * Merges two lists, keeps the first element for each key, and sorts the result. Commits are
   * sorted with the timeline layout's ordering, which is the requested time on table version 6 and
   * the completion time on version 9. Commits are keyed by requested time and action, because a
   * savepoint instant reuses the requested time of the commit it pins.
   */
  private <T, K> List<T> mergeAndDedupLists(
      @NonNull List<T> list1,
      @NonNull List<T> list2,
      Function<T, K> keyExtractor,
      Comparator<T> ordering) {
    Map<K, T> dedupedByKey = new LinkedHashMap<>();
    Stream.concat(list1.stream(), list2.stream())
        .forEach(element -> dedupedByKey.putIfAbsent(keyExtractor.apply(element), element));
    return dedupedByKey.values().stream().sorted(ordering).collect(Collectors.toList());
  }

  private Comparator<HoodieInstant> instantOrdering() {
    return metaClient.getTimelineLayout().getInstantComparator().orderingComparator();
  }

  @Override
  public void close() {
    dataFileExtractor.close();
  }

  @Value
  @Builder
  private static class CommitsPair {
    @Builder.Default List<HoodieInstant> completedCommits = Collections.emptyList();
    @Builder.Default List<Instant> pendingCommits = Collections.emptyList();
  }
}
