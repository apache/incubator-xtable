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

import static org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH;
import static org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.SECS_INSTANT_ID_LENGTH;
import static org.apache.hudi.common.table.timeline.HoodieInstantTimeGenerator.SECS_INSTANT_TIMESTAMP_FORMAT;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import lombok.Builder;
import lombok.Value;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import io.onetable.exception.OneIOException;
import io.onetable.model.CurrentCommitState;
import io.onetable.model.InstantsForIncrementalSync;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.TableChange;
import io.onetable.model.exception.OneParseException;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.spi.extractor.SourceClient;

public class HudiClient implements SourceClient<HoodieInstant> {
  private static final ZoneId ZONE_ID = ZoneId.of("UTC");
  // Unfortunately millisecond format is not parsable as is
  // https://bugs.openjdk.java.net/browse/JDK-8031085. hence have to do appendValue()
  private static final DateTimeFormatter MILLIS_INSTANT_TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          .appendPattern(SECS_INSTANT_TIMESTAMP_FORMAT)
          .appendValue(ChronoField.MILLI_OF_SECOND, 3)
          .toFormatter()
          .withZone(ZONE_ID);
  private final HoodieTableMetaClient metaClient;
  private final HudiTableExtractor tableExtractor;
  private final HudiDataFileExtractor dataFileExtractor;

  public HudiClient(
      HoodieTableMetaClient metaClient,
      HudiSourcePartitionSpecExtractor sourcePartitionSpecExtractor) {
    this.metaClient = metaClient;
    this.tableExtractor =
        new HudiTableExtractor(new HudiSchemaExtractor(), sourcePartitionSpecExtractor);
    this.dataFileExtractor =
        new HudiDataFileExtractor(
            metaClient,
            new HudiPartitionValuesExtractor(
                sourcePartitionSpecExtractor.getPathToPartitionFieldFormat()),
            new HudiFileStatsExtractor(metaClient.getHadoopConf()));
  }

  @Override
  public OneTable getTable(HoodieInstant commit) {
    return tableExtractor.table(metaClient, commit);
  }

  @Override
  public SchemaCatalog getSchemaCatalog(OneTable table, HoodieInstant commit) {
    return HudiSchemaCatalogExtractor.catalogWithTableSchema(table);
  }

  @Override
  public OneSnapshot getCurrentSnapshot() {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline completedTimeline = activeTimeline.filterCompletedInstants();
    // get latest commit
    HoodieInstant latestCommit =
        completedTimeline
            .lastInstant()
            .orElseThrow(
                () -> new OneIOException("Unable to read latest commit from Hudi source table"));
    List<HoodieInstant> pendingInstants =
        activeTimeline
            .filterInflightsAndRequested()
            .findInstantsBefore(latestCommit.getTimestamp())
            .getInstants()
            .stream()
            .collect(Collectors.toList());
    OneTable table =
        getTable(latestCommit).toBuilder()
            .latestCommitTime(parseFromInstantTime(latestCommit.getTimestamp()))
            .pendingCommits(
                pendingInstants.stream()
                    .map(hoodieInstant -> parseFromInstantTime(hoodieInstant.getTimestamp()))
                    .collect(Collectors.toList()))
            .build();
    return OneSnapshot.builder()
        .table(table)
        .schemaCatalog(getSchemaCatalog(table, latestCommit))
        .dataFiles(dataFileExtractor.getFilesCurrentState(completedTimeline, table))
        .build();
  }

  @Override
  public TableChange getTableChangeForCommit(HoodieInstant hoodieInstantForDiff) {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline visibleTimeline =
        activeTimeline
            .filterCompletedInstants()
            .findInstantsBeforeOrEquals(hoodieInstantForDiff.getTimestamp());
    HoodieTimeline timelineForInstant =
        activeTimeline
            .filterCompletedInstants()
            .findInstantsInClosedRange(
                hoodieInstantForDiff.getTimestamp(), hoodieInstantForDiff.getTimestamp());
    OneTable table = getTable(hoodieInstantForDiff);
    return TableChange.builder()
        .currentTableState(table)
        .filesDiff(
            dataFileExtractor.getDiffForCommit(
                hoodieInstantForDiff, table, timelineForInstant, visibleTimeline))
        .build();
  }

  @Override
  public CurrentCommitState<HoodieInstant> getCommitsProcessState(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    Instant lastSyncInstant = instantsForIncrementalSync.getLastSyncInstant();
    List<Instant> lastPendingInstants = instantsForIncrementalSync.getPendingCommits();
    HoodieInstant lastInstantSynced = getCommitAtInstant(lastSyncInstant);
    CommitsPair commitsPair = getCompletedAndPendingCommitsAfterInstant(lastInstantSynced);
    CommitsPair lastPendingHoodieInstantsCommitsPair =
        getCompletedAndPendingCommitsForInstants(lastPendingInstants);
    // Remove pending commits from completed commits to avoid duplicate processing.
    List<HoodieInstant> commitsToProcessNext =
        removeCommitsFromPendingCommits(
            lastPendingHoodieInstantsCommitsPair.getCompletedCommits(),
            commitsPair.getCompletedCommits());
    List<Instant> pendingInstantsToProcessNext = new ArrayList<>();
    pendingInstantsToProcessNext.addAll(lastPendingHoodieInstantsCommitsPair.getPendingCommits());
    pendingInstantsToProcessNext.addAll(commitsPair.getPendingCommits());
    // combine updatedPendingHoodieInstants and commitsAfterLastInstant and sort and return.
    commitsToProcessNext.addAll(commitsPair.getCompletedCommits());
    return CurrentCommitState.<HoodieInstant>builder()
        .commitsToProcess(commitsToProcessNext)
        .pendingInstants(pendingInstantsToProcessNext)
        .build();
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
            .map(hoodieInstant -> parseFromInstantTime(hoodieInstant.getTimestamp()))
            .collect(Collectors.toList());
    return CommitsPair.builder()
        .completedCommits(lastPendingHoodieInstantsCompleted)
        .pendingCommits(lastPendingHoodieInstantsStillPending)
        .build();
  }

  /**
   * Copied mostly from {@link
   * org.apache.hudi.common.table.timeline.HoodieActiveTimeline#parseDateFromInstantTime(String)}
   * but forces the timestamp to use UTC unlike the Hudi code.
   *
   * @param timestamp input commit timestamp
   * @return timestamp parsed as Instant
   */
  public static Instant parseFromInstantTime(String timestamp) {
    try {
      String timestampInMillis = timestamp;
      if (isSecondGranularity(timestamp)) {
        timestampInMillis = timestamp + "999";
      } else if (timestamp.length() > MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH) {
        timestampInMillis = timestamp.substring(0, MILLIS_INSTANT_TIMESTAMP_FORMAT_LENGTH);
      }

      LocalDateTime dt = LocalDateTime.parse(timestampInMillis, MILLIS_INSTANT_TIME_FORMATTER);
      return dt.atZone(ZONE_ID).toInstant();
    } catch (DateTimeParseException ex) {
      throw new OneParseException("Unable to parse date from commit timestamp: " + timestamp, ex);
    }
  }

  private HoodieTimeline getCompletedCommits() {
    return metaClient.getActiveTimeline().filterCompletedInstants();
  }

  private CommitsPair getCompletedAndPendingCommitsAfterInstant(HoodieInstant commitInstant) {
    List<HoodieInstant> allInstants =
        metaClient
            .getActiveTimeline()
            .findInstantsAfter(commitInstant.getTimestamp())
            .getInstants();
    // collect the completed instants & inflight instants from all the instants.
    List<HoodieInstant> completedInstants =
        allInstants.stream().filter(HoodieInstant::isCompleted).collect(Collectors.toList());
    // Nothing to sync as there are only pending commits.
    if (completedInstants.isEmpty()) {
      return CommitsPair.builder().completedCommits(completedInstants).build();
    }
    // remove from pending instants that are larger than the last completed instant.
    List<Instant> pendingInstants =
        allInstants.stream()
            .filter(hoodieInstant -> hoodieInstant.isInflight() || hoodieInstant.isRequested())
            .filter(
                hoodieInstant ->
                    hoodieInstant.compareTo(completedInstants.get(completedInstants.size() - 1))
                        <= 0)
            .map(hoodieInstant -> parseFromInstantTime(hoodieInstant.getTimestamp()))
            .collect(Collectors.toList());
    return CommitsPair.builder()
        .completedCommits(completedInstants)
        .pendingCommits(pendingInstants)
        .build();
  }

  private HoodieInstant getCommitAtInstant(Instant instant) {
    return getCompletedCommits()
        .findInstantsBeforeOrEquals(MILLIS_INSTANT_TIME_FORMATTER.format(instant))
        .lastInstant()
        .get();
  }

  private List<HoodieInstant> getCommitsForInstants(List<Instant> instants) {
    Map<Instant, HoodieInstant> instantHoodieInstantMap =
        metaClient.getActiveTimeline().getInstants().stream()
            .collect(
                Collectors.toMap(
                    hoodieInstant -> parseFromInstantTime(hoodieInstant.getTimestamp()),
                    hoodieInstant -> hoodieInstant));
    return instants.stream()
        .map(instantHoodieInstantMap::get)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private List<HoodieInstant> removeCommitsFromPendingCommits(
      List<HoodieInstant> pendingCommits, List<HoodieInstant> commitList) {
    // If pending commits is null or empty, or if commit list is null or empty,
    // return the same pending commits.
    if (pendingCommits == null
        || pendingCommits.isEmpty()
        || commitList == null
        || commitList.isEmpty()) {
      return pendingCommits;
    }

    // Remove until the last commit in the pending commits list that is less than or equal to
    // the first commit in the commit list.
    int lastIndexToRemove = -1;
    for (int i = pendingCommits.size() - 1; i >= 0; i--) {
      if (pendingCommits.get(i).compareTo(commitList.get(0)) > 0) {
        lastIndexToRemove = i;
      } else {
        break;
      }
    }

    // If there is no overlap between pending commits and commit list,
    // return the same pending commits.
    if (lastIndexToRemove == -1) {
      return pendingCommits;
    }
    return pendingCommits.subList(0, lastIndexToRemove);
  }

  private static boolean isSecondGranularity(String instant) {
    return instant.length() == SECS_INSTANT_ID_LENGTH;
  }

  @Value
  @Builder
  private static class CommitsPair {
    @Builder.Default List<HoodieInstant> completedCommits = Collections.emptyList();
    @Builder.Default List<Instant> pendingCommits = Collections.emptyList();
  }
}
