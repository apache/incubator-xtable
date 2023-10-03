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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;

import io.onetable.exception.OneIOException;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.TableChange;
import io.onetable.model.exception.OneParseException;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.model.storage.OneDataFiles;
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
  public OneDataFiles getFilesForAllPartitions(HoodieInstant commit, OneTable tableDefinition) {
    return OneDataFiles.collectionBuilder()
        .files(dataFileExtractor.getOneDataFiles(commit, tableDefinition))
        .build();
  }

  @Override
  public OneSnapshot getCurrentFileState() {
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
  public TableChange getFilesDiffBetweenCommits(
      HoodieInstant startCommit, HoodieInstant endCommit, boolean includeStart) {
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
    HoodieTimeline completedTimeline = activeTimeline.filterCompletedInstants();
    HoodieTimeline visibleTimeline =
        activeTimeline
            .filterCompletedInstants()
            .findInstantsBeforeOrEquals(endCommit.getTimestamp());
    HoodieTimeline timelineForInstant;
    if (includeStart) {
      timelineForInstant =
          completedTimeline.findInstantsInClosedRange(
              startCommit.getTimestamp(), endCommit.getTimestamp());
    } else {
      timelineForInstant =
          completedTimeline.findInstantsInRange(
              startCommit.getTimestamp(), endCommit.getTimestamp());
    }
    OneTable table = getTable(endCommit);
    List<HoodieInstant> pendingInstants =
        activeTimeline
            .filterInflightsAndRequested()
            .findInstantsBefore(endCommit.getTimestamp())
            .getInstants()
            .stream()
            .collect(Collectors.toList());
    return TableChange.builder()
        .currentTableState(table)
        .pendingCommits(
            pendingInstants.stream()
                .map(hoodieInstant -> parseFromInstantTime(hoodieInstant.getTimestamp()))
                .collect(Collectors.toList()))
        .filesDiff(
            dataFileExtractor.getDiffBetweenCommits(
                startCommit, endCommit, table, includeStart, timelineForInstant, visibleTimeline))
        .build();
  }

  private HoodieTimeline getCompletedCommits() {
    return metaClient.getActiveTimeline().filterCompletedInstants();
  }

  @Override
  public List<HoodieInstant> getCommits(HoodieInstant commitInstant) {
    return getCompletedCommits().findInstantsAfter(commitInstant.getTimestamp()).getInstants();
  }

  @Override
  public HoodieInstant getCommitAtInstant(Instant instant) {
    return getCompletedCommits()
        .findInstantsBeforeOrEquals(MILLIS_INSTANT_TIME_FORMATTER.format(instant))
        .lastInstant()
        .get();
  }

  @Override
  public List<HoodieInstant> getCommitsForInstants(List<Instant> instants) {
    Map<Instant, HoodieInstant> instantHoodieInstantMap =
        getCompletedCommits().getInstants().stream()
            .collect(
                Collectors.toMap(
                    hoodieInstant -> parseFromInstantTime(hoodieInstant.getTimestamp()),
                    hoodieInstant -> hoodieInstant));
    return instants.stream()
        .map(instantHoodieInstantMap::get)
        .filter(hoodieInstant -> hoodieInstant != null)
        .collect(Collectors.toList());
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

  private static boolean isSecondGranularity(String instant) {
    return instant.length() == SECS_INSTANT_ID_LENGTH;
  }
}
