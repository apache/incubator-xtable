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
 
package org.apache.xtable.delta;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import lombok.Builder;

import org.apache.hadoop.fs.FileStatus;

import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.actions.Action;

import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import com.google.common.base.Preconditions;

/** Cache store for storing incremental table changes in the Delta table. */
public class DeltaIncrementalChangesState {
  private final Long startVersion;
  private final Long endVersion;

  private final Map<Long, List<Action>> incrementalChangesByVersion = new HashMap<>();
  private final Map<Long, Instant> commitTimestampByVersion = new HashMap<>();

  /**
   * Reloads the cache store with incremental changes. Intentionally thread safety is the
   * responsibility of the caller.
   *
   * @param deltaLog The DeltaLog instance.
   * @param versionToStartFrom The version to start from.
   */
  @Builder
  public DeltaIncrementalChangesState(DeltaLog deltaLog, Long versionToStartFrom) {
    List<Tuple2<Long, List<Action>>> changesList =
        getChangesList(deltaLog.getChanges(versionToStartFrom, false));
    Long maxSeenVersion = null;
    for (Tuple2<Long, List<Action>> change : changesList) {
      Long versionNumber = change._1();
      List<Action> actions = change._2();
      incrementalChangesByVersion.put(versionNumber, actions);
      maxSeenVersion =
          maxSeenVersion == null ? versionNumber : Math.max(maxSeenVersion, versionNumber);
    }
    // Carry each commit's file modification time alongside its actions. This is the same clock
    // Snapshot.timestamp() reads and the same one getCommitsBacklog matches the persisted
    // watermark against, so it stays consistent regardless of the writer's clock. The log has
    // already been listed once for getChanges above; this second pass reads no file contents.
    Iterator<Tuple2<Object, FileStatus>> logFiles =
        JavaConverters.asJavaIteratorConverter(
                deltaLog.getChangeLogFiles(versionToStartFrom, false))
            .asJava();
    while (logFiles.hasNext()) {
      Tuple2<Object, FileStatus> logFile = logFiles.next();
      commitTimestampByVersion.put(
          (Long) logFile._1(), Instant.ofEpochMilli(logFile._2().getModificationTime()));
    }
    startVersion = versionToStartFrom;
    endVersion = maxSeenVersion;
  }

  /**
   * Returns the versions in sorted order. The start version is the next one after the last sync
   * version to the target. The end version is the latest version in the Delta table at the time of
   * initialization.
   *
   * @return
   */
  public List<Long> getVersionsInSortedOrder() {
    List<Long> versions = new ArrayList<>(incrementalChangesByVersion.keySet());
    versions.sort(Long::compareTo);
    return versions;
  }

  public List<Action> getActionsForVersion(Long version) {
    Preconditions.checkArgument(
        incrementalChangesByVersion.containsKey(version),
        String.format("Version %s not found in the DeltaIncrementalChangesState.", version));
    return incrementalChangesByVersion.get(version);
  }

  /** Returns the commit-file modification time of the given version. */
  public Instant getCommitTimestamp(Long version) {
    Preconditions.checkArgument(
        commitTimestampByVersion.containsKey(version),
        String.format("Version %s not found in the DeltaIncrementalChangesState.", version));
    return commitTimestampByVersion.get(version);
  }

  private List<Tuple2<Long, List<Action>>> getChangesList(
      scala.collection.Iterator<Tuple2<Object, Seq<Action>>> scalaIterator) {
    List<Tuple2<Long, List<Action>>> changesList = new ArrayList<>();
    Iterator<Tuple2<Object, Seq<Action>>> javaIterator =
        JavaConverters.asJavaIteratorConverter(scalaIterator).asJava();
    while (javaIterator.hasNext()) {
      Tuple2<Object, Seq<Action>> currentChange = javaIterator.next();
      changesList.add(
          new Tuple2<>(
              (Long) currentChange._1(),
              JavaConverters.seqAsJavaListConverter(currentChange._2()).asJava()));
    }
    return changesList;
  }
}
