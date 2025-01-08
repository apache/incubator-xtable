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

package org.apache.xtable.iceberg;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import lombok.extern.log4j.Log4j2;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.SnapshotSummary;
import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Log4j2
public class IcebergMetadataFileCleaner extends IcebergMetadataCleanupStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(IcebergMetadataFileCleaner.class);

  IcebergMetadataFileCleaner(
      FileIO fileIO,
      ExecutorService deleteExecutorService,
      ExecutorService planExecutorService,
      Consumer<String> deleteFunc) {
    super(fileIO, deleteExecutorService, planExecutorService, deleteFunc);
  }

  @Override
  @SuppressWarnings({"checkstyle:CyclomaticComplexity", "MethodLength"})
  public void cleanFiles(Table table, List<Snapshot> removedSnapshots) {
    if (table.refs().size() > 1) {
      throw new UnsupportedOperationException(
          "Cannot incrementally clean files for tables with more than 1 ref");
    }

    table.refresh();
    // clean up the expired snapshots:
    // 1. Get a list of the snapshots that were removed
    // 2. Delete any data files that were deleted by those snapshots and are not in the table
    // 3. Delete any manifests that are no longer used by current snapshots
    // 4. Delete the manifest lists

    Set<Long> validIds = Sets.newHashSet();
    for (Snapshot snapshot : table.snapshots()) {
      validIds.add(snapshot.snapshotId());
    }

    Set<Long> expiredIds = Sets.newHashSet();
    for (Snapshot snapshot : removedSnapshots) {
      long snapshotId = snapshot.snapshotId();
      expiredIds.add(snapshotId);
    }

    if (expiredIds.isEmpty()) {
      // if no snapshots were expired, skip cleanup
      return;
    }

    SnapshotRef branchToCleanup = Iterables.getFirst(table.refs().values(), null);
    if (branchToCleanup == null) {
      return;
    }

    Snapshot latest = table.snapshot(branchToCleanup.snapshotId());
    Iterable<Snapshot> snapshots = table.snapshots();

    // this is the set of ancestors of the current table state. when removing snapshots, this must
    // only remove files that were deleted in an ancestor of the current table state to avoid
    // physically deleting files that were logically deleted in a commit that was rolled back.
    Set<Long> ancestorIds =
        Sets.newHashSet(SnapshotUtil.ancestorIds(latest, snapshotLookup(removedSnapshots, table)));

    Set<Long> pickedAncestorSnapshotIds = Sets.newHashSet();
    for (long snapshotId : ancestorIds) {
      String sourceSnapshotId =
          table
              .snapshot(snapshotId)
              .summary()
              .get(SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP);
      if (sourceSnapshotId != null) {
        // protect any snapshot that was cherry-picked into the current table state
        pickedAncestorSnapshotIds.add(Long.parseLong(sourceSnapshotId));
      }
    }

    // find manifests to clean up that are still referenced by a valid snapshot, but written by an
    // expired snapshot
    Set<String> validManifests = Sets.newHashSet();

    // Reads and deletes are done using Tasks.foreach(...).suppressFailureWhenFinished to complete
    // as much of the delete work as possible and avoid orphaned data or manifest files.
    Tasks.foreach(snapshots)
        .retry(3)
        .suppressFailureWhenFinished()
        .onFailure(
            (snapshot, exc) ->
                LOG.warn(
                    "Failed on snapshot {} while reading manifest list: {}",
                    snapshot.snapshotId(),
                    snapshot.manifestListLocation(),
                    exc))
        .run(
            snapshot -> {
              try (CloseableIterable<ManifestFile> manifests = readManifests(snapshot)) {
                for (ManifestFile manifest : manifests) {
                  validManifests.add(manifest.path());
                }

              } catch (IOException e) {
                throw new RuntimeException(
                    "Failed to close manifest list: " + snapshot.manifestListLocation(), e);
              }
            });

    // find manifests to clean up that were only referenced by snapshots that have expired
    Set<String> manifestListsToDelete = Sets.newHashSet();
    Set<String> manifestsToDelete = Sets.newHashSet();
    Tasks.foreach(removedSnapshots)
        .retry(3)
        .suppressFailureWhenFinished()
        .onFailure(
            (snapshot, exc) ->
                LOG.warn(
                    "Failed on snapshot {} while reading manifest list: {}",
                    snapshot.snapshotId(),
                    snapshot.manifestListLocation(),
                    exc))
        .run(
            snapshot -> {
              long snapshotId = snapshot.snapshotId();
              if (!validIds.contains(snapshotId)) {
                // determine whether the changes in this snapshot are in the current table state
                if (pickedAncestorSnapshotIds.contains(snapshotId)) {
                  // this snapshot was cherry-picked into the current table state, so skip cleaning
                  // it up.
                  // its changes will expire when the picked snapshot expires.
                  // A -- C -- D (source=B)
                  //  `- B <-- this commit
                  return;
                }

                long sourceSnapshotId =
                    PropertyUtil.propertyAsLong(
                        snapshot.summary(), SnapshotSummary.SOURCE_SNAPSHOT_ID_PROP, -1);
                if (ancestorIds.contains(sourceSnapshotId)) {
                  // this commit was cherry-picked from a commit that is in the current table state.
                  // do not clean up its changes because it would revert data file additions that
                  // are in the current
                  // table.
                  // A -- B -- C
                  //  `- D (source=B) <-- this commit
                  return;
                }

                if (pickedAncestorSnapshotIds.contains(sourceSnapshotId)) {
                  // this commit was cherry-picked from a commit that is in the current table state.
                  // do not clean up its changes because it would revert data file additions that
                  // are in the current
                  // table.
                  // A -- C -- E (source=B)
                  //  `- B `- D (source=B) <-- this commit
                  return;
                }

                // find any manifests that are no longer needed
                try (CloseableIterable<ManifestFile> manifests = readManifests(snapshot)) {
                  for (ManifestFile manifest : manifests) {
                    if (!validManifests.contains(manifest.path())) {
                      manifestsToDelete.add(manifest.path());
                    }
                  }
                } catch (IOException e) {
                  throw new RuntimeException(
                      "Failed to close manifest list: " + snapshot.manifestListLocation(), e);
                }

                // add the manifest list to the delete set, if present
                if (snapshot.manifestListLocation() != null) {
                  manifestListsToDelete.add(snapshot.manifestListLocation());
                }
              }
            });

    deleteFiles(manifestsToDelete, "manifest");
    deleteFiles(manifestListsToDelete, "manifest list");

    if (!table.statisticsFiles().isEmpty()) {
      Set<String> expiredStatisticsFiles = new HashSet<>();
      for (StatisticsFile statisticsFile : table.statisticsFiles()) {
        if (expiredIds.contains(statisticsFile.snapshotId())) {
          expiredStatisticsFiles.add(statisticsFile.path());
        }
      }
      deleteFiles(expiredStatisticsFiles, "statistics files");
    }
  }

  private static Function<Long, Snapshot> snapshotLookup(List<Snapshot> removedSnapshots, Table table) {
    Map<Long, Snapshot> snapshotMap = new HashMap<>();
    removedSnapshots.forEach(snapshot -> snapshotMap.put(snapshot.snapshotId(), snapshot));
    table.snapshots().forEach(snapshot -> snapshotMap.put(snapshot.snapshotId(), snapshot));
    return snapshotMap::get;
  }
}
