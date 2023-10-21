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
 
package io.onetable.delta;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.actions.Action;

import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import com.google.common.base.Preconditions;

/** Cache store for storing incremental table changes in the Delta table. */
public class DeltaIncrementalChangesCacheStore {
  private Long startVersion;
  private Long endVersion;
  private boolean initialized;
  // Map of version number to list of actions and it is thread safe.
  private Map<Long, List<Action>> incrementalChangesByVersion;
  private final Lock lock = new ReentrantLock();

  public DeltaIncrementalChangesCacheStore() {
    this.resetState();
  }

  /**
   * Initializes or reloads the cache store with incremental changes.
   *
   * @param deltaLog The DeltaLog instance.
   * @param versionToStartFrom The version to start from.
   */
  public void initializeOrReload(DeltaLog deltaLog, Long versionToStartFrom) {
    // TODO: Should fail on data loss(due to vacuum) and fall back to snapshot sync.
    if (!isUsable(deltaLog, versionToStartFrom)) {
      reload(deltaLog, versionToStartFrom);
    }
  }

  public List<Long> getVersionsInSortedOrder() {
    lock.lock();
    try {
      List<Long> versions = new ArrayList<>(incrementalChangesByVersion.keySet());
      versions.sort(Long::compareTo);
      return versions;
    } finally {
      lock.unlock();
    }
  }

  public List<Action> getActionsForVersion(Long version) {
    lock.lock();
    try {
      Preconditions.checkArgument(
          incrementalChangesByVersion.containsKey(version),
          "Version %s not found in the DeltaIncrementalChangesCacheStore.");
      return incrementalChangesByVersion.get(version);
    } finally {
      lock.unlock();
    }
  }

  private boolean isUsable(DeltaLog deltaLog, Long versionToStartFrom) {
    lock.lock();
    try {
      if (!initialized) {
        return false;
      }
      long latestVersion = deltaLog.snapshot().version();
      return versionToStartFrom >= startVersion && latestVersion == endVersion;
    } finally {
      lock.unlock();
    }
  }

  private void reload(DeltaLog deltaLog, Long versionToStartFrom) {
    Iterator<Tuple2<Object, Seq<Action>>> iterator = deltaLog.getChanges(versionToStartFrom, false);
    List<Tuple2<Object, List<Action>>> changesList = new ArrayList<>();
    while (iterator.hasNext()) {
      Tuple2<Object, Seq<Action>> currentChange = iterator.next();
      Object versionNumberObj = currentChange._1();
      List<Action> currentActions = JavaConverters.seqAsJavaList(currentChange._2());
      changesList.add(new Tuple2<>(versionNumberObj, currentActions));
    }

    // Use a lock to fill the map in a thread-safe manner.
    lock.lock();
    try {
      this.resetState();
      for (Tuple2<Object, List<Action>> change : changesList) {
        Long versionNumber = (Long) change._1();
        List<Action> actions = change._2();
        incrementalChangesByVersion.put(versionNumber, actions);
        endVersion = endVersion == -1L ? versionNumber : Math.max(endVersion, versionNumber);
      }
      startVersion = versionToStartFrom;
      this.initialized = true;
    } finally {
      lock.unlock();
    }
  }

  private void resetState() {
    this.initialized = false;
    this.startVersion = -1L;
    this.endVersion = -1L;
    this.incrementalChangesByVersion = new HashMap<>();
  }
}
