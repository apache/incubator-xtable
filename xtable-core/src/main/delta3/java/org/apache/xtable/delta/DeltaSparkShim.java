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

import java.sql.Timestamp;

import org.apache.hadoop.fs.FileStatus;

import org.apache.spark.sql.delta.DeltaHistoryManager;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.Snapshot;
import org.apache.spark.sql.delta.actions.Action;

import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Seq;

/**
 * The Delta Lake calls whose signatures differ between Delta 3.x and Delta 4.x, for Delta 3.x. No
 * catalog table is passed, so Delta resolves the table by its path.
 */
final class DeltaSparkShim {
  private DeltaSparkShim() {}

  static Snapshot getSnapshotAt(DeltaLog deltaLog, long version) {
    return deltaLog.getSnapshotAt(version, Option.empty(), Option.empty());
  }

  static DeltaHistoryManager.Commit getActiveCommitAtTime(
      DeltaLog deltaLog,
      Timestamp timestamp,
      boolean canReturnLastCommit,
      boolean mustBeRecreatable,
      boolean canReturnEarliestCommit) {
    return deltaLog
        .history()
        .getActiveCommitAtTime(
            timestamp, canReturnLastCommit, mustBeRecreatable, canReturnEarliestCommit);
  }

  static Iterator<Tuple2<Object, Seq<Action>>> getChanges(DeltaLog deltaLog, long startVersion) {
    return deltaLog.getChanges(startVersion, false);
  }

  static Iterator<Tuple2<Object, FileStatus>> getChangeLogFiles(
      DeltaLog deltaLog, long startVersion) {
    return deltaLog.getChangeLogFiles(startVersion, false);
  }

}
