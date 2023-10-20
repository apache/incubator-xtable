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

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import lombok.extern.log4j.Log4j2;

import org.apache.spark.sql.SparkSession;

import org.apache.spark.sql.delta.DeltaHistory;
import org.apache.spark.sql.delta.DeltaHistoryManager;
import org.apache.spark.sql.delta.DeltaLog;
import org.apache.spark.sql.delta.actions.Action;

import scala.Option;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import io.delta.tables.DeltaTable;

import io.onetable.model.CurrentCommitState;
import io.onetable.model.InstantsForIncrementalSync;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.TableChange;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.spi.extractor.SourceClient;

@Log4j2
public class DeltaSourceClient implements SourceClient<Long> {
  private final SparkSession sparkSession;
  private final DeltaLog deltaLog;
  private final DeltaTable deltaTable;

  public DeltaSourceClient(SparkSession sparkSession, String basePath) {
    this.sparkSession = sparkSession;
    this.deltaLog = DeltaLog.forTable(sparkSession, basePath);
    this.deltaTable = DeltaTable.forPath(sparkSession, basePath);
  }

  @Override
  public OneTable getTable(Long versionNumber) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public SchemaCatalog getSchemaCatalog(OneTable table, Long versionNumber) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public OneSnapshot getCurrentSnapshot() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public TableChange getTableChangeForCommit(Long versionNumber) {
    // TODO(vamshigv): Implement this method.
    // TODO(vamshigv): Still look for a method to process a single version to avoid processing lot
    // of commits at once and hence fixed memory footprint.
    Iterator<Tuple2<Object, Seq<Action>>> x = deltaLog.getChanges(1, false);
    java.util.List<Tuple2<Object, List<Action>>> y = new ArrayList<>();
    while (x.hasNext()) {
      Tuple2<Object, Seq<Action>> scalaChange = x.next();
      Object key = scalaChange._1();
      List<Action> actions = JavaConverters.seqAsJavaListConverter(scalaChange._2()).asJava();
      y.add(new Tuple2<>(key, actions));
    }
    return null;
  }

  @Override
  public CurrentCommitState<Long> getCurrentCommitState(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    // TODO(vamshigv): Implement this method.
    DeltaHistoryManager.Commit commit =
        deltaLog
            .history()
            .getActiveCommitAtTime(
                Timestamp.from(instantsForIncrementalSync.getLastSyncInstant()), true, false, true);
    DeltaHistoryManager x = new DeltaHistoryManager(deltaLog, 100);
    List<DeltaHistory> y = JavaConverters.seqAsJavaList(x.getHistory(1, Option.empty()));
    // TODO(vamshigv): update this.
    return null;
  }
}
