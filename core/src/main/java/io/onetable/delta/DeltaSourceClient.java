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

import io.onetable.model.CurrentCommitState;
import io.onetable.model.InstantsForIncrementalSync;
import io.onetable.model.OneSnapshot;
import io.onetable.model.OneTable;
import io.onetable.model.TableChange;
import io.onetable.model.schema.SchemaCatalog;
import io.onetable.spi.extractor.SourceClient;
import lombok.extern.log4j.Log4j2;
import org.apache.spark.sql.delta.Snapshot;

@Log4j2
public class DeltaSourceClient implements SourceClient<Snapshot> {
  @Override
  public OneTable getTable(Snapshot snapshot) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public SchemaCatalog getSchemaCatalog(OneTable table, Snapshot snapshot) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public OneSnapshot getCurrentSnapshot() {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public TableChange getTableChangeForCommit(Snapshot snapshot) {
    // TODO(vamshigv): Implement this method.
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public CurrentCommitState<Snapshot> getCurrentCommitState(
      InstantsForIncrementalSync instantsForIncrementalSync) {
    // TODO(vamshigv): Implement this method.
    throw new UnsupportedOperationException("Not implemented");
  }
}
