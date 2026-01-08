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
 
package org.apache.xtable.kernel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import lombok.Builder;

import com.google.common.base.Preconditions;

import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.internal.actions.RemoveFile;
import io.delta.kernel.internal.actions.RowBackedAction;
import io.delta.kernel.utils.CloseableIterator;

import org.apache.xtable.exception.ReadException;

/** Cache store for storing incremental table changes in the Delta table. */
public class DeltaKernelIncrementalChangesState {

  private final Map<Long, List<RowBackedAction>> incrementalChangesByVersion = new HashMap<>();

  /**
   * Reloads the cache store with incremental changes. Intentionally thread safety is the
   * responsibility of the caller.
   *
   * @param engine The kernel engine.
   * @param versionToStartFrom The version to start from.
   */
  @Builder
  public DeltaKernelIncrementalChangesState(
      long versionToStartFrom, Engine engine, Table table, long endVersion) {
    Set<DeltaLogActionUtils.DeltaAction> actionSet = new HashSet<>();
    actionSet.add(DeltaLogActionUtils.DeltaAction.ADD);
    actionSet.add(DeltaLogActionUtils.DeltaAction.REMOVE);
    TableImpl tableImpl = (TableImpl) Table.forPath(engine, table.getPath(engine));

    // getChanges returns CloseableIterator<ColumnarBatch>
    try (CloseableIterator<ColumnarBatch> iter =
        tableImpl.getChanges(engine, versionToStartFrom, endVersion, actionSet)) {
      while (iter.hasNext()) {
        ColumnarBatch batch = iter.next();
        int addFileIndex = batch.getSchema().indexOf(DeltaLogActionUtils.DeltaAction.ADD.colName);
        int removeFileIndex =
            batch.getSchema().indexOf(DeltaLogActionUtils.DeltaAction.REMOVE.colName);

        try (CloseableIterator<Row> rows = batch.getRows()) {
          while (rows.hasNext()) {
            Row row = rows.next();

            // Get version (first column)
            long version = row.getLong(0);
            List<RowBackedAction> actions =
                incrementalChangesByVersion.computeIfAbsent(version, k -> new ArrayList<>());

            if (!row.isNullAt(addFileIndex)) {
              Row addFile = row.getStruct(addFileIndex);
              AddFile addAction = new AddFile(addFile);
              actions.add(addAction);
            }
            if (!row.isNullAt(removeFileIndex)) {
              Row removeFile = row.getStruct(removeFileIndex);
              RemoveFile removeAction = new RemoveFile(removeFile);
              actions.add(removeAction);
            }
          }
        }
      }
    } catch (IOException ioException) {
      throw new ReadException("Error reading kernel changes", ioException);
    }
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

  public List<RowBackedAction> getActionsForVersion(Long version) {
    Preconditions.checkArgument(
        incrementalChangesByVersion.containsKey(version),
        String.format("Version %s not found in the DeltaIncrementalChangesState.", version));
    return incrementalChangesByVersion.get(version);
  }
}
