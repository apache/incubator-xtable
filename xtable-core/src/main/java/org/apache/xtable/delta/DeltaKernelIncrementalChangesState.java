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
import io.delta.kernel.Table;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.DeltaLogActionUtils;
import io.delta.kernel.internal.TableImpl;
import io.delta.kernel.engine.Engine;

import java.util.*;
import java.util.stream.Collectors;
import io.delta.kernel.types.StructType;
import io.delta.kernel.internal.actions.*;
import io.delta.kernel.internal.actions.AddFile;
import io.delta.kernel.utils.CloseableIterator;
import lombok.Builder;
import org.apache.iceberg.expressions.False;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import com.google.common.base.Preconditions;

import javax.swing.*;

/** Cache store for storing incremental table changes in the Delta table. */
public class DeltaKernelIncrementalChangesState {

    private final Map<Long, List<Action>> incrementalChangesByVersion = new HashMap<>();

    /**
     * Reloads the cache store with incremental changes. Intentionally thread safety is the
     * responsibility of the caller.
     *
     * @param engine             The kernel engine.
     * @param versionToStartFrom The version to start from.
     */
    @Builder
    public DeltaKernelIncrementalChangesState(Long versionToStartFrom, Engine engine, Table table, Long endVersion) {
        Set<DeltaLogActionUtils.DeltaAction> actionSet = new HashSet<>();
        actionSet.add(DeltaLogActionUtils.DeltaAction.ADD);
        actionSet.add(DeltaLogActionUtils.DeltaAction.COMMITINFO);
        List<ColumnarBatch> kernelChanges = new ArrayList<>();
        TableImpl tableImpl = (TableImpl) Table.forPath(engine, table.getPath(engine));

        // getChanges returns CloseableIterator<ColumnarBatch>
        try (CloseableIterator<ColumnarBatch> iter = tableImpl.getChanges(engine, versionToStartFrom, endVersion, actionSet)) {
            while (iter.hasNext()) {
                kernelChanges.add(iter.next());
                ColumnarBatch batch = iter.next();

                CloseableIterator<Row> rows = batch.getRows();
                try {
                    while (rows.hasNext()) {
                        Row row = rows.next();

                        // Get version (first column)
                        long version = row.getLong(0);

                        // Get commit timestamp (second column)
                        long timestamp = row.getLong(1);

                        // Get commit info (third column)
                        Row commitInfo = row.getStruct(2);

                        // Get add file (fourth column)
                        Row addFile = !row.isNullAt(3) ? row.getStruct(3) : null;

                        List<Action> actions = new ArrayList<>();

                        AddFile addAction = new   AddFile(addFile);
//
//                        Integer actionIdx = null;
//
//                        for (int i = 2; i < row.getSchema().length(); i++) {
//                            if (!row.isNullAt(i)) {
//                                actionIdx = i;
//                                break;
//                            }
//                        }
//

                    }
                } finally {
                    rows.close();
                }

            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading kernel changes", e);
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

    public List<Action> getActionsForVersion(Long version) {
        Preconditions.checkArgument(
                incrementalChangesByVersion.containsKey(version),
                String.format("Version %s not found in the DeltaIncrementalChangesState.", version));
        return incrementalChangesByVersion.get(version);
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
