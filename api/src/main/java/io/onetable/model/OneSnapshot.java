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
 
package io.onetable.model;

import java.time.Instant;
import java.util.Collections;
import java.util.List;

import lombok.Builder;
import lombok.Value;

import io.onetable.model.schema.SchemaCatalog;
import io.onetable.model.storage.OneDataFiles;
import io.onetable.spi.OneTableSnapshotVisitor;

/**
 * Snapshot represents the view of the table at a specific point in time. Snapshot captures all the
 * required information (schemas, table metadata, files etc) which can be used by a query engine to
 * query the table as of {@link #version}. Additionally, it also captures the pending instants
 * before the last completed instant on the table. This can be useful for subsequent incremental
 * processing.
 *
 * @since 0.1
 */
@Value
@Builder
public class OneSnapshot {
  // The instant of the Snapshot
  String version;
  // Table reference
  OneTable table;
  // Schema catalog referencing the written schema for each data file in the snapshot
  SchemaCatalog schemaCatalog;
  // List of data file groupings
  OneDataFiles dataFiles;
  // pending commits before latest commit on the table.
  @Builder.Default List<Instant> pendingCommits = Collections.emptyList();

  public void acceptVisitor(OneTableSnapshotVisitor defaultDataFileVisitor) {
    defaultDataFileVisitor.visit(this);
  }
}
