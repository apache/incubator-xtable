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
 
package io.onetable.persistence;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import lombok.Builder;
import lombok.Value;

import io.onetable.model.OneTable;
import io.onetable.model.storage.TableFormat;
import io.onetable.model.sync.SyncResult;

/**
 * OneTableState is a model to hold the state of a Sync process. {@link OneTable} is the canonical
 * representation of the table which needs to be synced. It also stores list of table formats to
 * sync to, last sync result for each table format. last successful sync result for each table
 * format.
 */
@Value
@Builder
public class OneTableState {
  OneTable table;
  List<TableFormat> tableFormatsToSync;
  @Builder.Default Map<TableFormat, SyncResult> lastSyncResult = new HashMap<>();
  @Builder.Default Map<TableFormat, SyncResult> lastSuccessfulSyncResult = new HashMap<>();
}
