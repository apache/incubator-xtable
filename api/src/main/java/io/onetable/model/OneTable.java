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
import java.util.Set;

import lombok.Builder;
import lombok.Value;

import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;
import io.onetable.model.schema.OneSchema;
import io.onetable.model.storage.DataLayoutStrategy;
import io.onetable.model.storage.TableFormat;

/**
 * Represents a reference to the table in the format {@link #tableFormat}.
 *
 * @since 0.1
 */
@Value
@Builder(toBuilder = true)
public class OneTable {
  // name of the table
  String name;
  // table format the table currently has data in
  TableFormat tableFormat;
  // Schema to use for reading the table
  OneSchema readSchema;
  // Data layout strategy
  DataLayoutStrategy layoutStrategy;
  // Base path
  String basePath;
  // Partitioning fields if table is partitioned
  List<OnePartitionField> partitioningFields;
  // record keys for the table if exists.
  @Builder.Default Set<OneField> recordKeyFields = Collections.emptySet();
  // latest commit(write) on the table.
  Instant latestCommitTime;
}
