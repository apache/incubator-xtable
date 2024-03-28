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
 
package org.apache.xtable.model;

import java.time.Instant;
import java.util.List;

import lombok.Builder;
import lombok.Value;

import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.storage.DataLayoutStrategy;

/**
 * Represents a reference to the table in the format {@link #tableFormat}.
 *
 * @since 0.1
 */
@Value
@Builder(toBuilder = true)
public class InternalTable {
  // name of the table
  String name;
  // table format the table currently has data in
  String tableFormat;
  // Schema to use for reading the table
  InternalSchema readSchema;
  // Data layout strategy
  DataLayoutStrategy layoutStrategy;
  // Base path
  String basePath;
  // Partitioning fields if table is partitioned
  List<InternalPartitionField> partitioningFields;
  // latest commit(write) on the table.
  Instant latestCommitTime;
}
