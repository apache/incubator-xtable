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
 
package org.apache.xtable.spark;

import java.util.List;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

/** One table to sync: its source, its target formats and where its data files live. */
@Value
@Builder
public class TableSyncSpec {
  /** A name for the table, used in log messages. */
  @NonNull String key;

  /** Absolute base path of the source table. */
  @NonNull String basePath;

  /** Optional path to the data files; defaults to {@link #basePath} downstream when null. */
  String dataPath;

  /** Optional namespace segments for the table. */
  String[] namespace;

  /** Optional partition spec for a partitioned Hudi source, e.g. {@code level:VALUE}. */
  String partitionSpec;

  /** The source table format, e.g. {@code HUDI} (see {@code TableFormat}). */
  @NonNull String sourceFormat;

  /** The target formats to sync to, e.g. {@code [ICEBERG, DELTA]}. */
  @NonNull List<String> targets;

  /** Use the Spark-free Delta Kernel implementation for the Delta source and/or target. */
  boolean useDeltaKernel;
}
