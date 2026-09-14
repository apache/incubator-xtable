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

/**
 * Immutable description of a single table to keep in sync, resolved from {@code spark.xtable.*}
 * configuration. One {@link TableSyncSpec} is produced per key listed in {@code
 * spark.xtable.tables}.
 */
@Value
@Builder
public class TableSyncSpec {
  /** The logical key for this table (the token listed in {@code spark.xtable.tables}). */
  @NonNull String key;

  /** Absolute base path of the source table. */
  @NonNull String basePath;

  /** Optional path to the data files; defaults to {@link #basePath} downstream when null. */
  String dataPath;

  /** Optional namespace segments for the table. */
  String[] namespace;

  /**
   * Optional Hudi source partition spec (e.g. {@code level:VALUE}); only applies to a partitioned
   * Hudi source. Maps to {@code xtable.hudi.source.partition_field_spec_config}.
   */
  String partitionSpec;

  /** The source table format, e.g. {@code HUDI} (see {@code TableFormat}). */
  @NonNull String sourceFormat;

  /** The target formats to sync to, e.g. {@code [ICEBERG, DELTA]}. */
  @NonNull List<String> targets;
}
