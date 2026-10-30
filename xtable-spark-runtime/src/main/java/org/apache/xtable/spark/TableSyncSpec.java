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
import java.util.Map;

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

  /**
   * Optional source partition spec (e.g. {@code level:VALUE}); applies to partitioned Hudi and
   * Parquet sources. Maps to {@code xtable.hudi.source.partition_field_spec_config} for Hudi and
   * {@code xtable.parquet.source.partition_field_spec_config} for Parquet.
   */
  String partitionSpec;

  /** The source table format, e.g. {@code HUDI} (see {@code TableFormat}). */
  @NonNull String sourceFormat;

  /** The target formats to sync to, e.g. {@code [ICEBERG, DELTA]}. */
  @NonNull List<String> targets;

  /** Use the Spark-free Delta Kernel implementation for the Delta source and/or target. */
  boolean useDeltaKernel;

  /**
   * Optional properties of the target tables, keyed by the target format, for example {@code
   * xtable.hudi.target.secondary.index.column} for a {@code HUDI} target.
   */
  Map<String, Map<String, String>> targetProperties;
}
