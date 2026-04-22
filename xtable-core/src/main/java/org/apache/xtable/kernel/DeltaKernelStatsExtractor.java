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

import java.util.List;
import java.util.Set;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;

import io.delta.kernel.internal.actions.AddFile;

import org.apache.xtable.delta.DeltaStatsUtils;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.stat.ColumnStat;
import org.apache.xtable.model.stat.FileStats;

/**
 * Delta Kernel stats extractor - delegates to {@link DeltaStatsUtils} for shared logic.
 *
 * @deprecated This class is a thin wrapper around DeltaStatsUtils. Consider using DeltaStatsUtils
 *     directly.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaKernelStatsExtractor {
  private static final DeltaKernelStatsExtractor INSTANCE = new DeltaKernelStatsExtractor();

  public static DeltaKernelStatsExtractor getInstance() {
    return INSTANCE;
  }

  /**
   * Converts XTable column statistics to Delta format JSON.
   *
   * @param schema the table schema
   * @param numRecords the number of records
   * @param columnStats the column statistics
   * @return JSON string in Delta format
   * @throws JsonProcessingException if serialization fails
   */
  public String convertStatsToDeltaFormat(
      InternalSchema schema, long numRecords, List<ColumnStat> columnStats)
      throws JsonProcessingException {
    return DeltaStatsUtils.convertStatsToDeltaFormat(schema, numRecords, columnStats);
  }

  /**
   * Extracts column statistics from Delta Kernel AddFile.
   *
   * @param addFile the Delta Kernel AddFile action
   * @param fields the fields to extract stats for
   * @return FileStats containing column statistics
   */
  public FileStats getColumnStatsForFile(AddFile addFile, List<InternalField> fields) {
    // Delta Kernel wraps stats in Optional, extract it or use empty string
    String statsJson = addFile.getStatsJson().orElse("");
    return DeltaStatsUtils.parseColumnStatsFromJson(statsJson, fields);
  }

  /**
   * Returns unsupported stats discovered during parsing.
   *
   * @return set of unsupported stat names
   */
  @VisibleForTesting
  Set<String> getUnsupportedStats() {
    return DeltaStatsUtils.getUnsupportedStats();
  }
}
