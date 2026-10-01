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
 
package org.apache.xtable.hudi.engine;

import java.util.List;
import java.util.Map;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

/**
 * Abstracts the Hudi engine (Java or Spark) used by the Hudi conversion target to commit the
 * registered files and to maintain the table and its metadata table indexes.
 */
public interface HudiExecutionEngineProvider {

  /** Returns the engine context used for file listing, index generation and table services. */
  HoodieEngineContext getEngineContext();

  /** Creates the engine specific {@link HoodieTable} for cleaning and archival. */
  HoodieTable<?, ?, ?, ?> createTable(
      HoodieWriteConfig writeConfig, HoodieTableMetaClient metaClient);

  /**
   * Completes the inflight replace commit with the given write statuses. The metadata table indexes
   * (files, column stats, record index and secondary index) are updated as part of the commit.
   */
  void commit(
      HoodieWriteConfig writeConfig,
      String instantTime,
      List<WriteStatus> writeStatuses,
      Option<Map<String, String>> extraMetadata,
      String commitActionType,
      Map<String, List<String>> partitionToReplacedFileIds);
}
