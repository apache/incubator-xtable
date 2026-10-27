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

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieSparkTable;
import org.apache.hudi.table.HoodieTable;

/**
 * Runs the Hudi commit and table services on Spark so the metadata table index generation is
 * distributed across the cluster.
 */
public class SparkExecutionEngineProvider implements HudiExecutionEngineProvider {
  private final HoodieSparkEngineContext engineContext;

  public SparkExecutionEngineProvider(SparkSession sparkSession) {
    this.engineContext =
        new HoodieSparkEngineContext(
            JavaSparkContext.fromSparkContext(sparkSession.sparkContext()));
  }

  @Override
  public HoodieEngineContext getEngineContext() {
    return engineContext;
  }

  @Override
  public HoodieTable<?, ?, ?, ?> createTable(
      HoodieWriteConfig writeConfig, HoodieTableMetaClient metaClient) {
    return HoodieSparkTable.create(writeConfig, engineContext, metaClient);
  }

  @Override
  public void commit(
      HoodieWriteConfig writeConfig,
      String instantTime,
      List<WriteStatus> writeStatuses,
      Option<Map<String, String>> extraMetadata,
      String commitActionType,
      Map<String, List<String>> partitionToReplacedFileIds) {
    try (SparkRDDWriteClient<?> writeClient =
        new SparkRDDWriteClient<>(engineContext, writeConfig)) {
      writeClient.setOperationType(WriteOperationType.UNKNOWN);
      writeClient.commit(
          instantTime,
          engineContext.getJavaSparkContext().parallelize(writeStatuses),
          extraMetadata,
          commitActionType,
          partitionToReplacedFileIds);
    }
  }
}
