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
 
package io.onetable.delta;

import java.util.Map;

import org.apache.spark.sql.SparkSession;

import io.delta.tables.DeltaTable;

import io.onetable.client.SourceClientProvider;
import io.onetable.client.SourceTable;

/** A concrete implementation of {@link SourceClientProvider} for Delta Lake table format. */
public class DeltaSourceClientProvider extends SourceClientProvider<Long> {
  @Override
  public DeltaSourceClient getSourceClientInstance(
      SourceTable sourceTable, Map<String, String> clientConfigs) {
    SparkSession sparkSession = DeltaClientUtils.buildSparkSession(hadoopConf);
    DeltaTable deltaTable = DeltaTable.forPath(sparkSession, sourceTable.getBasePath());
    DeltaSourceClient deltaSourceClient =
        DeltaSourceClient.builder()
            .sparkSession(sparkSession)
            .tableName(sourceTable.getName())
            .basePath(sourceTable.getBasePath())
            .deltaTable(deltaTable)
            .deltaLog(deltaTable.deltaLog())
            .build();
    return deltaSourceClient;
  }
}
