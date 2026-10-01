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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.sql.SparkSession;

import org.apache.xtable.hudi.HudiTargetConfig;

/** Creates the {@link HudiExecutionEngineProvider} selected by the Hudi target configuration. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HudiExecutionEngineProviderFactory {

  public static HudiExecutionEngineProvider createProvider(
      HudiTargetConfig targetConfig, Configuration configuration) {
    if (targetConfig.isSparkEngine()) {
      return new SparkExecutionEngineProvider(getOrCreateSparkSession(configuration));
    }
    return new JavaExecutionEngineProvider(configuration);
  }

  /**
   * Reuses the active Spark session when the sync runs inside a Spark application, otherwise starts
   * a session that carries the Hadoop configuration of the sync.
   */
  private static SparkSession getOrCreateSparkSession(Configuration configuration) {
    SparkSession.Builder builder = SparkSession.builder();
    if (!SparkSession.getActiveSession().isDefined()
        && !SparkSession.getDefaultSession().isDefined()) {
      builder.appName("xtable").config("spark.serializer", KryoSerializer.class.getName());
      configuration.forEach(
          entry -> builder.config("spark.hadoop." + entry.getKey(), entry.getValue()));
    }
    return builder.getOrCreate();
  }
}
