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
 
package org.apache.xtable.service;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.enterprise.context.ApplicationScoped;

@ApplicationScoped
public class ConversionServiceConfig {

  public static final String HADOOP_DEFAULTS_XML = "xtable-hadoop-defaults.xml";

  @ConfigProperty(name = "xtable.hadoop-config-path", defaultValue = HADOOP_DEFAULTS_XML)
  private String hadoopConfigPath;

  /**
   * Whether the bundled web UI is served.
   *
   * <p>Off by default on purpose. The service ships with no authentication and no authorization
   * (see the deployment notes in the module README), so an existing deployment must not gain a
   * browser interface simply by upgrading.
   */
  @ConfigProperty(name = "xtable.ui.enabled", defaultValue = "false")
  private boolean uiEnabled;

  /** Number of conversion runs retained in memory before the oldest is evicted. */
  @ConfigProperty(name = "xtable.run-history.capacity", defaultValue = "100")
  private int runHistoryCapacity;

  /** Progress events retained per run before the oldest is evicted. */
  @ConfigProperty(name = "xtable.run-history.max-events-per-run", defaultValue = "500")
  private int runMaxEvents;

  /** Threads available to run asynchronous conversions. */
  @ConfigProperty(name = "xtable.conversion.async-worker-threads", defaultValue = "2")
  private int asyncWorkerThreads;

  public String getHadoopConfigPath() {
    return hadoopConfigPath;
  }

  public boolean isUiEnabled() {
    return uiEnabled;
  }

  public int getRunHistoryCapacity() {
    return runHistoryCapacity;
  }

  public int getRunMaxEvents() {
    return runMaxEvents;
  }

  public int getAsyncWorkerThreads() {
    return asyncWorkerThreads;
  }
}
