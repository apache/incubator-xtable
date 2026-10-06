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

  // Kernel is the default Delta source as of https://github.com/apache/incubator-xtable/issues/886.
  // Set to false to fall back to the Delta Standalone source instead.
  @ConfigProperty(name = "xtable.delta.source.use_kernel", defaultValue = "true")
  private boolean deltaSourceUseKernel;

  public String getHadoopConfigPath() {
    return hadoopConfigPath;
  }

  public boolean isDeltaSourceUseKernel() {
    return deltaSourceUseKernel;
  }
}
