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
 
package io.onetable.client;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import io.onetable.spi.extractor.SourceClient;

/**
 * Provides the functionality to provide an instance of the {@link SourceClient} for a given table
 * format. The provider can create a new instance of the client for each table format or reuse the
 * same instance.
 */
public abstract class SourceClientProvider<COMMIT> {
  /** The Hadoop configuration to use when reading from the source table. */
  protected Configuration hadoopConf;

  /** Initializes the provider various client specific configurations. */
  public void init(Configuration hadoopConf) {
    this.hadoopConf = hadoopConf; // todo can we move this into some other config?
  }

  /**
   * Returns an instance of the {@link SourceClient} for the given table format. The {@link
   * SourceClient} is source table aware. A source table may require creation of a new instance of
   * the client. In some cases reusing the same instance across table configurations may be
   * efficient. The provider can decide how to manage this.
   *
   * @return the source client
   */
  public abstract SourceClient<COMMIT> getSourceClientInstance(
      SourceTable sourceTableConfig, Map<String, String> clientConf);
}
