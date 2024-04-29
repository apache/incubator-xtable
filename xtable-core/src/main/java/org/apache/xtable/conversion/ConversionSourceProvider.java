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
 
package org.apache.xtable.conversion;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.xtable.spi.extractor.ConversionSource;

/**
 * Provides the functionality to provide an instance of the {@link ConversionSource} for a given
 * table format. The provider can create a new instance of the conversion source for each table
 * format or reuse the same instance.
 */
public abstract class ConversionSourceProvider<COMMIT> {
  /** The Hadoop configuration to use when reading from the source table. */
  protected Configuration hadoopConf;

  /** The configuration for the source. */
  protected Map<String, String> sourceConf;

  /** The configuration for the table to read from. */
  protected PerTableConfig sourceTableConfig;

  /** Initializes the provider various source specific configurations. */
  public void init(Configuration hadoopConf, Map<String, String> sourceConf) {
    this.hadoopConf = hadoopConf;
    this.sourceConf = sourceConf;
  }

  /**
   * Returns an instance of the {@link ConversionSource} for the given table format. The {@link
   * ConversionSource} is source table aware. A source table may require creation of a new instance
   * of the source. In some cases reusing the same instance across table configurations may be
   * efficient. The provider can decide how to manage this.
   *
   * @return the conversion source
   */
  public abstract ConversionSource<COMMIT> getConversionSourceInstance(
      PerTableConfig sourceTableConfig);
}
