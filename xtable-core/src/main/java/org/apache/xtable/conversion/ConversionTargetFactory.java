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

import java.util.ServiceLoader;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.hadoop.conf.Configuration;

import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.spi.sync.ConversionTarget;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConversionTargetFactory {
  private static final ConversionTargetFactory INSTANCE = new ConversionTargetFactory();

  public static ConversionTargetFactory getInstance() {
    return INSTANCE;
  }

  /**
   * Create a fully initialized instance of the ConversionTarget represented by the given Table
   * Format name. Initialization is done with the config provided through PerTableConfig and
   * Configuration params.
   *
   * @param tableFormat
   * @param perTableConfig
   * @param configuration
   * @return
   */
  public ConversionTarget createForFormat(
      String tableFormat, PerTableConfig perTableConfig, Configuration configuration) {
    ConversionTarget conversionTarget = createConversionTargetForName(tableFormat);

    conversionTarget.init(perTableConfig, configuration);
    return conversionTarget;
  }

  /**
   * Create an instance of the ConversionTarget via the default no-arg constructor. Expectation is
   * that target conversion specific settings may be provided prior to the calling of
   * ConversionTarget.init() which must be called prior to actual use of the conversion target
   * returned by this factory method.
   *
   * @param tableFormatName
   * @return
   */
  public ConversionTarget createConversionTargetForName(String tableFormatName) {
    ServiceLoader<ConversionTarget> loader = ServiceLoader.load(ConversionTarget.class);
    for (ConversionTarget target : loader) {
      if (target.getTableFormat().equalsIgnoreCase(tableFormatName)) {
        return target;
      }
    }
    throw new NotSupportedException("Target format is not yet supported: " + tableFormatName);
  }
}
