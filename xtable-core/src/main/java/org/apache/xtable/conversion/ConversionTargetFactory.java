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

import java.util.Properties;
import java.util.ServiceLoader;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.hadoop.conf.Configuration;

import org.apache.xtable.delta.DeltaConversionTargetConfig;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.kernel.DeltaKernelConversionTarget;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.sync.ConversionTarget;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ConversionTargetFactory {
  private static final ConversionTargetFactory INSTANCE = new ConversionTargetFactory();

  public static ConversionTargetFactory getInstance() {
    return INSTANCE;
  }

  /**
   * Create a fully initialized instance of the ConversionTarget represented by the given Table
   * Format name. Initialization is done with the config provided through TargetTable and
   * Configuration params.
   *
   * @param targetTable the spec of the target
   * @param configuration hadoop configuration
   * @return an intialized {@link ConversionTarget}
   */
  public ConversionTarget createForFormat(TargetTable targetTable, Configuration configuration) {
    ConversionTarget conversionTarget =
        createConversionTargetForName(
            targetTable.getFormatName(), targetTable.getAdditionalProperties());

    conversionTarget.init(targetTable, configuration);
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
    return createConversionTargetForName(tableFormatName, new Properties());
  }

  /**
   * Resolves the ConversionTarget for the given format, using the target properties to pick between
   * the Delta Standalone and Delta Kernel implementations (both registered under {@link
   * TableFormat#DELTA}) via {@link DeltaConversionTargetConfig#USE_KERNEL} (default {@code false}).
   * Other formats have a single implementation, so the flag has no effect.
   *
   * @param tableFormatName the target table format name
   * @param properties target table additional properties used to resolve the implementation
   * @return an uninitialized {@link ConversionTarget}
   */
  public ConversionTarget createConversionTargetForName(
      String tableFormatName, Properties properties) {
    boolean useKernel =
        TableFormat.DELTA.equalsIgnoreCase(tableFormatName)
            && DeltaConversionTargetConfig.fromProperties(properties).isUseKernel();
    ServiceLoader<ConversionTarget> loader = ServiceLoader.load(ConversionTarget.class);
    for (ConversionTarget target : loader) {
      if (target.getTableFormat().equalsIgnoreCase(tableFormatName)
          && isDeltaKernelTarget(target) == useKernel) {
        return target;
      }
    }
    throw new NotSupportedException("Target format is not yet supported: " + tableFormatName);
  }

  private static boolean isDeltaKernelTarget(ConversionTarget target) {
    return target instanceof DeltaKernelConversionTarget;
  }
}
