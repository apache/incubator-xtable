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

import java.util.Iterator;
import java.util.Properties;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.xtable.delta.DeltaConversionTargetConfig;
import org.apache.xtable.exception.NotSupportedException;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.spi.sync.ConversionTarget;

@Log4j2
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
    Iterator<ConversionTarget> iterator = loader.iterator();
    while (iterator.hasNext()) {
      ConversionTarget target;
      try {
        target = iterator.next();
      } catch (ServiceConfigurationError | LinkageError error) {
        // A registered target whose engine library is not on the classpath (e.g. Delta when only
        // Hudi/Iceberg are provided). Skip it so a subset of engines can still be used; a missing
        // engine for the requested format surfaces below as NotSupportedException.
        log.debug(
            "Skipping ConversionTarget whose engine is not available on the classpath", error);
        continue;
      }
      if (target.getTableFormat().equalsIgnoreCase(tableFormatName)
          && isDeltaKernelTarget(target) == useKernel) {
        return target;
      }
    }
    throw new NotSupportedException("Target format is not yet supported: " + tableFormatName);
  }

  private static final String DELTA_KERNEL_TARGET_CLASS =
      "org.apache.xtable.kernel.DeltaKernelConversionTarget";

  // Name-based to avoid loading DeltaKernelConversionTarget (and its io.delta.kernel dependencies)
  // when Delta is not on the classpath; the target passed in is already loaded.
  private static boolean isDeltaKernelTarget(ConversionTarget target) {
    return DELTA_KERNEL_TARGET_CLASS.equals(target.getClass().getName());
  }
}
