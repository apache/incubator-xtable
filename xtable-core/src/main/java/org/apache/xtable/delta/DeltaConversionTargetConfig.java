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
 
package org.apache.xtable.delta;

import java.util.Properties;

import lombok.Value;

/**
 * Configuration of the Delta target format for the sync process.
 *
 * <p>Controls which Delta writer implementation backs a Delta {@code ConversionTarget}. By default
 * syncs use the Delta Kernel based {@code DeltaKernelConversionTarget}, per
 * https://github.com/apache/incubator-xtable/issues/886. Setting {@link #USE_KERNEL} to {@code
 * false} in the target table's additional properties falls back to the Delta Standalone based
 * {@link DeltaConversionTarget} instead -- the escape hatch for a regression that only exists on
 * the Kernel path, e.g. schema evolution on an existing table (Kernel fails fast on that rather
 * than silently committing a stale schema; see delta-io/delta#4305).
 */
@Value
public class DeltaConversionTargetConfig {
  /**
   * When {@code false}, Delta syncs fall back to the Delta Standalone implementation instead of the
   * default Delta Kernel implementation. Defaults to {@code true}.
   */
  public static final String USE_KERNEL = "xtable.delta.target.use_kernel";

  boolean useKernel;

  public static DeltaConversionTargetConfig fromProperties(Properties properties) {
    boolean useKernel =
        properties == null
            || Boolean.parseBoolean(properties.getProperty(USE_KERNEL, Boolean.TRUE.toString()));
    return new DeltaConversionTargetConfig(useKernel);
  }
}
