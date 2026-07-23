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
 
package org.apache.xtable.hudi;

import java.util.Properties;

import lombok.Value;

import org.apache.hudi.common.table.HoodieTableVersion;

/** Configuration of the Hudi conversion target. */
@Value
public class HudiTargetConfig {
  /**
   * Table format version to write for the Hudi target. Supported values are {@code 6} (the legacy
   * 0.x timeline layout, column-stats index V1) and {@code 9} (the Hudi 1.x timeline layout,
   * column-stats index V2). Defaults to {@code 9}.
   */
  public static final String HUDI_TABLE_VERSION = "xtable.hudi.target.table_version";

  static final HoodieTableVersion DEFAULT_TABLE_VERSION = HoodieTableVersion.SIX;

  HoodieTableVersion tableVersion;

  public static HudiTargetConfig fromProperties(Properties properties) {
    HoodieTableVersion tableVersion = DEFAULT_TABLE_VERSION;
    if (properties != null) {
      String configured = properties.getProperty(HUDI_TABLE_VERSION);
      if (configured != null && !configured.trim().isEmpty()) {
        tableVersion = HoodieTableVersion.fromVersionCode(Integer.parseInt(configured.trim()));
      }
    }
    if (tableVersion != HoodieTableVersion.SIX && tableVersion != HoodieTableVersion.NINE) {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported Hudi target table version %s. Only table versions 6 and 9 are supported via %s.",
              tableVersion.versionCode(), HUDI_TABLE_VERSION));
    }
    return new HudiTargetConfig(tableVersion);
  }
}
