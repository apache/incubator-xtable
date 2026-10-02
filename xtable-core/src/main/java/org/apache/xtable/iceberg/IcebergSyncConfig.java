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
 
package org.apache.xtable.iceberg;

import java.util.Properties;

import org.apache.xtable.conversion.TargetTable;

public class IcebergSyncConfig {
  public static final String USE_METADATA_CLEANER =
      "xtable.iceberg.target.metadata.cleaner.enabled";
  public static final String METADATA_CLEANER_THREAD_POOL_SIZE =
      "xtable.iceberg.target.metadata.cleaner.thread_pool.size";

  private static final int DEFAULT_CLEANER_THREAD_POOL_SIZE = 1;

  public static boolean canUseMetadataCleaner(TargetTable table) {
    return Boolean.parseBoolean(getProperty(table, USE_METADATA_CLEANER, Boolean.FALSE.toString()));
  }

  public static int getMetadataCleanerThreadPoolSize(TargetTable table) {
    return Integer.parseInt(
        getProperty(
            table,
            METADATA_CLEANER_THREAD_POOL_SIZE,
            String.valueOf(DEFAULT_CLEANER_THREAD_POOL_SIZE)));
  }

  private static String getProperty(TargetTable table, String key, String defaultValue) {
    Properties properties = table.getAdditionalProperties();
    return properties == null ? defaultValue : properties.getProperty(key, defaultValue);
  }
}
