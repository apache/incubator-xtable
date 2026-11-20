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

import lombok.Value;

/**
 * Configuration of the Iceberg source format for the sync process, read from the source table's
 * additional properties.
 */
@Value
public class IcebergConversionSourceConfig {
  /**
   * Allows the sync to continue when the source has position deletes, equality deletes or deletion
   * vectors, which the conversion targets cannot represent. Defaults to {@code false}, so the sync
   * fails instead of producing targets that still return the deleted rows.
   */
  public static final String ALLOW_UNSUPPORTED_DELETE_FILES =
      "xtable.iceberg.source.allow_unsupported_delete_files";

  boolean allowUnsupportedDeleteFiles;

  public static IcebergConversionSourceConfig fromProperties(Properties properties) {
    boolean allowUnsupportedDeleteFiles =
        properties != null
            && Boolean.parseBoolean(
                properties.getProperty(ALLOW_UNSUPPORTED_DELETE_FILES, Boolean.FALSE.toString()));
    return new IcebergConversionSourceConfig(allowUnsupportedDeleteFiles);
  }
}
