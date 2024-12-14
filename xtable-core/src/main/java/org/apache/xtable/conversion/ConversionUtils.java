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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.xtable.model.storage.TableFormat;

public class ConversionUtils {

  /**
   * Few table formats need the metadata to be located at the root level of the data files. Eg: An
   * iceberg table generated through spark will have two directories basePath/data and
   * basePath/metadata For synchronising the iceberg metadata to hudi and delta, they need to be
   * present in basePath/data/.hoodie and basePath/data/_delta_log.
   *
   * @param config conversion config for synchronizing source and target tables
   * @return updated table config.
   */
  public static ConversionConfig normalizeTargetPaths(ConversionConfig config) {
    if (!config.getSourceTable().getDataPath().equals(config.getSourceTable().getBasePath())
        && config.getSourceTable().getFormatName().equals(TableFormat.ICEBERG)) {
      List<TargetTable> updatedTargetTables =
          config.getTargetTables().stream()
              .filter(
                  targetTable ->
                      targetTable.getFormatName().equals(TableFormat.HUDI)
                          || targetTable.getFormatName().equals(TableFormat.DELTA))
              .map(
                  targetTable ->
                      targetTable.toBuilder()
                          .basePath(config.getSourceTable().getDataPath())
                          .build())
              .collect(Collectors.toList());
      return new ConversionConfig(
          config.getSourceTable(), updatedTargetTables, config.getSyncMode());
    }
    return config;
  }
}
