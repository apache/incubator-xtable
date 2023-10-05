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
 
package io.onetable.hudi;

import java.io.IOException;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.TableNotFoundException;

import io.onetable.exception.OneIOException;
import io.onetable.model.OneTable;
import io.onetable.model.schema.OneField;
import io.onetable.model.schema.OnePartitionField;

/** A class used to create or read Hudi tables. */
@Slf4j
@RequiredArgsConstructor(staticName = "of")
class HudiTableManager {
  private final Configuration configuration;

  /**
   * Loads the meta client for the table at the base path if it exists
   *
   * @param basePath the path for the table
   * @return {@link HoodieTableMetaClient} if table exists, otherwise null
   */
  HoodieTableMetaClient loadTableIfExists(String basePath) {
    try {
      return HoodieTableMetaClient.builder()
          .setBasePath(basePath)
          .setConf(configuration)
          .setLoadActiveTimelineOnLoad(false)
          .build();
    } catch (TableNotFoundException ex) {
      log.info("Hudi table does not exist, will be created on first sync");
      return null;
    }
  }

  /**
   * Initializes a Hudi table with properties matching the provided {@link OneTable}
   *
   * @param table the table to initialize
   * @return {@link HoodieTableMetaClient} for the table that was created
   */
  HoodieTableMetaClient initializeHudiTable(OneTable table) {
    try {
      return HoodieTableMetaClient.withPropertyBuilder()
          .setTableType(HoodieTableType.COPY_ON_WRITE)
          .setTableName(table.getName())
          .setPayloadClass(HoodieAvroPayload.class)
          .setRecordKeyFields("") // TODO
          // other formats will not populate meta fields, so we disable it for consistency
          .setPopulateMetaFields(false)
          .setPartitionFields(
              table.getPartitioningFields().stream()
                  .map(OnePartitionField::getSourceField)
                  .map(OneField::getPath)
                  .collect(Collectors.joining(",")))
          .initTable(configuration, table.getBasePath());
    } catch (IOException ex) {
      throw new OneIOException("Unable to initialize Hudi table", ex);
    }
  }
}
