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

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.exception.TableNotFoundException;

import org.apache.xtable.exception.UpdateException;
import org.apache.xtable.model.InternalTable;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalPartitionField;
import org.apache.xtable.model.storage.DataLayoutStrategy;

/** A class used to initialize new Hudi tables and load the metadata of existing tables. */
@Log4j2
@RequiredArgsConstructor(staticName = "of")
class HudiTableManager {
  private static final String NONPARTITIONED_KEY_GENERATOR =
      "org.apache.hudi.keygen.NonpartitionedKeyGenerator";
  private static final String CUSTOM_KEY_GENERATOR = "org.apache.hudi.keygen.CustomKeyGenerator";
  private static final String TIMESTAMP_BASED_KEY_GENERATOR =
      "org.apache.hudi.keygen.TimestampBasedKeyGenerator";
  private static final String COMPLEX_KEY_GENERATOR = "org.apache.hudi.keygen.ComplexKeyGenerator";
  private static final String SIMPLE_KEY_GENERATOR = "org.apache.hudi.keygen.SimpleKeyGenerator";

  private final Configuration configuration;

  /**
   * Loads the meta client for the table at the base path if it exists
   *
   * @param tableDataPath the path for the table
   * @return {@link HoodieTableMetaClient} if table exists, otherwise null
   */
  Optional<HoodieTableMetaClient> loadTableMetaClientIfExists(String tableDataPath) {
    try {
      return Optional.of(
          HoodieTableMetaClient.builder()
              .setBasePath(tableDataPath)
              .setConf(configuration)
              .setLoadActiveTimelineOnLoad(false)
              .build());
    } catch (TableNotFoundException ex) {
      log.info("Hudi table does not exist, will be created on first sync");
      return Optional.empty();
    }
  }

  /**
   * Initializes a Hudi table with properties matching the provided {@link InternalTable}
   *
   * @param tableDataPath the base path for the data files in the table
   * @param table the table to initialize
   * @return {@link HoodieTableMetaClient} for the table that was created
   */
  HoodieTableMetaClient initializeHudiTable(String tableDataPath, InternalTable table) {
    String recordKeyField = "";
    if (table.getReadSchema() != null) {
      List<String> recordKeys =
          table.getReadSchema().getRecordKeyFields().stream()
              .map(InternalField::getName)
              .collect(Collectors.toList());
      if (!recordKeys.isEmpty()) {
        recordKeyField = String.join(",", recordKeys);
      }
    }
    String keyGeneratorClass;
    keyGeneratorClass =
        getKeyGeneratorClass(
            table.getPartitioningFields(), table.getReadSchema().getRecordKeyFields());
    boolean hiveStylePartitioningEnabled =
        DataLayoutStrategy.HIVE_STYLE_PARTITION == table.getLayoutStrategy();
    try {
      return HoodieTableMetaClient.withPropertyBuilder()
          .setCommitTimezone(HoodieTimelineTimeZone.UTC)
          .setHiveStylePartitioningEnable(hiveStylePartitioningEnabled)
          .setTableType(HoodieTableType.COPY_ON_WRITE)
          .setTableName(table.getName())
          .setPayloadClass(HoodieAvroPayload.class)
          .setRecordKeyFields(recordKeyField)
          .setKeyGeneratorClassProp(keyGeneratorClass)
          // other formats will not populate meta fields, so we disable it for consistency
          .setPopulateMetaFields(false)
          .setPartitionFields(
              table.getPartitioningFields().stream()
                  .map(InternalPartitionField::getSourceField)
                  .map(InternalField::getPath)
                  .collect(Collectors.joining(",")))
          .initTable(configuration, tableDataPath);
    } catch (IOException ex) {
      throw new UpdateException("Unable to initialize Hudi table", ex);
    }
  }

  @VisibleForTesting
  static String getKeyGeneratorClass(
      List<InternalPartitionField> partitionFields, List<InternalField> recordKeyFields) {
    boolean multipleRecordKeyFields = recordKeyFields.size() > 1;
    boolean multiplePartitionFields = partitionFields.size() > 1;
    String keyGeneratorClass;
    if (partitionFields.isEmpty()) {
      keyGeneratorClass = NONPARTITIONED_KEY_GENERATOR;
    } else {
      if (partitionFields.stream()
          .anyMatch(partitionField -> partitionField.getTransformType().isTimeBased())) {
        if (multiplePartitionFields) {
          // if there is more than one partition field and one of them is a date, we need to use
          // CustomKeyGenerator
          keyGeneratorClass = CUSTOM_KEY_GENERATOR;
        } else {
          // if there is only one partition field and it is a date, we can use
          // TimestampBasedKeyGenerator
          keyGeneratorClass = TIMESTAMP_BASED_KEY_GENERATOR;
        }
      } else {
        keyGeneratorClass =
            multipleRecordKeyFields || multiplePartitionFields
                ? COMPLEX_KEY_GENERATOR
                : SIMPLE_KEY_GENERATOR;
      }
    }
    return keyGeneratorClass;
  }
}
