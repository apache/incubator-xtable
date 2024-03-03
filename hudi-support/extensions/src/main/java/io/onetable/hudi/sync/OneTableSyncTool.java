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
package io.onetable.hudi.sync;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.sync.common.HoodieSyncConfig;
import org.apache.hudi.sync.common.HoodieSyncTool;

import io.onetable.client.OneTableClient;
import io.onetable.client.PerTableConfig;
import io.onetable.client.PerTableConfigImpl;
import io.onetable.hudi.HudiSourceClientProvider;
import io.onetable.hudi.HudiSourceConfigImpl;
import io.onetable.model.schema.PartitionTransformType;
import io.onetable.model.sync.SyncMode;
import io.onetable.model.sync.SyncResult;

/** A HoodieSyncTool for syncing a Hudi table to other formats (Delta and Iceberg) with OneTable. */
public class OneTableSyncTool extends HoodieSyncTool {
  private final OneTableSyncConfig config;
  private final HudiSourceClientProvider hudiSourceClientProvider;

  public OneTableSyncTool(Properties props, Configuration hadoopConf) {
    super(props, hadoopConf);
    this.config = new OneTableSyncConfig(props);
    this.hudiSourceClientProvider = new HudiSourceClientProvider();
    hudiSourceClientProvider.init(hadoopConf, Collections.emptyMap());
  }

  @Override
  public void syncHoodieTable() {
    List<String> formatsToSync =
        Arrays.stream(config.getString(OneTableSyncConfig.ONE_TABLE_FORMATS).split(","))
            .map(format -> format.toUpperCase())
            .collect(Collectors.toList());
    String basePath = config.getString(HoodieSyncConfig.META_SYNC_BASE_PATH);
    String tableName = config.getString(HoodieTableConfig.HOODIE_TABLE_NAME_KEY);
    PerTableConfig perTableConfig =
        PerTableConfigImpl.builder()
            .tableName(tableName)
            .tableBasePath(basePath)
            .targetTableFormats(formatsToSync)
            .hudiSourceConfig(
                HudiSourceConfigImpl.builder()
                    .partitionFieldSpecConfig(getPartitionSpecConfig())
                    .build())
            .syncMode(SyncMode.INCREMENTAL)
            .targetMetadataRetentionInHours(
                config.getInt(OneTableSyncConfig.ONE_TABLE_TARGET_METADATA_RETENTION_HOURS))
            .build();
    Map<String, SyncResult> results =
        new OneTableClient(hadoopConf).sync(perTableConfig, hudiSourceClientProvider);
    String failingFormats =
        results.entrySet().stream()
            .filter(
                entry ->
                    entry.getValue().getStatus().getStatusCode()
                        != SyncResult.SyncStatusCode.SUCCESS)
            .map(entry -> entry.getKey().toString())
            .collect(Collectors.joining(","));
    if (!failingFormats.isEmpty()) {
      throw new HoodieException("Unable to sync to OneTable for formats: " + failingFormats);
    }
  }

  private String getPartitionSpecConfig() {
    String partitionPathFields =
        config.getStringOrDefault(KeyGeneratorOptions.PARTITIONPATH_FIELD_NAME, "");
    String timestampOutputFormat =
        config.getString(KeyGeneratorOptions.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP);
    return Arrays.stream(partitionPathFields.split(","))
        .map(
            partitionPathField -> {
              String[] parts = partitionPathField.split(":");
              if (StringUtils.isEmpty(parts[0])) {
                return "";
              }
              if (parts.length == 1 || parts[1].equalsIgnoreCase("SIMPLE")) {
                return parts[0] + ":" + PartitionTransformType.VALUE;
              } else {
                PartitionTransformType type =
                    getPartitionTransformTypeFromFormat(timestampOutputFormat);
                return parts[0] + ":" + type + ":" + timestampOutputFormat;
              }
            })
        .collect(Collectors.joining(","));
  }

  /**
   * Determines the granularity of a date based partition
   *
   * @param timestampOutputFormat format specified for the Hudi partition value
   * @return {@link PartitionTransformType} for the provided format
   */
  private PartitionTransformType getPartitionTransformTypeFromFormat(String timestampOutputFormat) {
    if (timestampOutputFormat.contains("HH")) {
      return PartitionTransformType.HOUR;
    } else if (timestampOutputFormat.contains("dd")) {
      return PartitionTransformType.DAY;
    } else if (timestampOutputFormat.contains("MM")) {
      return PartitionTransformType.MONTH;
    } else {
      return PartitionTransformType.YEAR;
    }
  }
}
