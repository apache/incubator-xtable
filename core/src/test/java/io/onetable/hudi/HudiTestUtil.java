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

import static org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY;

import java.util.Map;
import java.util.Properties;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;

import io.onetable.model.schema.SchemaVersion;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HudiTestUtil {
  static final SchemaVersion SCHEMA_VERSION = new SchemaVersion(1, "");

  @SneakyThrows
  static HoodieTableMetaClient initTableAndGetMetaClient(
      String tableBasePath, String partitionFields) {
    return HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName("test_table")
        .setPayloadClass(HoodieAvroPayload.class)
        .setPartitionFields(partitionFields)
        .initTable(new Configuration(), tableBasePath);
  }

  static HoodieWriteConfig getHoodieWriteConfig(HoodieTableMetaClient metaClient) {
    Properties properties = new Properties();
    properties.setProperty(HoodieMetadataConfig.AUTO_INITIALIZE.key(), "false");
    return HoodieWriteConfig.newBuilder()
        .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(INMEMORY).build())
        .withPath(metaClient.getBasePathV2().toString())
        .withEmbeddedTimelineServerEnabled(false)
        .withMetadataConfig(
            HoodieMetadataConfig.newBuilder()
                .withMaxNumDeltaCommitsBeforeCompaction(2)
                .enable(true)
                .withMetadataIndexColumnStats(true)
                .withProperties(properties)
                .build())
        .withArchivalConfig(HoodieArchivalConfig.newBuilder().archiveCommitsWith(1, 2).build())
        .withTableServicesEnabled(true)
        .build();
  }

  static WriteStatus createWriteStatus(
      String fileName,
      String partitionPath,
      String commitTime,
      long recordCount,
      long fileSize,
      Map<String, HoodieColumnRangeMetadata<Comparable>> recordStats) {
    WriteStatus writeStatus = new WriteStatus();
    writeStatus.setFileId(fileName);
    writeStatus.setPartitionPath(partitionPath);
    HoodieDeltaWriteStat writeStat = new HoodieDeltaWriteStat();
    writeStat.setFileId(fileName);
    writeStat.setPartitionPath(partitionPath);
    writeStat.setPath(
        ExternalFilePathUtil.appendCommitTimeAndExternalFileMarker(
            partitionPath.isEmpty() ? fileName : String.format("%s/%s", partitionPath, fileName),
            commitTime));
    writeStat.setNumWrites(recordCount);
    writeStat.setFileSizeInBytes(fileSize);
    writeStat.setTotalWriteBytes(fileSize);
    writeStat.putRecordsStats(recordStats);
    writeStatus.setStat(writeStat);
    return writeStatus;
  }
}
