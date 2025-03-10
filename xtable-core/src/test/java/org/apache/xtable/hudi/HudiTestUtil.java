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

import static org.apache.hudi.index.HoodieIndex.IndexType.INMEMORY;

import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.Value;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieColumnRangeMetadata;
import org.apache.hudi.common.model.HoodieDeltaWriteStat;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieTimelineTimeZone;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.ExternalFilePathUtil;
import org.apache.hudi.config.HoodieArchivalConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class HudiTestUtil {

  @SneakyThrows
  static HoodieTableMetaClient initTableAndGetMetaClient(
      String tableBasePath, String partitionFields) {
    return HoodieTableMetaClient.withPropertyBuilder()
        .setCommitTimezone(HoodieTimelineTimeZone.UTC)
        .setTableType(HoodieTableType.COPY_ON_WRITE)
        .setTableName("test_table")
        .setPayloadClass(HoodieAvroPayload.class)
        .setPartitionFields(partitionFields)
        .initTable(new Configuration(), tableBasePath);
  }

  public static HoodieWriteConfig getHoodieWriteConfig(HoodieTableMetaClient metaClient) {
    return getHoodieWriteConfig(metaClient, null);
  }

  static HoodieWriteConfig getHoodieWriteConfig(HoodieTableMetaClient metaClient, Schema schema) {
    Properties properties = new Properties();
    properties.setProperty(HoodieMetadataConfig.AUTO_INITIALIZE.key(), "false");
    return HoodieWriteConfig.newBuilder()
        .withSchema(schema == null ? "" : schema.toString())
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

  public static SparkConf getSparkConf(Path tempDir) {
    return new SparkConf()
        .setAppName("xtable-testing")
        .set("spark.serializer", KryoSerializer.class.getName())
        .set("spark.sql.catalog.default_iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .set("spark.sql.catalog.default_iceberg.type", "hadoop")
        .set("spark.sql.catalog.default_iceberg.warehouse", tempDir.toString())
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("parquet.avro.write-old-list-structure", "false")
        // Needed for ignoring not nullable constraints on nested columns in Delta.
        .set("spark.databricks.delta.constraints.allowUnenforcedNotNull.enabled", "true")
        .set("spark.sql.shuffle.partitions", "1")
        .set("spark.default.parallelism", "1")
        .set("spark.sql.session.timeZone", "UTC")
        .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .set("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .setMaster("local[4]");
  }

  @Value
  @AllArgsConstructor(staticName = "of")
  public static class PartitionConfig {
    String hudiConfig;
    String xTableConfig;
  }
}
