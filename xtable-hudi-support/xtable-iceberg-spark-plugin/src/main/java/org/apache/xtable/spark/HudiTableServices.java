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
 
package org.apache.xtable.spark;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hudi.client.SparkRDDWriteClient;
import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieClusteringConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.table.action.HoodieWriteMetadata;

/** Hudi table services run on behalf of Iceberg maintenance procedures. */
final class HudiTableServices {
  private static final Logger LOG = LoggerFactory.getLogger(HudiTableServices.class);

  private HudiTableServices() {}

  private static HoodieWriteConfig writeConfig(
      SparkSession spark,
      HudiTableContext context,
      HoodieClusteringConfig clustering,
      Map<String, String> overrides)
      throws Exception {
    StorageConfiguration<?> storageConf =
        HadoopFSUtils.getStorageConfWithCopy(spark.sessionState().newHadoopConf());
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder()
            .setConf(storageConf)
            .setBasePath(context.getBasePath())
            .build();
    String schema = new TableSchemaResolver(metaClient).getTableSchema().toAvroSchema().toString();
    Map<String, String> props = new HashMap<>(context.writeParams(HudiWriteOperation.APPEND, null));
    props.remove("path");
    props.remove("hoodie.datasource.write.operation");
    props.putAll(overrides);
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder()
            .withPath(context.getBasePath())
            .forTable(context.getTableName())
            .withSchema(schema)
            .withProps(props);
    if (clustering != null) {
      builder.withClusteringConfig(clustering);
    }
    return builder.build();
  }

  /** Outcome of one clustering run, in the shape {@code rewrite_data_files} reports. */
  static final class RewriteResult {
    final int rewrittenFiles;
    final int addedFiles;
    final long rewrittenBytes;

    RewriteResult(int rewrittenFiles, int addedFiles, long rewrittenBytes) {
      this.rewrittenFiles = rewrittenFiles;
      this.addedFiles = addedFiles;
      this.rewrittenBytes = rewrittenBytes;
    }
  }

  /**
   * {@code rewrite_position_delete_files} for a managed merge-on-read table is one Hudi compaction:
   * the deletion vectors and new data files of the log are folded into new base files, and the
   * compaction commit is published as an Iceberg overwrite that drops the delete files.
   */
  static RewriteResult compact(SparkSession spark, HudiTableContext context) throws Exception {
    // An explicit maintenance call compacts whatever is pending, not only after N delta commits
    Map<String, String> overrides = new HashMap<>();
    overrides.put("hoodie.compact.inline.max.delta.commits", "1");
    overrides.put("hoodie.compact.inline.trigger.strategy", "NUM_COMMITS");
    HoodieWriteConfig config = writeConfig(spark, context, null, overrides);
    HoodieSparkEngineContext engineContext =
        new HoodieSparkEngineContext(JavaSparkContext.fromSparkContext(spark.sparkContext()));
    try (SparkRDDWriteClient<?> client = new SparkRDDWriteClient<>(engineContext, config)) {
      Option<String> instant = client.scheduleCompaction(Option.empty());
      if (!instant.isPresent()) {
        LOG.info("No compaction plan for {}: nothing to compact", context.getTableName());
        return new RewriteResult(0, 0, 0L);
      }
      HoodieWriteMetadata<?> result = client.compact(instant.get(), true);
      int added = 0;
      long bytes = 0L;
      if (result.getWriteStats().isPresent()) {
        for (HoodieWriteStat stat : result.getWriteStats().get()) {
          added++;
          bytes += stat.getTotalWriteBytes();
        }
      }
      LOG.info(
          "Compacted {} at instant {}: {} base files written",
          context.getTableName(),
          instant.get(),
          added);
      return new RewriteResult(added, added, bytes);
    }
  }

  /**
   * {@code rewrite_data_files} for a managed table is one Hudi clustering run: schedule a plan over
   * the small files and execute it. The replace commit it produces is published to Iceberg by the
   * table format like any other commit.
   */
  static RewriteResult cluster(
      SparkSession spark, HudiTableContext context, Map<String, String> options) throws Exception {
    HoodieClusteringConfig.Builder clustering =
        HoodieClusteringConfig.newBuilder().withInlineClustering(false);
    String targetFileSize = options.get("target-file-size-bytes");
    if (targetFileSize != null) {
      clustering.withClusteringTargetFileMaxBytes(Long.parseLong(targetFileSize));
    }
    HoodieWriteConfig config =
        writeConfig(spark, context, clustering.build(), Collections.emptyMap());
    HoodieSparkEngineContext engineContext =
        new HoodieSparkEngineContext(JavaSparkContext.fromSparkContext(spark.sparkContext()));
    try (SparkRDDWriteClient<?> client = new SparkRDDWriteClient<>(engineContext, config)) {
      Option<String> instant = client.scheduleClustering(Option.empty());
      if (!instant.isPresent()) {
        LOG.info("No clustering plan for {}: nothing to rewrite", context.getTableName());
        return new RewriteResult(0, 0, 0L);
      }
      HoodieWriteMetadata<?> result = client.cluster(instant.get(), true);
      int replaced = 0;
      for (List<String> fileIds : result.getPartitionToReplaceFileIds().values()) {
        replaced += fileIds.size();
      }
      int added = 0;
      long bytes = 0L;
      if (result.getWriteStats().isPresent()) {
        for (HoodieWriteStat stat : result.getWriteStats().get()) {
          added++;
          bytes += stat.getTotalWriteBytes();
        }
      }
      LOG.info(
          "Clustered {} at instant {}: {} files replaced by {}",
          context.getTableName(),
          instant.get(),
          replaced,
          added);
      return new RewriteResult(replaced, added, bytes);
    }
  }
}
