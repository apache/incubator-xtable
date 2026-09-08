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

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.HoodieStorageUtils;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;

import org.apache.xtable.model.storage.TableFormat;

/**
 * Creates the Hudi side of a managed table before its first write: {@code .hoodie/} with the
 * Iceberg table format and the catalog the format has to publish to. The catalog settings are
 * written with {@link HoodieTableConfig#update} because the table builder keeps only a fixed set of
 * keys, and they must be in place before the first commit fires the format hook.
 */
final class HudiTableInitializer {
  private static final Logger LOG = LoggerFactory.getLogger(HudiTableInitializer.class);

  private HudiTableInitializer() {}

  static void ensureInitialized(Configuration hadoopConf, HudiTableContext context)
      throws IOException {
    StorageConfiguration<?> storageConf = HadoopFSUtils.getStorageConfWithCopy(hadoopConf);
    StoragePath basePath = new StoragePath(context.getBasePath());
    HoodieStorage storage = HoodieStorageUtils.getStorage(basePath, storageConf);
    StoragePath metaPath = new StoragePath(basePath, HoodieTableMetaClient.METAFOLDER_NAME);
    StoragePath propertiesPath =
        new StoragePath(metaPath, HoodieTableConfig.HOODIE_PROPERTIES_FILE);
    if (storage.exists(propertiesPath)) {
      return;
    }
    LOG.info(
        "Initializing Hudi table for Iceberg table {} at {}", context.getTableName(), basePath);
    HoodieTableMetaClient.TableBuilder builder =
        HoodieTableMetaClient.newTableBuilder()
            .setTableType(context.tableType())
            .setTableName(context.getTableName())
            .setTableFormat(TableFormat.ICEBERG)
            .setHiveStylePartitioningEnable(true)
            .setUrlEncodePartitioning(false)
            .setPartitionFields(String.join(",", context.getPartitionFields()));
    if (context.databaseName() != null) {
      builder.setDatabaseName(context.databaseName());
    }
    if (context.isKeyed()) {
      builder
          .setRecordKeyFields(String.join(",", context.getRecordKeyFields()))
          .setKeyGeneratorClassProp(context.keyGeneratorClass());
    }
    if (context.isMergeOnRead()) {
      builder.setRecordMergeMode(RecordMergeMode.COMMIT_TIME_ORDERING);
      if (context.getOrderingField() != null) {
        LOG.warn(
            "Ignoring {}={} on merge-on-read table {}: deletion-vector updates use commit-time ordering",
            HudiIcebergConf.TABLE_PROP_ORDERING_FIELD,
            context.getOrderingField(),
            context.getTableName());
      }
    } else if (context.getOrderingField() != null) {
      builder.setOrderingFields(context.getOrderingField());
    }
    builder.initTable(storageConf, basePath);
    Properties formatProps = context.formatConfig().toProperties();
    HoodieTableConfig.update(storage, metaPath, formatProps);
  }
}
