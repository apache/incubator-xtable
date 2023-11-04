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
 
package io.onetable.client;

import java.util.List;

import javax.annotation.Nonnull;

import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

import org.apache.hadoop.fs.Path;

import com.google.common.base.Preconditions;

import io.onetable.hudi.HudiSourceConfig;
import io.onetable.iceberg.IcebergCatalogConfig;
import io.onetable.model.storage.TableFormat;
import io.onetable.model.sync.SyncMode;

/** Represents input configuration to the sync process. */
@Value
public class PerTableConfig {
  /** table base path in local file system or HDFS or object stores like S3, GCS etc. */
  @Nonnull String tableBasePath;

  /** The name of the table */
  @Nonnull String tableName;

  /** The namespace of the table (optional) */
  String[] namespace;

  /**
   * HudiSourceConfig is a config that allows us to infer partition values for hoodie source tables.
   * If the table is not partitioned, leave it blank. If it is partitioned, you can specify a spec
   * with a comma separated list with format path:type:format.
   *
   * <p><ui>
   * <li>partitionSpecExtractorClass: class to extract partition fields from the given
   *     spec.ConfigurationBasedPartitionSpecExtractor is the default class
   * <li>partitionFieldSpecConfig: path:type:format spec to infer partition values </ui>
   *
   *     <ul>
   *       <li>path: is a dot separated path to the partition field
   *       <li>type: describes how the partition value was generated from the column value
   *           <ul>
   *             <li>VALUE: an identity transform of field value to partition value
   *             <li>YEAR: data is partitioned by a field representing a date and year granularity
   *                 is used
   *             <li>MONTH: same as YEAR but with month granularity
   *             <li>DAY: same as YEAR but with day granularity
   *             <li>HOUR: same as YEAR but with hour granularity
   *           </ul>
   *       <li>format: if your partition type is YEAR, MONTH, DAY, or HOUR specify the format for
   *           the date string as it appears in your file paths
   *     </ul>
   */
  @Nonnull HudiSourceConfig hudiSourceConfig;

  /** List of table formats to sync. */
  @Nonnull List<TableFormat> targetTableFormats;

  /** Configuration options for integrating with an existing Iceberg Catalog (optional) */
  IcebergCatalogConfig icebergCatalogConfig;

  /**
   * Mode of a sync. FULL is only supported right now.
   *
   * <ul>
   *   <li>FULL: Full sync will create a checkpoint of ALL the files relevant at a certain point in
   *       time
   *   <li>INCREMENTAL: Incremental will sync differential structures to bring the table state from
   *       and to points in the timeline
   * </ul>
   */
  @Nonnull SyncMode syncMode;

  /**
   * The retention for metadata or versions of the table in the target systems to bound the size of
   * any metadata tracked in the target system. Specified in hours.
   */
  int targetMetadataRetentionInHours;

  @Builder
  PerTableConfig(
      @NonNull String tableBasePath,
      @NonNull String tableName,
      String[] namespace,
      HudiSourceConfig hudiSourceConfig,
      @NonNull List<TableFormat> targetTableFormats,
      IcebergCatalogConfig icebergCatalogConfig,
      SyncMode syncMode,
      Integer targetMetadataRetentionInHours) {
    // sanitize source path
    Path path = new Path(tableBasePath);
    Preconditions.checkArgument(path.isAbsolute(), "Table base path must be absolute");
    if (path.isAbsoluteAndSchemeAuthorityNull()) {
      // assume this is local file system and append scheme
      this.tableBasePath = "file:/" + path;
    } else {
      this.tableBasePath = path.toString();
    }
    this.tableName = tableName;
    this.namespace = namespace;
    this.hudiSourceConfig =
        hudiSourceConfig == null ? HudiSourceConfig.builder().build() : hudiSourceConfig;
    Preconditions.checkArgument(
        targetTableFormats.size() > 0, "At least one target table format must be specified");
    this.targetTableFormats = targetTableFormats;
    this.icebergCatalogConfig = icebergCatalogConfig;
    this.syncMode = syncMode == null ? SyncMode.FULL : syncMode;
    this.targetMetadataRetentionInHours =
        targetMetadataRetentionInHours == null ? 24 * 7 : targetMetadataRetentionInHours;
  }
}
