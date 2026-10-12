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
 
package org.apache.xtable.index;

import java.util.Collections;
import java.util.Properties;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;

import org.apache.hudi.common.table.HoodieTableVersion;

import com.google.common.base.Preconditions;

import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.hudi.HudiTargetConfig;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.model.sync.SyncStatusCode;

/**
 * Builds and maintains a secondary index for a table of any source format that XTable reads, backed
 * by the Hudi metadata table. {@link HudiBackedSecondaryIndexReader} reads the index.
 *
 * <p>The index is built by syncing the table to Hudi with the record index and a secondary index
 * enabled; the Hudi metadata lives under {@code <data path>/.hoodie/metadata}, where the data path
 * is {@link SourceTable#getDataPath()}. Because the data files carry no Hudi record keys, the
 * record key of a row is {@code <file path relative to the data path>_<row position>}. Hudi
 * requires every data file to be under the data path, so files outside it are not supported.
 *
 * <p>The parts that depend on the source format are parameters: the {@link
 * ConversionSourceProvider} reads the table, its schema gives the type of the indexed column, and
 * the caller sets the data path of the {@link SourceTable}, for example with {@link
 * org.apache.xtable.catalog.TableFormatUtils#getTableDataLocation} for an Iceberg table. Hudi
 * tables are not supported as a source, because they have their own metadata table.
 *
 * <p>The sync runs Hudi's commit on its Spark engine, which reuses the active Spark session or
 * starts one, because Hudi's Java engine cannot build the secondary index yet. The API of this
 * class does not depend on Spark.
 */
@Log4j2
public class HudiBackedSecondaryIndexWriter implements IndexWriter<SourceTable> {
  private final String tableLocation;
  private final String dataPath;
  private final SourceTable sourceTable;
  private final ConversionSourceProvider<?> sourceProvider;
  private final Configuration configuration;
  private final Properties targetTableProperties;
  private final String indexedColumn;

  /**
   * @param sourceTable the table to index. A table of a catalog sets its catalog config, name and
   *     namespace. The Hudi table is created under its data path
   * @param sourceProvider the provider of the conversion source for the format of the table. The
   *     index initializes it with the given configuration
   * @param configuration the Hadoop configuration used to read the table and write the index
   * @param targetTableProperties {@link HudiTargetConfig} properties for the sync. {@link
   *     HudiTargetConfig#SECONDARY_INDEX_COLUMN} names the column to index, and other properties
   *     such as the record index file group counts are optional
   */
  public HudiBackedSecondaryIndexWriter(
      SourceTable sourceTable,
      ConversionSourceProvider<?> sourceProvider,
      Configuration configuration,
      Properties targetTableProperties) {
    Preconditions.checkArgument(
        !TableFormat.HUDI.equals(sourceTable.getFormatName()),
        "A Hudi table has its own metadata table and cannot be the source of the index");
    this.sourceTable = sourceTable;
    this.tableLocation = sourceTable.getBasePath();
    this.dataPath = sourceTable.getDataPath();
    this.configuration = configuration;
    this.targetTableProperties = getIndexTargetProperties(targetTableProperties);
    this.indexedColumn =
        HudiTargetConfig.fromProperties(this.targetTableProperties)
            .getSecondaryIndexColumn()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        "Set "
                            + HudiTargetConfig.SECONDARY_INDEX_COLUMN
                            + " to the column to index"));
    this.sourceProvider = sourceProvider;
    sourceProvider.init(configuration);
    HudiBackedSecondaryIndexUtils.getIndexableField(
        HudiBackedSecondaryIndexUtils.readSchema(sourceProvider, sourceTable), indexedColumn);
  }

  /**
   * Returns the given {@link HudiTargetConfig} properties with the settings the index requires: the
   * Spark engine and table version 9.
   */
  private static Properties getIndexTargetProperties(Properties targetTableProperties) {
    Properties indexTargetProperties = new Properties();
    indexTargetProperties.putAll(targetTableProperties);
    indexTargetProperties.setProperty(
        HudiTargetConfig.EXECUTION_ENGINE, HudiTargetConfig.ExecutionEngine.SPARK.getConfigValue());
    // the secondary index is only available from Hudi table version 8 onwards
    indexTargetProperties.setProperty(
        HudiTargetConfig.HUDI_TABLE_VERSION, String.valueOf(HoodieTableVersion.NINE.versionCode()));
    return indexTargetProperties;
  }

  /**
   * Syncs the table to Hudi up to the latest commit at the start of the sync. The sync applies the
   * new commits incrementally, or syncs a full snapshot when an incremental sync is not safe. It
   * writes the record index and the secondary index of the configured column together with the Hudi
   * table. A sync with no new commit changes nothing.
   *
   * @throws IllegalStateException when the sync fails, or when the index does not exist after it,
   *     for example because the indexed column changed
   */
  @Override
  public void syncIndex(SourceTable table) {
    HudiBackedSecondaryIndexUtils.checkIndexedTable(tableLocation, table);
    // The sync reports a failed target in its result instead of throwing. It returns no result
    // for the target when the table has no new commits.
    SyncResult syncResult =
        new ConversionController(configuration)
            .sync(getConversionConfig(), sourceProvider)
            .get(TableFormat.HUDI);
    if (syncResult != null
        && (syncResult.getTableFormatSyncStatus() == null
            || syncResult.getTableFormatSyncStatus().getStatusCode() != SyncStatusCode.SUCCESS)) {
      throw new IllegalStateException(
          "Failed to sync the secondary index of column "
              + indexedColumn
              + " of table "
              + tableLocation
              + ": "
              + syncResult.getErrorMessage());
    }
    // A sync without a new snapshot returns no result, so check that the index exists.
    if (!HudiBackedSecondaryIndexUtils.getIndexedColumns(dataPath, configuration)
        .contains(indexedColumn)) {
      throw new IllegalStateException(
          "The secondary index of column "
              + indexedColumn
              + " of table "
              + tableLocation
              + " does not exist. The index is built by the first sync of a table with a snapshot,"
              + " and changing the indexed column of an existing index is not supported yet.");
    }
    log.info("Synced the secondary index of column {} of table {}", indexedColumn, tableLocation);
  }

  private ConversionConfig getConversionConfig() {
    TargetTable targetTable =
        TargetTable.builder()
            .name(sourceTable.getName())
            .basePath(dataPath)
            .formatName(TableFormat.HUDI)
            .additionalProperties(targetTableProperties)
            .build();
    return ConversionConfig.builder()
        .sourceTable(sourceTable)
        .targetTables(Collections.singletonList(targetTable))
        .build();
  }
}
