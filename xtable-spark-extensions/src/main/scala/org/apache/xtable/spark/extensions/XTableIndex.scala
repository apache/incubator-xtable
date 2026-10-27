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
 
package org.apache.xtable.spark.extensions

import java.util.{Collections, Properties}

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import org.apache.hudi.common.table.HoodieTableMetaClient

import org.apache.xtable.conversion.{
  ConversionConfig,
  ConversionController,
  SourceTable,
  TargetTable
}
import org.apache.xtable.hudi.HudiTargetConfig
import org.apache.xtable.hudi.index.HudiBackedSecondaryIndexReader
import org.apache.xtable.iceberg.IcebergConversionSourceProvider
import org.apache.xtable.model.storage.TableFormat
import org.apache.xtable.model.sync.SyncStatusCode

/**
 * The XTable index of an Iceberg table. The index is a Hudi table at the data path of the Iceberg
 * table, built and kept current by a sync to a Hudi target with a secondary index on the column of
 * the index definition, and read by [[HudiBackedSecondaryIndexReader]].
 */
case class XTableIndex(spark: SparkSession, sourceTable: SourceTable, definition: IndexDefinition) {

  def reader: HudiBackedSecondaryIndexReader =
    new HudiBackedSecondaryIndexReader(sourceTable, new IcebergConversionSourceProvider, spark)

  /** Syncs the index with the current snapshot of the table, and builds it if it does not exist. */
  def sync(): Unit = {
    val properties = new Properties()
    definition.options.foreach { case (key, value) =>
      properties.setProperty(key, value)
    }
    properties.setProperty(HudiTargetConfig.HUDI_TABLE_VERSION, "9")
    properties.setProperty(
      HudiTargetConfig.EXECUTION_ENGINE,
      HudiTargetConfig.ExecutionEngine.SPARK.getConfigValue)
    properties.setProperty(HudiTargetConfig.SECONDARY_INDEX_COLUMN, definition.column)
    val targetTable = TargetTable
      .builder()
      .name(sourceTable.getName)
      .basePath(sourceTable.getDataPath)
      .formatName(TableFormat.HUDI)
      .additionalProperties(properties)
      .build()
    val conversionConfig = ConversionConfig
      .builder()
      .sourceTable(sourceTable)
      .targetTables(Collections.singletonList(targetTable))
      .build()
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val sourceProvider = new IcebergConversionSourceProvider
    sourceProvider.init(hadoopConf)
    val syncResult = new ConversionController(hadoopConf)
      .sync(conversionConfig, sourceProvider)
      .get(TableFormat.HUDI)
    // no result means that the table has no commit the index has not seen
    if (syncResult == null) {
      if (!reader.doesIndexExist(definition.column)) {
        throw new IllegalStateException(
          s"The index ${definition.name} of ${sourceTable.getBasePath} was not built")
      }
    } else if (syncResult.getTableFormatSyncStatus.getStatusCode != SyncStatusCode.SUCCESS) {
      throw new IllegalStateException(
        s"Failed to sync the index ${definition.name} of ${sourceTable.getBasePath}: " +
          syncResult.getErrorMessage)
    }
  }

  /**
   * Drops the index by deleting the Hudi metadata that holds it. The Iceberg data files are kept,
   * and a later sync builds the index again from all files of the table, also on another column.
   */
  def drop(): Unit = {
    val metaPath = new Path(sourceTable.getDataPath, HoodieTableMetaClient.METAFOLDER_NAME)
    val fs = metaPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (fs.exists(metaPath) && !fs.delete(metaPath, true)) {
      throw new IllegalStateException(s"Failed to delete $metaPath")
    }
  }
}
