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

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX;

import java.util.Collections;
import java.util.Properties;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.InternalRow;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodieData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Partitioning;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;

import org.apache.xtable.catalog.TableFormatUtils;
import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.hudi.HudiTargetConfig;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.model.storage.TableFormat;

/**
 * Secondary index for an Iceberg table backed by the Hudi metadata table. The index is built by
 * syncing the Iceberg table to Hudi with the record index and a secondary index enabled; the Hudi
 * metadata lives under {@code <data location>/.hoodie/metadata}. Because the Iceberg data files
 * carry no Hudi record keys, the record key of a row is {@code <file path relative to the data
 * location>_<row position>}, which a lookup resolves back to a file path and row position.
 */
@Log4j2
public class HudiBackedIcebergSecondaryIndex implements Index<Table> {
  private final String tableLocation;
  private final String dataPath;
  private final Properties targetTableProperties;
  private final JavaSparkContext javaSparkContext;
  private final HoodieSparkEngineContext engineContext;

  /**
   * @param icebergTable the table to index
   * @param sparkSession the Spark session used to build and query the index
   * @param targetTableProperties additional {@link HudiTargetConfig} properties for the sync, for
   *     example the record index file group counts
   */
  public HudiBackedIcebergSecondaryIndex(
      Table icebergTable, SparkSession sparkSession, Properties targetTableProperties) {
    this.tableLocation = icebergTable.location();
    this.dataPath =
        TableFormatUtils.getTableDataLocation(
            TableFormat.ICEBERG, tableLocation, icebergTable.properties());
    this.targetTableProperties = targetTableProperties;
    this.javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
    this.engineContext = new HoodieSparkEngineContext(javaSparkContext);
  }

  @Override
  public boolean doesIndexExist(String columnName) {
    return HoodieTableMetadataUtil.metadataPartitionExists(
        dataPath, engineContext, PARTITION_NAME_SECONDARY_INDEX_PREFIX + columnName);
  }

  @Override
  public void syncIndex(Table icebergTable, String columnName) {
    Configuration configuration = javaSparkContext.hadoopConfiguration();
    IcebergConversionSourceProvider sourceProvider = new IcebergConversionSourceProvider();
    sourceProvider.init(configuration);
    new ConversionController(configuration)
        .sync(getConversionConfig(icebergTable, columnName), sourceProvider);
    log.info("Synced secondary index for column {} of table {}", columnName, tableLocation);
  }

  private ConversionConfig getConversionConfig(Table icebergTable, String columnName) {
    SourceTable sourceTable =
        SourceTable.builder()
            .name(icebergTable.name())
            .basePath(tableLocation)
            .dataPath(dataPath)
            .formatName(TableFormat.ICEBERG)
            .build();
    Properties mergedProperties = new Properties();
    mergedProperties.putAll(targetTableProperties);
    mergedProperties.setProperty(HudiTargetConfig.SECONDARY_INDEX_COLUMN, columnName);
    mergedProperties.setProperty(
        HudiTargetConfig.EXECUTION_ENGINE, HudiTargetConfig.EXECUTION_ENGINE_SPARK);
    // the secondary index is only available from Hudi table version 8 onwards
    mergedProperties.setProperty(
        HudiTargetConfig.HUDI_TABLE_VERSION, String.valueOf(HoodieTableVersion.NINE.versionCode()));
    TargetTable targetTable =
        TargetTable.builder()
            .name(icebergTable.name())
            .basePath(dataPath)
            .formatName(TableFormat.HUDI)
            .additionalProperties(mergedProperties)
            .build();
    return ConversionConfig.builder()
        .sourceTable(sourceTable)
        .targetTables(Collections.singletonList(targetTable))
        .build();
  }

  @Override
  public RDD<IndexLookupResult> lookup(Table icebergTable, RDD<String> keys, String columnName) {
    Types.StructType partitionType = Partitioning.partitionType(icebergTable);
    PartitionSpec spec = icebergTable.spec();
    HoodieStorage storage =
        new HoodieHadoopStorage(dataPath, javaSparkContext.hadoopConfiguration());
    HoodieTableMetaClient metaClient =
        HoodieTableMetaClient.builder().setStorage(storage).setBasePath(dataPath).build();
    HoodieMetadataConfig metadataConfig =
        HoodieMetadataConfig.newBuilder().enable(true).withSecondaryIndexEnabled(true).build();
    try (HoodieBackedTableMetadata tableMetadata =
        (HoodieBackedTableMetadata)
            metaClient
                .getTableFormat()
                .getMetadataFactory()
                .create(engineContext, storage, metadataConfig, dataPath)) {
      HoodiePairData<String, String> recordKeysBySecondaryKey =
          tableMetadata.readSecondaryIndexDataTableRecordKeysWithKeys(
              HoodieJavaRDD.of(keys.toJavaRDD()),
              PARTITION_NAME_SECONDARY_INDEX_PREFIX + columnName);
      // capture the field so the closure does not serialize the index instance
      String dataPath = this.dataPath;
      HoodieData<IndexLookupResult> lookupResults =
          recordKeysBySecondaryKey.map(
              recordKeyBySecondaryKey ->
                  toLookupResult(
                      dataPath,
                      recordKeyBySecondaryKey.getKey(),
                      recordKeyBySecondaryKey.getValue(),
                      partitionType,
                      spec));
      return HoodieJavaRDD.getJavaRDD(lookupResults).rdd();
    }
  }

  private static IndexLookupResult toLookupResult(
      String dataPath,
      String secondaryKey,
      String recordKey,
      Types.StructType partitionType,
      PartitionSpec spec) {
    // record keys generated by Hudi for files without record keys are "<relative path>_<row
    // position>"
    int positionSeparator = recordKey.lastIndexOf('_');
    if (positionSeparator == -1) {
      throw new IllegalArgumentException(
          "Expected a record key of the form <file path>_<row position> but got " + recordKey);
    }
    String relativeFilePath = recordKey.substring(0, positionSeparator);
    int partitionSeparator = relativeFilePath.lastIndexOf('/');
    String partitionPath =
        partitionSeparator == -1 ? "" : relativeFilePath.substring(0, partitionSeparator);
    InternalRow partitionRow =
        IcebergPartitionConverter.convertPartitionToInternalRow(partitionPath, partitionType, spec);
    return IndexLookupResult.builder()
        .key(secondaryKey)
        .file(
            dataPath.endsWith("/")
                ? dataPath + relativeFilePath
                : dataPath + "/" + relativeFilePath)
        .position(Long.parseLong(recordKey.substring(positionSeparator + 1)))
        .partition(partitionRow)
        .build();
  }
}
