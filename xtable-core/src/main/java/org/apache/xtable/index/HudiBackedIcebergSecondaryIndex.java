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
import static org.apache.spark.sql.functions.col;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import lombok.extern.log4j.Log4j2;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import org.apache.iceberg.Table;

import scala.Tuple2;

import com.google.common.base.Preconditions;

import org.apache.xtable.catalog.TableFormatUtils;
import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.hudi.HudiTargetConfig;
import org.apache.xtable.iceberg.IcebergConversionSourceProvider;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.model.sync.SyncStatusCode;

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
  private final SourceTable sourceTable;
  private final Properties targetTableProperties;
  private final SparkSession sparkSession;
  private final JavaSparkContext javaSparkContext;
  private final HoodieSparkEngineContext engineContext;

  /**
   * @param icebergTable the table to index
   * @param sourceTable how XTable loads the table for a sync, because a loaded table does not
   *     expose the catalog it came from. A table of a catalog sets its catalog config, name and
   *     namespace, and a table without a catalog config is loaded from its location
   * @param sparkSession the Spark session used to build and query the index
   * @param targetTableProperties additional {@link HudiTargetConfig} properties for the sync, for
   *     example the record index file group counts
   */
  public HudiBackedIcebergSecondaryIndex(
      Table icebergTable,
      SourceTable sourceTable,
      SparkSession sparkSession,
      Properties targetTableProperties) {
    Preconditions.checkArgument(
        TableFormat.ICEBERG.equals(sourceTable.getFormatName()),
        "Expected an Iceberg source table but got format %s",
        sourceTable.getFormatName());
    this.tableLocation = icebergTable.location();
    this.dataPath =
        TableFormatUtils.getTableDataLocation(
            TableFormat.ICEBERG, tableLocation, icebergTable.properties());
    // the Hudi table lives under the data location, which Iceberg resolves from the table
    // properties
    this.sourceTable = sourceTable.toBuilder().dataPath(dataPath).build();
    this.targetTableProperties = targetTableProperties;
    this.sparkSession = sparkSession;
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
    // The sync reports a failed target in its result instead of throwing. It returns no result
    // for the target when the table has no new commits, which only means the index is current
    // when the index for the column already exists.
    SyncResult syncResult =
        new ConversionController(configuration)
            .sync(getConversionConfig(icebergTable, columnName), sourceProvider)
            .get(TableFormat.HUDI);
    if (syncResult == null) {
      if (!doesIndexExist(columnName)) {
        throw new IllegalStateException(
            "Failed to sync the secondary index for column "
                + columnName
                + " of table "
                + tableLocation
                + ": no sync result for the Hudi target and no existing index");
      }
      log.info(
          "Secondary index for column {} of table {} is already up to date",
          columnName,
          tableLocation);
      return;
    }
    if (syncResult.getTableFormatSyncStatus() == null
        || syncResult.getTableFormatSyncStatus().getStatusCode() != SyncStatusCode.SUCCESS) {
      throw new IllegalStateException(
          "Failed to sync the secondary index for column "
              + columnName
              + " of table "
              + tableLocation
              + ": "
              + syncResult.getTableFormatSyncStatus());
    }
    log.info("Synced secondary index for column {} of table {}", columnName, tableLocation);
  }

  private ConversionConfig getConversionConfig(Table icebergTable, String columnName) {
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
  public Dataset<Row> lookup(Table icebergTable, Dataset<Row> keys, String columnName) {
    StructType resultSchema =
        new StructType()
            .add(columnName, DataTypes.StringType, false)
            .add(FILE_COLUMN, DataTypes.StringType, false)
            .add(POSITION_COLUMN, DataTypes.LongType, false);
    // the secondary index stores the values of the indexed column as strings
    JavaRDD<String> secondaryKeys =
        keys.where(col(columnName).isNotNull())
            .select(col(columnName).cast(DataTypes.StringType))
            .as(Encoders.STRING())
            .toJavaRDD();
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
              HoodieJavaRDD.of(secondaryKeys), PARTITION_NAME_SECONDARY_INDEX_PREFIX + columnName);
      // capture the field so the closure does not serialize the index instance
      String dataPath = this.dataPath;
      JavaRDD<Row> lookupResults =
          toJavaPairRDD(recordKeysBySecondaryKey)
              .map(
                  recordKeyBySecondaryKey ->
                      toLookupResult(
                          dataPath, recordKeyBySecondaryKey._1(), recordKeyBySecondaryKey._2()));
      return sparkSession.createDataFrame(lookupResults, resultSchema);
    }
  }

  /**
   * Hudi returns an RDD backed result for keys in an RDD, but a list backed result when there are
   * no keys to look up, for example when every key is null.
   */
  private JavaPairRDD<String, String> toJavaPairRDD(HoodiePairData<String, String> pairData) {
    if (pairData instanceof HoodieJavaPairRDD) {
      return HoodieJavaPairRDD.getJavaPairRDD(pairData);
    }
    List<Tuple2<String, String>> pairs =
        pairData.collectAsList().stream()
            .map(pair -> new Tuple2<>(pair.getKey(), pair.getValue()))
            .collect(Collectors.toList());
    return javaSparkContext.parallelizePairs(pairs);
  }

  private static Row toLookupResult(String dataPath, String secondaryKey, String recordKey) {
    // record keys generated by Hudi for files without record keys are "<relative path>_<row
    // position>"
    int positionSeparator = recordKey.lastIndexOf('_');
    if (positionSeparator == -1) {
      throw new IllegalArgumentException(
          "Expected a record key of the form <file path>_<row position> but got " + recordKey);
    }
    String relativeFilePath = recordKey.substring(0, positionSeparator);
    return RowFactory.create(
        secondaryKey,
        dataPath.endsWith("/") ? dataPath + relativeFilePath : dataPath + "/" + relativeFilePath,
        Long.parseLong(recordKey.substring(positionSeparator + 1)));
  }
}
