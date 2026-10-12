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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
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
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DateType;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.TimestampNTZType;
import org.apache.spark.sql.types.TimestampType;

import org.apache.hudi.client.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.exception.TableNotFoundException;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.hadoop.HoodieHadoopStorage;

import scala.Tuple2;

import com.google.common.base.Preconditions;

import org.apache.xtable.conversion.ConversionConfig;
import org.apache.xtable.conversion.ConversionController;
import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.conversion.TargetTable;
import org.apache.xtable.hudi.HudiTargetConfig;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.model.sync.SyncResult;
import org.apache.xtable.model.sync.SyncStatusCode;
import org.apache.xtable.schema.SparkSchemaExtractor;
import org.apache.xtable.spi.extractor.ConversionSource;

/**
 * Secondary index for a table of any source format that XTable reads, backed by the Hudi metadata
 * table. The index is built by syncing the table to Hudi with the record index and a secondary
 * index enabled; the Hudi metadata lives under {@code <data path>/.hoodie/metadata}, where the data
 * path is {@link SourceTable#getDataPath()}. Because the data files carry no Hudi record keys, the
 * record key of a row is {@code <file path relative to the data path>_<row position>}, which a
 * lookup resolves back to a file path and row position. Hudi requires every data file to be under
 * the data path, so files outside it are not supported.
 *
 * <p>The parts that depend on the source format are parameters: the {@link
 * ConversionSourceProvider} reads the table, its schema gives the type of the indexed column, and
 * the caller sets the data path of the {@link SourceTable}, for example with {@link
 * org.apache.xtable.catalog.TableFormatUtils#getTableDataLocation} for an Iceberg table. Hudi
 * tables are not supported as a source, because they have their own metadata table.
 *
 * <p>Hudi keys the index by the string of Spark's internal value of the column: the day count of a
 * DATE, the microseconds of a TIMESTAMP (with or without time zone) and the plain string of other
 * types. A lookup renders its keys the same way and returns them in the type of the column. Columns
 * of other types, such as BINARY or UUID, are not supported.
 */
@Log4j2
public class HudiBackedSecondaryIndex implements Index<SourceTable> {
  /**
   * Lookup result column with the full path of the data file that holds the row. The name matches
   * Iceberg's {@code _file} metadata column, so the result joins to an Iceberg table directly.
   */
  public static final String FILE_COLUMN = "_file";

  /**
   * Lookup result column with the zero based position of the row within the file. The name matches
   * Iceberg's {@code _pos} metadata column.
   */
  public static final String POSITION_COLUMN = "_pos";

  // the key as Hudi renders it in the secondary index
  private static final String SECONDARY_KEY_COLUMN = "secondary_key";

  private static final String NTZ_EPOCH = "TIMESTAMP_NTZ'1970-01-01 00:00:00'";

  // types whose values Hudi renders as their plain string, a day count or microseconds
  private static final Set<InternalType> SUPPORTED_KEY_TYPES =
      EnumSet.of(
          InternalType.STRING,
          InternalType.INT,
          InternalType.LONG,
          InternalType.BOOLEAN,
          InternalType.FLOAT,
          InternalType.DOUBLE,
          InternalType.DECIMAL,
          InternalType.DATE,
          InternalType.TIMESTAMP,
          InternalType.TIMESTAMP_NTZ);

  private final String tableLocation;
  private final String dataPath;
  private final SourceTable sourceTable;
  private final ConversionSourceProvider<?> sourceProvider;
  private final Properties targetTableProperties;
  private final String indexedColumn;
  private final DataType keyType;
  private final SparkSession sparkSession;
  private final JavaSparkContext javaSparkContext;
  private final HoodieSparkEngineContext engineContext;

  /**
   * @param sourceTable the table to index. A table of a catalog sets its catalog config, name and
   *     namespace. The Hudi table is created under its data path
   * @param sourceProvider the provider of the conversion source for the format of the table. The
   *     index initializes it with the Hadoop configuration of the Spark session
   * @param sparkSession the Spark session used to build and query the index
   * @param targetTableProperties {@link HudiTargetConfig} properties for the sync. {@link
   *     HudiTargetConfig#SECONDARY_INDEX_COLUMN} names the column to index, and other properties
   *     such as the record index file group counts are optional
   */
  public HudiBackedSecondaryIndex(
      SourceTable sourceTable,
      ConversionSourceProvider<?> sourceProvider,
      SparkSession sparkSession,
      Properties targetTableProperties) {
    Preconditions.checkArgument(
        !TableFormat.HUDI.equals(sourceTable.getFormatName()),
        "A Hudi table has its own metadata table and cannot be the source of the index");
    this.sourceTable = sourceTable;
    this.tableLocation = sourceTable.getBasePath();
    this.dataPath = sourceTable.getDataPath();
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
    this.sparkSession = sparkSession;
    this.javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
    this.engineContext = new HoodieSparkEngineContext(javaSparkContext);
    this.sourceProvider = sourceProvider;
    sourceProvider.init(javaSparkContext.hadoopConfiguration());
    this.keyType = getKeyType(readSchema(), indexedColumn);
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

  /** Reads the current schema of the source table through its conversion source. */
  private InternalSchema readSchema() {
    try (ConversionSource<?> conversionSource =
        sourceProvider.getConversionSourceInstance(sourceTable)) {
      return conversionSource.getCurrentTable().getReadSchema();
    } catch (IOException e) {
      throw new UncheckedIOException(
          "Failed to close the conversion source of " + tableLocation, e);
    }
  }

  /** Returns the Spark type of the indexed column, which decides how Hudi renders its values. */
  private static DataType getKeyType(InternalSchema schema, String column) {
    InternalField field =
        schema.getAllFields().stream()
            .filter(candidate -> candidate.getPath().equals(column))
            .findFirst()
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format("Column %s is not in the table schema", column)));
    InternalType type = field.getSchema().getDataType();
    // Hudi renders a BINARY, FIXED or UUID value as an object reference
    Preconditions.checkArgument(
        SUPPORTED_KEY_TYPES.contains(type),
        String.format("A secondary index on column %s of type %s is not supported", column, type));
    InternalSchema keySchema =
        InternalSchema.builder()
            .name("key")
            .dataType(InternalType.RECORD)
            .fields(Collections.singletonList(field))
            .build();
    return SparkSchemaExtractor.getInstance().fromInternalSchema(keySchema).fields()[0].dataType();
  }

  /** Renders a key of {@link #keyType} the way Hudi renders the value in the secondary index. */
  private String toSecondaryKey(String key) {
    if (keyType instanceof DateType) {
      return "CAST(unix_date(" + key + ") AS STRING)";
    }
    if (keyType instanceof TimestampType) {
      return "CAST(unix_micros(" + key + ") AS STRING)";
    }
    if (keyType instanceof TimestampNTZType) {
      // unix_micros takes only a TIMESTAMP, and a cast to one would apply the session time zone
      return "CAST(timestampdiff(MICROSECOND, " + NTZ_EPOCH + ", " + key + ") AS STRING)";
    }
    return "CAST(" + key + " AS STRING)";
  }

  /** Reverses {@link #toSecondaryKey} to return a key in the type of the indexed column. */
  private String fromSecondaryKey(String key) {
    if (keyType instanceof DateType) {
      return "date_from_unix_date(CAST(" + key + " AS INT))";
    }
    if (keyType instanceof TimestampType) {
      return "timestamp_micros(CAST(" + key + " AS BIGINT))";
    }
    if (keyType instanceof TimestampNTZType) {
      // timestampadd takes an INT count, so add the exact interval between two instants instead
      return NTZ_EPOCH
          + " + (timestamp_micros(CAST("
          + key
          + " AS BIGINT)) - TIMESTAMP'1970-01-01 00:00:00+00:00')";
    }
    return "CAST(" + key + " AS " + keyType.sql() + ")";
  }

  @Override
  public boolean doesIndexExist(String columnName) {
    return getIndexedColumns().contains(columnName);
  }

  @Override
  public void syncIndex(SourceTable table) {
    checkIndexedTable(table);
    Configuration configuration = javaSparkContext.hadoopConfiguration();
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
              + SyncResult.getErrorMessage(syncResult));
    }
    // A sync without a new snapshot returns no result, so check that the index exists.
    if (!getIndexedColumns().contains(indexedColumn)) {
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

  /** Returns the columns whose index exists, or an empty set when there is no Hudi table yet. */
  private Set<String> getIndexedColumns() {
    try {
      return getIndexedColumns(getMetaClient());
    } catch (TableNotFoundException e) {
      return Collections.emptySet();
    }
  }

  /**
   * Returns the columns whose index build has completed. A metadata table partition directory can
   * exist while the build is in flight or after it failed, so the directory alone does not show
   * that the index is usable.
   */
  private static Set<String> getIndexedColumns(HoodieTableMetaClient metaClient) {
    // The commit that creates the metadata table marks its partitions complete before the data
    // commit completes, so the partitions of a table without a completed commit are not an index.
    if (metaClient.getActiveTimeline().getWriteTimeline().filterCompletedInstants().empty()) {
      return Collections.emptySet();
    }
    return metaClient.getTableConfig().getMetadataPartitions().stream()
        .filter(partition -> partition.startsWith(PARTITION_NAME_SECONDARY_INDEX_PREFIX))
        .map(partition -> partition.substring(PARTITION_NAME_SECONDARY_INDEX_PREFIX.length()))
        .collect(Collectors.toSet());
  }

  private HoodieTableMetaClient getMetaClient() {
    HoodieStorage storage =
        new HoodieHadoopStorage(dataPath, javaSparkContext.hadoopConfiguration());
    return HoodieTableMetaClient.builder().setStorage(storage).setBasePath(dataPath).build();
  }

  /** Checks that a table passed to the index is the table it was created for. */
  private void checkIndexedTable(SourceTable table) {
    Preconditions.checkArgument(
        tableLocation.equals(table.getBasePath()),
        String.format(
            "The index belongs to table %s but got table %s", tableLocation, table.getBasePath()));
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

  @Override
  public Dataset<Row> lookup(SourceTable table, Dataset<Row> keys, String columnName) {
    checkIndexedTable(table);
    Preconditions.checkArgument(
        indexedColumn.equals(columnName),
        String.format("Column %s is not the indexed column %s", columnName, indexedColumn));
    JavaRDD<String> secondaryKeys =
        keys.select(col(columnName).cast(keyType).as(SECONDARY_KEY_COLUMN))
            .where(col(SECONDARY_KEY_COLUMN).isNotNull())
            .selectExpr(toSecondaryKey(SECONDARY_KEY_COLUMN))
            .as(Encoders.STRING())
            .toJavaRDD();
    HoodieTableMetaClient metaClient = getMetaClient();
    if (!getIndexedColumns(metaClient).contains(columnName)) {
      throw new IllegalStateException(
          "The secondary index of column "
              + columnName
              + " of table "
              + tableLocation
              + " is not built");
    }
    // The reader only needs the metadata table enabled, and the table config above shows that the
    // index exists. Without it, the factory returns a file system backed reader without indexes.
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
    try (HoodieBackedTableMetadata tableMetadata =
        (HoodieBackedTableMetadata)
            metaClient
                .getTableFormat()
                .getMetadataFactory()
                .create(engineContext, metaClient.getStorage(), metadataConfig, dataPath)) {
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
      StructType resultSchema =
          new StructType()
              .add(SECONDARY_KEY_COLUMN, DataTypes.StringType, false)
              .add(FILE_COLUMN, DataTypes.StringType, false)
              .add(POSITION_COLUMN, DataTypes.LongType, false);
      return sparkSession
          .createDataFrame(lookupResults, resultSchema)
          .selectExpr(
              fromSecondaryKey(SECONDARY_KEY_COLUMN)
                  + " AS `"
                  + columnName.replace("`", "``")
                  + "`",
              FILE_COLUMN,
              POSITION_COLUMN);
    }
  }

  /**
   * Hudi returns an RDD backed result for keys in an RDD, but a list backed result when there are
   * no keys to look up, for example when every key is null, and when the index has one file group.
   * For one file group Hudi collects the keys and reads the file group on the driver, so the
   * matches are collected there as well.
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
        FSUtils.constructAbsolutePath(dataPath, relativeFilePath).toString(),
        Long.parseLong(recordKey.substring(positionSeparator + 1)));
  }
}
