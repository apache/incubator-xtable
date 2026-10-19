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
 
package org.apache.xtable.hudi.index;

import static org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_SECONDARY_INDEX_PREFIX;
import static org.apache.spark.sql.functions.col;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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
import org.apache.hudi.data.HoodieJavaPairRDD;
import org.apache.hudi.data.HoodieJavaRDD;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;

import scala.Tuple2;

import com.google.common.base.Preconditions;

import org.apache.xtable.conversion.ConversionSourceProvider;
import org.apache.xtable.conversion.SourceTable;
import org.apache.xtable.index.IndexReader;
import org.apache.xtable.model.schema.InternalField;
import org.apache.xtable.model.schema.InternalSchema;
import org.apache.xtable.model.schema.InternalType;
import org.apache.xtable.model.storage.TableFormat;
import org.apache.xtable.schema.SparkSchemaExtractor;

/**
 * Reads the secondary index of a table of any source format, and returns the file and row position
 * of each match. The index is built by syncing the table to a Hudi target with {@link
 * org.apache.xtable.hudi.HudiTargetConfig#SECONDARY_INDEX_COLUMN} set, with the base path of the
 * source table as the base path of the target. Because the data files carry no Hudi record keys,
 * the record key of a row is {@code <file path relative to the base path>_<row position>}, which
 * the lookup resolves back to a file path and row position.
 *
 * <p>Hudi keys the index by the string of Spark's internal value of the column: the day count of a
 * DATE, the microseconds of a TIMESTAMP (with or without time zone) and the plain string of other
 * types. A lookup renders its keys the same way and returns them in the type of the column.
 */
public class HudiBackedSecondaryIndexReader
    implements IndexReader<SourceTable, Dataset<Row>, Dataset<Row>> {
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

  private final String tableLocation;
  private final SourceTable sourceTable;
  private final ConversionSourceProvider<?> sourceProvider;
  private final SparkSession sparkSession;
  private final JavaSparkContext javaSparkContext;
  private final HoodieSparkEngineContext engineContext;

  /**
   * @param sourceTable the indexed table. Its base path is the base path of the Hudi target
   * @param sourceProvider the provider of the conversion source for the format of the table, which
   *     reads the type of the looked up column. The lookup initializes it with the Hadoop
   *     configuration of the Spark session
   * @param sparkSession the Spark session used to read the index
   */
  public HudiBackedSecondaryIndexReader(
      SourceTable sourceTable,
      ConversionSourceProvider<?> sourceProvider,
      SparkSession sparkSession) {
    Preconditions.checkArgument(
        !TableFormat.HUDI.equals(sourceTable.getFormatName()),
        "A Hudi table has its own metadata table and cannot be the source of the index");
    this.sourceTable = sourceTable;
    this.tableLocation = sourceTable.getBasePath();
    this.sparkSession = sparkSession;
    this.javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
    this.engineContext = new HoodieSparkEngineContext(javaSparkContext);
    this.sourceProvider = sourceProvider;
    sourceProvider.init(javaSparkContext.hadoopConfiguration());
  }

  /** Returns the Spark type of the column, which decides how Hudi renders its values. */
  private DataType getKeyType(String column) {
    InternalField field =
        HudiBackedSecondaryIndexUtils.getIndexableField(
            HudiBackedSecondaryIndexUtils.readSchema(sourceProvider, sourceTable), column);
    InternalSchema keySchema =
        InternalSchema.builder()
            .name("key")
            .dataType(InternalType.RECORD)
            .fields(Collections.singletonList(field))
            .build();
    return SparkSchemaExtractor.getInstance().fromInternalSchema(keySchema).fields()[0].dataType();
  }

  /** Renders a key of the given type the way Hudi renders the value in the secondary index. */
  private static String toSecondaryKey(DataType keyType, String key) {
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
  private static String fromSecondaryKey(DataType keyType, String key) {
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
    return HudiBackedSecondaryIndexUtils.getIndexedColumns(
            tableLocation, javaSparkContext.hadoopConfiguration())
        .contains(columnName);
  }

  @Override
  public Optional<String> getLastSyncedSourceIdentifier() {
    return HudiBackedSecondaryIndexUtils.getLastSyncedSourceIdentifier(
        tableLocation, javaSparkContext.hadoopConfiguration());
  }

  @Override
  public Dataset<Row> lookup(SourceTable table, Dataset<Row> keys, String columnName) {
    HudiBackedSecondaryIndexUtils.checkIndexedTable(tableLocation, table);
    HoodieTableMetaClient metaClient =
        HudiBackedSecondaryIndexUtils.getMetaClient(
            tableLocation, javaSparkContext.hadoopConfiguration());
    if (!HudiBackedSecondaryIndexUtils.getIndexedColumns(metaClient).contains(columnName)) {
      throw new IllegalStateException(
          "The secondary index of column "
              + columnName
              + " of table "
              + tableLocation
              + " is not built");
    }
    DataType keyType = getKeyType(columnName);
    JavaRDD<String> secondaryKeys =
        keys.select(col(columnName).cast(keyType).as(SECONDARY_KEY_COLUMN))
            .where(col(SECONDARY_KEY_COLUMN).isNotNull())
            .selectExpr(toSecondaryKey(keyType, SECONDARY_KEY_COLUMN))
            .as(Encoders.STRING())
            .toJavaRDD();
    // The reader only needs the metadata table enabled, and the table config above shows that the
    // index exists. Without it, the factory returns a file system backed reader without indexes.
    HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(true).build();
    try (HoodieBackedTableMetadata tableMetadata =
        (HoodieBackedTableMetadata)
            metaClient
                .getTableFormat()
                .getMetadataFactory()
                .create(engineContext, metaClient.getStorage(), metadataConfig, tableLocation)) {
      HoodiePairData<String, String> recordKeysBySecondaryKey =
          tableMetadata.readSecondaryIndexDataTableRecordKeysWithKeys(
              HoodieJavaRDD.of(secondaryKeys), PARTITION_NAME_SECONDARY_INDEX_PREFIX + columnName);
      // capture the field so the closure does not serialize the index instance
      String tableLocation = this.tableLocation;
      JavaRDD<Row> lookupResults =
          toJavaPairRDD(recordKeysBySecondaryKey)
              .map(
                  recordKeyBySecondaryKey ->
                      toLookupResult(
                          tableLocation,
                          recordKeyBySecondaryKey._1(),
                          recordKeyBySecondaryKey._2()));
      StructType resultSchema =
          new StructType()
              .add(SECONDARY_KEY_COLUMN, DataTypes.StringType, false)
              .add(FILE_COLUMN, DataTypes.StringType, false)
              .add(POSITION_COLUMN, DataTypes.LongType, false);
      return sparkSession
          .createDataFrame(lookupResults, resultSchema)
          .selectExpr(
              fromSecondaryKey(keyType, SECONDARY_KEY_COLUMN)
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

  private static Row toLookupResult(String tableLocation, String secondaryKey, String recordKey) {
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
        FSUtils.constructAbsolutePath(tableLocation, relativeFilePath).toString(),
        Long.parseLong(recordKey.substring(positionSeparator + 1)));
  }
}
