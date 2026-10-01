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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hudi.AvroConversionUtils$;
import org.apache.hudi.HoodieSchemaConversionUtils$;
import org.apache.hudi.HoodieSparkSqlWriter$;
import org.apache.hudi.common.schema.HoodieSchema;

import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConverters;

/**
 * The V1 relation a rewritten write plan inserts into. {@code insert} is the whole Hudi write: it
 * makes sure the Hudi table exists, hands the DataFrame to {@code HoodieSparkSqlWriter}, whose
 * commit fires the Iceberg table format hook that publishes the snapshot to the catalog, and then
 * drops the session's cached copy of the Iceberg table so the next read sees the new snapshot.
 */
public class HudiIcebergRelation extends BaseRelation implements InsertableRelation {
  private static final Logger LOG = LoggerFactory.getLogger(HudiIcebergRelation.class);

  private final SparkSession spark;
  private final HudiSparkTable sparkTable;
  private final HudiWriteOperation operation;
  private final TableCatalog catalog;
  private final Identifier identifier;
  /** Reshapes the command's query output into the rows Hudi should write; identity when null. */
  private final Function<Dataset<Row>, Dataset<Row>> transform;

  public HudiIcebergRelation(
      SparkSession spark,
      HudiSparkTable sparkTable,
      HudiWriteOperation operation,
      TableCatalog catalog,
      Identifier identifier) {
    this(spark, sparkTable, operation, catalog, identifier, null);
  }

  public HudiIcebergRelation(
      SparkSession spark,
      HudiSparkTable sparkTable,
      HudiWriteOperation operation,
      TableCatalog catalog,
      Identifier identifier,
      Function<Dataset<Row>, Dataset<Row>> transform) {
    this.spark = spark;
    this.sparkTable = sparkTable;
    this.operation = operation;
    this.catalog = catalog;
    this.identifier = identifier;
    this.transform = transform;
  }

  @Override
  public SQLContext sqlContext() {
    return spark.sqlContext();
  }

  @Override
  public StructType schema() {
    return sparkTable.schema();
  }

  @Override
  public void insert(Dataset<Row> data, boolean overwrite) {
    HudiTableContext context = sparkTable.context();
    try {
      HudiTableInitializer.ensureInitialized(spark.sessionState().newHadoopConf(), context);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Failed to initialize the Hudi table for " + context.getTableName(), e);
    }
    Map<String, String> params = context.writeParams(operation, sessionOverrides());
    Dataset<Row> rows = transform == null ? data : transform.apply(data);
    LOG.info(
        "Writing to Iceberg table {} through Hudi ({}, keyed={}, partitions={})",
        context.getTableName(),
        operation,
        context.isKeyed(),
        context.getPartitionFields());
    // The Iceberg schema is the table schema: handing it to Hudi as the catalog schema makes the
    // first write's Hudi schema match it (nullability included) instead of the DataFrame's.
    Tuple2<String, String> recordName =
        AvroConversionUtils$.MODULE$.getAvroRecordNameAndNamespace(context.getTableName());
    HoodieSchema catalogSchema =
        HoodieSchemaConversionUtils$.MODULE$.convertStructTypeToHoodieSchema(
            sparkTable.schema(), recordName._1(), recordName._2());
    HoodieSparkSqlWriter$.MODULE$.write(
        spark.sqlContext(),
        SaveMode.Append,
        toScalaMap(params),
        rows,
        Option.empty(),
        Option.empty(),
        Option.apply(catalogSchema));
    refreshIcebergView();
  }

  private void refreshIcebergView() {
    // The table format published the snapshot through its own catalog instance; the session's
    // CachingCatalog entry and this SparkTable's metadata are stale until told otherwise.
    LOG.info(
        "Refreshing Iceberg view of {} (catalog={}, identifier={})",
        sparkTable.name(),
        catalog == null ? null : catalog.getClass().getSimpleName(),
        identifier);
    if (catalog != null && identifier != null) {
      catalog.invalidateTable(identifier);
    }
    sparkTable.table().refresh();
    LOG.info(
        "Iceberg table {} now at snapshot {}",
        sparkTable.name(),
        sparkTable.table().currentSnapshot() == null
            ? null
            : sparkTable.table().currentSnapshot().snapshotId());
  }

  private Map<String, String> sessionOverrides() {
    Map<String, String> overrides = new HashMap<>();
    scala.collection.Iterator<Tuple2<String, String>> it = spark.conf().getAll().iterator();
    while (it.hasNext()) {
      Tuple2<String, String> entry = it.next();
      if (entry._1().startsWith(HudiIcebergConf.WRITE_CONF_PREFIX)) {
        overrides.put(
            "hoodie." + entry._1().substring(HudiIcebergConf.WRITE_CONF_PREFIX.length()),
            entry._2());
      }
    }
    return overrides;
  }

  @SuppressWarnings("unchecked")
  private static scala.collection.immutable.Map<String, String> toScalaMap(
      Map<String, String> map) {
    return (scala.collection.immutable.Map<String, String>)
        JavaConverters.mapAsScalaMapConverter(map)
            .asScala()
            .toMap(scala.Predef$.MODULE$.$conforms());
  }

  @Override
  public String toString() {
    return "HudiIcebergWrite(" + sparkTable.name() + ", " + operation + ")";
  }
}
