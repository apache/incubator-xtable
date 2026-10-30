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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.Identifier

import org.apache.iceberg.{CatalogProperties, CatalogUtil, Table}
import org.apache.iceberg.types.Type.TypeID

import scala.collection.JavaConverters._

import org.apache.xtable.catalog.TableFormatUtils
import org.apache.xtable.conversion.SourceTable
import org.apache.xtable.hudi.index.HudiBackedSecondaryIndexReader
import org.apache.xtable.hudi.index.HudiBackedSecondaryIndexUtils.SyncedVersion
import org.apache.xtable.iceberg.{IcebergCatalogConfig, IcebergConversionSourceProvider}
import org.apache.xtable.model.storage.TableFormat

/**
 * The XTable index of an Iceberg table, read by [[HudiBackedSecondaryIndexReader]]. The index is
 * built and kept current by an XTable sync of the table to a Hudi target with a secondary index on
 * a column, for example with {@code RunSync} or {@code XTableSparkSync}. The Hudi target is stored
 * at the data path of the Iceberg table, so every reader of the table finds the index there.
 */
case class XTableIndex(spark: SparkSession, sourceTable: SourceTable) {

  lazy val reader: HudiBackedSecondaryIndexReader =
    new HudiBackedSecondaryIndexReader(sourceTable, new IcebergConversionSourceProvider, spark)

  /** Whether the index of the column is built. */
  def isIndexed(column: String): Boolean = reader.doesIndexExist(column)

  /**
   * The version of the index, if it is built. Its source identifier is the id of the Iceberg
   * snapshot that the index was last synced with.
   */
  def syncedVersion: Option[SyncedVersion] = {
    val version = reader.getLastSyncedVersion
    if (version.isPresent) Some(version.get()) else None
  }
}

object XTableIndex {

  /** Column types the pruning rule converts filter literals of to Iceberg values. */
  val SupportedTypes: Set[TypeID] = Set(TypeID.STRING, TypeID.INTEGER, TypeID.LONG)

  /**
   * The XTable index of a table.
   *
   * @param catalogName the Spark catalog the table was loaded from, if any
   * @param ident the identifier of the table in that catalog, if any
   */
  def of(
      spark: SparkSession,
      table: Table,
      catalogName: Option[String],
      ident: Option[Identifier]): XTableIndex =
    XTableIndex(
      spark,
      toSourceTable(
        table.name(),
        table.location(),
        TableFormatUtils
          .getTableDataLocation(TableFormat.ICEBERG, table.location(), table.properties()),
        catalogName,
        ident,
        spark.conf.getAll
      )
    )

  /**
   * Describes how XTable loads the table to read the type of the indexed column. A table from a
   * Spark catalog is loaded through an Iceberg catalog built from the same
   * {@code spark.sql.catalog.<name>.*} options, and a table read by path is loaded from its
   * location.
   *
   * @param dataPath the location of the data files of the table, where the Hudi metadata that holds
   *     the index is kept
   * @param sparkConf the Spark session configuration, which holds the catalog options
   */
  def toSourceTable(
      tableName: String,
      tableLocation: String,
      dataPath: String,
      catalogName: Option[String],
      ident: Option[Identifier],
      sparkConf: Map[String, String]): SourceTable = {
    val builder = SourceTable
      .builder()
      .name(tableName)
      .basePath(tableLocation)
      .dataPath(dataPath)
      .formatName(TableFormat.ICEBERG)
    for (catalog <- catalogName; identifier <- ident) {
      val prefix = s"spark.sql.catalog.$catalog."
      val options = sparkConf.collect {
        case (key, value) if key.startsWith(prefix) => key.substring(prefix.length) -> value
      }
      builder
        .name(identifier.name())
        .namespace(identifier.namespace())
        .catalogConfig(
          IcebergCatalogConfig
            .builder()
            .catalogName(catalog)
            .catalogImpl(catalogImpl(options))
            .catalogOptions(options.asJava)
            .build())
    }
    builder.build()
  }

  /** Resolves the catalog implementation from the options the way Iceberg's Spark catalog does. */
  private def catalogImpl(options: Map[String, String]): String =
    options.getOrElse(
      CatalogProperties.CATALOG_IMPL,
      options
        .getOrElse(CatalogUtil.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE)
        .toLowerCase match {
        case CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE => CatalogUtil.ICEBERG_CATALOG_HIVE
        case CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP => CatalogUtil.ICEBERG_CATALOG_HADOOP
        case CatalogUtil.ICEBERG_CATALOG_TYPE_REST => CatalogUtil.ICEBERG_CATALOG_REST
        case CatalogUtil.ICEBERG_CATALOG_TYPE_GLUE => CatalogUtil.ICEBERG_CATALOG_GLUE
        case CatalogUtil.ICEBERG_CATALOG_TYPE_NESSIE => CatalogUtil.ICEBERG_CATALOG_NESSIE
        case CatalogUtil.ICEBERG_CATALOG_TYPE_JDBC => CatalogUtil.ICEBERG_CATALOG_JDBC
        case other =>
          throw new IllegalArgumentException(s"Unknown Iceberg catalog type $other")
      }
    )
}
